/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_parquet_reader.h"
#include "ob_file_scan_utils.h"
#include "share/ob_errno.h"

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{

const uint8_t PQ_T_STOP = 0;
const uint8_t PQ_T_BOOL_TRUE = 1;
const uint8_t PQ_T_BOOL_FALSE = 2;
const uint8_t PQ_T_BYTE = 3;
const uint8_t PQ_T_I16 = 4;
const uint8_t PQ_T_I32 = 5;
const uint8_t PQ_T_I64 = 6;
const uint8_t PQ_T_DOUBLE = 7;
const uint8_t PQ_T_BINARY = 8;
const uint8_t PQ_T_LIST = 9;
const uint8_t PQ_T_SET = 10;
const uint8_t PQ_T_MAP = 11;
const uint8_t PQ_T_STRUCT = 12;

const int32_t PQ_PAGE_DATA = 0;
const int32_t PQ_PAGE_INDEX = 1;
const int32_t PQ_PAGE_DICTIONARY = 2;
const int32_t PQ_PAGE_DATA_V2 = 3;

const int32_t PQ_ENC_PLAIN = 0;
const int32_t PQ_ENC_PLAIN_DICTIONARY = 2;
const int32_t PQ_ENC_RLE = 3;
const int32_t PQ_ENC_BIT_PACKED = 4;
const int32_t PQ_ENC_RLE_DICTIONARY = 8;

const int32_t PQ_REP_REQUIRED = 0;
const int32_t PQ_REP_OPTIONAL = 1;
const int32_t PQ_REP_REPEATED = 2;

// Reader of the thrift compact protocol, the encoding used by the parquet
// metadata and page headers.
class PQReader
{
public:
  PQReader(const uint8_t *buf, const int64_t len)
    : start_(buf), p_(buf), end_(buf + len) {}

  int64_t consumed() const { return static_cast<int64_t>(p_ - start_); }

  int read_byte(uint8_t &v)
  {
    int ret = OB_SUCCESS;
    if (p_ >= end_) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      v = *p_++;
    }
    return ret;
  }

  int read_varint(uint64_t &v)
  {
    int ret = OB_SUCCESS;
    v = 0;
    int shift = 0;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      uint8_t b = 0;
      if (shift > 63) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_FAIL(read_byte(b))) {
      } else {
        v |= (static_cast<uint64_t>(b & 0x7f) << shift);
        shift += 7;
        done = (0 == (b & 0x80));
      }
    }
    return ret;
  }

  int read_zigzag(int64_t &v)
  {
    uint64_t u = 0;
    int ret = read_varint(u);
    if (OB_SUCC(ret)) {
      v = static_cast<int64_t>((u >> 1) ^ (~(u & 1) + 1));
    }
    return ret;
  }

  int read_i32(int32_t &v)
  {
    int64_t z = 0;
    int ret = read_zigzag(z);
    if (OB_SUCC(ret)) {
      v = static_cast<int32_t>(z);
    }
    return ret;
  }

  int read_i64(int64_t &v) { return read_zigzag(v); }

  int read_double(double &v)
  {
    int ret = OB_SUCCESS;
    if (end_ - p_ < 8) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      uint64_t bits = 0;
      for (int i = 7; i >= 0; --i) {
        bits = (bits << 8) | p_[i];
      }
      MEMCPY(&v, &bits, sizeof(v));
      p_ += 8;
    }
    return ret;
  }

  int read_binary(ObString &s)
  {
    int ret = OB_SUCCESS;
    uint64_t len = 0;
    if (OB_FAIL(read_varint(len))) {
    } else if (len > static_cast<uint64_t>(end_ - p_)) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      s.assign_ptr(reinterpret_cast<const char *>(p_), static_cast<int32_t>(len));
      p_ += len;
    }
    return ret;
  }

  // Returns OB_ITER_END on the STOP byte of the enclosing struct.
  int read_field_header(uint8_t &type, int32_t &field_id, int32_t &last_id)
  {
    int ret = OB_SUCCESS;
    uint8_t b = 0;
    if (OB_FAIL(read_byte(b))) {
    } else if (PQ_T_STOP == (b & 0x0f)) {
      ret = OB_ITER_END;
    } else {
      type = b & 0x0f;
      const uint8_t delta = b >> 4;
      if (0 == delta) {
        int64_t id = 0;
        if (OB_FAIL(read_zigzag(id))) {
        } else if (id < 0 || id > INT32_MAX) {
          ret = OB_INVALID_DATA;
        } else {
          field_id = static_cast<int32_t>(id);
          last_id = field_id;
        }
      } else {
        field_id = last_id + delta;
        last_id = field_id;
      }
    }
    return ret;
  }

  int read_list_header(uint8_t &elem_type, int32_t &size)
  {
    int ret = OB_SUCCESS;
    uint8_t b = 0;
    if (OB_FAIL(read_byte(b))) {
    } else {
      elem_type = b & 0x0f;
      size = b >> 4;
      if (15 == size) {
        uint64_t n = 0;
        if (OB_FAIL(read_varint(n))) {
        } else if (n > INT32_MAX) {
          ret = OB_SIZE_OVERFLOW;
        } else {
          size = static_cast<int32_t>(n);
        }
      }
    }
    return ret;
  }

  int skip(const uint8_t type)
  {
    int ret = OB_SUCCESS;
    switch (type) {
      case PQ_T_STOP:
      case PQ_T_BOOL_TRUE:
      case PQ_T_BOOL_FALSE:
        break;
      case PQ_T_BYTE: {
        uint8_t b = 0;
        ret = read_byte(b);
        break;
      }
      case PQ_T_I16:
      case PQ_T_I32:
      case PQ_T_I64: {
        int64_t v = 0;
        ret = read_zigzag(v);
        break;
      }
      case PQ_T_DOUBLE: {
        double d = 0;
        ret = read_double(d);
        break;
      }
      case PQ_T_BINARY: {
        ObString s;
        ret = read_binary(s);
        break;
      }
      case PQ_T_LIST:
      case PQ_T_SET: {
        uint8_t et = 0;
        int32_t n = 0;
        if (OB_FAIL(read_list_header(et, n))) {
        } else {
          for (int32_t i = 0; OB_SUCC(ret) && i < n; ++i) {
            ret = skip(et);
          }
        }
        break;
      }
      case PQ_T_MAP: {
        uint64_t n = 0;
        if (OB_FAIL(read_varint(n))) {
        } else if (0 != n) {
          uint8_t b = 0;
          if (OB_FAIL(read_byte(b))) {
          } else {
            const uint8_t kt = b >> 4;
            const uint8_t vt = b & 0x0f;
            for (uint64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
              if (OB_FAIL(skip(kt))) {
              } else {
                ret = skip(vt);
              }
            }
          }
        }
        break;
      }
      case PQ_T_STRUCT: {
        uint8_t ft = 0;
        int32_t fid = 0;
        int32_t last = 0;
        int tmp = OB_SUCCESS;
        while (OB_SUCC(ret) && OB_SUCCESS == (tmp = read_field_header(ft, fid, last))) {
          ret = skip(ft);
        }
        if (OB_SUCC(ret) && OB_ITER_END != tmp) {
          ret = tmp;
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        break;
    }
    return ret;
  }

private:
  const uint8_t *start_;
  const uint8_t *p_;
  const uint8_t *end_;
};

struct PQSchemaElement
{
  PQSchemaElement()
    : has_type_(false), type_(PQ_BYTE_ARRAY), repetition_(PQ_REP_REQUIRED),
      num_children_(0), converted_type_(PQ_CT_NONE), type_length_(0),
      scale_(0), precision_(0), name_()
  {}
  bool has_type_;
  int32_t type_;
  int32_t repetition_;
  int32_t num_children_;
  int32_t converted_type_;
  int32_t type_length_;
  int32_t scale_;
  int32_t precision_;
  ObString name_;
};

struct PQColumnMetaParsed
{
  PQColumnMetaParsed()
    : physical_type_(PQ_BYTE_ARRAY), codec_(0), num_values_(0),
      total_compressed_size_(0), data_page_offset_(0), dictionary_page_offset_(0)
  {}
  int32_t physical_type_;
  int32_t codec_;
  int64_t num_values_;
  int64_t total_compressed_size_;
  int64_t data_page_offset_;
  int64_t dictionary_page_offset_;
};

struct PQPageHeaderParsed
{
  PQPageHeaderParsed()
    : type_(PQ_PAGE_DATA), uncompressed_page_size_(0), compressed_page_size_(0),
      num_values_(0), num_nulls_(0), encoding_(PQ_ENC_PLAIN),
      is_compressed_(true), def_levels_byte_length_(0), rep_levels_byte_length_(0)
  {}
  int32_t type_;
  int32_t uncompressed_page_size_;
  int32_t compressed_page_size_;
  int32_t num_values_;
  int32_t num_nulls_;
  int32_t encoding_;
  bool is_compressed_;
  int32_t def_levels_byte_length_;
  int32_t rep_levels_byte_length_;
};

int parse_schema_element(PQReader &r, PQSchemaElement &el)
{
  int ret = OB_SUCCESS;
  uint8_t type = 0;
  int32_t fid = 0;
  int32_t last = 0;
  int tmp = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCCESS == (tmp = r.read_field_header(type, fid, last))) {
    switch (fid) {
      case 1:
        if (OB_FAIL(r.read_i32(el.type_))) {
        } else {
          el.has_type_ = true;
        }
        break;
      case 2:
        ret = r.read_i32(el.type_length_);
        break;
      case 3:
        ret = r.read_i32(el.repetition_);
        break;
      case 4:
        ret = r.read_binary(el.name_);
        break;
      case 5:
        ret = r.read_i32(el.num_children_);
        break;
      case 6:
        ret = r.read_i32(el.converted_type_);
        break;
      case 7:
        ret = r.read_i32(el.scale_);
        break;
      case 8:
        ret = r.read_i32(el.precision_);
        break;
      default:
        ret = r.skip(type);
        break;
    }
  }
  if (OB_SUCC(ret) && OB_ITER_END != tmp) {
    ret = tmp;
  }
  return ret;
}

int parse_column_meta(PQReader &r, PQColumnMetaParsed &meta)
{
  int ret = OB_SUCCESS;
  uint8_t type = 0;
  int32_t fid = 0;
  int32_t last = 0;
  int tmp = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCCESS == (tmp = r.read_field_header(type, fid, last))) {
    switch (fid) {
      case 1:
        ret = r.read_i32(meta.physical_type_);
        break;
      case 3: {
        uint8_t et = 0;
        int32_t n = 0;
        if (OB_FAIL(r.read_list_header(et, n))) {
        } else {
          for (int32_t i = 0; OB_SUCC(ret) && i < n; ++i) {
            if (PQ_T_BINARY == et) {
              ObString path;
              ret = r.read_binary(path);
            } else {
              ret = r.skip(et);
            }
          }
        }
        break;
      }
      case 4:
        ret = r.read_i32(meta.codec_);
        break;
      case 5:
        ret = r.read_i64(meta.num_values_);
        break;
      case 7:
        ret = r.read_i64(meta.total_compressed_size_);
        break;
      case 9:
        ret = r.read_i64(meta.data_page_offset_);
        break;
      case 11:
        ret = r.read_i64(meta.dictionary_page_offset_);
        break;
      default:
        ret = r.skip(type);
        break;
    }
  }
  if (OB_SUCC(ret) && OB_ITER_END != tmp) {
    ret = tmp;
  }
  return ret;
}

int parse_column_chunk(PQReader &r, PQColumnMetaParsed &meta)
{
  int ret = OB_SUCCESS;
  uint8_t type = 0;
  int32_t fid = 0;
  int32_t last = 0;
  int tmp = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCCESS == (tmp = r.read_field_header(type, fid, last))) {
    switch (fid) {
      case 1: {
        ObString file_path;
        if (OB_FAIL(r.read_binary(file_path))) {
        } else if (!file_path.empty()) {
          // a column stored outside of the file is not supported
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("external parquet column file is not supported", K(ret));
        }
        break;
      }
      case 2: {
        int64_t file_offset = 0;
        ret = r.read_i64(file_offset);
        break;
      }
      case 3:
        ret = parse_column_meta(r, meta);
        break;
      default:
        ret = r.skip(type);
        break;
    }
  }
  if (OB_SUCC(ret) && OB_ITER_END != tmp) {
    ret = tmp;
  }
  return ret;
}

int parse_row_group(PQReader &r, ObParquetRowGroup &rg)
{
  int ret = OB_SUCCESS;
  uint8_t type = 0;
  int32_t fid = 0;
  int32_t last = 0;
  int tmp = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCCESS == (tmp = r.read_field_header(type, fid, last))) {
    if (1 == fid) {
      uint8_t et = 0;
      int32_t n = 0;
      if (OB_FAIL(r.read_list_header(et, n))) {
      } else if (PQ_T_STRUCT != et) {
        ret = OB_INVALID_DATA;
      } else {
        for (int32_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          PQColumnMetaParsed meta;
          ObParquetColumnChunk chunk;
          if (OB_FAIL(parse_column_chunk(r, meta))) {
          } else {
            chunk.data_page_offset_ = meta.data_page_offset_;
            chunk.dict_page_offset_ = meta.dictionary_page_offset_;
            chunk.total_compressed_size_ = meta.total_compressed_size_;
            chunk.num_values_ = meta.num_values_;
            chunk.codec_ = meta.codec_;
            chunk.physical_type_ = meta.physical_type_;
            ret = rg.chunks_.push_back(chunk);
          }
        }
      }
    } else if (3 == fid) {
      ret = r.read_i64(rg.num_rows_);
    } else {
      ret = r.skip(type);
    }
  }
  if (OB_SUCC(ret) && OB_ITER_END != tmp) {
    ret = tmp;
  }
  return ret;
}

int parse_page_header(PQReader &r, PQPageHeaderParsed &h)
{
  int ret = OB_SUCCESS;
  uint8_t type = 0;
  int32_t fid = 0;
  int32_t last = 0;
  int tmp = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCCESS == (tmp = r.read_field_header(type, fid, last))) {
    switch (fid) {
      case 1:
        ret = r.read_i32(h.type_);
        break;
      case 2:
        ret = r.read_i32(h.uncompressed_page_size_);
        break;
      case 3:
        ret = r.read_i32(h.compressed_page_size_);
        break;
      case 5: {  // DataPageHeader
        uint8_t ft = 0;
        int32_t ffid = 0;
        int32_t flast = 0;
        int ftmp = OB_SUCCESS;
        while (OB_SUCC(ret) && OB_SUCCESS == (ftmp = r.read_field_header(ft, ffid, flast))) {
          switch (ffid) {
            case 1: ret = r.read_i32(h.num_values_); break;
            case 2: ret = r.read_i32(h.encoding_); break;
            default: ret = r.skip(ft); break;
          }
        }
        if (OB_SUCC(ret) && OB_ITER_END != ftmp) {
          ret = ftmp;
        }
        break;
      }
      case 7: {  // DictionaryPageHeader
        uint8_t ft = 0;
        int32_t ffid = 0;
        int32_t flast = 0;
        int ftmp = OB_SUCCESS;
        while (OB_SUCC(ret) && OB_SUCCESS == (ftmp = r.read_field_header(ft, ffid, flast))) {
          switch (ffid) {
            case 1: ret = r.read_i32(h.num_values_); break;
            case 2: ret = r.read_i32(h.encoding_); break;
            default: ret = r.skip(ft); break;
          }
        }
        if (OB_SUCC(ret) && OB_ITER_END != ftmp) {
          ret = ftmp;
        }
        break;
      }
      case 8: {  // DataPageHeaderV2
        uint8_t ft = 0;
        int32_t ffid = 0;
        int32_t flast = 0;
        int ftmp = OB_SUCCESS;
        while (OB_SUCC(ret) && OB_SUCCESS == (ftmp = r.read_field_header(ft, ffid, flast))) {
          switch (ffid) {
            case 1: ret = r.read_i32(h.num_values_); break;
            case 2: ret = r.read_i32(h.num_nulls_); break;
            case 4: ret = r.read_i32(h.encoding_); break;
            case 5: ret = r.read_i32(h.def_levels_byte_length_); break;
            case 6: ret = r.read_i32(h.rep_levels_byte_length_); break;
            case 7:
              // a bool field encodes its value in the field header type
              h.is_compressed_ = (PQ_T_BOOL_TRUE == ft);
              break;
            default: ret = r.skip(ft); break;
          }
        }
        if (OB_SUCC(ret) && OB_ITER_END != ftmp) {
          ret = ftmp;
        }
        break;
      }
      default:
        ret = r.skip(type);
        break;
    }
  }
  if (OB_SUCC(ret) && OB_ITER_END != tmp) {
    ret = tmp;
  }
  return ret;
}

// Snappy raw (block) format decompression.
int snappy_decompress(const char *src, const int64_t src_len, char *dst,
                      const int64_t dst_cap, int64_t &dst_len)
{
  int ret = OB_SUCCESS;
  const uint8_t *p = reinterpret_cast<const uint8_t *>(src);
  const uint8_t *const end = p + src_len;
  uint8_t *const out = reinterpret_cast<uint8_t *>(dst);
  uint8_t *op = out;
  const uint8_t *const oend = out + dst_cap;
  uint64_t need = 0;
  int shift = 0;
  bool got_len = false;
  while (OB_SUCC(ret) && !got_len) {
    if (p >= end || shift > 63) {
      ret = OB_INVALID_DATA;
    } else {
      const uint8_t b = *p++;
      need |= (static_cast<uint64_t>(b & 0x7f) << shift);
      shift += 7;
      got_len = (0 == (b & 0x80));
    }
  }
  if (OB_SUCC(ret) && need > static_cast<uint64_t>(dst_cap)) {
    ret = OB_SIZE_OVERFLOW;
  }
  while (OB_SUCC(ret) && p < end) {
    const uint8_t tag = *p++;
    const uint8_t kind = tag & 0x03;
    if (0 == kind) {
      // literal
      int64_t len = tag >> 2;
      if (len < 60) {
        ++len;
      } else {
        const int nbytes = static_cast<int>(len) - 59;
        if (p + nbytes > end) {
          ret = OB_INVALID_DATA;
        } else {
          uint64_t v = 0;
          for (int i = 0; i < nbytes; ++i) {
            v |= (static_cast<uint64_t>(p[i]) << (8 * i));
          }
          p += nbytes;
          len = static_cast<int64_t>(v) + 1;
        }
      }
      if (OB_SUCC(ret)) {
        if (p + len > end || op + len > oend) {
          ret = OB_INVALID_DATA;
        } else {
          MEMCPY(op, p, len);
          op += len;
          p += len;
        }
      }
    } else {
      int64_t len = 0;
      int64_t offset = 0;
      if (1 == kind) {
        if (p >= end) {
          ret = OB_INVALID_DATA;
        } else {
          len = 4 + ((tag >> 2) & 0x07);
          offset = ((static_cast<int64_t>(tag) >> 5) << 8) | *p++;
        }
      } else if (2 == kind) {
        if (p + 2 > end) {
          ret = OB_INVALID_DATA;
        } else {
          len = (tag >> 2) + 1;
          offset = static_cast<int64_t>(p[0]) | (static_cast<int64_t>(p[1]) << 8);
          p += 2;
        }
      } else {
        if (p + 4 > end) {
          ret = OB_INVALID_DATA;
        } else {
          len = (tag >> 2) + 1;
          offset = static_cast<int64_t>(p[0]) | (static_cast<int64_t>(p[1]) << 8)
                   | (static_cast<int64_t>(p[2]) << 16)
                   | (static_cast<int64_t>(p[3]) << 24);
          p += 4;
        }
      }
      if (OB_SUCC(ret)) {
        // copy len bytes from op - offset, byte by byte because the ranges
        // may overlap
        if (offset <= 0 || offset > (op - out) || op + len > oend) {
          ret = OB_INVALID_DATA;
        } else {
          const uint8_t *from = op - offset;
          for (int64_t i = 0; i < len; ++i) {
            op[i] = from[i];
          }
          op += len;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (static_cast<int64_t>(op - out) != static_cast<int64_t>(need)) {
      ret = OB_INVALID_DATA;
    } else {
      dst_len = static_cast<int64_t>(op - out);
    }
  }
  return ret;
}

// days since 1970-01-01 to a civil date
void civil_from_days(const int64_t days, int64_t &year, int &month, int &day)
{
  const int64_t z = days + 719468;
  const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  const int64_t doe = z - era * 146097;
  const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const int64_t y = yoe + era * 400;
  const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const int64_t mp = (5 * doy + 2) / 153;
  const int64_t d = doy - (153 * mp + 2) / 5 + 1;
  const int64_t m = (mp < 10 ? mp + 3 : mp - 9);
  year = y + (m <= 2 ? 1 : 0);
  month = static_cast<int>(m);
  day = static_cast<int>(d);
}

int format_datetime(const int64_t secs, const int64_t micros, const int digits,
                    char *buf, const int64_t cap)
{
  int64_t days = secs / 86400;
  int64_t rem = secs % 86400;
  if (rem < 0) {
    rem += 86400;
    --days;
  }
  int64_t year = 0;
  int month = 0;
  int day = 0;
  civil_from_days(days, year, month, day);
  const int hour = static_cast<int>(rem / 3600);
  const int minute = static_cast<int>((rem % 3600) / 60);
  const int second = static_cast<int>(rem % 60);
  int n = 0;
  if (digits > 0 && micros > 0) {
    n = snprintf(buf, cap, "%04lld-%02d-%02d %02d:%02d:%02d.%0*lld",
                 static_cast<long long>(year), month, day, hour, minute, second,
                 digits, static_cast<long long>(micros));
  } else {
    n = snprintf(buf, cap, "%04lld-%02d-%02d %02d:%02d:%02d",
                 static_cast<long long>(year), month, day, hour, minute, second);
  }
  return n > 0 ? OB_SUCCESS : OB_SIZE_OVERFLOW;
}

// A parquet decimal is a big-endian two's complement integer with an implied
// scale, so it is formatted as a plain decimal string here.
void format_decimal(const uint8_t *bytes, const int32_t n, const int32_t scale,
                    char *buf, const int64_t cap)
{
  bool negative = (n > 0 && 0 != (bytes[0] & 0x80));
  // big-endian magnitude, divided down base 10 in place
  uint8_t mag[16];
  const int32_t used = n < 16 ? n : 16;
  if (negative) {
    // two's complement to magnitude: invert and add one
    int carry = 1;
    for (int32_t i = used - 1; i >= 0; --i) {
      const int v = static_cast<int>(static_cast<uint8_t>(~bytes[i])) + carry;
      mag[i] = static_cast<uint8_t>(v & 0xff);
      carry = v >> 8;
    }
  } else {
    MEMCPY(mag, bytes, used);
  }
  char digits[64];
  int32_t nd = 0;
  bool zero = true;
  for (int32_t i = 0; i < used; ++i) {
    zero = zero && (0 == mag[i]);
  }
  while (!zero && nd < static_cast<int32_t>(sizeof(digits))) {
    int rem = 0;
    zero = true;
    for (int32_t i = 0; i < used; ++i) {
      const int cur = (rem << 8) | mag[i];
      mag[i] = static_cast<uint8_t>(cur / 10);
      rem = cur % 10;
      zero = zero && (0 == mag[i]);
    }
    digits[nd++] = static_cast<char>('0' + rem);
  }
  if (0 == nd) {
    digits[nd++] = '0';
  }
  char num[64];
  for (int32_t i = 0; i < nd; ++i) {
    num[i] = digits[nd - 1 - i];
  }
  int64_t pos = 0;
  if (negative) {
    buf[pos++] = '-';
  }
  if (scale <= 0) {
    for (int32_t i = 0; i < nd && pos < cap; ++i) {
      buf[pos++] = num[i];
    }
  } else if (nd <= scale) {
    // the value is smaller than one: the integer part is a single zero
    buf[pos++] = '0';
    buf[pos++] = '.';
    for (int32_t i = nd; i < scale && pos < cap; ++i) {
      buf[pos++] = '0';
    }
    for (int32_t i = 0; i < nd && pos < cap; ++i) {
      buf[pos++] = num[i];
    }
  } else {
    const int32_t int_digits = nd - scale;
    for (int32_t i = 0; i < int_digits && pos < cap; ++i) {
      buf[pos++] = num[i];
    }
    if (pos < cap) {
      buf[pos++] = '.';
    }
    for (int32_t i = int_digits; i < nd && pos < cap; ++i) {
      buf[pos++] = num[i];
    }
  }
  buf[pos < cap ? pos : cap - 1] = '\0';
}

const int32_t PQ_MAX_LEVEL = 1;

int bit_width_for(const int32_t max_level)
{
  int width = 0;
  while ((1 << width) <= max_level) {
    ++width;
  }
  return width;
}

// RLE/bit-packed hybrid decoding, used for definition levels and dictionary
// indices. Returns the number of bytes consumed in consumed_len.
int decode_hybrid(const uint8_t *data, const int64_t len, const int bt_width,
                  const int64_t count, uint32_t *out, int64_t &consumed_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t produced = 0;
  while (OB_SUCC(ret) && produced < count) {
    uint64_t header = 0;
    int shift = 0;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      if (pos >= len || shift > 63) {
        ret = OB_INVALID_DATA;
      } else {
        const uint8_t b = data[pos++];
        header |= (static_cast<uint64_t>(b & 0x7f) << shift);
        shift += 7;
        done = (0 == (b & 0x80));
      }
    }
    if (OB_FAIL(ret)) {
      break;
    }
    if (header & 1) {
      // bit-packed run of groups*8 values
      const int64_t groups = static_cast<int64_t>(header >> 1);
      const int64_t nvals = groups * 8;
      if (bt_width > 0) {
        const int64_t nbytes = (nvals * bt_width + 7) / 8;
        if (pos + nbytes > len) {
          ret = OB_INVALID_DATA;
          break;
        }
      }
      uint32_t bit_pos = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < nvals && produced < count; ++i) {
        uint32_t v = 0;
        for (int b = 0; b < bt_width; ++b) {
          const int64_t byte_idx = pos + (bit_pos + b) / 8;
          const int bit_idx = (bit_pos + b) % 8;
          if (0 != (data[byte_idx] & (1 << bit_idx))) {
            v |= (1u << b);
          }
        }
        out[produced++] = v;
        bit_pos += bt_width;
      }
      pos += (nvals * bt_width + 7) / 8;
    } else {
      const int64_t run = static_cast<int64_t>(header >> 1);
      const int nbytes = (bt_width + 7) / 8;
      if (pos + nbytes > len) {
        ret = OB_INVALID_DATA;
        break;
      }
      uint32_t v = 0;
      for (int i = 0; i < nbytes; ++i) {
        v |= (static_cast<uint32_t>(data[pos + i]) << (8 * i));
      }
      pos += nbytes;
      for (int64_t i = 0; OB_SUCC(ret) && i < run && produced < count; ++i) {
        out[produced++] = v;
      }
    }
  }
  if (OB_SUCC(ret)) {
    consumed_len = pos;
  }
  return ret;
}

}  // namespace

ObParquetReader::ObParquetReader()
  : fd_(-1), file_size_(0), num_rows_(0), expected_size_(-1), path_buf_(NULL),
    alloc_(NULL), columns_(), row_groups_(), states_(), needed_(), cur_rg_(0),
    cur_row_(0), loaded_rg_(-1)
{}

void ObParquetReader::close()
{
  for (int64_t i = 0; i < states_.count(); ++i) {
    ColumnState *st = states_.at(i);
    if (NULL != st) {
      st->~ColumnState();
      if (NULL != alloc_) {
        alloc_->free(st);
      }
    }
  }
  states_.reset();
  columns_.reset();
  row_groups_.reset();
  needed_.reset();
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  if (NULL != path_buf_ && NULL != alloc_) {
    alloc_->free(path_buf_);
  }
  path_buf_ = NULL;
  file_size_ = 0;
  num_rows_ = 0;
  expected_size_ = -1;
  alloc_ = NULL;
  cur_rg_ = 0;
  cur_row_ = 0;
  loaded_rg_ = -1;
}

int ObParquetReader::read_at(const int64_t offset, const int64_t len, char *buf)
{
  int ret = OB_SUCCESS;
  if (offset < 0 || len < 0 || offset + len > file_size_) {
    ret = OB_INVALID_DATA;
  } else {
    int64_t done = 0;
    while (OB_SUCC(ret) && done < len) {
      const ssize_t n = ::pread(fd_, buf + done, len - done, offset + done);
      if (n <= 0) {
        ret = OB_IO_ERROR;
      } else {
        done += n;
      }
    }
  }
  return ret;
}

int ObParquetReader::check_file_unchanged()
{
  int ret = OB_SUCCESS;
  if (expected_size_ >= 0) {
    struct stat st;
    if (0 != ::stat(path_buf_, &st)) {
      ret = OB_FILE_NOT_EXIST;
    } else if (static_cast<int64_t>(st.st_size) != expected_size_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("file changed after schema inference", K(ret),
               "size", static_cast<int64_t>(st.st_size), K_(expected_size));
    }
  }
  return ret;
}

int ObParquetReader::load_footer(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  char tail[8];
  struct stat st;
  if (OB_FAIL(check_file_unchanged())) {
  } else if (0 != ::fstat(fd_, &st)) {
    ret = OB_IO_ERROR;
  } else if ((file_size_ = static_cast<int64_t>(st.st_size)) < 12) {
    ret = OB_INVALID_DATA;
    LOG_WARN("parquet file is too small", K(ret), K_(file_size));
  } else if (OB_FAIL(read_at(file_size_ - 8, 8, tail))) {
  } else if (0 != MEMCMP(tail + 4, "PAR1", 4)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("bad parquet magic", K(ret));
  } else {
    uint32_t footer_len = 0;
    MEMCPY(&footer_len, tail, 4);
    if (footer_len > static_cast<uint32_t>(file_size_ - 8)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("bad parquet footer length", K(ret), K(footer_len), K_(file_size));
    } else {
      const int64_t footer_off = file_size_ - 8 - footer_len;
      char *footer = static_cast<char *>(alloc.alloc(footer_len));
      if (OB_ISNULL(footer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(read_at(footer_off, footer_len, footer))) {
      } else {
        PQReader r(reinterpret_cast<const uint8_t *>(footer), footer_len);
        uint8_t type = 0;
        int32_t fid = 0;
        int32_t last = 0;
        int tmp = OB_SUCCESS;
        ObSEArray<PQSchemaElement, 16> schema;
        while (OB_SUCC(ret) && OB_SUCCESS == (tmp = r.read_field_header(type, fid, last))) {
          if (1 == fid) {
            int32_t version = 0;
            ret = r.read_i32(version);
          } else if (2 == fid) {
            uint8_t et = 0;
            int32_t n = 0;
            if (OB_FAIL(r.read_list_header(et, n))) {
            } else if (PQ_T_STRUCT != et) {
              ret = OB_INVALID_DATA;
            } else {
              for (int32_t i = 0; OB_SUCC(ret) && i < n; ++i) {
                PQSchemaElement el;
                if (OB_FAIL(parse_schema_element(r, el))) {
                } else if (OB_FAIL(schema.push_back(el))) {
                }
              }
            }
          } else if (3 == fid) {
            ret = r.read_i64(num_rows_);
          } else if (4 == fid) {
            uint8_t et = 0;
            int32_t n = 0;
            if (OB_FAIL(r.read_list_header(et, n))) {
            } else if (PQ_T_STRUCT != et) {
              ret = OB_INVALID_DATA;
            } else {
              for (int32_t i = 0; OB_SUCC(ret) && i < n; ++i) {
                ObParquetRowGroup rg;
                if (OB_FAIL(parse_row_group(r, rg))) {
                } else if (OB_FAIL(row_groups_.push_back(rg))) {
                }
              }
            }
          } else {
            ret = r.skip(type);
          }
        }
        if (OB_SUCC(ret) && OB_ITER_END != tmp) {
          ret = tmp;
        }
        if (OB_SUCC(ret)) {
          if (schema.empty()) {
            ret = OB_INVALID_DATA;
            LOG_WARN("empty parquet schema", K(ret));
          } else {
            // the first element is the root group, the rest must be leaves:
            // nested and repeated schemas are out of scope
            for (int64_t i = 0; OB_SUCC(ret) && i < schema.count(); ++i) {
              const PQSchemaElement &el = schema.at(i);
              if (0 == i) {
                if (el.num_children_ <= 0) {
                  ret = OB_INVALID_DATA;
                }
              } else if (el.num_children_ > 0 || !el.has_type_
                         || PQ_REP_REPEATED == el.repetition_) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("nested or repeated parquet column is not supported", K(ret));
              } else {
                ObParquetColumn col;
                char *name_buf = static_cast<char *>(alloc.alloc(el.name_.length() + 1));
                if (OB_ISNULL(name_buf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                } else {
                  MEMCPY(name_buf, el.name_.ptr(), el.name_.length());
                  name_buf[el.name_.length()] = '\0';
                  col.name_.assign_ptr(name_buf, el.name_.length());
                  col.physical_type_ = el.type_;
                  col.converted_type_ = el.converted_type_;
                  col.type_length_ = el.type_length_;
                  col.scale_ = el.scale_;
                  col.precision_ = el.precision_;
                  col.optional_ = (PQ_REP_OPTIONAL == el.repetition_);
                  if (PQ_BOOLEAN == el.type_) {
                    col.sql_type_ = ObTinyIntType;
                  } else if (PQ_INT32 == el.type_) {
                    col.sql_type_ = (PQ_CT_DATE == el.converted_type_) ? ObDateType
                                  : (PQ_CT_DECIMAL == el.converted_type_) ? ObDoubleType
                                  : ObIntType;
                  } else if (PQ_INT64 == el.type_) {
                    col.sql_type_ = (PQ_CT_TIMESTAMP_MILLIS == el.converted_type_
                                     || PQ_CT_TIMESTAMP_MICROS == el.converted_type_)
                                      ? ObDateTimeType
                                  : (PQ_CT_DECIMAL == el.converted_type_) ? ObDoubleType
                                  : ObIntType;
                  } else if (PQ_INT96 == el.type_) {
                    col.sql_type_ = ObDateTimeType;
                  } else if (PQ_FLOAT == el.type_ || PQ_DOUBLE == el.type_) {
                    col.sql_type_ = ObDoubleType;
                  } else {
                    col.sql_type_ = ObVarcharType;
                  }
                  if (OB_FAIL(columns_.push_back(col))) {
                  }
                }
              }
            }
          }
        }
        alloc.free(footer);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (row_groups_.empty() && num_rows_ > 0) {
      ret = OB_INVALID_DATA;
      LOG_WARN("parquet file has rows but no row group", K(ret), K_(num_rows));
    }
  }
  return ret;
}

int ObParquetReader::open(const ObString &path, const ObFileScanOptions &opt,
                          ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  close();
  alloc_ = &alloc;
  expected_size_ = opt.file_size_;
  char *cpath = NULL;
  if (path.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(cpath = static_cast<char *>(alloc.alloc(path.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FALSE_IT(MEMCPY(cpath, path.ptr(), path.length()))) {
  } else {
    cpath[path.length()] = '\0';
    path_buf_ = cpath;
    if ((fd_ = ::open(cpath, O_RDONLY)) < 0) {
      fd_ = -1;
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("open parquet file failed", K(ret), K(path), "errno", errno);
    } else if (OB_FAIL(load_footer(alloc))) {
    } else if (OB_FAIL(alloc_states(alloc))) {
    } else if (OB_FAIL(rescan())) {
    }
  }
  if (OB_FAIL(ret)) {
    close();
    alloc_ = &alloc;
  }
  return ret;
}

int ObParquetReader::set_needed_columns(const ObIArray<bool> &needed)
{
  int ret = OB_SUCCESS;
  needed_.reuse();
  if (needed.count() != columns_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parquet projection does not match the file schema", K(ret),
             K(needed.count()), K(columns_.count()));
  } else if (OB_FAIL(needed_.assign(needed))) {
    LOG_WARN("copy parquet projection failed", K(ret));
  }
  return ret;
}

int ObParquetReader::alloc_states(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
    void *buf = alloc.alloc(sizeof(ColumnState));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ColumnState *st = new (buf) ColumnState();
      if (OB_FAIL(states_.push_back(st))) {
        st->~ColumnState();
        alloc.free(buf);
      }
    }
  }
  return ret;
}

int ObParquetReader::rescan()
{
  int ret = OB_SUCCESS;
  if (fd_ < 0) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(check_file_unchanged())) {
  } else {
    cur_rg_ = 0;
    cur_row_ = 0;
    loaded_rg_ = -1;
  }
  return ret;
}

int ObParquetReader::load_row_group(const int64_t rg_idx)
{
  int ret = OB_SUCCESS;
  if (rg_idx < 0 || rg_idx >= row_groups_.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObParquetRowGroup &rg = row_groups_.at(rg_idx);
    if (rg.chunks_.count() != columns_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parquet row group does not match the schema", K(ret),
               K(rg.chunks_.count()), K(columns_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rg.chunks_.count(); ++i) {
      ColumnState *st = states_.at(i);
      st->page_alloc_.reuse();
      st->dict_alloc_.reuse();
      st->texts_ = NULL;
      st->nulls_ = NULL;
      st->cap_ = 0;
      st->pos_ = 0;
      st->page_num_values_ = 0;
      st->values_left_ = 0;
      st->dict_ = NULL;
      st->dict_count_ = 0;
      if (!is_needed(i)) {
        // a pruned column keeps no page state at all
      } else {
        const ObParquetColumnChunk &chunk = rg.chunks_.at(i);
        st->values_left_ = chunk.num_values_;
        if (chunk.total_compressed_size_ <= 0 || chunk.data_page_offset_ < 0) {
          ret = OB_INVALID_DATA;
          LOG_WARN("bad parquet column chunk", K(ret), K(chunk.total_compressed_size_));
        } else {
          const int64_t start = chunk.dict_page_offset_ > 0
                                ? std::min(chunk.dict_page_offset_, chunk.data_page_offset_)
                                : chunk.data_page_offset_;
          st->next_page_offset_ = start;
          st->chunk_end_ = start + chunk.total_compressed_size_;
          if (st->chunk_end_ > file_size_) {
            ret = OB_INVALID_DATA;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      loaded_rg_ = rg_idx;
      cur_row_ = 0;
    }
  }
  return ret;
}

int ObParquetReader::decode_dictionary_page(ColumnState &st, const ObParquetColumn &col,
                                            const uint8_t *body, const int64_t body_len,
                                            const int64_t num_values)
{
  int ret = OB_SUCCESS;
  if (num_values < 0 || num_values > (INT64_MAX / 64)) {
    ret = OB_INVALID_DATA;
  } else if (num_values > 0) {
    st.dict_ = static_cast<ObString *>(st.dict_alloc_.alloc(sizeof(ObString) * num_values));
    st.dict_count_ = num_values;
    if (OB_ISNULL(st.dict_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t i = 0; i < num_values; ++i) {
        new (st.dict_ + i) ObString();
      }
    }
  }
  const uint8_t *p = body;
  const uint8_t *const end = body + body_len;
  for (int64_t i = 0; OB_SUCC(ret) && i < num_values; ++i) {
    ObString text;
    switch (col.physical_type_) {
      case PQ_INT32: {
        if (end - p < 4) {
          ret = OB_INVALID_DATA;
        } else {
          int32_t v = 0;
          MEMCPY(&v, p, 4);
          p += 4;
          char *buf = static_cast<char *>(st.dict_alloc_.alloc(64));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            if (PQ_CT_DATE == col.converted_type_) {
              int64_t year = 0;
              int month = 0;
              int day = 0;
              civil_from_days(v, year, month, day);
              snprintf(buf, 64, "%04lld-%02d-%02d", static_cast<long long>(year), month, day);
            } else if (PQ_CT_DECIMAL == col.converted_type_ && col.scale_ > 0) {
              snprintf(buf, 64, "%.*f", col.scale_,
                       static_cast<double>(v) / pow(10.0, col.scale_));
            } else {
              snprintf(buf, 64, "%d", v);
            }
            text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
          }
        }
        break;
      }
      case PQ_INT64: {
        if (end - p < 8) {
          ret = OB_INVALID_DATA;
        } else {
          int64_t v = 0;
          MEMCPY(&v, p, 8);
          p += 8;
          char *buf = static_cast<char *>(st.dict_alloc_.alloc(64));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            if (PQ_CT_TIMESTAMP_MILLIS == col.converted_type_) {
              format_datetime(v / 1000, (v % 1000) * 1000, 3, buf, 64);
            } else if (PQ_CT_TIMESTAMP_MICROS == col.converted_type_) {
              format_datetime(v / 1000000, v % 1000000, 6, buf, 64);
            } else if (PQ_CT_DECIMAL == col.converted_type_ && col.scale_ > 0) {
              snprintf(buf, 64, "%.*f", col.scale_,
                       static_cast<double>(v) / pow(10.0, col.scale_));
            } else {
              snprintf(buf, 64, "%lld", static_cast<long long>(v));
            }
            text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
          }
        }
        break;
      }
      case PQ_FLOAT: {
        if (end - p < 4) {
          ret = OB_INVALID_DATA;
        } else {
          float v = 0;
          MEMCPY(&v, p, 4);
          p += 4;
          char *buf = static_cast<char *>(st.dict_alloc_.alloc(64));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            snprintf(buf, 64, "%.9g", static_cast<double>(v));
            text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
          }
        }
        break;
      }
      case PQ_DOUBLE: {
        if (end - p < 8) {
          ret = OB_INVALID_DATA;
        } else {
          double v = 0;
          MEMCPY(&v, p, 8);
          p += 8;
          char *buf = static_cast<char *>(st.dict_alloc_.alloc(64));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            snprintf(buf, 64, "%.17g", v);
            text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
          }
        }
        break;
      }
      case PQ_BYTE_ARRAY: {
        if (end - p < 4) {
          ret = OB_INVALID_DATA;
        } else {
          uint32_t n = 0;
          MEMCPY(&n, p, 4);
          p += 4;
          if (n > static_cast<uint32_t>(end - p)) {
            ret = OB_INVALID_DATA;
          } else {
            text.assign_ptr(reinterpret_cast<const char *>(p), static_cast<int32_t>(n));
            p += n;
          }
        }
        break;
      }
      case PQ_FIXED_LEN_BYTE_ARRAY: {
        if (col.type_length_ <= 0 || end - p < col.type_length_) {
          ret = OB_INVALID_DATA;
        } else if (PQ_CT_DECIMAL == col.converted_type_ && col.type_length_ <= 16) {
          char *buf = static_cast<char *>(st.dict_alloc_.alloc(64));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            format_decimal(p, col.type_length_, col.scale_, buf, 64);
            text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
            p += col.type_length_;
          }
        } else {
          text.assign_ptr(reinterpret_cast<const char *>(p), col.type_length_);
          p += col.type_length_;
        }
        break;
      }
      case PQ_BOOLEAN: {
        // booleans are bit-packed in a dictionary page as well
        if (i % 8 == 0 && p + i / 8 >= end) {
          ret = OB_INVALID_DATA;
        } else if (OB_SUCC(ret)) {
          const bool b = (0 != (p[i / 8] & (1 << (i % 8))));
          char *buf = static_cast<char *>(st.dict_alloc_.alloc(8));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            MEMCPY(buf, b ? "true" : "false", b ? 4 : 5);
            text.assign_ptr(buf, b ? 4 : 5);
          }
          if (i + 1 == num_values) {
            p += (num_values + 7) / 8;
          }
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        break;
    }
    if (OB_SUCC(ret)) {
      st.dict_[i] = text;
    }
  }
  return ret;
}

int ObParquetReader::decode_data_page(ColumnState &st, const ObParquetColumn &col,
                                      const uint8_t *body, const int64_t body_len,
                                      const uint8_t *value_data, const int64_t value_len,
                                      const int64_t num_values, const int32_t encoding,
                                      const bool has_levels_prefix)
{
  int ret = OB_SUCCESS;
  const uint8_t *levels_end = body;
  const uint8_t *values_start = value_data;
  if (num_values < 0 || num_values > (INT64_MAX / 16)) {
    ret = OB_INVALID_DATA;
  } else if (num_values > 0) {
    st.texts_ = static_cast<ObString *>(st.page_alloc_.alloc(sizeof(ObString) * num_values));
    st.nulls_ = static_cast<bool *>(st.page_alloc_.alloc(sizeof(bool) * num_values));
    st.cap_ = num_values;
    if (OB_ISNULL(st.texts_) || OB_ISNULL(st.nulls_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t i = 0; i < num_values; ++i) {
        new (st.texts_ + i) ObString();
      }
      MEMSET(st.nulls_, 0, sizeof(bool) * num_values);
    }
  }
  uint32_t *levels = NULL;
  if (OB_FAIL(ret)) {
  } else if (col.optional_) {
    const int32_t max_level = PQ_MAX_LEVEL;
    const int width = bit_width_for(max_level);
    if (has_levels_prefix) {
      if (body_len < 4) {
        ret = OB_INVALID_DATA;
      } else {
        uint32_t len = 0;
        MEMCPY(&len, body, 4);
        levels_end = body + 4;
        if (len > static_cast<uint32_t>(body_len - 4)) {
          ret = OB_INVALID_DATA;
        } else if (num_values > 0) {
          levels = static_cast<uint32_t *>(st.page_alloc_.alloc(sizeof(uint32_t) * num_values));
          int64_t consumed = 0;
          if (OB_ISNULL(levels)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_FAIL(decode_hybrid(levels_end, len, width, num_values,
                                           levels, consumed))) {
          } else {
            // the values follow the definition levels in the same page
            levels_end += len;
            values_start = levels_end;
          }
        }
      }
    } else {
      if (body_len < 0) {
        ret = OB_INVALID_DATA;
      } else if (num_values > 0) {
        levels = static_cast<uint32_t *>(st.page_alloc_.alloc(sizeof(uint32_t) * num_values));
        int64_t consumed = 0;
        if (OB_ISNULL(levels)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(decode_hybrid(body, body_len, width, num_values,
                                         levels, consumed))) {
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t present = 0;
    for (int64_t i = 0; i < num_values; ++i) {
      const bool is_null = (NULL != levels && levels[i] < PQ_MAX_LEVEL);
      st.nulls_[i] = is_null;
      if (!is_null) {
        ++present;
      }
    }
    const uint8_t *p = values_start;
    const uint8_t *const end = value_data + value_len;
    // dictionary indices, if any
    uint32_t *indices = NULL;
    uint32_t *rle_values = NULL;
    if (PQ_ENC_PLAIN_DICTIONARY == encoding || PQ_ENC_RLE_DICTIONARY == encoding) {
      if (present > 0) {
        // an all-null page still carries an (empty) dictionary page, so the
        // dictionary is only required when the page holds a value
        if (NULL == st.dict_ || st.dict_count_ <= 0) {
          ret = OB_INVALID_DATA;
          LOG_WARN("dictionary encoded page without a dictionary", K(ret));
        } else if (p >= end) {
          ret = OB_INVALID_DATA;
        } else {
          const int width = *p++;
          indices = static_cast<uint32_t *>(st.page_alloc_.alloc(sizeof(uint32_t) * present));
          int64_t consumed = 0;
          if (OB_ISNULL(indices)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_FAIL(decode_hybrid(p, end - p, width, present, indices, consumed))) {
          } else {
            p += consumed;
          }
        }
      }
    } else if (PQ_ENC_RLE == encoding) {
      // the only plain RLE encoded value type: booleans occupy one bit each
      if (PQ_BOOLEAN != col.physical_type_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("RLE values are only supported for booleans", K(ret));
      } else if (present > 0) {
        // unlike the levels, RLE encoded values carry a 4 byte length prefix
        uint32_t enc_len = 0;
        if (end - p < 4) {
          ret = OB_INVALID_DATA;
        } else {
          MEMCPY(&enc_len, p, 4);
          if (enc_len > static_cast<uint32_t>(end - p - 4)) {
            ret = OB_INVALID_DATA;
          } else {
            p += 4;
            rle_values = static_cast<uint32_t *>(st.page_alloc_.alloc(sizeof(uint32_t) * present));
            int64_t consumed = 0;
            if (OB_ISNULL(rle_values)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else if (OB_FAIL(decode_hybrid(p, enc_len, 1, present, rle_values, consumed))) {
            } else {
              p += consumed;
            }
          }
        }
      }
    }
    int64_t value_idx = 0;
    int64_t row_idx = 0;
    while (OB_SUCC(ret) && row_idx < num_values) {
      if (st.nulls_[row_idx]) {
        st.texts_[row_idx].reset();
        ++row_idx;
        continue;
      }
      if (PQ_ENC_PLAIN_DICTIONARY == encoding || PQ_ENC_RLE_DICTIONARY == encoding) {
        if (value_idx >= present) {
          ret = OB_INVALID_DATA;
        } else {
          const uint32_t di = indices[value_idx++];
          if (di >= static_cast<uint32_t>(st.dict_count_)) {
            ret = OB_INVALID_DATA;
          } else {
            st.texts_[row_idx] = st.dict_[di];
          }
          ++row_idx;
        }
      } else if (PQ_ENC_RLE == encoding) {
        if (value_idx >= present) {
          ret = OB_INVALID_DATA;
        } else {
          st.texts_[row_idx].assign_ptr(0 != rle_values[value_idx] ? "true" : "false",
                                        0 != rle_values[value_idx] ? 4 : 5);
          ++value_idx;
          ++row_idx;
        }
      } else if (PQ_ENC_PLAIN == encoding) {
        ObString text;
        switch (col.physical_type_) {
          case PQ_INT32: {
            if (end - p < 4) {
              ret = OB_INVALID_DATA;
            } else {
              int32_t v = 0;
              MEMCPY(&v, p, 4);
              p += 4;
              char *buf = static_cast<char *>(st.page_alloc_.alloc(64));
              if (OB_ISNULL(buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                if (PQ_CT_DATE == col.converted_type_) {
                  int64_t year = 0;
                  int month = 0;
                  int day = 0;
                  civil_from_days(v, year, month, day);
                  snprintf(buf, 64, "%04lld-%02d-%02d",
                           static_cast<long long>(year), month, day);
                } else if (PQ_CT_DECIMAL == col.converted_type_ && col.scale_ > 0) {
                  snprintf(buf, 64, "%.*f", col.scale_,
                           static_cast<double>(v) / pow(10.0, col.scale_));
                } else {
                  snprintf(buf, 64, "%d", v);
                }
                text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
              }
            }
            break;
          }
          case PQ_INT64: {
            if (end - p < 8) {
              ret = OB_INVALID_DATA;
            } else {
              int64_t v = 0;
              MEMCPY(&v, p, 8);
              p += 8;
              char *buf = static_cast<char *>(st.page_alloc_.alloc(64));
              if (OB_ISNULL(buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                if (PQ_CT_TIMESTAMP_MILLIS == col.converted_type_) {
                  format_datetime(v / 1000, (v % 1000) * 1000, 3, buf, 64);
                } else if (PQ_CT_TIMESTAMP_MICROS == col.converted_type_) {
                  format_datetime(v / 1000000, v % 1000000, 6, buf, 64);
                } else if (PQ_CT_DECIMAL == col.converted_type_ && col.scale_ > 0) {
                  snprintf(buf, 64, "%.*f", col.scale_,
                           static_cast<double>(v) / pow(10.0, col.scale_));
                } else {
                  snprintf(buf, 64, "%lld", static_cast<long long>(v));
                }
                text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
              }
            }
            break;
          }
          case PQ_INT96: {
            if (end - p < 12) {
              ret = OB_INVALID_DATA;
            } else {
              int64_t nanos = 0;
              int32_t julian = 0;
              MEMCPY(&nanos, p, 8);
              MEMCPY(&julian, p + 8, 4);
              p += 12;
              char *buf = static_cast<char *>(st.page_alloc_.alloc(64));
              if (OB_ISNULL(buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                const int64_t days = static_cast<int64_t>(julian) - 2440588;
                const int64_t secs = days * 86400 + nanos / 1000000000;
                format_datetime(secs, (nanos % 1000000000) / 1000, 6, buf, 64);
                text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
              }
            }
            break;
          }
          case PQ_FLOAT: {
            if (end - p < 4) {
              ret = OB_INVALID_DATA;
            } else {
              float v = 0;
              MEMCPY(&v, p, 4);
              p += 4;
              char *buf = static_cast<char *>(st.page_alloc_.alloc(64));
              if (OB_ISNULL(buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                snprintf(buf, 64, "%.9g", static_cast<double>(v));
                text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
              }
            }
            break;
          }
          case PQ_DOUBLE: {
            if (end - p < 8) {
              ret = OB_INVALID_DATA;
            } else {
              double v = 0;
              MEMCPY(&v, p, 8);
              p += 8;
              char *buf = static_cast<char *>(st.page_alloc_.alloc(64));
              if (OB_ISNULL(buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                snprintf(buf, 64, "%.17g", v);
                text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
              }
            }
            break;
          }
          case PQ_BYTE_ARRAY: {
            if (end - p < 4) {
              ret = OB_INVALID_DATA;
            } else {
              uint32_t n = 0;
              MEMCPY(&n, p, 4);
              p += 4;
              if (n > static_cast<uint32_t>(end - p)) {
                ret = OB_INVALID_DATA;
              } else {
                text.assign_ptr(reinterpret_cast<const char *>(p), static_cast<int32_t>(n));
                p += n;
              }
            }
            break;
          }
          case PQ_FIXED_LEN_BYTE_ARRAY: {
            if (col.type_length_ <= 0 || end - p < col.type_length_) {
              ret = OB_INVALID_DATA;
            } else if (PQ_CT_DECIMAL == col.converted_type_ && col.type_length_ <= 16) {
              char *buf = static_cast<char *>(st.page_alloc_.alloc(64));
              if (OB_ISNULL(buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                format_decimal(p, col.type_length_, col.scale_, buf, 64);
                text.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
                p += col.type_length_;
              }
            } else {
              text.assign_ptr(reinterpret_cast<const char *>(p), col.type_length_);
              p += col.type_length_;
            }
            break;
          }
          case PQ_BOOLEAN: {
            const int64_t byte_off = value_idx / 8;
            if (p + byte_off >= end) {
              ret = OB_INVALID_DATA;
            } else {
              const bool b = (0 != (p[byte_off] & (1 << (value_idx % 8))));
              text.assign_ptr(b ? "true" : "false", b ? 4 : 5);
              ++value_idx;
            }
            break;
          }
          default:
            ret = OB_NOT_SUPPORTED;
            break;
        }
        if (OB_SUCC(ret)) {
          st.texts_[row_idx] = text;
          if (PQ_BOOLEAN != col.physical_type_) {
            ++value_idx;
          }
          ++row_idx;
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("parquet encoding is not supported", K(ret), K(encoding));
      }
    }
    if (OB_SUCC(ret)) {
      st.pos_ = 0;
      st.page_num_values_ = num_values;
    }
  }
  return ret;
}

int ObParquetReader::load_next_page(ColumnState &st, const ObParquetColumnChunk &chunk,
                                    const ObParquetColumn &col)
{
  int ret = OB_SUCCESS;
  bool got_page = false;
  char hdr_buf[4096];
  while (OB_SUCC(ret) && !got_page) {
    if (st.next_page_offset_ >= st.chunk_end_) {
      ret = OB_ITER_END;
    } else {
      const int64_t avail = st.chunk_end_ - st.next_page_offset_;
      const int64_t hdr_len = std::min(avail, static_cast<int64_t>(sizeof(hdr_buf)));
      if (OB_FAIL(read_at(st.next_page_offset_, hdr_len, hdr_buf))) {
      } else {
        PQReader r(reinterpret_cast<const uint8_t *>(hdr_buf), hdr_len);
        PQPageHeaderParsed ph;
        if (OB_FAIL(parse_page_header(r, ph))) {
          LOG_WARN("parse parquet page header failed", K(ret));
        } else {
          const int64_t body_off = st.next_page_offset_ + r.consumed();
          if (ph.compressed_page_size_ < 0 || ph.uncompressed_page_size_ < 0
              || body_off + ph.compressed_page_size_ > st.chunk_end_) {
            ret = OB_INVALID_DATA;
          } else {
            // dictionary pages are kept for the whole row group, so their
            // buffers (and the strings pointing into them) live in the
            // dictionary arena
            ObArenaAllocator &body_alloc = (PQ_PAGE_DICTIONARY == ph.type_)
                                           ? st.dict_alloc_ : st.page_alloc_;
            char *body = static_cast<char *>(body_alloc.alloc(ph.compressed_page_size_ + 1));
            if (OB_ISNULL(body)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else if (OB_FAIL(read_at(body_off, ph.compressed_page_size_, body))) {
            } else {
              const uint8_t *raw = reinterpret_cast<const uint8_t *>(body);
              char *uncomp = NULL;
              const bool compressed = (0 != chunk.codec_);
              if (compressed && 0 != chunk.codec_ && 1 != chunk.codec_) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("parquet compression codec is not supported", K(ret), K(chunk.codec_));
              } else if (ph.type_ == PQ_PAGE_DICTIONARY) {
                const uint8_t *data = raw;
                int64_t data_len = ph.compressed_page_size_;
                if (compressed) {
                  int64_t out_len = 0;
                  uncomp = static_cast<char *>(body_alloc.alloc(ph.uncompressed_page_size_ + 1));
                  if (OB_ISNULL(uncomp)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                  } else if (OB_FAIL(snappy_decompress(body, ph.compressed_page_size_,
                                                       uncomp, ph.uncompressed_page_size_,
                                                       out_len))) {
                    LOG_WARN("snappy decompress failed", K(ret));
                  } else {
                    data = reinterpret_cast<const uint8_t *>(uncomp);
                    data_len = out_len;
                  }
                }
                if (OB_SUCC(ret)
                    && OB_FAIL(decode_dictionary_page(st, col, data, data_len, ph.num_values_))) {
                }
              } else if (ph.type_ == PQ_PAGE_DATA) {
                const uint8_t *data = raw;
                int64_t data_len = ph.compressed_page_size_;
                if (compressed) {
                  int64_t out_len = 0;
                  uncomp = static_cast<char *>(st.page_alloc_.alloc(ph.uncompressed_page_size_ + 1));
                  if (OB_ISNULL(uncomp)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                  } else if (OB_FAIL(snappy_decompress(body, ph.compressed_page_size_,
                                                       uncomp, ph.uncompressed_page_size_,
                                                       out_len))) {
                    LOG_WARN("snappy decompress failed", K(ret));
                  } else {
                    data = reinterpret_cast<const uint8_t *>(uncomp);
                    data_len = out_len;
                  }
                }
                if (OB_SUCC(ret)
                    && OB_FAIL(decode_data_page(st, col, data, data_len, data, data_len,
                                                ph.num_values_, ph.encoding_,
                                                true /* levels carry a length prefix */))) {
                }
              } else if (ph.type_ == PQ_PAGE_DATA_V2) {
                const int64_t levels_len = ph.rep_levels_byte_length_
                                           + ph.def_levels_byte_length_;
                if (levels_len < 0 || levels_len > ph.compressed_page_size_) {
                  ret = OB_INVALID_DATA;
                } else {
                  const uint8_t *levels_data = raw + ph.rep_levels_byte_length_;
                  const uint8_t *values_data = raw + levels_len;
                  int64_t values_len = ph.compressed_page_size_ - levels_len;
                  if (ph.is_compressed_ && compressed) {
                    int64_t out_len = 0;
                    const int64_t out_cap = ph.uncompressed_page_size_ - levels_len;
                    uncomp = static_cast<char *>(st.page_alloc_.alloc(out_cap + 1));
                    if (OB_ISNULL(uncomp)) {
                      ret = OB_ALLOCATE_MEMORY_FAILED;
                    } else if (OB_FAIL(snappy_decompress(
                                 reinterpret_cast<const char *>(values_data), values_len,
                                 uncomp, out_cap, out_len))) {
                      LOG_WARN("snappy decompress failed", K(ret));
                    } else {
                      values_data = reinterpret_cast<const uint8_t *>(uncomp);
                      values_len = out_len;
                    }
                  }
                  if (OB_SUCC(ret)
                      && OB_FAIL(decode_data_page(st, col, levels_data,
                                                  ph.def_levels_byte_length_,
                                                  values_data, values_len,
                                                  ph.num_values_, ph.encoding_,
                                                  false /* v2 levels have no prefix */))) {
                  }
                }
              } else {
                // index pages and unknown pages are skipped
              }
      if (OB_SUCC(ret)) {
        st.next_page_offset_ = body_off + ph.compressed_page_size_;
        got_page = (PQ_PAGE_DATA == ph.type_ || PQ_PAGE_DATA_V2 == ph.type_);
      }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObParquetReader::next_row(ObIArray<ObString> &fields, ObIArray<bool> &is_null)
{
  int ret = OB_SUCCESS;
  if (fd_ < 0 || NULL == alloc_ || states_.count() != columns_.count()) {
    ret = OB_NOT_INIT;
  }
  if (OB_SUCC(ret)) {
    while (OB_SUCC(ret) && cur_rg_ < row_groups_.count()
           && cur_row_ >= row_groups_.at(cur_rg_).num_rows_) {
      ++cur_rg_;
      cur_row_ = 0;
    }
    if (OB_SUCC(ret)) {
      if (cur_rg_ >= row_groups_.count()) {
        ret = OB_ITER_END;
      } else if (loaded_rg_ != cur_rg_) {
        ret = load_row_group(cur_rg_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    fields.reuse();
    is_null.reuse();
    const ObParquetRowGroup &rg = row_groups_.at(cur_rg_);
    for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
      ColumnState *st = states_.at(i);
      if (!is_needed(i)) {
        // pruned column: keep the value slot so the row stays aligned
        if (OB_FAIL(fields.push_back(ObString())) || OB_FAIL(is_null.push_back(false))) {
        }
        continue;
      }
      const ObParquetColumnChunk &chunk = rg.chunks_.at(i);
      if (st->pos_ >= st->page_num_values_) {
        if (st->values_left_ <= 0) {
          ret = OB_INVALID_DATA;
          LOG_WARN("parquet column chunk is shorter than the row group", K(ret), K(i));
        } else if (OB_FAIL(load_next_page(*st, chunk, columns_.at(i)))) {
          if (OB_ITER_END == ret) {
            ret = OB_INVALID_DATA;
            LOG_WARN("parquet column chunk ran out of pages", K(ret), K(i));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(fields.push_back(st->texts_[st->pos_]))
            || OB_FAIL(is_null.push_back(st->nulls_[st->pos_]))) {
        } else {
          ++st->pos_;
          --st->values_left_;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ++cur_row_;
    }
  }
  return ret;
}

int ObParquetReader::read_schema(ObIAllocator &alloc, const ObString &path,
                                 const ObFileScanOptions &opt,
                                 ObIArray<ObFileColumnMeta> &cols)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc;
  ObParquetReader reader;
  if (OB_FAIL(reader.open(path, opt, tmp_alloc))) {
    LOG_WARN("open parquet file failed", K(ret), K(path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < reader.columns_.count(); ++i) {
      const ObParquetColumn &col = reader.columns_.at(i);
      ObFileColumnMeta meta;
      ObString name;
      if (OB_FAIL(ob_write_string(alloc, col.name_, name))) {
      } else {
        meta.name_ = name;
        meta.type_ = col.sql_type_;
        // the physical size is not bounded by the footer, keep the maximum
        meta.max_len_ = (ObVarcharType == col.sql_type_) ? 262143 : 0;
        if (OB_FAIL(cols.push_back(meta))) {
        }
      }
    }
    reader.close();
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
