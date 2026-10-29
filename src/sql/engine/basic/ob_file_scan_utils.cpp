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

#include "ob_file_scan_utils.h"
#include "ob_parquet_reader.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/charset/ob_charset.h"
#include "common/timezone/ob_time_convert.h"
#include "share/ob_errno.h"

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#include <direct.h>
#else
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#endif

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
#ifdef _WIN32
int fs_open(const char *path) { return _open(path, _O_RDONLY | _O_BINARY); }
int fs_read(int fd, char *buf, int len) { return _read(fd, buf, len); }
void fs_close(int fd) { _close(fd); }
#else
int fs_open(const char *path) { return ::open(path, O_RDONLY); }
int fs_read(int fd, char *buf, int len) { return static_cast<int>(::read(fd, buf, len)); }
void fs_close(int fd) { ::close(fd); }
#endif

const int64_t READ_CHUNK_SIZE = 64 * 1024;

char *my_strndup(const char *ptr, const int64_t len, ObIAllocator &alloc)
{
  char *buf = static_cast<char *>(alloc.alloc(len + 1));
  if (NULL != buf) {
    if (len > 0) {
      MEMCPY(buf, ptr, len);
    }
    buf[len] = '\0';
  }
  return buf;
}

bool is_space(char c) { return ' ' == c || '\t' == c || '\r' == c || '\n' == c; }

bool is_digit(char c) { return c >= '0' && c <= '9'; }

int days_of_month(const int year, const int month)
{
  static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int ret = 0;
  if (month < 1 || month > 12) {
    ret = 0;
  } else if (2 == month) {
    const bool leap = (0 == year % 4 && 0 != year % 100) || 0 == year % 400;
    ret = leap ? 29 : 28;
  } else {
    ret = days[month - 1];
  }
  return ret;
}

// YYYY-MM-DD, strict: fixed width and a legal calendar date.
bool parse_date_strict(const ObString &text, int32_t &date_val)
{
  bool ok = false;
  if (10 == text.length()) {
    const char *p = text.ptr();
    bool digits = true;
    for (int i = 0; i < 10 && digits; ++i) {
      if (4 != i && 7 != i && !is_digit(p[i])) {
        digits = false;
      }
    }
    if (digits && '-' == p[4] && '-' == p[7]) {
      const int year = (p[0] - '0') * 1000 + (p[1] - '0') * 100 + (p[2] - '0') * 10 + (p[3] - '0');
      const int month = (p[5] - '0') * 10 + (p[6] - '0');
      const int day = (p[8] - '0') * 10 + (p[9] - '0');
      if (year >= 1 && month >= 1 && month <= 12 && day >= 1 && day <= days_of_month(year, month)) {
        date_val = year * 10000 + month * 100 + day;
        ok = true;
      }
    }
  }
  return ok;
}

// YYYY-MM-DD[ T]HH:MM:SS[.fraction]
bool parse_datetime_strict(const ObString &text, int64_t &int_part, int64_t &dec_part,
                           int16_t &scale, bool &only_date)
{
  bool ok = false;
  only_date = false;
  int32_t date_val = 0;
  if (text.length() >= 10) {
    ObString date_str(10, text.ptr());
    if (parse_date_strict(date_str, date_val)) {
      if (10 == text.length()) {
        int_part = static_cast<int64_t>(date_val) * 1000000;
        dec_part = 0;
        scale = 0;
        only_date = true;
        ok = true;
      } else if (text.length() >= 19 && (' ' == text.ptr()[10] || 'T' == text.ptr()[10])) {
        const char *p = text.ptr();
        bool valid = true;
        for (int i = 11; i < 19 && valid; ++i) {
          if (13 == i || 16 == i) {
            valid = (':' == p[i]);
          } else if (!is_digit(p[i])) {
            valid = false;
          }
        }
        if (valid) {
          const int hour = (p[11] - '0') * 10 + (p[12] - '0');
          const int minute = (p[14] - '0') * 10 + (p[15] - '0');
          const int second = (p[17] - '0') * 10 + (p[18] - '0');
          int64_t usec = 0;
          int16_t frac_scale = 0;
          bool frac_ok = true;
          if (text.length() > 19) {
            if ('.' != p[19]) {
              frac_ok = false;
            } else {
              int64_t frac_len = text.length() - 20;
              if (frac_len <= 0 || frac_len > 6) {
                frac_ok = false;
              } else {
                for (int64_t i = 20; i < text.length() && frac_ok; ++i) {
                  if (!is_digit(p[i])) {
                    frac_ok = false;
                  } else {
                    usec = usec * 10 + (p[i] - '0');
                  }
                }
                for (int64_t i = frac_len; i < 6; ++i) {
                  usec *= 10;
                }
                frac_scale = static_cast<int16_t>(frac_len);
              }
            }
          }
          if (frac_ok && hour >= 0 && hour < 24 && minute >= 0 && minute < 60
              && second >= 0 && second < 60) {
            int_part = static_cast<int64_t>(date_val) * 1000000
                       + hour * 10000 + minute * 100 + second;
            dec_part = usec;
            scale = frac_scale;
            ok = true;
          }
        }
      }
    }
  }
  return ok;
}

bool parse_int64_strict(const ObString &text, int64_t &value)
{
  bool ok = false;
  if (text.length() > 0 && text.length() < 24) {
    char buf[32];
    MEMCPY(buf, text.ptr(), text.length());
    buf[text.length()] = '\0';
    char *end = NULL;
    errno = 0;
    const long long v = strtoll(buf, &end, 10);
    if (0 == errno && end == buf + text.length() && end != buf) {
      value = static_cast<int64_t>(v);
      ok = true;
    }
  }
  return ok;
}

bool parse_double_strict(const ObString &text, double &value)
{
  bool ok = false;
  if (text.length() > 0 && text.length() < 64) {
    char buf[80];
    MEMCPY(buf, text.ptr(), text.length());
    buf[text.length()] = '\0';
    char *end = NULL;
    errno = 0;
    const double v = strtod(buf, &end);
    // nan/inf parse as doubles but are not numeric values of the file
    if (0 == errno && end == buf + text.length() && end != buf && isfinite(v)) {
      value = v;
      ok = true;
    }
  }
  return ok;
}

bool is_bool_text(const ObString &text, bool &value)
{
  bool ok = false;
  if (0 == text.case_compare("true")) {
    value = true;
    ok = true;
  } else if (0 == text.case_compare("false")) {
    value = false;
    ok = true;
  }
  return ok;
}

} // namespace

void ObFileScanOptions::reset()
{
  format_ = OB_FILE_FORMAT_CSV;
  has_header_ = true;
  delimiter_ = ',';
  quote_ = '"';
  escape_ = '"';
  empty_as_null_ = true;
  skip_lines_ = 0;
  max_line_size_ = DEFAULT_MAX_LINE_SIZE;
  file_size_ = -1;
  file_mtime_ = 0;
  null_str_len_ = 0;
  MEMSET(null_str_, 0, sizeof(null_str_));
}

int ObFileScanOptions::set_option(const ObString &key, const ObString &value, bool &recognized)
{
  int ret = OB_SUCCESS;
  recognized = true;
  if (key.case_compare("header") == 0 || key.case_compare("has_header") == 0) {
    bool bv = false;
    if (is_bool_text(value, bv)) {
      has_header_ = bv;
    } else {
      int64_t iv = 0;
      if (parse_int64_strict(value, iv)) {
        has_header_ = (0 != iv);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid header option value", K(key), K(value));
      }
    }
  } else if (key.case_compare("delimiter") == 0 || key.case_compare("delim") == 0
             || key.case_compare("sep") == 0) {
    if (value.length() < 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty delimiter", K(value));
    } else {
      delimiter_ = value.ptr()[0];
    }
  } else if (key.case_compare("quote") == 0) {
    if (value.length() < 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty quote", K(value));
    } else {
      quote_ = value.ptr()[0];
    }
  } else if (key.case_compare("escape") == 0) {
    if (value.length() < 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty escape", K(value));
    } else {
      escape_ = value.ptr()[0];
    }
  } else if (key.case_compare("nullstr") == 0 || key.case_compare("null_str") == 0
             || key.case_compare("null") == 0) {
    if (value.length() > MAX_NULL_STR_LEN) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("null string too long", K(value));
    } else {
      MEMCPY(null_str_, value.ptr(), value.length());
      null_str_len_ = value.length();
    }
  } else if (key.case_compare("empty_as_null") == 0) {
    bool bv = false;
    if (is_bool_text(value, bv)) {
      empty_as_null_ = bv;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid empty_as_null value", K(value));
    }
  } else if (key.case_compare("skip") == 0 || key.case_compare("skip_lines") == 0) {
    int64_t iv = 0;
    if (!parse_int64_strict(value, iv) || iv < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid skip option", K(value));
    } else {
      skip_lines_ = iv;
    }
  } else if (key.case_compare("format") == 0) {
    if (value.case_compare("csv") == 0) {
      format_ = OB_FILE_FORMAT_CSV;
    } else if (value.case_compare("json") == 0) {
      format_ = OB_FILE_FORMAT_JSON;
    } else if (value.case_compare("ndjson") == 0 || value.case_compare("jsonl") == 0
               || value.case_compare("newline_delimited") == 0) {
      format_ = OB_FILE_FORMAT_NDJSON;
    } else if (value.case_compare("parquet") == 0) {
      format_ = OB_FILE_FORMAT_PARQUET;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unknown file format", K(value));
    }
  } else if (key.case_compare("max_line_size") == 0) {
    int64_t iv = 0;
    if (!parse_int64_strict(value, iv) || iv <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid max_line_size", K(value));
    } else {
      max_line_size_ = iv;
    }
  } else if (key.case_compare("sample_size") == 0) {
    int64_t iv = 0;
    if (!parse_int64_strict(value, iv) || -1 != iv) {
      ret = OB_NOT_SUPPORTED;
      char detail[128];
      snprintf(detail, sizeof(detail), "sample_size %.*s (the whole file is always sampled, use -1)",
               value.length(), value.ptr());
      LOG_USER_ERROR(OB_NOT_SUPPORTED, detail);
      LOG_WARN("only sample_size = -1 is supported", K(value));
    }
  } else {
    recognized = false;
  }
  return ret;
}

int ObFileScanOptions::encode(ObIAllocator &alloc, ObString &out) const
{
  int ret = OB_SUCCESS;
  char body[1024];
  int64_t pos = 0;
  // Only the characters that would break the ';' / '=' encoding are escaped.
  const auto append_char = [&](char c) {
    if (';' == c || '=' == c || '\\' == c) {
      body[pos++] = '\\';
      body[pos++] = c;
    } else if ('\n' == c) {
      body[pos++] = '\\';
      body[pos++] = 'n';
    } else if ('\t' == c) {
      body[pos++] = '\\';
      body[pos++] = 't';
    } else if ('\r' == c) {
      body[pos++] = '\\';
      body[pos++] = 'r';
    } else if (' ' == c) {
      body[pos++] = '\\';
      body[pos++] = 's';
    } else {
      body[pos++] = c;
    }
  };
  const auto append_kv = [&](const char *key, const char *val, int64_t len) {
    const int64_t key_len = static_cast<int64_t>(strlen(key));
    MEMCPY(body + pos, key, key_len);
    pos += key_len;
    body[pos++] = '=';
    for (int64_t i = 0; i < len; ++i) {
      append_char(val[i]);
    }
    body[pos++] = ';';
  };
  char num_buf[32];
  snprintf(num_buf, sizeof(num_buf), "%d", format_);
  append_kv("f", num_buf, static_cast<int64_t>(strlen(num_buf)));
  append_kv("h", has_header_ ? "1" : "0", 1);
  append_kv("d", &delimiter_, 1);
  append_kv("q", &quote_, 1);
  append_kv("e", &escape_, 1);
  append_kv("n", empty_as_null_ ? "1" : "0", 1);
  snprintf(num_buf, sizeof(num_buf), "%ld", skip_lines_);
  append_kv("k", num_buf, static_cast<int64_t>(strlen(num_buf)));
  snprintf(num_buf, sizeof(num_buf), "%ld", max_line_size_);
  append_kv("l", num_buf, static_cast<int64_t>(strlen(num_buf)));
  snprintf(num_buf, sizeof(num_buf), "%ld", file_size_);
  append_kv("s", num_buf, static_cast<int64_t>(strlen(num_buf)));
  snprintf(num_buf, sizeof(num_buf), "%ld", file_mtime_);
  append_kv("m", num_buf, static_cast<int64_t>(strlen(num_buf)));
  if (null_str_len_ > 0) {
    append_kv("z", null_str_, null_str_len_);
  }
  char *buf = static_cast<char *>(alloc.alloc(pos));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(pos));
  } else {
    MEMCPY(buf, body, pos);
    out.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObFileScanOptions::decode(const ObString &str)
{
  int ret = OB_SUCCESS;
  char key[32];
  char val[256];
  int64_t pos = 0;
  while (OB_SUCC(ret) && pos < str.length()) {
    int64_t key_len = 0;
    int64_t val_len = 0;
    while (pos < str.length() && '=' != str.ptr()[pos] && ';' != str.ptr()[pos]) {
      if (key_len < static_cast<int64_t>(sizeof(key)) - 1) {
        key[key_len++] = str.ptr()[pos];
      }
      ++pos;
    }
    if (pos < str.length() && '=' == str.ptr()[pos]) {
      ++pos;
      while (pos < str.length() && ';' != str.ptr()[pos]) {
        char c = str.ptr()[pos];
        if ('\\' == c && pos + 1 < str.length()) {
          ++pos;
          c = str.ptr()[pos];
          if ('n' == c) {
            c = '\n';
          } else if ('t' == c) {
            c = '\t';
          } else if ('r' == c) {
            c = '\r';
          } else if ('s' == c) {
            c = ' ';
          }
        }
        if (val_len < static_cast<int64_t>(sizeof(val)) - 1) {
          val[val_len++] = c;
        }
        ++pos;
      }
    }
    if (pos < str.length() && ';' == str.ptr()[pos]) {
      ++pos;
    }
    if (key_len > 0) {
      ObString key_str(static_cast<int32_t>(key_len), key);
      ObString val_str(static_cast<int32_t>(val_len), val);
      bool recognized = false;
      int tmp_ret = OB_SUCCESS;
      // encode() writes single-letter keys; map them back to the option names.
      // The numeric ones (format/size/mtime) have no public option name.
      if (1 == key_str.length()) {
        switch (key_str.ptr()[0]) {
          case 'f': {
            int64_t iv = 0;
            if (parse_int64_strict(val_str, iv)) {
              format_ = static_cast<ObFileFormatType>(iv);
            } else {
              tmp_ret = OB_INVALID_ARGUMENT;
            }
            break;
          }
          case 's': {
            int64_t iv = 0;
            if (parse_int64_strict(val_str, iv)) {
              file_size_ = iv;
            } else {
              tmp_ret = OB_INVALID_ARGUMENT;
            }
            break;
          }
          case 'm': {
            int64_t iv = 0;
            if (parse_int64_strict(val_str, iv)) {
              file_mtime_ = iv;
            } else {
              tmp_ret = OB_INVALID_ARGUMENT;
            }
            break;
          }
          case 'h': tmp_ret = set_option(ObString("header"), val_str, recognized); break;
          case 'd': tmp_ret = set_option(ObString("delimiter"), val_str, recognized); break;
          case 'q': tmp_ret = set_option(ObString("quote"), val_str, recognized); break;
          case 'e': tmp_ret = set_option(ObString("escape"), val_str, recognized); break;
          case 'n': {
            // encode() writes 0/1 here; empty_as_null only accepts true/false
            int64_t iv = 0;
            if (parse_int64_strict(val_str, iv)) {
              empty_as_null_ = (0 != iv);
            } else {
              tmp_ret = OB_INVALID_ARGUMENT;
            }
            break;
          }
          case 'k': tmp_ret = set_option(ObString("skip_lines"), val_str, recognized); break;
          case 'l': tmp_ret = set_option(ObString("max_line_size"), val_str, recognized); break;
          case 'z': tmp_ret = set_option(ObString("nullstr"), val_str, recognized); break;
          default: recognized = false; break;
        }
      } else if (OB_SUCCESS != (tmp_ret = set_option(key_str, val_str, recognized))) {
      }
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("decode file option failed", K(tmp_ret), K(key_str));
      }
    }
  }
  return ret;
}

bool ObFileScanOptions::is_null_text(const ObString &field) const
{
  return null_str_len_ > 0 && field.length() == null_str_len_
         && 0 == MEMCMP(field.ptr(), null_str_, null_str_len_);
}

ObFileRecordReader::ObFileRecordReader()
  : fd_(-1),
    alloc_(NULL),
    path_(),
    buf_(NULL),
    buf_cap_(0),
    buf_pos_(0),
    buf_end_(0),
    eof_(false),
    record_no_(0),
    opt_(),
    tz_ctx_(NULL),
    expected_size_(-1),
    expected_mtime_(0),
    json_array_checked_(false),
    json_array_(false),
    json_array_done_(false)
{
}

int ObFileRecordReader::open(const ObString &path, const ObFileScanOptions &opt,
                             ObIAllocator &alloc, const ObTimeConvertCtx *tz_ctx)
{
  int ret = OB_SUCCESS;
  char *path_buf = NULL;
  opt_ = opt;
  alloc_ = &alloc;
  tz_ctx_ = tz_ctx;
  if (OB_ISNULL(path_buf = my_strndup(path.ptr(), path.length(), alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(path));
  } else if (FALSE_IT(path_ = ObString(static_cast<int32_t>(path.length()), path_buf))) {
  } else if (FALSE_IT(fd_ = fs_open(path_buf))) {
  } else if (fd_ < 0) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("open file failed", K(ret), K(path), "errno", errno);
  } else if (OB_ISNULL(buf_ = static_cast<char *>(alloc.alloc(
                         std::min<int64_t>(READ_CHUNK_SIZE, opt.max_line_size_ + 2) + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    buf_cap_ = std::min<int64_t>(READ_CHUNK_SIZE, opt.max_line_size_ + 2);
    buf_pos_ = 0;
    buf_end_ = 0;
    eof_ = false;
    record_no_ = 0;
    expected_size_ = opt.file_size_;
    expected_mtime_ = opt.file_mtime_;
    json_array_checked_ = false;
    json_array_ = false;
    json_array_done_ = false;
  }
  return ret;
}

void ObFileRecordReader::close()
{
  if (fd_ >= 0) {
    fs_close(fd_);
    fd_ = -1;
  }
}

int ObFileRecordReader::rescan()
{
  int ret = OB_SUCCESS;
  buf_pos_ = 0;
  buf_end_ = 0;
  eof_ = false;
  record_no_ = 0;
  json_array_checked_ = false;
  json_array_ = false;
  json_array_done_ = false;
#ifdef _WIN32
  if (fd_ >= 0 && _lseeki64(fd_, 0, SEEK_SET) < 0) {
    ret = OB_IO_ERROR;
  }
#else
  if (fd_ >= 0 && lseek(fd_, 0, SEEK_SET) < 0) {
    ret = OB_IO_ERROR;
  }
#endif
  return ret;
}

int ObFileRecordReader::check_file_unchanged()
{
  int ret = OB_SUCCESS;
  // Only the size is verified: a cached plan reuses the inferred schema, and
  // mtime changes (e.g. rewriting the same content) must not break the query.
  if (expected_size_ >= 0 && !path_.empty()) {
    int64_t size = 0;
    int64_t mtime = 0;
    if (OB_FAIL(ObFileScanUtils::stat_file(path_, size, mtime))) {
      LOG_WARN("stat file failed", K(ret), K(path_));
    } else if (size != expected_size_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("file changed after schema inference", K(ret), K(path_),
               K(size), K_(expected_size));
    }
  }
  return ret;
}

int ObFileRecordReader::read_more()
{
  int ret = OB_SUCCESS;
  if (eof_) {
    // nothing to do
  } else if (buf_pos_ > 0) {
    const int64_t remain = buf_end_ - buf_pos_;
    if (remain > 0) {
      MEMMOVE(buf_, buf_ + buf_pos_, remain);
    }
    buf_pos_ = 0;
    buf_end_ = remain;
  }
  if (OB_SUCC(ret) && !eof_) {
    const int64_t used = buf_end_;
    const int64_t cap_limit = opt_.max_line_size_ + 2;
    if (used >= buf_cap_) {
      const int64_t new_cap = std::min<int64_t>(buf_cap_ * 2, cap_limit);
      if (buf_cap_ >= cap_limit) {
        char detail[512];
        snprintf(detail, sizeof(detail),
                 "record %ld of %.*s exceeds max_line_size %ld",
                 static_cast<long>(record_no_ + 1), path_.length(), path_.ptr(),
                 static_cast<long>(opt_.max_line_size_));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, detail);
        // the client only shows the user message when it matches the error code
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("one record exceeds max_line_size", K(ret), K(buf_cap_));
      } else {
        char *new_buf = static_cast<char *>(alloc_->alloc(new_cap + 1));
        if (OB_ISNULL(new_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(new_cap));
        } else {
          MEMCPY(new_buf, buf_, buf_end_);
          buf_ = new_buf;
          buf_cap_ = new_cap;
        }
      }
    }
    if (OB_SUCC(ret)) {
      const int n = fs_read(fd_, buf_ + buf_end_, static_cast<int>(buf_cap_ - buf_end_));
      if (n < 0) {
        ret = OB_IO_ERROR;
        LOG_WARN("read file failed", K(ret), "errno", errno);
      } else if (0 == n) {
        eof_ = true;
      } else {
        buf_end_ += n;
      }
    }
  }
  return ret;
}

int ObFileRecordReader::scan_record_end(int64_t &record_end, bool &got_record)
{
  int ret = OB_SUCCESS;
  got_record = false;
  bool in_quote = false;
  // keep the scan cursor across read_more() calls: read_more() shifts the
  // unread bytes to the beginning of the buffer, so a rescan from buf_pos_
  // would lose the quote state of a record that spans two chunks.
  int64_t pos = buf_pos_;
  while (OB_SUCC(ret) && !got_record) {
    bool need_more = false;
    for (; pos < buf_end_; ++pos) {
      const char c = buf_[pos];
      if (in_quote) {
        if (opt_.escape_ == opt_.quote_) {
          if (c == opt_.quote_) {
            if (pos + 1 >= buf_end_) {
              if (!eof_) {
                need_more = true;
                break;
              }
              in_quote = false;
            } else if (buf_[pos + 1] == opt_.quote_) {
              ++pos;
            } else {
              in_quote = false;
            }
          }
        } else if (c == opt_.escape_) {
          ++pos;
        } else if (c == opt_.quote_) {
          in_quote = false;
        }
      } else if (c == opt_.quote_) {
        in_quote = true;
      } else if (c == '\n') {
        record_end = pos;
        got_record = true;
        break;
      }
    }
    if (OB_FAIL(ret) || got_record) {
    } else if (eof_) {
      if (in_quote) {
        char detail[512];
        snprintf(detail, sizeof(detail), "unterminated quoted field at record %ld of %.*s",
                 static_cast<long>(record_no_ + 1), path_.length(), path_.ptr());
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, detail);
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unterminated quoted field at end of file", K(record_no_));
      } else if (pos > buf_pos_) {
        record_end = pos;
        got_record = true;
      } else {
        // no data left: stop scanning so the caller sees end of file
      }
      break;
    } else if (need_more || pos >= buf_end_) {
      const int64_t shift = buf_pos_;
      if (OB_FAIL(read_more())) {
      } else {
        pos -= shift;
      }
    }
  }
  return ret;
}

int ObFileRecordReader::split_csv_record(const char *record_start, const int64_t record_len,
                                         ObIArray<ObString> &fields,
                                         ObIArray<bool> &is_null)
{
  int ret = OB_SUCCESS;
  char *base = const_cast<char *>(record_start);
  int64_t read_pos = 0;
  int64_t write_pos = 0;
  while (OB_SUCC(ret) && read_pos <= record_len) {
    const int64_t field_start = write_pos;
    bool quoted = false;
    bool closed = false;
    if (read_pos < record_len && base[read_pos] == opt_.quote_) {
      quoted = true;
      ++read_pos;
      while (read_pos < record_len) {
        const char c = base[read_pos];
        if (opt_.escape_ == opt_.quote_) {
          if (c == opt_.quote_) {
            if (read_pos + 1 < record_len && base[read_pos + 1] == opt_.quote_) {
              base[write_pos++] = opt_.quote_;
              read_pos += 2;
            } else {
              ++read_pos;
              closed = true;
              break;
            }
          } else {
            base[write_pos++] = c;
            ++read_pos;
          }
        } else if (c == opt_.escape_) {
          if (read_pos + 1 < record_len) {
            const char n = base[read_pos + 1];
            char out = n;
            if ('n' == n) {
              out = '\n';
            } else if ('t' == n) {
              out = '\t';
            } else if ('r' == n) {
              out = '\r';
            } else if ('0' == n) {
              out = '\0';
            }
            base[write_pos++] = out;
            read_pos += 2;
          } else {
            base[write_pos++] = c;
            ++read_pos;
          }
        } else if (c == opt_.quote_) {
          ++read_pos;
          closed = true;
          break;
        } else {
          base[write_pos++] = c;
          ++read_pos;
        }
      }
      if (!closed) {
        char detail[512];
        snprintf(detail, sizeof(detail), "unterminated quoted field at record %ld of %.*s",
                 static_cast<long>(record_no_), path_.length(), path_.ptr());
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, detail);
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unterminated quoted field", K(ret), K(record_no_));
      }
      // skip trailing characters (should be only delimiter or spaces)
      while (OB_SUCC(ret) && read_pos < record_len && base[read_pos] != opt_.delimiter_) {
        ++read_pos;
      }
    } else {
      while (read_pos < record_len && base[read_pos] != opt_.delimiter_) {
        base[write_pos++] = base[read_pos++];
      }
      // trim a trailing '\r' of the last field of a CRLF file
      if (read_pos == record_len && write_pos > field_start
          && '\r' == base[write_pos - 1]) {
        --write_pos;
      }
    }
    if (OB_SUCC(ret)) {
      ObString field(static_cast<int32_t>(write_pos - field_start), base + field_start);
      bool null_flag = false;
      if (quoted) {
        null_flag = opt_.is_null_text(field);
      } else {
        null_flag = (field.empty() && opt_.empty_as_null_) || opt_.is_null_text(field);
      }
      if (OB_FAIL(fields.push_back(field)) || OB_FAIL(is_null.push_back(null_flag))) {
        LOG_WARN("push field failed", K(ret));
      } else if (read_pos < record_len && base[read_pos] == opt_.delimiter_) {
        ++read_pos;
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObFileRecordReader::next_record(ObIArray<ObString> &fields, ObIArray<bool> &is_null)
{
  int ret = OB_SUCCESS;
  fields.reuse();
  is_null.reuse();
  bool got_record = false;
  while (OB_SUCC(ret) && !got_record) {
    int64_t record_end = 0;
    if (OB_FAIL(scan_record_end(record_end, got_record))) {
      LOG_WARN("scan record failed", K(ret));
    } else if (!got_record) {
      ret = OB_ITER_END;
    } else {
      const char *record_start = buf_ + buf_pos_;
      int64_t record_len = record_end - buf_pos_;
      if (record_len > 0 && '\r' == record_start[record_len - 1]) {
        --record_len;
      }
      buf_pos_ = record_end + 1;
      ++record_no_;
      if (record_no_ <= opt_.skip_lines_) {
        // skipped line: keep scanning for the next record
        got_record = false;
      } else if (OB_FAIL(split_csv_record(record_start, record_len, fields, is_null))) {
        LOG_WARN("split csv record failed", K(ret), K(record_no_));
      }
    }
  }
  return ret;
}

int ObFileRecordReader::detect_json_array()
{
  int ret = OB_SUCCESS;
  if (json_array_checked_) {
  } else {
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      while (buf_pos_ < buf_end_
             && (' ' == buf_[buf_pos_] || '\t' == buf_[buf_pos_]
                 || '\r' == buf_[buf_pos_] || '\n' == buf_[buf_pos_])) {
        ++buf_pos_;
      }
      if (buf_pos_ < buf_end_) {
        json_array_ = ('[' == buf_[buf_pos_]);
        if (json_array_) {
          ++buf_pos_;
        }
        json_array_checked_ = true;
        done = true;
      } else if (eof_) {
        json_array_checked_ = true;
        done = true;
      } else if (OB_FAIL(read_more())) {
        LOG_WARN("read more failed", K(ret));
      }
    }
  }
  return ret;
}

int ObFileRecordReader::next_json_array_line(ObString &line)
{
  int ret = OB_SUCCESS;
  line.reset();
  if (!is_opened()) {
    ret = OB_NOT_INIT;
  } else if (json_array_done_) {
    ret = OB_ITER_END;
  } else {
    bool found = false;
    while (OB_SUCC(ret) && !found) {
      while (buf_pos_ < buf_end_
             && (' ' == buf_[buf_pos_] || '\t' == buf_[buf_pos_]
                 || '\r' == buf_[buf_pos_] || '\n' == buf_[buf_pos_]
                 || ',' == buf_[buf_pos_])) {
        ++buf_pos_;
      }
      if (buf_pos_ >= buf_end_) {
        if (eof_) {
          char detail[512];
          snprintf(detail, sizeof(detail), "unterminated json array in %.*s",
                   path_.length(), path_.ptr());
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, detail);
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unterminated json array", K_(record_no));
        } else if (OB_FAIL(read_more())) {
          LOG_WARN("read more failed", K(ret));
        }
      } else if (']' == buf_[buf_pos_]) {
        ++buf_pos_;
        json_array_done_ = true;
        ret = OB_ITER_END;
      } else {
        int64_t start = buf_pos_;
        int64_t pos = buf_pos_;
        int64_t end = -1;
        int64_t depth = 0;
        bool in_str = false;
        bool esc = false;
        while (OB_SUCC(ret) && end < 0) {
          while (pos < buf_end_ && end < 0) {
            const char c = buf_[pos];
            if (in_str) {
              if (esc) {
                esc = false;
              } else if ('\\' == c) {
                esc = true;
              } else if ('"' == c) {
                in_str = false;
              }
            } else if ('"' == c) {
              in_str = true;
            } else if ('{' == c || '[' == c) {
              ++depth;
            } else if ('}' == c) {
              if (depth > 0) {
                --depth;
              }
            } else if (']' == c) {
              if (0 == depth) {
                end = pos;
              } else {
                --depth;
              }
            } else if (',' == c && 0 == depth) {
              end = pos;
            }
            if (end < 0) {
              ++pos;
            }
          }
          if (OB_FAIL(ret) || end >= 0) {
          } else if (eof_) {
            char detail[512];
            snprintf(detail, sizeof(detail), "unterminated json value at line %ld of %.*s",
                     static_cast<long>(record_no_ + 1), path_.length(), path_.ptr());
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, detail);
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("unterminated json value", K_(record_no));
          } else {
            const int64_t shift = buf_pos_;
            if (OB_FAIL(read_more())) {
              LOG_WARN("read more failed", K(ret));
            } else {
              start -= shift;
              pos -= shift;
            }
          }
        }
        if (OB_SUCC(ret)) {
          const int64_t element_end = end;
          while (end > start
                 && (' ' == buf_[end - 1] || '\t' == buf_[end - 1]
                     || '\r' == buf_[end - 1] || '\n' == buf_[end - 1])) {
            --end;
          }
          line.assign_ptr(buf_ + start, static_cast<int32_t>(end - start));
          if (']' == buf_[element_end]) {
            buf_pos_ = element_end;
            json_array_done_ = true;
          } else {
            buf_pos_ = element_end + 1;
          }
          ++record_no_;
          found = true;
        }
      }
    }
  }
  return ret;
}

int ObFileRecordReader::next_json_value_line(ObString &line)
{
  int ret = OB_SUCCESS;
  line.reset();
  if (!is_opened()) {
    ret = OB_NOT_INIT;
  } else {
    bool found = false;
    while (OB_SUCC(ret) && !found) {
      while (buf_pos_ < buf_end_
             && (' ' == buf_[buf_pos_] || '\t' == buf_[buf_pos_]
                 || '\r' == buf_[buf_pos_] || '\n' == buf_[buf_pos_])) {
        ++buf_pos_;
      }
      if (buf_pos_ >= buf_end_) {
        if (eof_) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(read_more())) {
          LOG_WARN("read more failed", K(ret));
        }
      } else if ('{' != buf_[buf_pos_]) {
        char detail[512];
        snprintf(detail, sizeof(detail), "invalid json object at line %ld of %.*s",
                 static_cast<long>(record_no_ + 1), path_.length(), path_.ptr());
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, detail);
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("json record does not start with an object", K_(record_no));
      } else {
        int64_t start = buf_pos_;
        int64_t pos = buf_pos_;
        int64_t end = -1;
        int64_t depth = 0;
        bool in_str = false;
        bool esc = false;
        while (OB_SUCC(ret) && end < 0) {
          while (pos < buf_end_ && end < 0) {
            const char c = buf_[pos];
            if (in_str) {
              if (esc) {
                esc = false;
              } else if ('\\' == c) {
                esc = true;
              } else if ('"' == c) {
                in_str = false;
              }
            } else if ('"' == c) {
              in_str = true;
            } else if ('{' == c || '[' == c) {
              ++depth;
            } else if ('}' == c) {
              if (depth > 0) {
                --depth;
                if (0 == depth) {
                  end = pos + 1;
                }
              }
            } else if (']' == c) {
              if (depth > 0) {
                --depth;
              }
            }
            if (end < 0) {
              ++pos;
            }
          }
          if (OB_FAIL(ret) || end >= 0) {
          } else if (eof_) {
            char detail[512];
            snprintf(detail, sizeof(detail), "unterminated json value at line %ld of %.*s",
                     static_cast<long>(record_no_ + 1), path_.length(), path_.ptr());
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, detail);
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("unterminated json value", K_(record_no));
          } else {
            const int64_t shift = buf_pos_;
            if (OB_FAIL(read_more())) {
              LOG_WARN("read more failed", K(ret));
            } else {
              start -= shift;
              pos -= shift;
            }
          }
        }
        if (OB_SUCC(ret)) {
          line.assign_ptr(buf_ + start, static_cast<int32_t>(end - start));
          buf_pos_ = end;
          ++record_no_;
          found = true;
        }
      }
    }
  }
  return ret;
}

int ObFileRecordReader::next_line(ObString &line)
{
  int ret = OB_SUCCESS;
  line.reset();
  if (!is_opened()) {
    ret = OB_NOT_INIT;
  } else if (OB_FILE_FORMAT_JSON == opt_.format_ && OB_FAIL(detect_json_array())) {
    LOG_WARN("detect json array failed", K(ret));
  } else if (json_array_) {
    ret = next_json_array_line(line);
  } else if (OB_FILE_FORMAT_JSON == opt_.format_) {
    ret = next_json_value_line(line);
  } else {
    bool found = false;
    while (OB_SUCC(ret) && !found) {
      for (int64_t i = buf_pos_; i < buf_end_; ++i) {
        if ('\n' == buf_[i]) {
          int64_t len = i - buf_pos_;
          const char *start = buf_ + buf_pos_;
          if (len > 0 && '\r' == start[len - 1]) {
            --len;
          }
          line.assign_ptr(const_cast<char *>(start), static_cast<int32_t>(len));
          buf_pos_ = i + 1;
          ++record_no_;
          found = true;
          break;
        }
      }
      if (OB_FAIL(ret) || found) {
      } else if (eof_) {
        if (buf_end_ > buf_pos_) {
          int64_t len = buf_end_ - buf_pos_;
          const char *start = buf_ + buf_pos_;
          if (len > 0 && '\r' == start[len - 1]) {
            --len;
          }
          line.assign_ptr(const_cast<char *>(start), static_cast<int32_t>(len));
          buf_pos_ = buf_end_;
          ++record_no_;
          found = true;
        } else {
          ret = OB_ITER_END;
        }
      } else if (OB_FAIL(read_more())) {
        LOG_WARN("read more failed", K(ret));
      }
    }
  }
  return ret;
}

int ObFileScanUtils::stat_file(const ObString &path, int64_t &size, int64_t &mtime)
{
  int ret = OB_SUCCESS;
  char buf[1024];
  if (path.empty() || path.length() >= static_cast<int32_t>(sizeof(buf))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(buf, path.ptr(), path.length());
    buf[path.length()] = '\0';
    struct stat st;
    if (0 != stat(buf, &st)) {
      ret = OB_FILE_NOT_EXIST;
    } else {
      size = static_cast<int64_t>(st.st_size);
      mtime = static_cast<int64_t>(st.st_mtime);
    }
  }
  return ret;
}

// Collapse "." and ".." components without touching the filesystem, the path is
// expected to be absolute. Used when realpath() fails because a component does
// not exist yet (a missing file or a glob pattern).
static int lexical_normalize_path(const char *path, char *out, const int64_t out_len)
{
  int ret = OB_SUCCESS;
  int64_t comps = 0;
  ObSEArray<const char *, 32> starts;
  ObSEArray<int64_t, 32> lengths;
  const char *p = path;
  while ('\0' != *p) {
    while ('/' == *p) { ++p; }
    const char *start = p;
    while ('\0' != *p && '/' != *p) { ++p; }
    const int64_t len = static_cast<int64_t>(p - start);
    if (0 == len) {
      // nothing
    } else if (0 == MEMCMP(start, ".", len)) {
      // nothing
    } else if (2 == len && 0 == MEMCMP(start, "..", len)) {
      if (comps > 0) {
        --comps;
        starts.pop_back();
        lengths.pop_back();
      }
    } else if (OB_FAIL(starts.push_back(start)) || OB_FAIL(lengths.push_back(len))) {
      // pass
    } else {
      ++comps;
    }
  }
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < comps; ++i) {
    const int64_t need = lengths.at(i) + 1;
    if (pos + need >= out_len) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      out[pos] = '/';
      MEMCPY(out + pos + 1, starts.at(i), lengths.at(i));
      pos += need;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 == pos) {
    out[0] = '/';
    out[1] = '\0';
  } else if (pos >= out_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    out[pos] = '\0';
  }
  return ret;
}

int ObFileScanUtils::canonicalize_path(ObIAllocator &alloc, const ObString &path,
                                       ObString &canonical)
{
  int ret = OB_SUCCESS;
  canonical.reset();
  char raw[MAX_PATH_LEN];
  if (path.empty() || path.length() >= static_cast<int32_t>(sizeof(raw))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file path length", K(ret), K(path));
  } else if (NULL != memchr(path.ptr(), '\0', path.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file path contains a terminator", K(ret));
  } else {
    MEMCPY(raw, path.ptr(), path.length());
    raw[path.length()] = '\0';
    char absolute[MAX_PATH_LEN];
    if ('/' == raw[0]) {
      MEMCPY(absolute, raw, path.length() + 1);
    } else {
      char cwd[MAX_PATH_LEN];
      if (NULL == getcwd(cwd, sizeof(cwd))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get cwd failed", K(ret), K(path));
      } else if (static_cast<int64_t>(strlen(cwd)) + path.length() + 2 >= static_cast<int64_t>(sizeof(absolute))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("file path is too long", K(ret), K(path));
      } else {
        snprintf(absolute, sizeof(absolute), "%s/%s", cwd, raw);
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      char resolved_local[MAX_PATH_LEN];
      char *out = resolved_local;
      bool normalized = false;
      const int64_t out_cap = static_cast<int64_t>(sizeof(resolved_local));
#ifdef _WIN32
      // _fullpath normalizes lexically and never fails on a missing component.
      if (NULL != _fullpath(out, absolute, out_cap)) {
        normalized = true;
      }
#else
      char *last_sep = strrchr(absolute, '/');
      if (NULL == last_sep) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("absolute file path has no separator", K(ret), K(path));
      } else {
        char parent[MAX_PATH_LEN];
        const int64_t parent_len = static_cast<int64_t>(last_sep - absolute);
        if (0 == parent_len) {
          // the parent is the root
          MEMCPY(parent, "/", 2);
        } else if (parent_len >= static_cast<int64_t>(sizeof(parent))) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          MEMCPY(parent, absolute, parent_len);
          parent[parent_len] = '\0';
        }
        if (OB_FAIL(ret)) {
        } else if (NULL != realpath(parent, out)) {
          const char *name = last_sep + 1;
          if ('\0' == name[0]) {
            // a directory path, the resolved parent is the answer
          } else if (static_cast<int64_t>(strlen(out)) + static_cast<int64_t>(strlen(name)) + 2 >= out_cap) {
            ret = OB_INVALID_ARGUMENT;
          } else {
            char joined[MAX_PATH_LEN];
            snprintf(joined, sizeof(joined), "%s/%s", out, name);
            MEMCPY(out, joined, strlen(joined) + 1);
          }
          normalized = OB_SUCC(ret);
        } else if (OB_SUCCESS == lexical_normalize_path(absolute, out, out_cap)) {
          normalized = true;
        }
      }
#endif
      if (OB_FAIL(ret)) {
      } else if (!normalized) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("canonicalize file path failed", K(ret), K(path));
      } else {
        const ObString resolved_str(static_cast<int32_t>(strlen(out)), out);
        if (OB_FAIL(ob_write_string(alloc, resolved_str, canonical, true))) {
          LOG_WARN("copy canonical path failed", K(ret), K(resolved_str));
        }
      }
    }
  }
  return ret;
}

bool ObFileScanUtils::has_glob_wildcard(const ObString &pattern)
{
  bool has = false;
  for (int64_t i = 0; i < pattern.length() && !has; ++i) {
    const char c = pattern.ptr()[i];
    has = ('*' == c || '?' == c || '[' == c);
  }
  return has;
}

#ifdef _WIN32
int ObFileScanUtils::glob_paths(ObIAllocator &alloc, const ObString &pattern,
                                ObIArray<ObString> &paths)
{
  UNUSED(alloc);
  UNUSED(pattern);
  UNUSED(paths);
  return OB_NOT_SUPPORTED;
}
#else
static bool match_name(const char *name, const char *pat, const int64_t pat_len)
{
  // Simple shell wildcard match: '*' '?' only, no character classes.
  int64_t ni = 0;
  int64_t name_len = static_cast<int64_t>(strlen(name));
  int64_t pi = 0;
  int64_t star_pi = -1;
  int64_t star_ni = -1;
  while (ni < name_len) {
    if (pi < pat_len && (pat[pi] == '?' || pat[pi] == name[ni])) {
      ++pi;
      ++ni;
    } else if (pi < pat_len && pat[pi] == '*') {
      star_pi = pi++;
      star_ni = ni;
    } else if (star_pi >= 0) {
      pi = star_pi + 1;
      ni = ++star_ni;
    } else {
      return false;
    }
  }
  while (pi < pat_len && pat[pi] == '*') {
    ++pi;
  }
  return pi == pat_len;
}

int ObFileScanUtils::glob_paths(ObIAllocator &alloc, const ObString &pattern,
                                ObIArray<ObString> &paths)
{
  int ret = OB_SUCCESS;
  char *pat = NULL;
  if (OB_ISNULL(pat = my_strndup(pattern.ptr(), pattern.length(), alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    int64_t slash = -1;
    for (int64_t i = pattern.length() - 1; i >= 0; --i) {
      if ('/' == pat[i]) {
        slash = i;
        break;
      }
    }
    const char *dir_path = NULL;
    const char *name_pat = NULL;
    int64_t name_pat_len = 0;
    if (slash < 0) {
      dir_path = ".";
      name_pat = pat;
      name_pat_len = pattern.length();
    } else {
      pat[slash] = '\0';
      dir_path = slash == 0 ? "/" : pat;
      name_pat = pat + slash + 1;
      name_pat_len = pattern.length() - slash - 1;
    }
    DIR *dir = opendir(dir_path);
    if (OB_ISNULL(dir)) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("open dir failed", K(ret), K(dir_path), "errno", errno);
    } else {
      ObSEArray<ObString, 16> local_paths;
      struct dirent *entry = NULL;
      while (OB_NOT_NULL(entry = readdir(dir))) {
        if (0 == strcmp(entry->d_name, ".") || 0 == strcmp(entry->d_name, "..")) {
          continue;
        }
        if (match_name(entry->d_name, name_pat, name_pat_len)) {
          const int64_t dir_len = static_cast<int64_t>(strlen(dir_path));
          const int64_t name_len = static_cast<int64_t>(strlen(entry->d_name));
          int64_t full_len = dir_len + 1 + name_len;
          char *full = static_cast<char *>(alloc.alloc(full_len + 1));
          if (OB_ISNULL(full)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            break;
          }
          MEMCPY(full, dir_path, dir_len);
          if (!(1 == dir_len && '/' == dir_path[0])) {
            full[dir_len] = '/';
            MEMCPY(full + dir_len + 1, entry->d_name, name_len);
          } else {
            MEMCPY(full + dir_len, entry->d_name, name_len);
            --full_len;
          }
          full[full_len] = '\0';
          ObString s(static_cast<int32_t>(full_len), full);
          if (OB_FAIL(local_paths.push_back(s))) {
            break;
          }
        }
      }
      closedir(dir);
      // sort for a deterministic output order
      for (int64_t i = 0; OB_SUCC(ret) && i < local_paths.count(); ++i) {
        int64_t min_idx = i;
        for (int64_t j = i + 1; j < local_paths.count(); ++j) {
          if (local_paths.at(j).compare(local_paths.at(min_idx)) < 0) {
            min_idx = j;
          }
        }
        if (min_idx != i) {
          ObString tmp = local_paths.at(i);
          local_paths.at(i) = local_paths.at(min_idx);
          local_paths.at(min_idx) = tmp;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(paths.assign(local_paths))) {
        LOG_WARN("assign paths failed", K(ret));
      }
    }
  }
  return ret;
}
#endif

ObFileValueKind ObFileScanUtils::classify(const ObString &text,
                                          const ObTimeConvertCtx *tz_ctx,
                                          const bool empty_as_null)
{
  UNUSED(tz_ctx);
  ObFileValueKind kind = FVK_STR;
  bool bv = false;
  int64_t iv = 0;
  double dv = 0;
  int32_t date_val = 0;
  int64_t dt_int = 0;
  int64_t dt_dec = 0;
  int16_t dt_scale = 0;
  bool only_date = false;
  if (text.empty()) {
    kind = empty_as_null ? FVK_NULL : FVK_STR;
  } else if (is_bool_text(text, bv)) {
    kind = FVK_BOOL;
  } else if (parse_int64_strict(text, iv)) {
    kind = FVK_INT;
  } else if (parse_double_strict(text, dv)) {
    kind = FVK_DOUBLE;
  } else if (parse_date_strict(text, date_val)) {
    kind = FVK_DATE;
  } else if (parse_datetime_strict(text, dt_int, dt_dec, dt_scale, only_date)) {
    kind = only_date ? FVK_DATE : FVK_DATETIME;
  } else {
    kind = FVK_STR;
  }
  return kind;
}

ObObjType ObFileScanUtils::merge_type(ObObjType cur, const ObFileValueKind kind)
{
  ObObjType ret = cur;
  const bool cur_null = (ObNullType == cur);
  if (FVK_NULL == kind) {
    // nothing
  } else if (cur_null) {
    switch (kind) {
      case FVK_BOOL: ret = ObTinyIntType; break;
      case FVK_INT: ret = ObIntType; break;
      case FVK_DOUBLE: ret = ObDoubleType; break;
      case FVK_DATE: ret = ObDateType; break;
      case FVK_DATETIME: ret = ObDateTimeType; break;
      default: ret = ObVarcharType; break;
    }
  } else if (ObVarcharType == cur || FVK_STR == kind) {
    ret = ObVarcharType;
  } else if (FVK_DATE == kind || FVK_DATETIME == kind
             || ObDateType == cur || ObDateTimeType == cur) {
    const bool cur_is_date = (ObDateType == cur || ObDateTimeType == cur);
    const bool kind_is_date = (FVK_DATE == kind || FVK_DATETIME == kind);
    if (cur_is_date && kind_is_date) {
      // date and datetime widen to datetime, whatever the order they appear in
      ret = (ObDateTimeType == cur || FVK_DATETIME == kind) ? ObDateTimeType : ObDateType;
    } else {
      // a date/datetime mixed with a numeric or bool value falls back to text
      ret = ObVarcharType;
    }
  } else {
    // numeric family
    if (FVK_DOUBLE == kind || ObDoubleType == cur) {
      ret = ObDoubleType;
    } else if (FVK_INT == kind || ObIntType == cur) {
      ret = ObIntType;
    } else {
      ret = ObTinyIntType;
    }
  }
  return ret;
}

int ObFileScanUtils::text_to_datum(const ObString &text, ObObjType type,
                                   ObIAllocator &alloc,
                                   const ObTimeConvertCtx *tz_ctx,
                                   ObDatum &datum)
{
  int ret = OB_SUCCESS;
  UNUSED(tz_ctx);
  if (text.empty()) {
    datum.set_null();
  } else {
    switch (type) {
      case ObIntType: {
        int64_t v = 0;
        if (!parse_int64_strict(text, v)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid int value", K(ret), K(text));
        } else {
          datum.set_int(v);
        }
        break;
      }
      case ObDoubleType: {
        double v = 0;
        if (!parse_double_strict(text, v)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid double value", K(ret), K(text));
        } else {
          datum.set_double(v);
        }
        break;
      }
      case ObTinyIntType: {
        bool bv = false;
        int64_t iv = 0;
        if (is_bool_text(text, bv)) {
          datum.set_bool(bv);
        } else if (parse_int64_strict(text, iv)) {
          datum.set_int(iv);
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid bool value", K(ret), K(text));
        }
        break;
      }
      case ObDateType: {
        int32_t date_val = 0;
        if (!parse_date_strict(text, date_val)) {
          int64_t dt_int = 0;
          int64_t dt_dec = 0;
          int16_t dt_scale = 0;
          bool only_date = false;
          if (parse_datetime_strict(text, dt_int, dt_dec, dt_scale, only_date) && only_date) {
            date_val = static_cast<int32_t>(dt_int / 1000000);
          }
        }
        if (0 == date_val) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid date value", K(ret), K(text));
        } else {
          int32_t value = 0;
          if (OB_FAIL(ObTimeConverter::int_to_date(static_cast<int64_t>(date_val), value, 0))) {
            LOG_WARN("int to date failed", K(ret), K(text));
          } else {
            datum.set_date(value);
          }
        }
        break;
      }
      case ObDateTimeType: {
        int64_t dt_int = 0;
        int64_t dt_dec = 0;
        int16_t dt_scale = 0;
        bool only_date = false;
        const bool parsed = parse_datetime_strict(text, dt_int, dt_dec, dt_scale, only_date);
        if (!parsed) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid datetime value", K(ret), K(text));
        } else {
          int64_t value = 0;
          ObTimeConvertCtx cvrt_ctx(NULL, false);
          ObDateSqlMode sql_mode;
          if (only_date) {
            dt_int *= 1000000L;
          }
          if (OB_FAIL(ObTimeConverter::int_to_datetime(dt_int, dt_dec, cvrt_ctx, value, sql_mode))) {
            LOG_WARN("int to datetime failed", K(ret), K(text));
          } else {
            datum.set_datetime(value);
          }
        }
        break;
      }
      case ObVarcharType:
      case ObLongTextType:
      case ObMediumTextType:
      case ObTinyTextType:
      case ObTextType:
      case ObCharType: {
        char *buf = static_cast<char *>(alloc.alloc(text.length()));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          if (text.length() > 0) {
            MEMCPY(buf, text.ptr(), text.length());
          }
          datum.set_string(buf, text.length());
        }
        break;
      }
      default: {
        // fall back to text
        char *buf = static_cast<char *>(alloc.alloc(text.length()));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          if (text.length() > 0) {
            MEMCPY(buf, text.ptr(), text.length());
          }
          datum.set_string(buf, text.length());
        }
        break;
      }
    }
  }
  return ret;
}

namespace
{
// Parse a flat JSON object into key/value pairs. Nested objects and arrays
// are rejected: this reader only supports flat records.
int parse_flat_json_object(const ObString &line,
                           ObIArray<ObString> &keys,
                           ObIArray<ObString> &values,
                           ObIArray<bool> &is_null,
                           ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  const char *p = line.ptr();
  int64_t i = 0;
  const int64_t len = line.length();
  while (i < len && is_space(p[i])) {
    ++i;
  }
  if (i >= len || '{' != p[i]) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json record must be an object", K(ret), K(line));
  } else {
    ++i;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      while (i < len && is_space(p[i])) {
        ++i;
      }
      if (i < len && '}' == p[i]) {
        done = true;
        ++i;
      } else if (i < len && '"' == p[i]) {
        // key
        const int64_t key_start = ++i;
        bool closed = false;
        while (i < len) {
          if ('\\' == p[i]) {
            i += 2;
          } else if ('"' == p[i]) {
            closed = true;
            break;
          } else {
            ++i;
          }
        }
        if (!closed) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unterminated json key", K(ret));
          break;
        }
        char *key = my_strndup(p + key_start, i - key_start, alloc);
        if (OB_ISNULL(key)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
        ++i;  // skip closing quote
        while (i < len && is_space(p[i])) {
          ++i;
        }
        if (i >= len || ':' != p[i]) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("json key without value", K(ret));
          break;
        }
        ++i;
        while (i < len && is_space(p[i])) {
          ++i;
        }
        bool value_is_null = false;
        char *value = NULL;
        int64_t value_len = 0;
        if (i < len && '"' == p[i]) {
          // string value with unescaping
          ++i;
          char *buf = static_cast<char *>(alloc.alloc(len - i + 1));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            break;
          }
          int64_t out = 0;
          bool closed = false;
          while (i < len) {
            const char c = p[i];
            if ('\\' == c && i + 1 < len) {
              const char n = p[i + 1];
              char o = n;
              if ('n' == n) {
                o = '\n';
              } else if ('t' == n) {
                o = '\t';
              } else if ('r' == n) {
                o = '\r';
              } else if ('b' == n) {
                o = '\b';
              } else if ('f' == n) {
                o = '\f';
              } else if ('u' == n && i + 5 < len) {
                int code = 0;
                bool hex_ok = true;
                for (int k = 0; k < 4 && hex_ok; ++k) {
                  const char h = p[i + 2 + k];
                  int d = -1;
                  if (h >= '0' && h <= '9') {
                    d = h - '0';
                  } else if (h >= 'a' && h <= 'f') {
                    d = h - 'a' + 10;
                  } else if (h >= 'A' && h <= 'F') {
                    d = h - 'A' + 10;
                  } else {
                    hex_ok = false;
                  }
                  code = code * 16 + d;
                }
                if (hex_ok) {
                  char utf8[4];
                  int utf8_len = 0;
                  if (code < 0x80) {
                    utf8[utf8_len++] = static_cast<char>(code);
                  } else if (code < 0x800) {
                    utf8[utf8_len++] = static_cast<char>(0xC0 | (code >> 6));
                    utf8[utf8_len++] = static_cast<char>(0x80 | (code & 0x3F));
                  } else {
                    utf8[utf8_len++] = static_cast<char>(0xE0 | (code >> 12));
                    utf8[utf8_len++] = static_cast<char>(0x80 | ((code >> 6) & 0x3F));
                    utf8[utf8_len++] = static_cast<char>(0x80 | (code & 0x3F));
                  }
                  MEMCPY(buf + out, utf8, utf8_len);
                  out += utf8_len;
                  i += 6;
                  continue;
                }
              }
              buf[out++] = o;
              i += 2;
              continue;
            } else if ('"' == c) {
              closed = true;
              ++i;
              break;
            } else {
              buf[out++] = c;
              ++i;
            }
          }
          if (!closed) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("unterminated json string", K(ret));
            break;
          }
          value = buf;
          value_len = out;
        } else if (i < len && ('{' == p[i] || '[' == p[i])) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("nested json value is not supported", K(ret));
          break;
        } else {
          // number / true / false / null
          const int64_t start = i;
          while (i < len && ',' != p[i] && '}' != p[i] && !is_space(p[i])) {
            ++i;
          }
          if (i == start) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("empty json value", K(ret));
            break;
          }
          if (0 == MEMCMP(p + start, "null", 4) && i - start == 4) {
            value_is_null = true;
          }
          if (OB_ISNULL(value = my_strndup(p + start, i - start, alloc))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            break;
          }
          value_len = i - start;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(keys.push_back(ObString(static_cast<int32_t>(strlen(key)), key)))
              || OB_FAIL(values.push_back(ObString(static_cast<int32_t>(value_len), value)))
              || OB_FAIL(is_null.push_back(value_is_null))) {
            LOG_WARN("push json field failed", K(ret));
          }
        }
        while (i < len && is_space(p[i])) {
          ++i;
        }
        if (OB_SUCC(ret) && i < len && ',' == p[i]) {
          ++i;
        } else if (OB_SUCC(ret) && i < len && '}' == p[i]) {
          done = true;
          ++i;
        } else if (OB_SUCC(ret) && !done) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("bad json object separator", K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("json record must be a flat object", K(ret));
      }
    }
  }
  return ret;
}
} // namespace

int ObFileScanUtils::parse_json_record(const ObString &line,
                                       ObIArray<ObString> &keys,
                                       ObIArray<ObString> &values,
                                       ObIArray<bool> &is_null,
                                       ObIAllocator &alloc)
{
  return parse_flat_json_object(line, keys, values, is_null, alloc);
}

int ObFileSchemaInferer::infer(ObIAllocator &alloc,
                               const ObString &path,
                               const ObFileScanOptions &opt,
                               const ObTimeConvertCtx *tz_ctx,
                               ObIArray<ObFileColumnMeta> &cols)
{
  int ret = OB_SUCCESS;
  ObFileRecordReader reader;
  ObSEArray<ObString, 64> names;
  ObSEArray<ObObjType, 64> types;
  ObSEArray<int64_t, 64> max_lens;
  ObSEArray<bool, 64> nullables;
  ObSEArray<ObString, 32> fields;
  ObSEArray<bool, 32> nulls;
  if (OB_FILE_FORMAT_PARQUET == opt.format_) {
    // the schema of a parquet file is stored in its footer and is returned as is
    if (OB_FAIL(ObParquetReader::read_schema(alloc, path, opt, cols))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cols.count(); ++i) {
        static_cast<ObFileColumnMeta &>(cols.at(i)).nullable_ = true;
      }
    }
    return ret;
  }
  if (OB_FAIL(reader.open(path, opt, alloc, tz_ctx))) {
    LOG_WARN("open file failed", K(ret), K(path));
  } else if (OB_FILE_FORMAT_CSV == opt.format_) {
    int64_t line = 0;
    while (OB_SUCC(ret)) {
      int tmp_ret = reader.next_record(fields, nulls);
      if (OB_ITER_END == tmp_ret) {
        break;
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("read csv record failed", K(ret), K(reader.record_no()));
      } else if (line == 0 && opt.has_header_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
          char nb[32];
          const ObString &f = fields.at(i);
          ObString name;
          if (f.empty()) {
            snprintf(nb, sizeof(nb), "c%ld", i + 1);
            if (OB_FAIL(ob_write_string(alloc, ObString(static_cast<int32_t>(strlen(nb)), nb), name))) {
            }
          } else if (OB_FAIL(ob_write_string(alloc, f, name))) {
          }
          if (OB_SUCC(ret) && OB_FAIL(names.push_back(name))) {
          }
        }
        // DuckDB style duplicate header naming: id, id__2, id__3, ...
        for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
          const ObString base = names.at(i);
          bool duplicated = false;
          for (int64_t j = 0; !duplicated && j < i; ++j) {
            duplicated = ObCharset::case_insensitive_equal(names.at(j), base);
          }
          if (duplicated) {
            bool renamed = false;
            for (int64_t seq = 2; OB_SUCC(ret) && !renamed; ++seq) {
              char suffix[32];
              const int64_t suffix_len = snprintf(suffix, sizeof(suffix), "__%ld", static_cast<long>(seq));
              char *buf = static_cast<char *>(alloc.alloc(base.length() + suffix_len));
              if (OB_ISNULL(buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                MEMCPY(buf, base.ptr(), base.length());
                MEMCPY(buf + base.length(), suffix, suffix_len);
                const ObString candidate(static_cast<int32_t>(base.length() + suffix_len), buf);
                bool taken = false;
                for (int64_t j = 0; !taken && j < i; ++j) {
                  taken = ObCharset::case_insensitive_equal(names.at(j), candidate);
                }
                if (!taken) {
                  names.at(i) = candidate;
                  renamed = true;
                }
              }
            }
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
          if (OB_FAIL(types.push_back(ObNullType)) || OB_FAIL(max_lens.push_back(0))
              || OB_FAIL(nullables.push_back(false))) {
          }
        }
      } else {
        if (names.empty()) {
          for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
            char nb[32];
            snprintf(nb, sizeof(nb), "c%ld", i + 1);
            ObString name;
            if (OB_FAIL(ob_write_string(alloc, ObString(static_cast<int32_t>(strlen(nb)), nb), name))) {
            } else if (OB_FAIL(names.push_back(name))
                       || OB_FAIL(types.push_back(ObNullType))
                       || OB_FAIL(max_lens.push_back(0))
                       || OB_FAIL(nullables.push_back(false))) {
            }
          }
        }
        if (OB_SUCC(ret) && fields.count() != names.count()) {
          char detail[512];
          snprintf(detail, sizeof(detail),
                   "csv record %ld of %.*s has %ld fields, expected %ld",
                   static_cast<long>(reader.record_no()), path.length(), path.ptr(),
                   static_cast<long>(fields.count()), static_cast<long>(names.count()));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, detail);
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("csv field count mismatch", K(ret), K(fields.count()), K(names.count()),
                   K(reader.record_no()));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
          const ObString &f = fields.at(i);
          if (nulls.at(i)) {
            nullables.at(i) = true;
          } else {
            const ObFileValueKind kind = ObFileScanUtils::classify(f, tz_ctx, opt.empty_as_null_);
            types.at(i) = ObFileScanUtils::merge_type(types.at(i), kind);
            if (f.length() > max_lens.at(i)) {
              max_lens.at(i) = f.length();
            }
          }
        }
      }
      ++line;
    }
  } else if (OB_FILE_FORMAT_JSON == opt.format_ || OB_FILE_FORMAT_NDJSON == opt.format_) {
    ObSEArray<ObString, 32> keys;
    ObSEArray<ObString, 32> values;
    ObSEArray<bool, 32> value_nulls;
    while (OB_SUCC(ret)) {
      ObString line;
      const int tmp_ret = reader.next_line(line);
      if (OB_ITER_END == tmp_ret) {
        break;
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("read json line failed", K(ret));
      } else if (line.empty()) {
        continue;
      } else {
        keys.reuse();
        values.reuse();
        value_nulls.reuse();
        if (OB_FAIL(parse_flat_json_object(line, keys, values, value_nulls, alloc))) {
          char detail[512];
          if (OB_NOT_SUPPORTED == ret) {
            snprintf(detail, sizeof(detail), "nested json value at line %ld of %.*s",
                     static_cast<long>(reader.record_no()), path.length(), path.ptr());
            LOG_USER_ERROR(OB_NOT_SUPPORTED, detail);
          } else {
            snprintf(detail, sizeof(detail), "invalid json object at line %ld of %.*s",
                     static_cast<long>(reader.record_no()), path.length(), path.ptr());
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, detail);
          }
          LOG_WARN("parse json record failed", K(ret), K(reader.record_no()));
        }
        for (int64_t k = 0; OB_SUCC(ret) && k < keys.count(); ++k) {
          int64_t idx = -1;
          for (int64_t i = 0; i < names.count(); ++i) {
            if (names.at(i).length() == keys.at(k).length()
                && 0 == MEMCMP(names.at(i).ptr(), keys.at(k).ptr(), keys.at(k).length())) {
              idx = i;
              break;
            }
          }
          if (idx < 0) {
            char *copy = static_cast<char *>(alloc.alloc(keys.at(k).length()));
            if (OB_ISNULL(copy)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              break;
            }
            MEMCPY(copy, keys.at(k).ptr(), keys.at(k).length());
            idx = names.count();
            if (OB_FAIL(names.push_back(ObString(keys.at(k).length(), copy)))
                || OB_FAIL(types.push_back(ObNullType))
                || OB_FAIL(max_lens.push_back(0))
                || OB_FAIL(nullables.push_back(false))) {
              break;
            }
          }
          if (OB_SUCC(ret) && value_nulls.at(k)) {
            nullables.at(idx) = true;
          } else if (OB_SUCC(ret) && !value_nulls.at(k)) {
            const ObString &v = values.at(k);
            const ObFileValueKind kind = ObFileScanUtils::classify(v, tz_ctx, opt.empty_as_null_);
            types.at(idx) = ObFileScanUtils::merge_type(types.at(idx), kind);
            if (v.length() > max_lens.at(idx)) {
              max_lens.at(idx) = v.length();
            }
          }
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("file format not supported yet", K(ret), K(opt.format_));
  }
  if (OB_SUCC(ret) && names.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no column found in file", K(ret), K(path));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
    ObFileColumnMeta meta;
    meta.name_ = names.at(i);
    meta.type_ = types.at(i);
    // a column that is NULL in every row is typed as VARCHAR
    if (ObNullType == meta.type_) {
      meta.type_ = ObVarcharType;
    }
    meta.max_len_ = max_lens.at(i);
    // a column that is NULL in every row is typed as VARCHAR and is nullable
    meta.nullable_ = (ObNullType == types.at(i)) || nullables.at(i);
    if (OB_FAIL(cols.push_back(meta))) {
      LOG_WARN("push column failed", K(ret));
    }
  }
  reader.close();
  return ret;
}

} // namespace sql
} // namespace oceanbase
