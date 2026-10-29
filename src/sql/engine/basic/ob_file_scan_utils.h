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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SCAN_UTILS_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SCAN_UTILS_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "common/object/ob_object.h"
#include "common/datum/ob_datum.h"

namespace oceanbase
{
namespace common
{
struct ObTimeConvertCtx;
}
namespace sql
{

enum ObFileFormatType
{
  OB_FILE_FORMAT_CSV = 0,
  OB_FILE_FORMAT_JSON = 1,
  OB_FILE_FORMAT_NDJSON = 2,
  OB_FILE_FORMAT_PARQUET = 3,
  OB_FILE_FORMAT_GLOB = 4,
};

// Options of a file scan table function. The options are carried from the
// resolver (where the file schema is inferred) to the executor through one
// encoded string, so only the encoding lives here.
class ObFileScanOptions
{
public:
  static const int64_t MAX_NULL_STR_LEN = 32;
  static const int64_t DEFAULT_MAX_LINE_SIZE = 1024 * 1024;
  static const int64_t MAX_FILE_SIZE = 8LL * 1024 * 1024 * 1024;

  ObFileScanOptions() { reset(); }
  void reset();
  // Set one option. recognized is false when the option name is unknown.
  int set_option(const ObString &key, const ObString &value, bool &recognized);
  // Decode/encode options. The encoding is a ';' separated key=value list
  // with C-style escapes for the characters that matter.
  int encode(common::ObIAllocator &alloc, common::ObString &out) const;
  int decode(const common::ObString &str);
  common::ObString null_str() const
  { return common::ObString(static_cast<int32_t>(null_str_len_), null_str_); }
  bool is_null_text(const common::ObString &field) const;
  TO_STRING_KV(K_(format), K_(has_header), K_(delimiter), K_(quote), K_(escape),
               K_(empty_as_null), K_(skip_lines), "null_str", null_str(),
               K_(file_size), K_(file_mtime));

public:
  int32_t format_;      // ObFileFormatType
  bool has_header_;
  char delimiter_;
  char quote_;
  char escape_;
  bool empty_as_null_;
  int64_t skip_lines_;
  int64_t max_line_size_;
  int64_t file_size_;   // recorded at resolve time to detect file changes
  int64_t file_mtime_;
  char null_str_[MAX_NULL_STR_LEN];
  int64_t null_str_len_;
};

enum ObFileValueKind
{
  FVK_NULL = 0,
  FVK_BOOL,
  FVK_INT,
  FVK_DOUBLE,
  FVK_DATE,
  FVK_DATETIME,
  FVK_STR,
  FVK_MAX,
};

struct ObFileColumnMeta
{
  ObFileColumnMeta() : name_(), type_(ObNullType), max_len_(0), nullable_(false) {}
  common::ObString name_;
  ObObjType type_;
  int64_t max_len_;
  // true when any record holds a NULL (or empty) value for the column
  bool nullable_;
  TO_STRING_KV(K_(name), K_(type), K_(max_len), K_(nullable));
};

// Streaming reader for local files. At most one record (bounded by
// max_line_size_) is buffered, the whole file is never materialized.
class ObFileRecordReader
{
public:
  ObFileRecordReader();
  ~ObFileRecordReader() { close(); }

  int open(const common::ObString &path, const ObFileScanOptions &opt,
           common::ObIAllocator &alloc, const common::ObTimeConvertCtx *tz_ctx);
  void close();
  int rescan();
  bool is_opened() const { return fd_ >= 0; }
  int64_t record_no() const { return record_no_; }

  // CSV/NDJSON: one record, already split into fields. Quoted CSV fields are
  // unescaped in place; pointers stay valid until the next call.
  int next_record(common::ObIArray<common::ObString> &fields,
                  common::ObIArray<bool> &is_null);
  // JSON: one raw line (json object text), used by the JSON reader.
  int next_line(common::ObString &line);
  int check_file_unchanged();

private:
  int read_more();
  int ensure_record();
  int split_csv_record(const char *record_start, const int64_t record_len,
                       common::ObIArray<common::ObString> &fields,
                       common::ObIArray<bool> &is_null);
  int scan_record_end(int64_t &record_end, bool &got_record);
  // read_json accepts both one object per line and a single array of objects
  int detect_json_array();
  int next_json_array_line(common::ObString &line);
  // a json file may also be a pretty printed object or hold several objects
  int next_json_value_line(common::ObString &line);

private:
  int fd_;
  common::ObIAllocator *alloc_;
  common::ObString path_;
  char *buf_;
  int64_t buf_cap_;
  int64_t buf_pos_;
  int64_t buf_end_;
  bool eof_;
  int64_t record_no_;
  ObFileScanOptions opt_;
  const common::ObTimeConvertCtx *tz_ctx_;
  int64_t expected_size_;
  int64_t expected_mtime_;
  bool json_array_checked_;
  bool json_array_;
  bool json_array_done_;
};

// Full file schema inference. sample_size is always the whole file.
class ObFileSchemaInferer
{
public:
  // Extract the records of one JSON object: flat objects only.
  static int infer(common::ObIAllocator &alloc,
                   const common::ObString &path,
                   const ObFileScanOptions &opt,
                   const common::ObTimeConvertCtx *tz_ctx,
                   common::ObIArray<ObFileColumnMeta> &cols);
};

class ObFileScanUtils
{
public:
  static const int64_t MAX_PATH_LEN = 4096;
  // Classify a text value into the deterministic type lattice.
  static ObFileValueKind classify(const common::ObString &text,
                                  const common::ObTimeConvertCtx *tz_ctx,
                                  bool empty_as_null);
  static ObObjType merge_type(ObObjType cur, ObFileValueKind kind);
  static int text_to_datum(const common::ObString &text, ObObjType type,
                           common::ObIAllocator &alloc,
                           const common::ObTimeConvertCtx *tz_ctx,
                           common::ObDatum &datum);
  // glob() support: expand a path pattern, one file per element.
  static int glob_paths(common::ObIAllocator &alloc, const common::ObString &pattern,
                        common::ObIArray<common::ObString> &paths);
  // Parse one flat JSON object into key/value pairs. Nested values are
  // rejected; strings are unescaped into alloc.
  static int parse_json_record(const common::ObString &line,
                               common::ObIArray<common::ObString> &keys,
                               common::ObIArray<common::ObString> &values,
                               common::ObIArray<bool> &is_null,
                               common::ObIAllocator &alloc);
  static int stat_file(const common::ObString &path, int64_t &size, int64_t &mtime);
  static bool has_glob_wildcard(const common::ObString &pattern);
  // Turn a user supplied path into the absolute, component-normalized path that is
  // actually opened. secure_file_priv is matched against this value, so a path with
  // ".." cannot name a file outside the allowed directory. The last component is
  // kept verbatim because it may not exist yet or may be a glob pattern.
  static int canonicalize_path(common::ObIAllocator &alloc,
                               const common::ObString &path,
                               common::ObString &canonical);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SCAN_UTILS_H_
