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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_PARQUET_READER_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_PARQUET_READER_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace sql
{

class ObFileScanOptions;
struct ObFileColumnMeta;

enum ObParquetPhysicalType
{
  PQ_BOOLEAN = 0,
  PQ_INT32 = 1,
  PQ_INT64 = 2,
  PQ_INT96 = 3,
  PQ_FLOAT = 4,
  PQ_DOUBLE = 5,
  PQ_BYTE_ARRAY = 6,
  PQ_FIXED_LEN_BYTE_ARRAY = 7,
};

// ConvertedType of the parquet format that changes the SQL type of a column.
enum ObParquetConvertedType
{
  PQ_CT_NONE = -1,
  PQ_CT_UTF8 = 0,
  PQ_CT_DECIMAL = 5,
  PQ_CT_DATE = 6,
  PQ_CT_TIME_MILLIS = 7,
  PQ_CT_TIME_MICROS = 8,
  PQ_CT_TIMESTAMP_MILLIS = 9,
  PQ_CT_TIMESTAMP_MICROS = 10,
};

struct ObParquetColumn
{
  ObParquetColumn()
    : name_(), physical_type_(PQ_BYTE_ARRAY), converted_type_(PQ_CT_NONE),
      type_length_(0), scale_(0), precision_(0), optional_(true), sql_type_(ObVarcharType)
  {}
  common::ObString name_;
  int32_t physical_type_;
  int32_t converted_type_;
  int32_t type_length_;
  int32_t scale_;
  int32_t precision_;
  bool optional_;
  ObObjType sql_type_;
};

struct ObParquetColumnChunk
{
  ObParquetColumnChunk()
    : data_page_offset_(0), dict_page_offset_(0), total_compressed_size_(0),
      num_values_(0), codec_(0), physical_type_(PQ_BYTE_ARRAY), scale_(0)
  {}
  int64_t data_page_offset_;
  int64_t dict_page_offset_;
  int64_t total_compressed_size_;
  int64_t num_values_;
  int32_t codec_;
  int32_t physical_type_;
  int32_t scale_;
};

struct ObParquetRowGroup
{
  ObParquetRowGroup() : num_rows_(0), chunks_() {}
  int64_t num_rows_;
  common::ObSEArray<ObParquetColumnChunk, 8> chunks_;
};

// Streaming reader of a local parquet file with a flat schema. One page per
// column is decoded at a time, the file itself is never materialized.
class ObParquetReader
{
public:
  ObParquetReader();
  ~ObParquetReader() { close(); }

  // Parse the footer only and describe the columns the way the file schema
  // inference reports them.
  static int read_schema(common::ObIAllocator &alloc, const common::ObString &path,
                         const ObFileScanOptions &opt,
                         common::ObIArray<ObFileColumnMeta> &cols);

  int open(const common::ObString &path, const ObFileScanOptions &opt,
           common::ObIAllocator &alloc);
  void close();
  int rescan();
  bool is_opened() const { return fd_ >= 0; }
  int64_t column_count() const { return columns_.count(); }
  // Mark the columns the query actually reads. Unmarked columns are not read
  // from the file at all, which is how column pruning works for parquet.
  int set_needed_columns(const common::ObIArray<bool> &needed);
  bool is_needed(const int64_t idx) const
  { return idx < 0 || idx >= needed_.count() || needed_.at(idx); }
  // One row as text values, parallel to the file columns. Pointers are valid
  // until the next call.
  int next_row(common::ObIArray<common::ObString> &fields,
               common::ObIArray<bool> &is_null);

private:
  struct ColumnState
  {
    ColumnState()
      : page_alloc_(), dict_alloc_(), texts_(NULL), nulls_(NULL), cap_(0), pos_(0),
        page_num_values_(0), values_left_(0), next_page_offset_(0), chunk_end_(0),
        dict_(NULL), dict_count_(0)
    {}
    common::ObArenaAllocator page_alloc_;
    common::ObArenaAllocator dict_alloc_;
    common::ObString *texts_;
    bool *nulls_;
    int64_t cap_;
    int64_t pos_;
    int64_t page_num_values_;
    int64_t values_left_;
    int64_t next_page_offset_;
    int64_t chunk_end_;
    common::ObString *dict_;
    int64_t dict_count_;
  };

  int load_footer(common::ObIAllocator &alloc);
  int alloc_states(common::ObIAllocator &alloc);
  int load_row_group(const int64_t rg_idx);
  int load_next_page(ColumnState &st, const ObParquetColumnChunk &chunk,
                     const ObParquetColumn &col);
  int decode_dictionary_page(ColumnState &st, const ObParquetColumn &col,
                             const uint8_t *body, const int64_t body_len,
                             const int64_t num_values);
  int decode_data_page(ColumnState &st, const ObParquetColumn &col,
                       const uint8_t *body, const int64_t body_len,
                       const uint8_t *value_data, const int64_t value_len,
                       const int64_t num_values, const int32_t encoding,
                       const bool has_levels_prefix);
  int read_at(const int64_t offset, const int64_t len, char *buf);
  int check_file_unchanged();

private:
  int fd_;
  int64_t file_size_;
  int64_t num_rows_;
  int64_t expected_size_;
  char *path_buf_;
  common::ObIAllocator *alloc_;
  common::ObSEArray<ObParquetColumn, 8> columns_;
  common::ObSEArray<ObParquetRowGroup, 4> row_groups_;
  common::ObSEArray<ColumnState *, 8> states_;
  common::ObSEArray<bool, 16> needed_;
  int64_t cur_rg_;
  int64_t cur_row_;
  int64_t loaded_rg_;

  DISALLOW_COPY_AND_ASSIGN(ObParquetReader);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_PARQUET_READER_H_
