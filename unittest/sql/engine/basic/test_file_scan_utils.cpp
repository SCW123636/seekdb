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

#include <gtest/gtest.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "sql/engine/basic/ob_file_scan_utils.h"
#include "sql/engine/basic/ob_parquet_reader.h"

namespace oceanbase
{
namespace sql
{

using namespace common;

namespace
{

const char *const PARQUET_FIXTURE = "file_scan_utils_test.parquet";

// A 3 row / 4 column parquet file written by pyarrow with snappy compression:
// id INT64, name STRING, score DOUBLE, price DECIMAL(10,2).
extern const unsigned char kParquetFixture[];
extern const unsigned int kParquetFixtureLen;
const unsigned char kParquetFixture[] = {
    0x50, 0x41, 0x52, 0x31, 0x15, 0x04, 0x15, 0x30, 0x15, 0x2e, 0x4c, 0x15,
    0x06, 0x15, 0x00, 0x12, 0x00, 0x00, 0x18, 0x04, 0x01, 0x00, 0x09, 0x01,
    0x3c, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x15, 0x14, 0x15, 0x18, 0x2c,
    0x15, 0x06, 0x15, 0x10, 0x15, 0x06, 0x15, 0x06, 0x1c, 0x18, 0x08, 0x03,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x08, 0x01, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0x00, 0x28, 0x08, 0x03, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x08, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x11, 0x11, 0x00, 0x00, 0x00, 0x0a, 0x24, 0x02, 0x00,
    0x00, 0x00, 0x06, 0x01, 0x02, 0x03, 0x24, 0x00, 0x15, 0x04, 0x15, 0x22,
    0x15, 0x26, 0x4c, 0x15, 0x04, 0x15, 0x00, 0x12, 0x00, 0x00, 0x11, 0x40,
    0x05, 0x00, 0x00, 0x00, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x04, 0x00, 0x00,
    0x00, 0x62, 0x65, 0x74, 0x61, 0x15, 0x00, 0x15, 0x12, 0x15, 0x16, 0x2c,
    0x15, 0x06, 0x15, 0x10, 0x15, 0x06, 0x15, 0x06, 0x1c, 0x36, 0x02, 0x28,
    0x04, 0x62, 0x65, 0x74, 0x61, 0x18, 0x05, 0x61, 0x6c, 0x70, 0x68, 0x61,
    0x11, 0x11, 0x00, 0x00, 0x00, 0x09, 0x20, 0x02, 0x00, 0x00, 0x00, 0x03,
    0x03, 0x01, 0x03, 0x02, 0x15, 0x04, 0x15, 0x30, 0x15, 0x2c, 0x4c, 0x15,
    0x06, 0x15, 0x00, 0x12, 0x00, 0x00, 0x18, 0x00, 0x00, 0x05, 0x01, 0x04,
    0xf8, 0x3f, 0x05, 0x07, 0x28, 0x00, 0x02, 0x40, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x0e, 0x40, 0x15, 0x00, 0x15, 0x14, 0x15, 0x18, 0x2c, 0x15,
    0x06, 0x15, 0x10, 0x15, 0x06, 0x15, 0x06, 0x1c, 0x18, 0x08, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x0e, 0x40, 0x18, 0x08, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0xf8, 0x3f, 0x16, 0x00, 0x28, 0x08, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x0e, 0x40, 0x18, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0xf8, 0x3f, 0x11, 0x11, 0x00, 0x00, 0x00, 0x0a, 0x24, 0x02, 0x00, 0x00,
    0x00, 0x06, 0x01, 0x02, 0x03, 0x24, 0x00, 0x15, 0x04, 0x15, 0x1e, 0x15,
    0x22, 0x4c, 0x15, 0x06, 0x15, 0x00, 0x12, 0x00, 0x00, 0x0f, 0x38, 0xff,
    0xff, 0xff, 0xfb, 0x2e, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x07, 0x5b,
    0xcd, 0x15, 0x15, 0x00, 0x15, 0x14, 0x15, 0x18, 0x2c, 0x15, 0x06, 0x15,
    0x10, 0x15, 0x06, 0x15, 0x06, 0x1c, 0x18, 0x05, 0x00, 0x07, 0x5b, 0xcd,
    0x15, 0x18, 0x05, 0xff, 0xff, 0xff, 0xfb, 0x2e, 0x16, 0x00, 0x28, 0x05,
    0x00, 0x07, 0x5b, 0xcd, 0x15, 0x18, 0x05, 0xff, 0xff, 0xff, 0xfb, 0x2e,
    0x11, 0x11, 0x00, 0x00, 0x00, 0x0a, 0x24, 0x02, 0x00, 0x00, 0x00, 0x06,
    0x01, 0x02, 0x03, 0x24, 0x00, 0x15, 0x04, 0x19, 0x5c, 0x35, 0x00, 0x18,
    0x06, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x15, 0x08, 0x00, 0x15, 0x04,
    0x25, 0x02, 0x18, 0x02, 0x69, 0x64, 0x00, 0x15, 0x0c, 0x25, 0x02, 0x18,
    0x04, 0x6e, 0x61, 0x6d, 0x65, 0x25, 0x00, 0x4c, 0x1c, 0x00, 0x00, 0x00,
    0x15, 0x0a, 0x25, 0x02, 0x18, 0x05, 0x73, 0x63, 0x6f, 0x72, 0x65, 0x00,
    0x15, 0x0e, 0x15, 0x0a, 0x15, 0x02, 0x18, 0x05, 0x70, 0x72, 0x69, 0x63,
    0x65, 0x25, 0x0a, 0x15, 0x04, 0x15, 0x14, 0x2c, 0x5c, 0x15, 0x04, 0x15,
    0x14, 0x00, 0x00, 0x00, 0x16, 0x06, 0x19, 0x1c, 0x19, 0x4c, 0x26, 0x00,
    0x1c, 0x15, 0x04, 0x19, 0x35, 0x00, 0x06, 0x10, 0x19, 0x18, 0x02, 0x69,
    0x64, 0x15, 0x02, 0x16, 0x06, 0x16, 0xde, 0x01, 0x16, 0xe0, 0x01, 0x26,
    0x52, 0x26, 0x08, 0x1c, 0x18, 0x08, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x18, 0x08, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x16, 0x00, 0x28, 0x08, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x18, 0x08, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11, 0x11,
    0x00, 0x19, 0x2c, 0x15, 0x04, 0x15, 0x00, 0x15, 0x02, 0x00, 0x15, 0x00,
    0x15, 0x10, 0x15, 0x02, 0x00, 0x3c, 0x29, 0x06, 0x19, 0x26, 0x00, 0x06,
    0x00, 0x00, 0x00, 0x26, 0x00, 0x1c, 0x15, 0x0c, 0x19, 0x35, 0x00, 0x06,
    0x10, 0x19, 0x18, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x15, 0x02, 0x16, 0x06,
    0x16, 0x98, 0x01, 0x16, 0xa0, 0x01, 0x26, 0xaa, 0x02, 0x26, 0xe8, 0x01,
    0x1c, 0x36, 0x02, 0x28, 0x04, 0x62, 0x65, 0x74, 0x61, 0x18, 0x05, 0x61,
    0x6c, 0x70, 0x68, 0x61, 0x11, 0x11, 0x00, 0x19, 0x2c, 0x15, 0x04, 0x15,
    0x00, 0x15, 0x02, 0x00, 0x15, 0x00, 0x15, 0x10, 0x15, 0x02, 0x00, 0x3c,
    0x16, 0x12, 0x19, 0x06, 0x19, 0x26, 0x02, 0x04, 0x00, 0x00, 0x00, 0x26,
    0x00, 0x1c, 0x15, 0x0a, 0x19, 0x35, 0x00, 0x06, 0x10, 0x19, 0x18, 0x05,
    0x73, 0x63, 0x6f, 0x72, 0x65, 0x15, 0x02, 0x16, 0x06, 0x16, 0xde, 0x01,
    0x16, 0xde, 0x01, 0x26, 0xd0, 0x03, 0x26, 0x88, 0x03, 0x1c, 0x18, 0x08,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0e, 0x40, 0x18, 0x08, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f, 0x16, 0x00, 0x28, 0x08, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x0e, 0x40, 0x18, 0x08, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0xf8, 0x3f, 0x11, 0x11, 0x00, 0x19, 0x2c, 0x15, 0x04, 0x15,
    0x00, 0x15, 0x02, 0x00, 0x15, 0x00, 0x15, 0x10, 0x15, 0x02, 0x00, 0x3c,
    0x29, 0x06, 0x19, 0x26, 0x00, 0x06, 0x00, 0x00, 0x00, 0x26, 0x00, 0x1c,
    0x15, 0x0e, 0x19, 0x35, 0x00, 0x06, 0x10, 0x19, 0x18, 0x05, 0x70, 0x72,
    0x69, 0x63, 0x65, 0x15, 0x02, 0x16, 0x06, 0x16, 0xb4, 0x01, 0x16, 0xbc,
    0x01, 0x26, 0xa4, 0x05, 0x26, 0xe6, 0x04, 0x1c, 0x18, 0x05, 0x00, 0x07,
    0x5b, 0xcd, 0x15, 0x18, 0x05, 0xff, 0xff, 0xff, 0xfb, 0x2e, 0x16, 0x00,
    0x28, 0x05, 0x00, 0x07, 0x5b, 0xcd, 0x15, 0x18, 0x05, 0xff, 0xff, 0xff,
    0xfb, 0x2e, 0x11, 0x11, 0x00, 0x19, 0x2c, 0x15, 0x04, 0x15, 0x00, 0x15,
    0x02, 0x00, 0x15, 0x00, 0x15, 0x10, 0x15, 0x02, 0x00, 0x3c, 0x29, 0x06,
    0x19, 0x26, 0x00, 0x06, 0x00, 0x00, 0x00, 0x16, 0x88, 0x06, 0x16, 0x06,
    0x26, 0x08, 0x16, 0x9a, 0x06, 0x00, 0x19, 0x1c, 0x18, 0x0c, 0x41, 0x52,
    0x52, 0x4f, 0x57, 0x3a, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x18, 0x80,
    0x03, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x78, 0x67, 0x42, 0x41, 0x41, 0x41,
    0x51, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x4b, 0x41, 0x41, 0x77,
    0x41, 0x42, 0x67, 0x41, 0x46, 0x41, 0x41, 0x67, 0x41, 0x43, 0x67, 0x41,
    0x41, 0x41, 0x41, 0x41, 0x42, 0x42, 0x41, 0x41, 0x4d, 0x41, 0x41, 0x41,
    0x41, 0x43, 0x41, 0x41, 0x49, 0x41, 0x41, 0x41, 0x41, 0x42, 0x41, 0x41,
    0x49, 0x41, 0x41, 0x41, 0x41, 0x42, 0x41, 0x41, 0x41, 0x41, 0x41, 0x51,
    0x41, 0x41, 0x41, 0x43, 0x30, 0x41, 0x41, 0x41, 0x41, 0x64, 0x41, 0x41,
    0x41, 0x41, 0x45, 0x41, 0x41, 0x41, 0x41, 0x41, 0x45, 0x41, 0x41, 0x41,
    0x41, 0x62, 0x50, 0x2f, 0x2f, 0x2f, 0x77, 0x41, 0x41, 0x41, 0x51, 0x63,
    0x51, 0x41, 0x41, 0x41, 0x41, 0x49, 0x41, 0x41, 0x41, 0x41, 0x41, 0x51,
    0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x42, 0x51, 0x41,
    0x41, 0x41, 0x48, 0x42, 0x79, 0x61, 0x57, 0x4e, 0x6c, 0x41, 0x41, 0x41,
    0x41, 0x43, 0x41, 0x41, 0x4d, 0x41, 0x41, 0x51, 0x41, 0x43, 0x41, 0x41,
    0x49, 0x41, 0x41, 0x41, 0x41, 0x43, 0x67, 0x41, 0x41, 0x41, 0x41, 0x49,
    0x41, 0x41, 0x41, 0x43, 0x6b, 0x2f, 0x2f, 0x2f, 0x2f, 0x41, 0x41, 0x41,
    0x42, 0x41, 0x78, 0x41, 0x41, 0x41, 0x41, 0x41, 0x63, 0x41, 0x41, 0x41,
    0x41, 0x42, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
    0x46, 0x41, 0x41, 0x41, 0x41, 0x63, 0x32, 0x4e, 0x76, 0x63, 0x6d, 0x55,
    0x41, 0x42, 0x67, 0x41, 0x49, 0x41, 0x41, 0x59, 0x41, 0x42, 0x67, 0x41,
    0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x67, 0x44, 0x55, 0x2f, 0x2f, 0x2f,
    0x2f, 0x41, 0x41, 0x41, 0x42, 0x42, 0x52, 0x41, 0x41, 0x41, 0x41, 0x41,
    0x63, 0x41, 0x41, 0x41, 0x41, 0x42, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
    0x41, 0x41, 0x41, 0x41, 0x45, 0x41, 0x41, 0x41, 0x41, 0x62, 0x6d, 0x46,
    0x74, 0x5a, 0x51, 0x41, 0x41, 0x41, 0x41, 0x41, 0x45, 0x41, 0x41, 0x51,
    0x41, 0x42, 0x41, 0x41, 0x41, 0x41, 0x42, 0x41, 0x41, 0x46, 0x41, 0x41,
    0x49, 0x41, 0x41, 0x59, 0x41, 0x42, 0x77, 0x41, 0x4d, 0x41, 0x41, 0x41,
    0x41, 0x45, 0x41, 0x41, 0x51, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
    0x42, 0x41, 0x68, 0x41, 0x41, 0x41, 0x41, 0x41, 0x63, 0x41, 0x41, 0x41,
    0x41, 0x42, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
    0x43, 0x41, 0x41, 0x41, 0x41, 0x61, 0x57, 0x51, 0x41, 0x41, 0x41, 0x67,
    0x41, 0x44, 0x41, 0x41, 0x49, 0x41, 0x41, 0x63, 0x41, 0x43, 0x41, 0x41,
    0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x46, 0x41, 0x41, 0x41, 0x41,
    0x41, 0x00, 0x18, 0x20, 0x70, 0x61, 0x72, 0x71, 0x75, 0x65, 0x74, 0x2d,
    0x63, 0x70, 0x70, 0x2d, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x20, 0x76, 0x65,
    0x72, 0x73, 0x69, 0x6f, 0x6e, 0x20, 0x32, 0x35, 0x2e, 0x30, 0x2e, 0x31,
    0x19, 0x4c, 0x1c, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x1c,
    0x00, 0x00, 0x00, 0xa6, 0x03, 0x00, 0x00, 0x50, 0x41, 0x52, 0x31,
};
const unsigned int kParquetFixtureLen = 1343;

}  // namespace

class ObFileScanUtilsTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    const char *dir = getenv("TEST_TMPDIR");
    ASSERT_NE(nullptr, dir);
    dir_.assign(dir);
  }

  std::string real_dir() const
  {
    // canonicalize_path resolves symlinks, so compare against the physical path
    char buf[4096];
    if (nullptr == realpath(dir_.c_str(), buf)) {
      return dir_;
    }
    return std::string(buf);
  }

  std::string write_file(const std::string &name, const std::string &body)
  {
    const std::string path = dir_ + "/" + name;
    FILE *file = fopen(path.c_str(), "wb");
    EXPECT_TRUE(nullptr != file);
    if (nullptr != file) {
      if (!body.empty()) {
        EXPECT_EQ(body.size(), fwrite(body.data(), 1, body.size(), file));
      }
      fclose(file);
    }
    return path;
  }

  static ObString to_str(const std::string &text)
  {
    return ObString(static_cast<int64_t>(text.length()), text.data());
  }

  static std::string to_std(const ObString &text)
  {
    return std::string(text.ptr(), text.length());
  }

  struct Row
  {
    std::vector<std::string> fields_;
    std::vector<bool> nulls_;
  };

  int read_all(ObFileRecordReader &reader, std::vector<Row> &rows)
  {
    int ret = OB_SUCCESS;
    ObSEArray<ObString, 16> fields;
    ObSEArray<bool, 16> nulls;
    while (OB_SUCC(ret)) {
      const int tmp_ret = reader.next_record(fields, nulls);
      if (OB_ITER_END == tmp_ret) {
        break;
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
      } else {
        Row row;
        for (int64_t i = 0; i < fields.count(); ++i) {
          row.fields_.push_back(to_std(fields.at(i)));
          row.nulls_.push_back(nulls.at(i));
        }
        rows.push_back(row);
      }
    }
    return ret;
  }

  ObArenaAllocator alloc_;
  std::string dir_;
};

TEST_F(ObFileScanUtilsTest, options_defaults_and_round_trip)
{
  ObFileScanOptions opt;
  EXPECT_EQ(OB_FILE_FORMAT_CSV, opt.format_);
  EXPECT_TRUE(opt.has_header_);
  EXPECT_EQ(',', opt.delimiter_);
  EXPECT_EQ('"', opt.quote_);
  EXPECT_EQ('"', opt.escape_);
  EXPECT_TRUE(opt.empty_as_null_);
  EXPECT_EQ(0, opt.skip_lines_);
  EXPECT_EQ(static_cast<int64_t>(1024 * 1024), opt.max_line_size_);

  bool recognized = false;
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("delimiter"), to_str("|"), recognized));
  EXPECT_TRUE(recognized);
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("quote"), to_str("'"), recognized));
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("escape"), to_str("\\"), recognized));
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("nullstr"), to_str("NULL"), recognized));
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("skip_lines"), to_str("2"), recognized));
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("max_line_size"), to_str("4096"), recognized));
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("format"), to_str("ndjson"), recognized));
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("sample_size"), to_str("-1"), recognized));
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("header"), to_str("false"), recognized));
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("no_such_option"), to_str("1"), recognized));
  EXPECT_FALSE(recognized);

  // sample_size may only be -1: inference always reads the whole file
  EXPECT_EQ(OB_INVALID_ARGUMENT, opt.set_option(to_str("sample_size"), to_str("100"), recognized));
  EXPECT_EQ(OB_INVALID_ARGUMENT, opt.set_option(to_str("max_line_size"), to_str("0"), recognized));
  EXPECT_EQ(OB_INVALID_ARGUMENT, opt.set_option(to_str("format"), to_str("orc"), recognized));

  opt.file_size_ = 12345;
  opt.file_mtime_ = 6789;
  ObString encoded;
  ASSERT_EQ(OB_SUCCESS, opt.encode(alloc_, encoded));
  ObFileScanOptions decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.decode(encoded));
  EXPECT_EQ(opt.format_, decoded.format_);
  EXPECT_EQ(opt.has_header_, decoded.has_header_);
  EXPECT_EQ(opt.delimiter_, decoded.delimiter_);
  EXPECT_EQ(opt.quote_, decoded.quote_);
  EXPECT_EQ(opt.escape_, decoded.escape_);
  EXPECT_EQ(opt.empty_as_null_, decoded.empty_as_null_);
  EXPECT_EQ(opt.skip_lines_, decoded.skip_lines_);
  EXPECT_EQ(opt.max_line_size_, decoded.max_line_size_);
  EXPECT_EQ(opt.file_size_, decoded.file_size_);
  EXPECT_EQ(opt.file_mtime_, decoded.file_mtime_);
  EXPECT_EQ(to_std(opt.null_str()), to_std(decoded.null_str()));
  EXPECT_TRUE(decoded.is_null_text(to_str("NULL")));
  EXPECT_FALSE(decoded.is_null_text(to_str("null")));
}

TEST_F(ObFileScanUtilsTest, classify_and_merge_type)
{
  EXPECT_EQ(FVK_NULL, ObFileScanUtils::classify(to_str(""), NULL, true));
  EXPECT_EQ(FVK_STR, ObFileScanUtils::classify(to_str(""), NULL, false));
  EXPECT_EQ(FVK_BOOL, ObFileScanUtils::classify(to_str("true"), NULL, true));
  EXPECT_EQ(FVK_BOOL, ObFileScanUtils::classify(to_str("FALSE"), NULL, true));
  EXPECT_EQ(FVK_INT, ObFileScanUtils::classify(to_str("42"), NULL, true));
  EXPECT_EQ(FVK_INT, ObFileScanUtils::classify(to_str("-9223372036854775808"), NULL, true));
  EXPECT_EQ(FVK_INT, ObFileScanUtils::classify(to_str("9223372036854775807"), NULL, true));
  // one past int64 falls back to DOUBLE instead of wrapping around
  EXPECT_EQ(FVK_DOUBLE, ObFileScanUtils::classify(to_str("9223372036854775808"), NULL, true));
  EXPECT_EQ(FVK_DOUBLE, ObFileScanUtils::classify(to_str("1.5"), NULL, true));
  EXPECT_EQ(FVK_DOUBLE, ObFileScanUtils::classify(to_str("1e10"), NULL, true));
  // nan/inf are accepted by strtod but are not numeric values of the file
  EXPECT_EQ(FVK_STR, ObFileScanUtils::classify(to_str("nan"), NULL, true));
  EXPECT_EQ(FVK_STR, ObFileScanUtils::classify(to_str("inf"), NULL, true));
  EXPECT_EQ(FVK_DATE, ObFileScanUtils::classify(to_str("2024-02-29"), NULL, true));
  EXPECT_EQ(FVK_DATETIME, ObFileScanUtils::classify(to_str("2025-01-02 00:00:00"), NULL, true));
  EXPECT_EQ(FVK_DATETIME, ObFileScanUtils::classify(to_str("2025-01-02T03:04:05.123"), NULL, true));
  EXPECT_EQ(FVK_STR, ObFileScanUtils::classify(to_str("2023-02-29"), NULL, true));
  EXPECT_EQ(FVK_STR, ObFileScanUtils::classify(to_str("2025-13-01"), NULL, true));
  EXPECT_EQ(FVK_STR, ObFileScanUtils::classify(to_str("hello"), NULL, true));

  // the first non NULL value fixes the column type, later values only widen it
  EXPECT_EQ(ObTinyIntType, ObFileScanUtils::merge_type(ObNullType, FVK_BOOL));
  EXPECT_EQ(ObIntType, ObFileScanUtils::merge_type(ObNullType, FVK_INT));
  EXPECT_EQ(ObDoubleType, ObFileScanUtils::merge_type(ObNullType, FVK_DOUBLE));
  EXPECT_EQ(ObDateType, ObFileScanUtils::merge_type(ObNullType, FVK_DATE));
  EXPECT_EQ(ObDateTimeType, ObFileScanUtils::merge_type(ObNullType, FVK_DATETIME));
  EXPECT_EQ(ObVarcharType, ObFileScanUtils::merge_type(ObNullType, FVK_STR));
  EXPECT_EQ(ObNullType, ObFileScanUtils::merge_type(ObNullType, FVK_NULL));
  EXPECT_EQ(ObIntType, ObFileScanUtils::merge_type(ObTinyIntType, FVK_INT));
  EXPECT_EQ(ObDoubleType, ObFileScanUtils::merge_type(ObIntType, FVK_DOUBLE));
  EXPECT_EQ(ObIntType, ObFileScanUtils::merge_type(ObIntType, FVK_BOOL));
  EXPECT_EQ(ObIntType, ObFileScanUtils::merge_type(ObIntType, FVK_NULL));
  EXPECT_EQ(ObDateTimeType, ObFileScanUtils::merge_type(ObDateType, FVK_DATETIME));
  EXPECT_EQ(ObDateTimeType, ObFileScanUtils::merge_type(ObDateTimeType, FVK_DATE));
  EXPECT_EQ(ObDateType, ObFileScanUtils::merge_type(ObDateType, FVK_DATE));
  // mixing types that have no common numeric representation widens to text
  EXPECT_EQ(ObVarcharType, ObFileScanUtils::merge_type(ObDateType, FVK_INT));
  EXPECT_EQ(ObVarcharType, ObFileScanUtils::merge_type(ObIntType, FVK_DATE));
  EXPECT_EQ(ObVarcharType, ObFileScanUtils::merge_type(ObIntType, FVK_STR));
  EXPECT_EQ(ObVarcharType, ObFileScanUtils::merge_type(ObVarcharType, FVK_INT));
}

TEST_F(ObFileScanUtilsTest, csv_basic_and_crlf)
{
  const std::string path = write_file("basic.csv", "id,name\r\n1,alice\r\n2,bob\r\n");
  ObFileScanOptions opt;
  ObFileRecordReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(to_str(path), opt, alloc_, NULL));
  std::vector<Row> rows;
  ASSERT_EQ(OB_SUCCESS, read_all(reader, rows));
  ASSERT_EQ(3, rows.size());
  EXPECT_EQ(2, rows.at(0).fields_.size());
  EXPECT_EQ("id", rows.at(0).fields_.at(0));
  EXPECT_EQ("name", rows.at(0).fields_.at(1));
  EXPECT_EQ("1", rows.at(1).fields_.at(0));
  EXPECT_EQ("alice", rows.at(1).fields_.at(1));
  EXPECT_EQ("2", rows.at(2).fields_.at(0));
  EXPECT_EQ("bob", rows.at(2).fields_.at(1));
  EXPECT_EQ(3, reader.record_no());
}

TEST_F(ObFileScanUtilsTest, csv_quoted_fields)
{
  const std::string path = write_file(
      "quoted.csv",
      "id,note\r\n1,\"line1\nline2\"\r\n2,\"say \"\"hi\"\"\"\r\n3,plain\r\n");
  ObFileScanOptions opt;
  ObFileRecordReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(to_str(path), opt, alloc_, NULL));
  std::vector<Row> rows;
  ASSERT_EQ(OB_SUCCESS, read_all(reader, rows));
  ASSERT_EQ(4, rows.size());
  EXPECT_EQ("line1\nline2", rows.at(1).fields_.at(1));
  EXPECT_EQ("say \"hi\"", rows.at(2).fields_.at(1));
  EXPECT_EQ("plain", rows.at(3).fields_.at(1));
}

TEST_F(ObFileScanUtilsTest, csv_backslash_escape)
{
  const std::string path = write_file("escaped.csv", "1,\"a\\\"b\"\n2,\"tab\\there\"\n");
  ObFileScanOptions opt;
  bool recognized = false;
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("escape"), to_str("\\"), recognized));
  EXPECT_TRUE(recognized);
  ObFileRecordReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(to_str(path), opt, alloc_, NULL));
  std::vector<Row> rows;
  ASSERT_EQ(OB_SUCCESS, read_all(reader, rows));
  ASSERT_EQ(2, rows.size());
  EXPECT_EQ("a\"b", rows.at(0).fields_.at(1));
  EXPECT_EQ("tab\there", rows.at(1).fields_.at(1));
}

TEST_F(ObFileScanUtilsTest, csv_null_text_and_empty_field)
{
  const std::string path = write_file("nulls.csv", "a,b\nNULL,1\n,\"\"\n");
  ObFileScanOptions opt;
  bool recognized = false;
  ASSERT_EQ(OB_SUCCESS, opt.set_option(to_str("nullstr"), to_str("NULL"), recognized));
  ObFileRecordReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(to_str(path), opt, alloc_, NULL));
  std::vector<Row> rows;
  ASSERT_EQ(OB_SUCCESS, read_all(reader, rows));
  ASSERT_EQ(3, rows.size());
  // the literal NULL is reported as a NULL field
  EXPECT_TRUE(rows.at(1).nulls_.at(0));
  EXPECT_FALSE(rows.at(1).nulls_.at(1));
  // an unquoted empty field is NULL, a quoted empty string is not
  EXPECT_TRUE(rows.at(2).nulls_.at(0));
  EXPECT_FALSE(rows.at(2).nulls_.at(1));
  EXPECT_EQ("", rows.at(2).fields_.at(1));
}

TEST_F(ObFileScanUtilsTest, csv_utf8_and_skip_lines)
{
  const std::string path = write_file("utf8.csv", "skip me\n名字,城市\n张三,北京\n李四,上海\n");
  ObFileScanOptions opt;
  opt.has_header_ = true;
  opt.skip_lines_ = 1;
  ObFileRecordReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(to_str(path), opt, alloc_, NULL));
  std::vector<Row> rows;
  ASSERT_EQ(OB_SUCCESS, read_all(reader, rows));
  ASSERT_EQ(3, rows.size());
  EXPECT_EQ("名字", rows.at(0).fields_.at(0));
  EXPECT_EQ("北京", rows.at(1).fields_.at(1));
  EXPECT_EQ("上海", rows.at(2).fields_.at(1));
}

TEST_F(ObFileScanUtilsTest, csv_record_over_max_line_size)
{
  std::string body = "id\n";
  body.append(200, 'x');
  body.append("\n");
  const std::string path = write_file("long.csv", body);
  ObFileScanOptions opt;
  opt.max_line_size_ = 32;
  ObFileRecordReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(to_str(path), opt, alloc_, NULL));
  std::vector<Row> rows;
  EXPECT_EQ(OB_INVALID_ARGUMENT, read_all(reader, rows));
  ASSERT_EQ(1, rows.size());
}

TEST_F(ObFileScanUtilsTest, csv_unterminated_quote)
{
  const std::string path = write_file("unterminated.csv", "1,\"open\n");
  ObFileScanOptions opt;
  ObFileRecordReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(to_str(path), opt, alloc_, NULL));
  std::vector<Row> rows;
  EXPECT_EQ(OB_INVALID_ARGUMENT, read_all(reader, rows));
}

TEST_F(ObFileScanUtilsTest, json_record_parsing)
{
  ObSEArray<ObString, 8> keys;
  ObSEArray<ObString, 8> values;
  ObSEArray<bool, 8> nulls;
  ASSERT_EQ(OB_SUCCESS, ObFileScanUtils::parse_json_record(
                            to_str("{\"a\":1,\"b\":\"x y\",\"c\":null,\"d\":true}"),
                            keys, values, nulls, alloc_));
  ASSERT_EQ(4, keys.count());
  EXPECT_EQ("a", to_std(keys.at(0)));
  EXPECT_EQ("1", to_std(values.at(0)));
  EXPECT_EQ("x y", to_std(values.at(1)));
  EXPECT_TRUE(nulls.at(2));
  EXPECT_EQ("true", to_std(values.at(3)));
  EXPECT_FALSE(nulls.at(3));

  // escaped strings are unescaped, embedded commas do not split fields
  keys.reuse();
  values.reuse();
  nulls.reuse();
  ASSERT_EQ(OB_SUCCESS, ObFileScanUtils::parse_json_record(
                            to_str("{\"s\":\"a\\\"b\\\\c\\nd\",\"t\":\"1,2\"}"),
                            keys, values, nulls, alloc_));
  ASSERT_EQ(2, keys.count());
  EXPECT_EQ("a\"b\\c\nd", to_std(values.at(0)));
  EXPECT_EQ("1,2", to_std(values.at(1)));

  // nested objects and arrays are out of scope
  keys.reuse();
  values.reuse();
  nulls.reuse();
  EXPECT_EQ(OB_NOT_SUPPORTED, ObFileScanUtils::parse_json_record(
                                  to_str("{\"a\":{\"b\":1}}"), keys, values, nulls, alloc_));
  keys.reuse();
  values.reuse();
  nulls.reuse();
  EXPECT_EQ(OB_NOT_SUPPORTED, ObFileScanUtils::parse_json_record(
                                  to_str("{\"a\":[1,2]}"), keys, values, nulls, alloc_));

  keys.reuse();
  values.reuse();
  nulls.reuse();
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObFileScanUtils::parse_json_record(
                                     to_str("not a json object"), keys, values, nulls, alloc_));
  keys.reuse();
  values.reuse();
  nulls.reuse();
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObFileScanUtils::parse_json_record(
                                     to_str("{\"a\":}"), keys, values, nulls, alloc_));
}

TEST_F(ObFileScanUtilsTest, csv_schema_inference)
{
  const std::string path = write_file(
      "infer.csv",
      "id,d,b,day,dt,s,allnull\n"
      "9223372036854775807,1.5,true,2024-02-29,2024-02-29 10:00:00,hello,\n"
      "-9223372036854775808,2,false,2023-02-29,2024-03-01T00:00:00,world,\n");
  ObFileScanOptions opt;
  ObSEArray<ObFileColumnMeta, 8> cols;
  ASSERT_EQ(OB_SUCCESS, ObFileSchemaInferer::infer(alloc_, to_str(path), opt, NULL, cols));
  ASSERT_EQ(7, cols.count());
  const char *names[] = {"id", "d", "b", "day", "dt", "s", "allnull"};
  const ObObjType types[] = {ObIntType, ObDoubleType, ObTinyIntType, ObVarcharType,
                             ObDateTimeType, ObVarcharType, ObVarcharType};
  for (int64_t i = 0; i < cols.count(); ++i) {
    EXPECT_EQ(names[i], to_std(cols.at(i).name_));
    EXPECT_EQ(types[i], cols.at(i).type_);
  }
  EXPECT_EQ(5, cols.at(5).max_len_);
}

TEST_F(ObFileScanUtilsTest, jsonl_schema_inference)
{
  const std::string union_path = write_file(
      "infer_union.jsonl",
      "{\"id\":1,\"name\":\"a\"}\n"
      "{\"id\":2,\"score\":3.5}\n"
      "{\"id\":3,\"name\":\"c\",\"score\":4,\"ok\":true}\n");
  ObFileScanOptions opt;
  opt.format_ = OB_FILE_FORMAT_NDJSON;
  ObSEArray<ObFileColumnMeta, 8> cols;
  ASSERT_EQ(OB_SUCCESS, ObFileSchemaInferer::infer(alloc_, to_str(union_path), opt, NULL, cols));
  ASSERT_EQ(4, cols.count());
  EXPECT_EQ("id", to_std(cols.at(0).name_));
  EXPECT_EQ(ObIntType, cols.at(0).type_);
  EXPECT_EQ("name", to_std(cols.at(1).name_));
  EXPECT_EQ(ObVarcharType, cols.at(1).type_);
  EXPECT_EQ("score", to_std(cols.at(2).name_));
  EXPECT_EQ(ObDoubleType, cols.at(2).type_);
  EXPECT_EQ("ok", to_std(cols.at(3).name_));
  EXPECT_EQ(ObTinyIntType, cols.at(3).type_);

  const std::string conflict_path = write_file(
      "infer_conflict.jsonl", "{\"id\":1,\"v\":10}\n{\"id\":2,\"v\":\"ten\"}\n");
  ObSEArray<ObFileColumnMeta, 8> conflict_cols;
  ASSERT_EQ(OB_SUCCESS, ObFileSchemaInferer::infer(alloc_, to_str(conflict_path), opt, NULL,
                                                   conflict_cols));
  ASSERT_EQ(2, conflict_cols.count());
  EXPECT_EQ(ObVarcharType, conflict_cols.at(1).type_);
}

TEST_F(ObFileScanUtilsTest, json_array_file_inference)
{
  const std::string path = write_file(
      "infer_array.json",
      "[{\"id\":1,\"name\":\"a\"},\n {\"id\":2,\"score\":3.5}]");
  ObFileScanOptions opt;
  opt.format_ = OB_FILE_FORMAT_JSON;
  ObSEArray<ObFileColumnMeta, 8> cols;
  ASSERT_EQ(OB_SUCCESS, ObFileSchemaInferer::infer(alloc_, to_str(path), opt, NULL, cols));
  ASSERT_EQ(3, cols.count());
  EXPECT_EQ("id", to_std(cols.at(0).name_));
  EXPECT_EQ(ObIntType, cols.at(0).type_);
  EXPECT_EQ("name", to_std(cols.at(1).name_));
  EXPECT_EQ(ObVarcharType, cols.at(1).type_);
  EXPECT_EQ("score", to_std(cols.at(2).name_));
  EXPECT_EQ(ObDoubleType, cols.at(2).type_);

  // the same file is also delivered one object at a time by the reader
  ObFileRecordReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(to_str(path), opt, alloc_, NULL));
  ObString line;
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, reader.next_line(line));
    ASSERT_FALSE(line.empty());
    EXPECT_EQ('{', line.ptr()[0]);
  }
  EXPECT_EQ(OB_ITER_END, reader.next_line(line));
  reader.close();

  // a pretty printed object spans lines, several objects may share one line
  const std::string pretty_path = write_file(
      "pretty.json", "{\n  \"id\": 1,\n  \"name\": \"a\"\n}\n{\"id\": 2}\n");
  ObFileRecordReader pretty;
  ASSERT_EQ(OB_SUCCESS, pretty.open(to_str(pretty_path), opt, alloc_, NULL));
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, pretty.next_line(line));
    ASSERT_FALSE(line.empty());
    EXPECT_EQ('{', line.ptr()[0]);
    EXPECT_EQ('}', line.ptr()[line.length() - 1]);
  }
  EXPECT_EQ(OB_ITER_END, pretty.next_line(line));
  pretty.close();
}

TEST_F(ObFileScanUtilsTest, glob_paths_are_sorted)
{
  write_file("g_a.csv", "1\n");
  write_file("g_b.csv", "2\n");
  write_file("g_c.txt", "3\n");
  ObSEArray<ObString, 8> paths;
  const std::string pattern = dir_ + "/g_*.csv";
  ASSERT_EQ(OB_SUCCESS, ObFileScanUtils::glob_paths(alloc_, to_str(pattern), paths));
  ASSERT_EQ(2, paths.count());
  EXPECT_EQ(dir_ + "/g_a.csv", to_std(paths.at(0)));
  EXPECT_EQ(dir_ + "/g_b.csv", to_std(paths.at(1)));
  EXPECT_TRUE(ObFileScanUtils::has_glob_wildcard(to_str(pattern)));
  EXPECT_FALSE(ObFileScanUtils::has_glob_wildcard(to_str(dir_ + "/g_a.csv")));

  ObSEArray<ObString, 8> missing;
  EXPECT_EQ(OB_SUCCESS, ObFileScanUtils::glob_paths(alloc_, to_str(dir_ + "/no_match_*.csv"),
                                                    missing));
  EXPECT_EQ(0, missing.count());
  EXPECT_EQ(OB_FILE_NOT_EXIST,
            ObFileScanUtils::glob_paths(alloc_, to_str(dir_ + "/no_such_dir/x.csv"), missing));
}

TEST_F(ObFileScanUtilsTest, canonicalize_path_normalizes_components)
{
  ObString out;
  const std::string base = real_dir();
  ASSERT_EQ(OB_SUCCESS, ObFileScanUtils::canonicalize_path(
                            alloc_, to_str(dir_ + "/./a/../b.csv"), out));
  EXPECT_EQ(base + "/b.csv", to_std(out));

  // a missing component cannot be resolved by the filesystem, the lexical
  // normalization still removes the ".." so the prefix check cannot be fooled
  ASSERT_EQ(OB_SUCCESS, ObFileScanUtils::canonicalize_path(
                            alloc_, to_str(dir_ + "/no/such/../dir/x.csv"), out));
  EXPECT_EQ(base + "/no/dir/x.csv", to_std(out));

  ASSERT_EQ(OB_SUCCESS, ObFileScanUtils::canonicalize_path(
                            alloc_, to_str(dir_ + "/a.csv"), out));
  EXPECT_EQ(base + "/a.csv", to_std(out));

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObFileScanUtils::canonicalize_path(alloc_, ObString(), out));
}

TEST_F(ObFileScanUtilsTest, canonicalize_path_blocks_escape)
{
  // a ".." that leaves the allowed directory must be visible to the
  // secure_file_priv prefix check, otherwise the check can be bypassed
  const std::string base = real_dir();
  const std::string parent = base.substr(0, base.find_last_of('/'));
  ObString out;
  ASSERT_EQ(OB_SUCCESS, ObFileScanUtils::canonicalize_path(
                            alloc_, to_str(dir_ + "/../escaped.csv"), out));
  const std::string escaped = to_std(out);
  EXPECT_EQ(parent + "/escaped.csv", escaped);
  EXPECT_NE(escaped, base + "/escaped.csv");

  // a path that stays inside is reported as the same path
  write_file("inside.csv", "id\n1\n");
  ASSERT_EQ(OB_SUCCESS, ObFileScanUtils::canonicalize_path(
                            alloc_, to_str(dir_ + "/inside.csv"), out));
  EXPECT_EQ(base + "/inside.csv", to_std(out));
}

TEST_F(ObFileScanUtilsTest, parquet_reader_basic_types)
{
  const std::string path = write_file(
      PARQUET_FIXTURE,
      std::string(reinterpret_cast<const char *>(kParquetFixture), kParquetFixtureLen));
  ObFileScanOptions opt;
  opt.format_ = OB_FILE_FORMAT_PARQUET;
  ObSEArray<ObFileColumnMeta, 8> cols;
  ASSERT_EQ(OB_SUCCESS, ObParquetReader::read_schema(alloc_, to_str(path), opt, cols));
  ASSERT_EQ(4, cols.count());
  const char *names[] = {"id", "name", "score", "price"};
  const ObObjType types[] = {ObIntType, ObVarcharType, ObDoubleType, ObVarcharType};
  for (int64_t i = 0; i < cols.count(); ++i) {
    EXPECT_EQ(names[i], to_std(cols.at(i).name_));
    EXPECT_EQ(types[i], cols.at(i).type_);
  }

  ObParquetReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(to_str(path), opt, alloc_));
  ObSEArray<ObString, 8> fields;
  ObSEArray<bool, 8> nulls;
  const char *expected[3][4] = {
      {"1", "alpha", "1.5", "-12.34"},
      {"2", "beta", "2.25", "0.05"},
      {"3", "", "3.75", "1234567.89"},
  };
  for (int64_t row = 0; row < 3; ++row) {
    fields.reuse();
    nulls.reuse();
    ASSERT_EQ(OB_SUCCESS, reader.next_row(fields, nulls));
    ASSERT_EQ(4, fields.count());
    EXPECT_EQ(expected[row][0], to_std(fields.at(0)));
    if (1 == row) {
      EXPECT_EQ(expected[row][1], to_std(fields.at(1)));
      EXPECT_FALSE(nulls.at(1));
    } else if (2 == row) {
      // the third name is stored as NULL
      EXPECT_TRUE(nulls.at(1));
    } else {
      EXPECT_EQ(expected[row][1], to_std(fields.at(1)));
    }
    EXPECT_EQ(expected[row][2], to_std(fields.at(2)));
    EXPECT_EQ(expected[row][3], to_std(fields.at(3)));
  }
  fields.reuse();
  nulls.reuse();
  EXPECT_EQ(OB_ITER_END, reader.next_row(fields, nulls));
  reader.close();
}

TEST_F(ObFileScanUtilsTest, parquet_projection_skips_unneeded_columns)
{
  const std::string path = write_file(
      "projected.parquet",
      std::string(reinterpret_cast<const char *>(kParquetFixture), kParquetFixtureLen));
  ObFileScanOptions opt;
  opt.format_ = OB_FILE_FORMAT_PARQUET;
  ObParquetReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(to_str(path), opt, alloc_));
  ASSERT_EQ(4, reader.column_count());
  ObSEArray<bool, 8> needed;
  ASSERT_EQ(OB_SUCCESS, needed.push_back(false));
  ASSERT_EQ(OB_SUCCESS, needed.push_back(false));
  ASSERT_EQ(OB_SUCCESS, needed.push_back(true));
  ASSERT_EQ(OB_SUCCESS, needed.push_back(false));
  ASSERT_EQ(OB_SUCCESS, reader.set_needed_columns(needed));
  ObSEArray<ObString, 8> fields;
  ObSEArray<bool, 8> nulls;
  ASSERT_EQ(OB_SUCCESS, reader.next_row(fields, nulls));
  // pruned columns keep an empty slot so the row stays aligned
  ASSERT_EQ(4, fields.count());
  EXPECT_TRUE(fields.at(0).empty());
  EXPECT_EQ("1.5", to_std(fields.at(2)));
  ObSEArray<bool, 8> wrong;
  ASSERT_EQ(OB_SUCCESS, wrong.push_back(true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, reader.set_needed_columns(wrong));
  reader.close();
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
