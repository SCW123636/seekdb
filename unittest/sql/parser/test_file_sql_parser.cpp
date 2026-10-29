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
#include <cstring>
#include <string>
#include "common/sql_mode/ob_sql_mode.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "sql/parser/ob_parser.h"

namespace oceanbase
{
namespace sql
{

using namespace common;

// Grammar level tests of the file table functions. The resolver (schema
// inference, secure_file_priv, enable_file_sql) is covered by the file scan
// unit tests and the mysqltest suite.
class ObFileSqlParserTest : public ::testing::Test
{
protected:
  ObFileSqlParserTest() : parser_(alloc_, DEFAULT_MYSQL_MODE) {}
  ~ObFileSqlParserTest() override {}

  static ObString to_str(const std::string &text)
  {
    return ObString(static_cast<int64_t>(text.length()), text.data());
  }

  static std::string text_of(const ParseNode *node)
  {
    if (NULL == node) {
      return std::string();
    }
    if (NULL != node->str_value_ && node->str_len_ > 0) {
      return std::string(node->str_value_, static_cast<size_t>(node->str_len_));
    }
    if (NULL != node->raw_text_ && node->text_len_ > 0) {
      return std::string(node->raw_text_, static_cast<size_t>(node->text_len_));
    }
    return std::string();
  }

  static ParseNode *find_first(ParseNode *node, const ObItemType type)
  {
    ParseNode *found = NULL;
    if (NULL != node) {
      if (type == node->type_) {
        found = node;
      } else {
        for (int32_t i = 0; NULL == found && NULL != node->children_ && i < node->num_child_; ++i) {
          found = find_first(node->children_[i], type);
        }
      }
    }
    return found;
  }

  static int64_t count_of(ParseNode *node, const ObItemType type)
  {
    int64_t count = 0;
    if (NULL != node) {
      if (type == node->type_) {
        ++count;
      }
      for (int32_t i = 0; NULL != node->children_ && i < node->num_child_; ++i) {
        count += count_of(node->children_[i], type);
      }
    }
    return count;
  }

  ParseNode *parse_ok(const std::string &sql)
  {
    ParseResult parse_result;
    EXPECT_EQ(OB_SUCCESS, parser_.parse(to_str(sql), parse_result));
    return parse_result.result_tree_;
  }

  void TearDown() override { alloc_.reset(); }

  ObArenaAllocator alloc_;
  ObParser parser_;
};

TEST_F(ObFileSqlParserTest, positional_path_argument)
{
  ParseNode *root = parse_ok("select * from read_csv('/home/lenovo2/fdata/a.csv')");
  ParseNode *file = find_first(root, T_FILE_TABLE_EXPRESSION);
  ASSERT_NE(nullptr, file);
  ASSERT_EQ(3, file->num_child_);
  EXPECT_EQ("read_csv", text_of(file->children_[0]));

  ParseNode *args = file->children_[1];
  ASSERT_NE(nullptr, args);
  EXPECT_EQ(T_EXPR_LIST, args->type_);
  ASSERT_EQ(1, args->num_child_);
  ParseNode *arg = args->children_[0];
  ASSERT_NE(nullptr, arg);
  EXPECT_EQ(T_FILE_TABLE_ARG, arg->type_);
  ASSERT_EQ(2, arg->num_child_);
  // a positional argument has no name
  EXPECT_EQ(nullptr, arg->children_[0]);
  EXPECT_EQ("/home/lenovo2/fdata/a.csv", text_of(arg->children_[1]));
  // no alias
  EXPECT_EQ("", text_of(file->children_[2]));
}

TEST_F(ObFileSqlParserTest, named_options)
{
  ParseNode *root = parse_ok(
      "select * from read_csv('/tmp/a.csv', header => false, delimiter => '|',"
      " max_line_size => 4096, nullstr => 'NULL')");
  ParseNode *file = find_first(root, T_FILE_TABLE_EXPRESSION);
  ASSERT_NE(nullptr, file);
  ParseNode *args = file->children_[1];
  ASSERT_NE(nullptr, args);
  ASSERT_EQ(5, args->num_child_);
  const char *names[] = {NULL, "header", "delimiter", "max_line_size", "nullstr"};
  const char *values[] = {"/tmp/a.csv", "false", "|", "4096", "NULL"};
  for (int64_t i = 0; i < args->num_child_; ++i) {
    ParseNode *arg = args->children_[i];
    ASSERT_NE(nullptr, arg);
    ASSERT_EQ(T_FILE_TABLE_ARG, arg->type_);
    ASSERT_EQ(2, arg->num_child_);
    EXPECT_EQ(NULL == names[i] ? std::string() : std::string(names[i]),
              text_of(arg->children_[0]));
    EXPECT_EQ(values[i], text_of(arg->children_[1]));
  }
}

TEST_F(ObFileSqlParserTest, all_file_functions)
{
  const char *functions[] = {"read_csv", "read_json", "read_ndjson", "read_parquet", "glob"};
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(functions) / sizeof(functions[0])); ++i) {
    const std::string sql = std::string("select * from ") + functions[i] + "('/tmp/f')";
    ParseNode *root = parse_ok(sql);
    ParseNode *file = find_first(root, T_FILE_TABLE_EXPRESSION);
    ASSERT_NE(nullptr, file);
    EXPECT_EQ(functions[i], text_of(file->children_[0]));
    ASSERT_NE(nullptr, file->children_[1]);
    EXPECT_EQ(1, file->children_[1]->num_child_);
  }
}

TEST_F(ObFileSqlParserTest, alias_and_join)
{
  ParseNode *root = parse_ok("select * from read_csv('/tmp/a.csv') as t");
  ParseNode *file = find_first(root, T_FILE_TABLE_EXPRESSION);
  ASSERT_NE(nullptr, file);
  EXPECT_EQ("t", text_of(file->children_[2]));

  root = parse_ok(
      "select * from read_csv('/tmp/a.csv') a"
      " join read_ndjson('/tmp/b.jsonl') b on a.id = b.id");
  EXPECT_EQ(2, count_of(root, T_FILE_TABLE_EXPRESSION));
}

TEST_F(ObFileSqlParserTest, invalid_syntax_is_rejected)
{
  const char *bad_sqls[] = {
      "select * from read_csv()",
      "select * from read_csv('/tmp/a.csv' header => true)",
      "select * from read_csv('/tmp/a.csv',, header => true)",
  };
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(bad_sqls) / sizeof(bad_sqls[0])); ++i) {
    ParseResult parse_result;
    EXPECT_NE(OB_SUCCESS, parser_.parse(to_str(bad_sqls[i]), parse_result));
    EXPECT_EQ(nullptr, find_first(parse_result.result_tree_, T_FILE_TABLE_EXPRESSION));
  }
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
