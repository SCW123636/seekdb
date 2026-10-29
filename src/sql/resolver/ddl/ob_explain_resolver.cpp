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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ob_resolver.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
const static int64_t EXPLAIN_FORMAT = 0;
const static int64_t EXPLAIN_DISPLAY_OPTION = 1;
const static int64_t EXPLAIN_INTO_TABLE = 2;
const static int64_t EXPLAIN_STMT_ID = 3;
const static int64_t EXPLAIN_CHILD_STMT = 4;

namespace
{
int push_describe_text(common::ObIAllocator &alloc, const common::ObString &text,
                       common::ObIArray<common::ObString> &rows)
{
  int ret = OB_SUCCESS;
  ObString copy;
  if (OB_FAIL(ob_write_string(alloc, text, copy))) {
  } else if (OB_FAIL(rows.push_back(copy))) {
  }
  return ret;
}

int push_describe_text(common::ObIAllocator &alloc, const char *text,
                       common::ObIArray<common::ObString> &rows)
{
  return push_describe_text(alloc, ObString::make_string(text), rows);
}

// DuckDB style type name of a file column.
void file_column_type_name(const ObObjType type, const char *&name)
{
  switch (type) {
    case ObTinyIntType: name = "BOOLEAN"; break;
    case ObIntType: name = "BIGINT"; break;
    case ObInt32Type: name = "INTEGER"; break;
    case ObDoubleType: name = "DOUBLE"; break;
    case ObFloatType: name = "FLOAT"; break;
    case ObDateType: name = "DATE"; break;
    case ObDateTimeType: name = "DATETIME"; break;
    default: name = "VARCHAR"; break;
  }
}

bool file_column_nullable(const ObSelectStmt &select, const ObString &name)
{
  bool nullable = false;
  for (int64_t i = 0; !nullable && i < select.get_table_size(); ++i) {
    const TableItem *item = select.get_table_item(i);
    if (NULL != item && TableItem::JSON_TABLE == item->type_ && NULL != item->json_table_def_
        && !item->json_table_def_->file_options_.empty()) {
      const ObJsonTableDef *def = item->json_table_def_;
      // index 0 is the hidden root column
      for (int64_t c = 1; !nullable && c < def->all_cols_.count(); ++c) {
        const ObJtColBaseInfo *col = def->all_cols_.at(c);
        if (NULL != col && ObCharset::case_insensitive_equal(col->col_name_, name)) {
          nullable = (0 != col->is_nullable_);
        }
      }
    }
  }
  return nullable;
}

// DESCRIBE of a query over file tables prints the inferred schema the way
// DuckDB does: column_name, column_type, null, key, default, extra.
int build_file_describe(common::ObIAllocator &alloc, ObDMLStmt *child, ObExplainStmt &explain)
{
  int ret = OB_SUCCESS;
  if (NULL == child || !child->is_select_stmt()) {
  } else {
    ObSelectStmt *select = static_cast<ObSelectStmt *>(child);
    bool has_file_table = false;
    for (int64_t i = 0; !has_file_table && i < select->get_table_size(); ++i) {
      const TableItem *item = select->get_table_item(i);
      if (NULL != item && TableItem::JSON_TABLE == item->type_
          && NULL != item->json_table_def_ && !item->json_table_def_->file_options_.empty()) {
        has_file_table = true;
      }
    }
    if (has_file_table && select->get_select_item_size() > 0) {
      common::ObIArray<common::ObString> &rows = explain.get_describe_rows();
      for (int64_t i = 0; OB_SUCC(ret) && i < select->get_select_item_size(); ++i) {
        const SelectItem &item = select->get_select_item(i);
        ObString name = item.alias_name_;
        if (name.empty()) {
          name = item.expr_name_;
        }
        if (name.empty() && NULL != item.expr_) {
          name = item.expr_->get_expr_name();
        }
        const char *type_name = NULL;
        file_column_type_name(NULL == item.expr_ ? ObVarcharType : item.expr_->get_data_type(),
                              type_name);
        const char *nullable = file_column_nullable(*select, name) ? "YES" : "NO";
        ret = push_describe_text(alloc, name, rows);
        if (OB_SUCC(ret)) { ret = push_describe_text(alloc, type_name, rows); }
        if (OB_SUCC(ret)) { ret = push_describe_text(alloc, nullable, rows); }
        for (int64_t c = 0; OB_SUCC(ret) && c < 3; ++c) {
          ret = push_describe_text(alloc, "NULL", rows);
        }
      }
      if (OB_SUCC(ret)) {
        explain.set_file_describe(true);
      }
    }
  }
  return ret;
}
} // namespace

int ObExplainResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObExplainStmt *explain_stmt = NULL;
  if (!(T_EXPLAIN == parse_tree.type_  && 5 == parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXPLAIN syntax",
             "type", parse_tree.type_,
             "# of child", parse_tree.num_child_);
  } else if (NULL == (explain_stmt = create_stmt<ObExplainStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create explain stmt");
  } else {
    ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    ObStmt *child_stmt = NULL;
    ObExplainDisplayOpt opt;
    opt.with_tree_line_ = true;
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for stmt");
    } else if (!(T_SELECT == child_node->type_
                 || T_DELETE == child_node->type_
                 || T_INSERT == child_node->type_
                 || T_UPDATE == child_node->type_
                 || T_REPLACE == child_node->type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid stmt type for EXPLAIN", "type", child_node->type_);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "stmt type for EXPLAIN");
    } else {
      ObResolver resolver(params_);
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                   *child_node,
                                   child_stmt))) {
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate stmt for child stmt");
      } else {
        explain_stmt->set_explain_query_stmt(static_cast<ObDMLStmt *>(child_stmt));
        if (OB_FAIL(build_file_describe(*params_.allocator_, static_cast<ObDMLStmt *>(child_stmt), *explain_stmt))) {
          LOG_WARN("build file describe failed", K(ret));
        }
        ParseNode *format = parse_tree.children_[EXPLAIN_FORMAT];
        if (NULL != format) {
          switch (format->type_) {
          case T_OUTLINE:
          {
            explain_stmt->set_explain_format(EXPLAIN_OUTLINE);
            opt.with_tree_line_ = true;
          } break;
          case T_EXTENDED:
          {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED);
            opt.with_tree_line_ = true;
          } break;
          case T_PARTITIONS:
          {
            explain_stmt->set_explain_format(EXPLAIN_PARTITIONS);
            opt.with_tree_line_ = true;
          } break;
          case T_TRADITIONAL:
          {
            explain_stmt->set_explain_format(EXPLAIN_TRADITIONAL);
            opt.with_tree_line_ = true;
          } break;
          case T_FORMAT_JSON:
          {
            explain_stmt->set_explain_format(EXPLAIN_FORMAT_JSON);
            opt.with_tree_line_ = false;
          } break;
          case T_BASIC: {
            explain_stmt->set_explain_format(EXPLAIN_BASIC);
            opt.with_tree_line_ = true;
            break;
          }
          case T_EXTENDED_NOADDR: {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED_NOADDR);
            opt.with_tree_line_ = true;
            break;
          }
          case T_PLANREGRESS: {
            explain_stmt->set_explain_format(EXPLAIN_PLANREGRESS);
            opt.with_tree_line_ = true;
            break;
          }
          default:
            ret = OB_ERROR;
            LOG_WARN("unknown explain format", "format", format->type_);
            break;
          }
        }
      }
    }
    ParseNode *opt_node = parse_tree.children_[EXPLAIN_DISPLAY_OPTION];
    if (OB_SUCC(ret) && NULL != opt_node) {
      switch (opt_node->type_) {
      case T_PRETTY_COLOR:
        opt.with_color_ = true;
        // pass
      case T_PRETTY:
        opt.with_tree_line_ = true;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      explain_stmt->set_display_opt(opt);
    }
    ParseNode *into_table_node = parse_tree.children_[EXPLAIN_INTO_TABLE];
    if (OB_SUCC(ret) && NULL != into_table_node) {
      ObString into_table(into_table_node->str_len_, into_table_node->str_value_);
      explain_stmt->set_into_table(into_table);
    }
    ParseNode *statement_id_node = parse_tree.children_[EXPLAIN_STMT_ID];
    if (OB_SUCC(ret) && NULL != statement_id_node) {
      ObString statement_id(statement_id_node->str_len_, statement_id_node->str_value_);
      explain_stmt->set_statement_id(statement_id);
    }
  }

  return ret;
}
