// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "core/metadata.h"

namespace gvdb {
namespace core {

// ============================================================================
// Filter Expression Abstract Syntax Tree (AST)
// ============================================================================

/**
 * @brief Base class for all filter expression nodes
 *
 * Filter expressions form an Abstract Syntax Tree (AST) that can be evaluated
 * against metadata to determine if a vector matches the filter criteria.
 *
 * Example AST for: price < 100 AND brand = 'Nike'
 *
 *         AND
 *        /   \
 *       <     =
 *      / \   / \
 *  price 100 brand 'Nike'
 */
class FilterNode {
 public:
  virtual ~FilterNode() = default;

  /**
   * @brief Evaluate this filter node against metadata
   * @param metadata The metadata to test
   * @return true if metadata matches the filter
   */
  virtual bool Evaluate(const Metadata& metadata) const = 0;
};

/**
 * @brief Comparison filter node (e.g., price < 100, brand = 'Nike')
 */
class ComparisonNode : public FilterNode {
 public:
  ComparisonNode(std::string field, ComparisonOp op, MetadataValue value);

  bool Evaluate(const Metadata& metadata) const override;

  const std::string& field() const { return field_; }
  ComparisonOp op() const { return op_; }
  const MetadataValue& value() const { return value_; }

 private:
  std::string field_;
  ComparisonOp op_;
  MetadataValue value_;
};

/**
 * @brief IN filter node (e.g., brand IN ('Nike', 'Adidas', 'Puma'))
 */
class InNode : public FilterNode {
 public:
  InNode(std::string field, bool negate, std::vector<MetadataValue> values);

  bool Evaluate(const Metadata& metadata) const override;

  const std::string& field() const { return field_; }
  bool is_negated() const { return negate_; }
  const std::vector<MetadataValue>& values() const { return values_; }

 private:
  std::string field_;
  bool negate_;  // true for NOT IN
  std::vector<MetadataValue> values_;
};

/**
 * @brief Logical operator node (AND, OR, NOT)
 */
class LogicalNode : public FilterNode {
 public:
  LogicalNode(LogicalOp op, std::unique_ptr<FilterNode> left,
              std::unique_ptr<FilterNode> right = nullptr);

  bool Evaluate(const Metadata& metadata) const override;

  LogicalOp op() const { return op_; }
  const FilterNode& left() const { return *left_; }
  const FilterNode& right() const { return *right_; }

 private:
  LogicalOp op_;
  std::unique_ptr<FilterNode> left_;
  std::unique_ptr<FilterNode> right_;  // nullptr for NOT operator
};

// ============================================================================
// Filter Parser
// ============================================================================

/**
 * @brief Parse SQL-like filter expressions into AST
 *
 * Supported syntax:
 * - Comparisons: field = value, field < value, field >= value, etc.
 * - LIKE: name LIKE 'Nike%', brand NOT LIKE '%test%'
 * - IN: category IN ('shoes', 'apparel'), status NOT IN (1, 2, 3)
 * - Logical: expr AND expr, expr OR expr, NOT expr
 * - Grouping: (expr AND expr) OR expr
 *
 * Literals:
 * - Strings in single or double quotes; a backslash escapes the next char
 * - Integers are int64 with an optional sign; out-of-range values are errors
 * - Doubles contain a '.', must be finite, and have no exponent notation
 * - Booleans are lowercase true / false
 *
 * The whole input must be consumed: trailing text is an error rather than
 * being silently ignored. Nesting through parentheses and NOT is limited to
 * kMaxNestingDepth; AND / OR chains of any length are built as balanced trees
 * so evaluating them never recurses deeper than O(log n).
 *
 * Examples:
 * - "price < 100"
 * - "price < 100 AND brand = 'Nike'"
 * - "category IN ('shoes', 'apparel') AND price >= 50"
 * - "(price < 100 OR discount > 20) AND in_stock = true"
 * - "brand LIKE 'Nike%' AND NOT (price > 200)"
 */
class FilterParser {
 public:
  // Maximum nesting of parenthesized groups and NOT operators.
  static constexpr int kMaxNestingDepth = 64;

  /**
   * @brief Parse a filter expression string into an AST
   * @param expression SQL-like filter expression
   * @return AST root node, or InvalidArgument for any malformed input
   */
  static absl::StatusOr<std::unique_ptr<FilterNode>> parse(
      const std::string& expression);

 private:
  FilterParser(const std::string& expression);

  // Parsing methods (recursive descent)
  absl::StatusOr<std::unique_ptr<FilterNode>> ParseExpression();
  absl::StatusOr<std::unique_ptr<FilterNode>> ParseOrExpression();
  absl::StatusOr<std::unique_ptr<FilterNode>> ParseAndExpression();
  absl::StatusOr<std::unique_ptr<FilterNode>> ParseNotExpression();
  absl::StatusOr<std::unique_ptr<FilterNode>> ParsePrimaryExpression();
  absl::StatusOr<std::unique_ptr<FilterNode>> ParseComparison();
  absl::StatusOr<std::unique_ptr<FilterNode>> ParseInExpression(
      const std::string& field, bool negate);

  // Token management
  void SkipWhitespace();
  bool Match(const std::string& keyword);
  bool MatchOperator(const std::string& op);
  absl::StatusOr<std::string> ParseIdentifier();
  absl::StatusOr<MetadataValue> ParseValue();
  absl::StatusOr<ComparisonOp> ParseComparisonOperator();

  // Helper methods
  char Peek() const;
  char Advance();
  bool IsAtEnd() const;
  std::string CurrentContext() const;

  // Enter one level of parenthesis / NOT nesting; fails past kMaxNestingDepth.
  absl::Status EnterNesting();

  std::string expression_;
  size_t pos_;
  int depth_;
};

}  // namespace core
}  // namespace gvdb