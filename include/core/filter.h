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
  virtual bool evaluate(const Metadata& metadata) const = 0;

  /**
   * @brief Convert this filter to a string representation
   */
  virtual std::string to_string() const = 0;
};

/**
 * @brief Comparison filter node (e.g., price < 100, brand = 'Nike')
 */
class ComparisonNode : public FilterNode {
 public:
  ComparisonNode(std::string field, ComparisonOp op, MetadataValue value);

  bool evaluate(const Metadata& metadata) const override;
  std::string to_string() const override;

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

  bool evaluate(const Metadata& metadata) const override;
  std::string to_string() const override;

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

  bool evaluate(const Metadata& metadata) const override;
  std::string to_string() const override;

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
 * Examples:
 * - "price < 100"
 * - "price < 100 AND brand = 'Nike'"
 * - "category IN ('shoes', 'apparel') AND price >= 50"
 * - "(price < 100 OR discount > 20) AND in_stock = true"
 * - "brand LIKE 'Nike%' AND NOT (price > 200)"
 */
class FilterParser {
 public:
  /**
   * @brief Parse a filter expression string into an AST
   * @param expression SQL-like filter expression
   * @return AST root node or error
   */
  static absl::StatusOr<std::unique_ptr<FilterNode>> parse(
      const std::string& expression);

 private:
  FilterParser(const std::string& expression);

  // Parsing methods (recursive descent)
  absl::StatusOr<std::unique_ptr<FilterNode>> parse_expression();
  absl::StatusOr<std::unique_ptr<FilterNode>> parse_or_expression();
  absl::StatusOr<std::unique_ptr<FilterNode>> parse_and_expression();
  absl::StatusOr<std::unique_ptr<FilterNode>> parse_not_expression();
  absl::StatusOr<std::unique_ptr<FilterNode>> parse_primary_expression();
  absl::StatusOr<std::unique_ptr<FilterNode>> parse_comparison();
  absl::StatusOr<std::unique_ptr<FilterNode>> parse_in_expression(
      const std::string& field, bool negate);

  // Token management
  void skip_whitespace();
  bool match(const std::string& keyword);
  bool match_operator(const std::string& op);
  absl::StatusOr<std::string> parse_identifier();
  absl::StatusOr<MetadataValue> parse_value();
  absl::StatusOr<ComparisonOp> parse_comparison_operator();

  // Helper methods
  char peek() const;
  char advance();
  bool is_at_end() const;
  std::string current_context() const;

  std::string expression_;
  size_t pos_;
};

// ============================================================================
// Filter Utilities
// ============================================================================

/**
 * @brief Evaluate a filter expression against metadata
 * @param filter_expr SQL-like filter expression
 * @param metadata The metadata to test
 * @return true if metadata matches, false if not, error on parse failure
 */
absl::StatusOr<bool> evaluate_filter(const std::string& filter_expr,
                                      const Metadata& metadata);

/**
 * @brief Validate a filter expression (parsing check)
 * @param filter_expr SQL-like filter expression
 * @return OkStatus if valid, error otherwise
 */
absl::Status validate_filter(const std::string& filter_expr);

}  // namespace core
}  // namespace gvdb
