// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "core/filter.h"

#include <algorithm>
#include <cmath>
#include <sstream>
#include <string_view>

#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"

namespace gvdb {
namespace core {

namespace {

// Combine the operands of an AND / OR chain into a balanced tree, keeping
// their left-to-right order, so evaluation, index resolution and destruction
// recurse O(log n) deep instead of O(n).
std::unique_ptr<FilterNode> BuildBalancedChain(
    LogicalOp op, std::vector<std::unique_ptr<FilterNode>>& operands,
    size_t begin, size_t end) {
  if (end - begin == 1) {
    return std::move(operands[begin]);
  }
  size_t mid = begin + (end - begin) / 2;
  auto left = BuildBalancedChain(op, operands, begin, mid);
  auto right = BuildBalancedChain(op, operands, mid, end);
  return std::make_unique<LogicalNode>(op, std::move(left), std::move(right));
}

}  // namespace

// ============================================================================
// ComparisonNode Implementation
// ============================================================================

ComparisonNode::ComparisonNode(std::string field, ComparisonOp op,
                               MetadataValue value)
    : field_(std::move(field)), op_(op), value_(std::move(value)) {}

bool ComparisonNode::evaluate(const Metadata& metadata) const {
  auto it = metadata.find(field_);
  if (it == metadata.end()) {
    return false;  // Field not present
  }

  return compare_metadata_values(it->second, op_, value_);
}

std::string ComparisonNode::to_string() const {
  std::stringstream ss;
  ss << field_ << " ";

  switch (op_) {
    case ComparisonOp::EQUAL: ss << "="; break;
    case ComparisonOp::NOT_EQUAL: ss << "!="; break;
    case ComparisonOp::LESS_THAN: ss << "<"; break;
    case ComparisonOp::LESS_EQUAL: ss << "<="; break;
    case ComparisonOp::GREATER_THAN: ss << ">"; break;
    case ComparisonOp::GREATER_EQUAL: ss << ">="; break;
    case ComparisonOp::LIKE: ss << "LIKE"; break;
    case ComparisonOp::NOT_LIKE: ss << "NOT LIKE"; break;
    default: ss << "?"; break;
  }

  ss << " ";

  if (std::holds_alternative<int64_t>(value_)) {
    ss << std::get<int64_t>(value_);
  } else if (std::holds_alternative<double>(value_)) {
    ss << std::get<double>(value_);
  } else if (std::holds_alternative<std::string>(value_)) {
    ss << "'" << std::get<std::string>(value_) << "'";
  } else if (std::holds_alternative<bool>(value_)) {
    ss << (std::get<bool>(value_) ? "true" : "false");
  }

  return ss.str();
}

// ============================================================================
// InNode Implementation
// ============================================================================

InNode::InNode(std::string field, bool negate,
               std::vector<MetadataValue> values)
    : field_(std::move(field)), negate_(negate), values_(std::move(values)) {}

bool InNode::evaluate(const Metadata& metadata) const {
  auto it = metadata.find(field_);
  if (it == metadata.end()) {
    return false;  // Field not present
  }

  return compare_metadata_value_in_list(it->second, negate_, values_);
}

std::string InNode::to_string() const {
  std::stringstream ss;
  ss << field_ << (negate_ ? " NOT IN (" : " IN (");

  for (size_t i = 0; i < values_.size(); ++i) {
    if (i > 0) ss << ", ";

    const auto& val = values_[i];
    if (std::holds_alternative<int64_t>(val)) {
      ss << std::get<int64_t>(val);
    } else if (std::holds_alternative<double>(val)) {
      ss << std::get<double>(val);
    } else if (std::holds_alternative<std::string>(val)) {
      ss << "'" << std::get<std::string>(val) << "'";
    } else if (std::holds_alternative<bool>(val)) {
      ss << (std::get<bool>(val) ? "true" : "false");
    }
  }

  ss << ")";
  return ss.str();
}

// ============================================================================
// LogicalNode Implementation
// ============================================================================

LogicalNode::LogicalNode(LogicalOp op, std::unique_ptr<FilterNode> left,
                         std::unique_ptr<FilterNode> right)
    : op_(op), left_(std::move(left)), right_(std::move(right)) {}

bool LogicalNode::evaluate(const Metadata& metadata) const {
  switch (op_) {
    case LogicalOp::AND:
      return left_->evaluate(metadata) && right_->evaluate(metadata);
    case LogicalOp::OR:
      return left_->evaluate(metadata) || right_->evaluate(metadata);
    case LogicalOp::NOT:
      return !left_->evaluate(metadata);
  }
  return false;
}

std::string LogicalNode::to_string() const {
  std::stringstream ss;

  switch (op_) {
    case LogicalOp::AND:
      ss << "(" << left_->to_string() << " AND " << right_->to_string() << ")";
      break;
    case LogicalOp::OR:
      ss << "(" << left_->to_string() << " OR " << right_->to_string() << ")";
      break;
    case LogicalOp::NOT:
      ss << "NOT (" << left_->to_string() << ")";
      break;
  }

  return ss.str();
}

// ============================================================================
// FilterParser Implementation
// ============================================================================

FilterParser::FilterParser(const std::string& expression)
    : expression_(expression), pos_(0), depth_(0) {}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::parse(
    const std::string& expression) {
  FilterParser parser(expression);
  auto root = parser.parse_expression();
  if (!root.ok()) {
    return root.status();
  }

  parser.skip_whitespace();
  if (!parser.is_at_end()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected input at position ", parser.pos_, ": ",
                     parser.current_context()));
  }
  return root;
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::parse_expression() {
  return parse_or_expression();
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::parse_or_expression() {
  std::vector<std::unique_ptr<FilterNode>> operands;
  do {
    auto operand = parse_and_expression();
    if (!operand.ok()) {
      return operand.status();
    }
    operands.push_back(std::move(*operand));
  } while (match("OR"));

  return BuildBalancedChain(LogicalOp::OR, operands, 0, operands.size());
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::parse_and_expression() {
  std::vector<std::unique_ptr<FilterNode>> operands;
  do {
    auto operand = parse_not_expression();
    if (!operand.ok()) {
      return operand.status();
    }
    operands.push_back(std::move(*operand));
  } while (match("AND"));

  return BuildBalancedChain(LogicalOp::AND, operands, 0, operands.size());
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::parse_not_expression() {
  if (match("NOT")) {
    auto nesting = enter_nesting();
    if (!nesting.ok()) {
      return nesting;
    }

    auto expr = parse_not_expression();
    if (!expr.ok()) {
      return expr.status();
    }
    --depth_;

    return std::make_unique<LogicalNode>(LogicalOp::NOT, std::move(*expr));
  }

  return parse_primary_expression();
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::parse_primary_expression() {
  skip_whitespace();

  // Handle parenthesized expressions
  if (peek() == '(') {
    advance();  // consume '('
    auto nesting = enter_nesting();
    if (!nesting.ok()) {
      return nesting;
    }

    auto expr = parse_expression();
    if (!expr.ok()) {
      return expr.status();
    }

    skip_whitespace();
    if (peek() != ')') {
      return absl::InvalidArgumentError(
          absl::StrCat("Expected ')' at position ", pos_, ": ", current_context()));
    }
    advance();  // consume ')'
    --depth_;

    return expr;
  }

  // Parse comparison or IN expression
  return parse_comparison();
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::parse_comparison() {
  auto field = parse_identifier();
  if (!field.ok()) {
    return field.status();
  }

  skip_whitespace();

  // Check for IN/NOT IN and NOT LIKE
  if (match("NOT")) {
    skip_whitespace();
    if (match("IN")) {
      return parse_in_expression(*field, true);
    } else if (match("LIKE")) {
      skip_whitespace();
      auto value = parse_value();
      if (!value.ok()) {
        return value.status();
      }
      return std::make_unique<ComparisonNode>(*field, ComparisonOp::NOT_LIKE, *value);
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("Expected 'IN' or 'LIKE' after 'NOT' at position ", pos_));
    }
  }

  if (match("IN")) {
    return parse_in_expression(*field, false);
  }

  // Parse comparison operator
  auto op = parse_comparison_operator();
  if (!op.ok()) {
    return op.status();
  }

  skip_whitespace();

  // Parse value
  auto value = parse_value();
  if (!value.ok()) {
    return value.status();
  }

  return std::make_unique<ComparisonNode>(*field, *op, *value);
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::parse_in_expression(
    const std::string& field, bool negate) {
  skip_whitespace();

  if (peek() != '(') {
    return absl::InvalidArgumentError(
        absl::StrCat("Expected '(' after IN at position ", pos_));
  }
  advance();  // consume '('

  std::vector<MetadataValue> values;

  skip_whitespace();
  if (peek() == ')') {
    return absl::InvalidArgumentError("IN list cannot be empty");
  }

  while (true) {
    skip_whitespace();

    auto value = parse_value();
    if (!value.ok()) {
      return value.status();
    }
    values.push_back(*value);

    skip_whitespace();

    if (peek() == ')') {
      advance();  // consume ')'
      break;
    }

    if (peek() != ',') {
      return absl::InvalidArgumentError(
          absl::StrCat("Expected ',' or ')' in IN list at position ", pos_));
    }
    advance();  // consume ','
  }

  return std::make_unique<InNode>(field, negate, std::move(values));
}

void FilterParser::skip_whitespace() {
  while (!is_at_end() && absl::ascii_isspace(peek())) {
    advance();
  }
}

bool FilterParser::match(const std::string& keyword) {
  skip_whitespace();

  size_t start = pos_;
  size_t i = 0;

  while (i < keyword.size() && !is_at_end() &&
         absl::ascii_toupper(peek()) == keyword[i]) {
    advance();
    ++i;
  }

  if (i == keyword.size()) {
    // Check that keyword is followed by non-identifier character
    if (!is_at_end() && (absl::ascii_isalnum(peek()) || peek() == '_')) {
      pos_ = start;  // Rollback
      return false;
    }
    return true;
  }

  pos_ = start;  // Rollback
  return false;
}

bool FilterParser::match_operator(const std::string& op) {
  skip_whitespace();

  size_t start = pos_;
  size_t i = 0;

  while (i < op.size() && !is_at_end() && peek() == op[i]) {
    advance();
    ++i;
  }

  if (i == op.size()) {
    return true;
  }

  pos_ = start;  // Rollback
  return false;
}

absl::StatusOr<std::string> FilterParser::parse_identifier() {
  skip_whitespace();

  if (is_at_end() || (!absl::ascii_isalpha(peek()) && peek() != '_')) {
    return absl::InvalidArgumentError(
        absl::StrCat("Expected identifier at position ", pos_));
  }

  std::string identifier;
  while (!is_at_end() && (absl::ascii_isalnum(peek()) || peek() == '_')) {
    identifier += advance();
  }

  return identifier;
}

absl::StatusOr<MetadataValue> FilterParser::parse_value() {
  skip_whitespace();

  if (is_at_end()) {
    return absl::InvalidArgumentError("Unexpected end of expression");
  }

  // String literal
  if (peek() == '\'' || peek() == '"') {
    char quote = advance();
    std::string value;

    while (!is_at_end() && peek() != quote) {
      if (peek() == '\\' && !is_at_end()) {
        advance();  // consume backslash
        if (!is_at_end()) {
          value += advance();  // escaped character
        }
      } else {
        value += advance();
      }
    }

    if (is_at_end()) {
      return absl::InvalidArgumentError("Unterminated string literal");
    }

    advance();  // consume closing quote
    return MetadataValue(value);
  }

  // Boolean - check for literal "true" or "false"
  if (absl::ascii_isalpha(peek())) {
    size_t start_pos = pos_;
    std::string word;
    while (!is_at_end() && absl::ascii_isalpha(peek())) {
      word += advance();
    }

    if (word == "true") {
      return MetadataValue(true);
    } else if (word == "false") {
      return MetadataValue(false);
    } else {
      // Not a boolean, rollback
      pos_ = start_pos;
    }
  }

  // Number: optional sign, digits, at most one '.'. The sign is converted
  // together with the digits so INT64_MIN is representable.
  if (absl::ascii_isdigit(peek()) || peek() == '-' || peek() == '+') {
    size_t start = pos_;
    if (peek() == '-' || peek() == '+') {
      advance();
    }

    bool has_digit = false;
    bool has_decimal = false;
    while (!is_at_end() && (absl::ascii_isdigit(peek()) || peek() == '.')) {
      if (peek() == '.') {
        if (has_decimal) {
          return absl::InvalidArgumentError(
              absl::StrCat("Invalid number format at position ", pos_));
        }
        has_decimal = true;
      } else {
        has_digit = true;
      }
      advance();
    }

    if (!has_digit) {
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid number format at position ", start));
    }

    std::string_view literal(expression_.data() + start, pos_ - start);
    if (has_decimal) {
      double value;
      if (!absl::SimpleAtod(literal, &value) || !std::isfinite(value)) {
        return absl::InvalidArgumentError(
            absl::StrCat("Numeric literal at position ", start,
                         " is out of range for a double"));
      }
      return MetadataValue(value);
    }

    int64_t value;
    if (!absl::SimpleAtoi(literal, &value)) {
      return absl::InvalidArgumentError(
          absl::StrCat("Integer literal at position ", start,
                       " is out of range for int64"));
    }
    return MetadataValue(value);
  }

  return absl::InvalidArgumentError(
      absl::StrCat("Expected value at position ", pos_, ": ", current_context()));
}

absl::StatusOr<ComparisonOp> FilterParser::parse_comparison_operator() {
  skip_whitespace();

  // Check for LIKE/NOT LIKE first
  if (match("NOT")) {
    skip_whitespace();
    if (!match("LIKE")) {
      return absl::InvalidArgumentError("Expected 'LIKE' after 'NOT'");
    }
    return ComparisonOp::NOT_LIKE;
  }

  if (match("LIKE")) {
    return ComparisonOp::LIKE;
  }

  // Check two-character operators first
  if (match_operator("!=")) return ComparisonOp::NOT_EQUAL;
  if (match_operator("<=")) return ComparisonOp::LESS_EQUAL;
  if (match_operator(">=")) return ComparisonOp::GREATER_EQUAL;

  // Single-character operators
  if (match_operator("=")) return ComparisonOp::EQUAL;
  if (match_operator("<")) return ComparisonOp::LESS_THAN;
  if (match_operator(">")) return ComparisonOp::GREATER_THAN;

  return absl::InvalidArgumentError(
      absl::StrCat("Expected comparison operator at position ", pos_));
}

char FilterParser::peek() const {
  if (is_at_end()) {
    return '\0';
  }
  return expression_[pos_];
}

char FilterParser::advance() {
  return expression_[pos_++];
}

bool FilterParser::is_at_end() const {
  return pos_ >= expression_.size();
}

std::string FilterParser::current_context() const {
  size_t start = (pos_ > 10) ? pos_ - 10 : 0;
  size_t end = std::min(pos_ + 10, expression_.size());
  return expression_.substr(start, end - start);
}

absl::Status FilterParser::enter_nesting() {
  if (++depth_ > kMaxNestingDepth) {
    return absl::InvalidArgumentError(
        absl::StrCat("Filter nesting exceeds the maximum depth of ",
                     kMaxNestingDepth, " at position ", pos_));
  }
  return absl::OkStatus();
}

// ============================================================================
// Filter Utilities
// ============================================================================

absl::StatusOr<bool> evaluate_filter(const std::string& filter_expr,
                                      const Metadata& metadata) {
  auto filter = FilterParser::parse(filter_expr);
  if (!filter.ok()) {
    return filter.status();
  }

  return (*filter)->evaluate(metadata);
}

absl::Status validate_filter(const std::string& filter_expr) {
  auto filter = FilterParser::parse(filter_expr);
  if (!filter.ok()) {
    return filter.status();
  }
  return absl::OkStatus();
}

}  // namespace core
}  // namespace gvdb