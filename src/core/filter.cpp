// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "core/filter.h"
#include "core/status.h"

#include <algorithm>
#include <cmath>
#include <string_view>
#include <utility>

#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"

namespace gvdb::core {

namespace {

// Combine the operands of an AND / OR chain into a balanced tree, keeping
// their left-to-right order, so evaluation, index resolution and destruction
// recurse O(log n) deep instead of O(n).
std::unique_ptr<FilterNode> BuildBalancedChain(LogicalOp op,
                                               std::vector<std::unique_ptr<FilterNode>>& operands,
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

ComparisonNode::ComparisonNode(std::string field, ComparisonOp op, MetadataValue value)
    : field_(std::move(field)), op_(op), value_(std::move(value)) {}

bool ComparisonNode::Evaluate(const Metadata& metadata) const {
  auto it = metadata.find(field_);
  if (it == metadata.end()) {
    return false;  // Field not present
  }

  return CompareMetadataValues(it->second, op_, value_);
}

// ============================================================================
// InNode Implementation
// ============================================================================

InNode::InNode(std::string field, bool negate, std::vector<MetadataValue> values)
    : field_(std::move(field)), negate_(negate), values_(std::move(values)) {}

bool InNode::Evaluate(const Metadata& metadata) const {
  auto it = metadata.find(field_);
  if (it == metadata.end()) {
    return false;  // Field not present
  }

  return CompareMetadataValueInList(it->second, negate_, values_);
}

// ============================================================================
// LogicalNode Implementation
// ============================================================================

LogicalNode::LogicalNode(LogicalOp op, std::unique_ptr<FilterNode> left,
                         std::unique_ptr<FilterNode> right)
    : op_(op), left_(std::move(left)), right_(std::move(right)) {}

bool LogicalNode::Evaluate(const Metadata& metadata) const {
  switch (op_) {
    case LogicalOp::AND:
      return left_->Evaluate(metadata) && right_->Evaluate(metadata);
    case LogicalOp::OR:
      return left_->Evaluate(metadata) || right_->Evaluate(metadata);
    case LogicalOp::NOT:
      return !left_->Evaluate(metadata);
  }
  return false;
}

// ============================================================================
// FilterParser Implementation
// ============================================================================

FilterParser::FilterParser(std::string expression) : expression_(std::move(expression)) {}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::parse(const std::string& expression) {
  FilterParser parser(expression);
  auto root = parser.ParseExpression();
  if (!root.ok()) {
    return root.status();
  }

  parser.SkipWhitespace();
  if (!parser.IsAtEnd()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected input at position ", parser.pos_, ": ", parser.CurrentContext()));
  }
  return root;
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::ParseExpression() {
  return ParseOrExpression();
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::ParseOrExpression() {
  std::vector<std::unique_ptr<FilterNode>> operands;
  do {
    auto operand = ParseAndExpression();
    if (!operand.ok()) {
      return operand.status();
    }
    operands.push_back(std::move(*operand));
  } while (Match("OR"));

  return BuildBalancedChain(LogicalOp::OR, operands, 0, operands.size());
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::ParseAndExpression() {
  std::vector<std::unique_ptr<FilterNode>> operands;
  do {
    auto operand = ParseNotExpression();
    if (!operand.ok()) {
      return operand.status();
    }
    operands.push_back(std::move(*operand));
  } while (Match("AND"));

  return BuildBalancedChain(LogicalOp::AND, operands, 0, operands.size());
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::ParseNotExpression() {
  if (Match("NOT")) {
    RETURN_IF_ERROR(EnterNesting());

    auto expr = ParseNotExpression();
    if (!expr.ok()) {
      return expr.status();
    }
    --depth_;

    return std::make_unique<LogicalNode>(LogicalOp::NOT, std::move(*expr));
  }

  return ParsePrimaryExpression();
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::ParsePrimaryExpression() {
  SkipWhitespace();

  // Handle parenthesized expressions
  if (Peek() == '(') {
    Advance();  // consume '('
    RETURN_IF_ERROR(EnterNesting());

    auto expr = ParseExpression();
    if (!expr.ok()) {
      return expr.status();
    }

    SkipWhitespace();
    if (Peek() != ')') {
      return absl::InvalidArgumentError(
          absl::StrCat("Expected ')' at position ", pos_, ": ", CurrentContext()));
    }
    Advance();  // consume ')'
    --depth_;

    return expr;
  }

  // Parse comparison or IN expression
  return ParseComparison();
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::ParseComparison() {
  auto field = ParseIdentifier();
  if (!field.ok()) {
    return field.status();
  }

  SkipWhitespace();

  // Check for IN/NOT IN and NOT LIKE
  if (Match("NOT")) {
    SkipWhitespace();
    if (Match("IN")) {
      return ParseInExpression(*field, true);
    } else if (Match("LIKE")) {
      SkipWhitespace();
      auto value = ParseValue();
      if (!value.ok()) {
        return value.status();
      }
      return std::make_unique<ComparisonNode>(*field, ComparisonOp::NOT_LIKE, *value);
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("Expected 'IN' or 'LIKE' after 'NOT' at position ", pos_));
    }
  }

  if (Match("IN")) {
    return ParseInExpression(*field, false);
  }

  // Parse comparison operator
  auto op = ParseComparisonOperator();
  if (!op.ok()) {
    return op.status();
  }

  SkipWhitespace();

  // Parse value
  auto value = ParseValue();
  if (!value.ok()) {
    return value.status();
  }

  return std::make_unique<ComparisonNode>(*field, *op, *value);
}

absl::StatusOr<std::unique_ptr<FilterNode>> FilterParser::ParseInExpression(
    const std::string& field, bool negate) {
  SkipWhitespace();

  if (Peek() != '(') {
    return absl::InvalidArgumentError(absl::StrCat("Expected '(' after IN at position ", pos_));
  }
  Advance();  // consume '('

  std::vector<MetadataValue> values;

  SkipWhitespace();
  if (Peek() == ')') {
    return absl::InvalidArgumentError("IN list cannot be empty");
  }

  while (true) {
    SkipWhitespace();

    auto value = ParseValue();
    if (!value.ok()) {
      return value.status();
    }
    values.push_back(*value);

    SkipWhitespace();

    if (Peek() == ')') {
      Advance();  // consume ')'
      break;
    }

    if (Peek() != ',') {
      return absl::InvalidArgumentError(
          absl::StrCat("Expected ',' or ')' in IN list at position ", pos_));
    }
    Advance();  // consume ','
  }

  return std::make_unique<InNode>(field, negate, std::move(values));
}

void FilterParser::SkipWhitespace() {
  while (!IsAtEnd() && absl::ascii_isspace(Peek())) {
    Advance();
  }
}

bool FilterParser::Match(const std::string& keyword) {
  SkipWhitespace();

  size_t start = pos_;
  size_t i = 0;

  while (i < keyword.size() && !IsAtEnd() && absl::ascii_toupper(Peek()) == keyword[i]) {
    Advance();
    ++i;
  }

  if (i == keyword.size()) {
    // Check that keyword is followed by non-identifier character
    if (!IsAtEnd() && (absl::ascii_isalnum(Peek()) || Peek() == '_')) {
      pos_ = start;  // Rollback
      return false;
    }
    return true;
  }

  pos_ = start;  // Rollback
  return false;
}

bool FilterParser::MatchOperator(const std::string& op) {
  SkipWhitespace();

  size_t start = pos_;
  size_t i = 0;

  while (i < op.size() && !IsAtEnd() && Peek() == op[i]) {
    Advance();
    ++i;
  }

  if (i == op.size()) {
    return true;
  }

  pos_ = start;  // Rollback
  return false;
}

absl::StatusOr<std::string> FilterParser::ParseIdentifier() {
  SkipWhitespace();

  if (IsAtEnd() || (!absl::ascii_isalpha(Peek()) && Peek() != '_')) {
    return absl::InvalidArgumentError(absl::StrCat("Expected identifier at position ", pos_));
  }

  std::string identifier;
  while (!IsAtEnd() && (absl::ascii_isalnum(Peek()) || Peek() == '_')) {
    identifier += Advance();
  }

  return identifier;
}

absl::StatusOr<MetadataValue> FilterParser::ParseValue() {
  SkipWhitespace();

  if (IsAtEnd()) {
    return absl::InvalidArgumentError("Unexpected end of expression");
  }

  // String literal
  if (Peek() == '\'' || Peek() == '"') {
    char quote = Advance();
    std::string value;

    while (!IsAtEnd() && Peek() != quote) {
      if (Peek() == '\\' && !IsAtEnd()) {
        Advance();  // consume backslash
        if (!IsAtEnd()) {
          value += Advance();  // escaped character
        }
      } else {
        value += Advance();
      }
    }

    if (IsAtEnd()) {
      return absl::InvalidArgumentError("Unterminated string literal");
    }

    Advance();  // consume closing quote
    return MetadataValue(value);
  }

  // Boolean - check for literal "true" or "false"
  if (absl::ascii_isalpha(Peek())) {
    size_t start_pos = pos_;
    std::string word;
    while (!IsAtEnd() && absl::ascii_isalpha(Peek())) {
      word += Advance();
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
  if (absl::ascii_isdigit(Peek()) || Peek() == '-' || Peek() == '+') {
    size_t start = pos_;
    if (Peek() == '-' || Peek() == '+') {
      Advance();
    }

    bool has_digit = false;
    bool has_decimal = false;
    while (!IsAtEnd() && (absl::ascii_isdigit(Peek()) || Peek() == '.')) {
      if (Peek() == '.') {
        if (has_decimal) {
          return absl::InvalidArgumentError(
              absl::StrCat("Invalid number format at position ", pos_));
        }
        has_decimal = true;
      } else {
        has_digit = true;
      }
      Advance();
    }

    if (!has_digit) {
      return absl::InvalidArgumentError(absl::StrCat("Invalid number format at position ", start));
    }

    std::string_view literal(expression_.data() + start, pos_ - start);
    if (has_decimal) {
      double value;
      if (!absl::SimpleAtod(literal, &value) || !std::isfinite(value)) {
        return absl::InvalidArgumentError(
            absl::StrCat("Numeric literal at position ", start, " is out of range for a double"));
      }
      return MetadataValue(value);
    }

    int64_t value;
    if (!absl::SimpleAtoi(literal, &value)) {
      return absl::InvalidArgumentError(
          absl::StrCat("Integer literal at position ", start, " is out of range for int64"));
    }
    return MetadataValue(value);
  }

  return absl::InvalidArgumentError(
      absl::StrCat("Expected value at position ", pos_, ": ", CurrentContext()));
}

absl::StatusOr<ComparisonOp> FilterParser::ParseComparisonOperator() {
  SkipWhitespace();

  // Check for LIKE/NOT LIKE first
  if (Match("NOT")) {
    SkipWhitespace();
    if (!Match("LIKE")) {
      return absl::InvalidArgumentError("Expected 'LIKE' after 'NOT'");
    }
    return ComparisonOp::NOT_LIKE;
  }

  if (Match("LIKE")) {
    return ComparisonOp::LIKE;
  }

  // Check two-character operators first
  if (MatchOperator("!=")) return ComparisonOp::NOT_EQUAL;
  if (MatchOperator("<=")) return ComparisonOp::LESS_EQUAL;
  if (MatchOperator(">=")) return ComparisonOp::GREATER_EQUAL;

  // Single-character operators
  if (MatchOperator("=")) return ComparisonOp::EQUAL;
  if (MatchOperator("<")) return ComparisonOp::LESS_THAN;
  if (MatchOperator(">")) return ComparisonOp::GREATER_THAN;

  return absl::InvalidArgumentError(
      absl::StrCat("Expected comparison operator at position ", pos_));
}

char FilterParser::Peek() const {
  if (IsAtEnd()) {
    return '\0';
  }
  return expression_[pos_];
}

char FilterParser::Advance() { return expression_[pos_++]; }

bool FilterParser::IsAtEnd() const { return pos_ >= expression_.size(); }

std::string FilterParser::CurrentContext() const {
  size_t start = (pos_ > 10) ? pos_ - 10 : 0;
  size_t end = std::min(pos_ + 10, expression_.size());
  return expression_.substr(start, end - start);
}

absl::Status FilterParser::EnterNesting() {
  if (++depth_ > kMaxNestingDepth) {
    return absl::InvalidArgumentError(absl::StrCat("Filter nesting exceeds the maximum depth of ",
                                                   kMaxNestingDepth, " at position ", pos_));
  }
  return absl::OkStatus();
}

}  // namespace gvdb::core
