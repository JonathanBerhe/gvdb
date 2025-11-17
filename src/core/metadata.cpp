#include "core/metadata.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <regex>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace core {

// ============================================================================
// Metadata Value Comparison
// ============================================================================

bool compare_metadata_values(
    const MetadataValue& left,
    ComparisonOp op,
    const MetadataValue& right) {

  // Type must match for most comparisons
  if (left.index() != right.index() && op != ComparisonOp::NOT_EQUAL) {
    return false;
  }

  switch (op) {
    case ComparisonOp::EQUAL:
      return left == right;

    case ComparisonOp::NOT_EQUAL:
      return left != right;

    case ComparisonOp::LESS_THAN:
      if (std::holds_alternative<int64_t>(left)) {
        return std::get<int64_t>(left) < std::get<int64_t>(right);
      } else if (std::holds_alternative<double>(left)) {
        return std::get<double>(left) < std::get<double>(right);
      } else if (std::holds_alternative<std::string>(left)) {
        return std::get<std::string>(left) < std::get<std::string>(right);
      }
      return false;

    case ComparisonOp::LESS_EQUAL:
      if (std::holds_alternative<int64_t>(left)) {
        return std::get<int64_t>(left) <= std::get<int64_t>(right);
      } else if (std::holds_alternative<double>(left)) {
        return std::get<double>(left) <= std::get<double>(right);
      } else if (std::holds_alternative<std::string>(left)) {
        return std::get<std::string>(left) <= std::get<std::string>(right);
      }
      return false;

    case ComparisonOp::GREATER_THAN:
      if (std::holds_alternative<int64_t>(left)) {
        return std::get<int64_t>(left) > std::get<int64_t>(right);
      } else if (std::holds_alternative<double>(left)) {
        return std::get<double>(left) > std::get<double>(right);
      } else if (std::holds_alternative<std::string>(left)) {
        return std::get<std::string>(left) > std::get<std::string>(right);
      }
      return false;

    case ComparisonOp::GREATER_EQUAL:
      if (std::holds_alternative<int64_t>(left)) {
        return std::get<int64_t>(left) >= std::get<int64_t>(right);
      } else if (std::holds_alternative<double>(left)) {
        return std::get<double>(left) >= std::get<double>(right);
      } else if (std::holds_alternative<std::string>(left)) {
        return std::get<std::string>(left) >= std::get<std::string>(right);
      }
      return false;

    case ComparisonOp::LIKE:
      if (std::holds_alternative<std::string>(left) &&
          std::holds_alternative<std::string>(right)) {
        return match_like_pattern(
            std::get<std::string>(left),
            std::get<std::string>(right));
      }
      return false;

    case ComparisonOp::NOT_LIKE:
      if (std::holds_alternative<std::string>(left) &&
          std::holds_alternative<std::string>(right)) {
        return !match_like_pattern(
            std::get<std::string>(left),
            std::get<std::string>(right));
      }
      return false;

    default:
      return false;
  }
}

bool compare_metadata_value_in_list(
    const MetadataValue& value,
    bool negate,
    const std::vector<MetadataValue>& list) {

  bool found = std::find(list.begin(), list.end(), value) != list.end();
  return negate ? !found : found;
}

// ============================================================================
// LIKE Pattern Matching
// ============================================================================

bool match_like_pattern(const std::string& value, const std::string& pattern) {
  // Convert SQL LIKE pattern to regex
  // % -> .*
  // _ -> .
  // Escape special regex characters

  std::string regex_pattern;
  regex_pattern.reserve(pattern.size() * 2);
  regex_pattern += '^';

  for (size_t i = 0; i < pattern.size(); ++i) {
    char c = pattern[i];

    if (c == '%') {
      regex_pattern += ".*";
    } else if (c == '_') {
      regex_pattern += '.';
    } else if (c == '\\' && i + 1 < pattern.size()) {
      // Escape sequence
      ++i;
      char next = pattern[i];
      if (next == '%' || next == '_') {
        regex_pattern += next;
      } else {
        regex_pattern += '\\';
        regex_pattern += next;
      }
    } else {
      // Escape regex special characters
      if (c == '.' || c == '*' || c == '+' || c == '?' ||
          c == '^' || c == '$' || c == '(' || c == ')' ||
          c == '[' || c == ']' || c == '{' || c == '}' ||
          c == '|' || c == '\\') {
        regex_pattern += '\\';
      }
      regex_pattern += c;
    }
  }

  regex_pattern += '$';

  try {
    std::regex re(regex_pattern);
    return std::regex_match(value, re);
  } catch (const std::regex_error&) {
    return false;
  }
}

// ============================================================================
// Validation
// ============================================================================

absl::Status validate_metadata_key(const std::string& key) {
  // Empty key
  if (key.empty()) {
    return absl::InvalidArgumentError("Metadata key cannot be empty");
  }

  // Length check
  if (key.size() > 255) {
    return absl::InvalidArgumentError(
        absl::StrCat("Metadata key too long (max 255 chars): ", key.size()));
  }

  // First character must be letter or underscore
  if (!std::isalpha(key[0]) && key[0] != '_') {
    return absl::InvalidArgumentError(
        absl::StrCat("Metadata key must start with letter or underscore: ", key));
  }

  // Remaining characters must be alphanumeric or underscore
  for (char c : key) {
    if (!std::isalnum(c) && c != '_') {
      return absl::InvalidArgumentError(
          absl::StrCat("Metadata key contains invalid character: ", key));
    }
  }

  return absl::OkStatus();
}

absl::Status validate_metadata_value(const MetadataValue& value) {
  if (std::holds_alternative<std::string>(value)) {
    const auto& str = std::get<std::string>(value);
    // String length limit: 64KB
    if (str.size() > 65536) {
      return absl::InvalidArgumentError(
          absl::StrCat("Metadata string value too long (max 64KB): ", str.size()));
    }
  } else if (std::holds_alternative<double>(value)) {
    double d = std::get<double>(value);
    if (std::isnan(d) || std::isinf(d)) {
      return absl::InvalidArgumentError("Metadata double value must be finite");
    }
  }

  return absl::OkStatus();
}

absl::Status validate_metadata(const Metadata& metadata) {
  // Maximum metadata fields per vector
  constexpr size_t MAX_METADATA_FIELDS = 100;

  if (metadata.size() > MAX_METADATA_FIELDS) {
    return absl::InvalidArgumentError(
        absl::StrCat("Too many metadata fields (max ", MAX_METADATA_FIELDS, "): ",
                     metadata.size()));
  }

  // Validate each key-value pair
  for (const auto& [key, value] : metadata) {
    auto key_status = validate_metadata_key(key);
    if (!key_status.ok()) {
      return key_status;
    }

    auto value_status = validate_metadata_value(value);
    if (!value_status.ok()) {
      return value_status;
    }
  }

  return absl::OkStatus();
}

}  // namespace core
}  // namespace gvdb
