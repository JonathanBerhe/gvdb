// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

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

// ============================================================================
// MetadataSerializer Implementation
// ============================================================================

MetadataSerializer::TypeTag MetadataSerializer::GetTypeTag(const MetadataValue& value) {
  if (std::holds_alternative<int64_t>(value)) {
    return TypeTag::INT64;
  } else if (std::holds_alternative<double>(value)) {
    return TypeTag::DOUBLE;
  } else if (std::holds_alternative<std::string>(value)) {
    return TypeTag::STRING;
  } else {
    return TypeTag::BOOL;
  }
}

void MetadataSerializer::SerializeEntry(
    const std::string& key,
    const MetadataValue& value,
    std::ostream& os) {
  // Write key length and key
  uint32_t key_len = static_cast<uint32_t>(key.size());
  os.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
  os.write(key.data(), key_len);

  // Write type tag
  TypeTag tag = GetTypeTag(value);
  uint8_t tag_byte = static_cast<uint8_t>(tag);
  os.write(reinterpret_cast<const char*>(&tag_byte), sizeof(tag_byte));

  // Write value based on type
  std::visit([&os](const auto& v) {
    using T = std::decay_t<decltype(v)>;
    if constexpr (std::is_same_v<T, int64_t>) {
      os.write(reinterpret_cast<const char*>(&v), sizeof(int64_t));
    } else if constexpr (std::is_same_v<T, double>) {
      os.write(reinterpret_cast<const char*>(&v), sizeof(double));
    } else if constexpr (std::is_same_v<T, std::string>) {
      uint32_t str_len = static_cast<uint32_t>(v.size());
      os.write(reinterpret_cast<const char*>(&str_len), sizeof(str_len));
      os.write(v.data(), str_len);
    } else if constexpr (std::is_same_v<T, bool>) {
      uint8_t bool_byte = v ? 1 : 0;
      os.write(reinterpret_cast<const char*>(&bool_byte), sizeof(bool_byte));
    }
  }, value);
}

absl::StatusOr<std::pair<std::string, MetadataValue>>
MetadataSerializer::DeserializeEntry(std::istream& is) {
  // Read key length and key
  uint32_t key_len;
  is.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
  if (!is || key_len > 255) {
    return absl::InvalidArgumentError("Invalid key length");
  }

  std::string key(key_len, '\0');
  is.read(&key[0], key_len);
  if (!is) {
    return absl::InvalidArgumentError("Failed to read key");
  }

  // Read type tag
  uint8_t tag_byte;
  is.read(reinterpret_cast<char*>(&tag_byte), sizeof(tag_byte));
  if (!is || tag_byte > 3) {
    return absl::InvalidArgumentError("Invalid type tag");
  }

  TypeTag tag = static_cast<TypeTag>(tag_byte);
  MetadataValue value;

  // Read value based on type
  switch (tag) {
    case TypeTag::INT64: {
      int64_t int_val;
      is.read(reinterpret_cast<char*>(&int_val), sizeof(int64_t));
      if (!is) return absl::InvalidArgumentError("Failed to read int64");
      value = int_val;
      break;
    }
    case TypeTag::DOUBLE: {
      double double_val;
      is.read(reinterpret_cast<char*>(&double_val), sizeof(double));
      if (!is) return absl::InvalidArgumentError("Failed to read double");
      value = double_val;
      break;
    }
    case TypeTag::STRING: {
      uint32_t str_len;
      is.read(reinterpret_cast<char*>(&str_len), sizeof(str_len));
      if (!is || str_len > 65536) {
        return absl::InvalidArgumentError("Invalid string length");
      }
      std::string str_val(str_len, '\0');
      is.read(&str_val[0], str_len);
      if (!is) return absl::InvalidArgumentError("Failed to read string");
      value = str_val;
      break;
    }
    case TypeTag::BOOL: {
      uint8_t bool_byte;
      is.read(reinterpret_cast<char*>(&bool_byte), sizeof(bool_byte));
      if (!is) return absl::InvalidArgumentError("Failed to read bool");
      value = (bool_byte != 0);
      break;
    }
  }

  return std::make_pair(key, value);
}

void MetadataSerializer::Serialize(const Metadata& metadata, std::ostream& os) {
  // Write entry count
  uint64_t count = static_cast<uint64_t>(metadata.size());
  os.write(reinterpret_cast<const char*>(&count), sizeof(count));

  // Write each entry
  for (const auto& [key, value] : metadata) {
    SerializeEntry(key, value, os);
  }
}

absl::StatusOr<Metadata> MetadataSerializer::Deserialize(std::istream& is) {
  // Read entry count
  uint64_t count;
  is.read(reinterpret_cast<char*>(&count), sizeof(count));
  if (!is || count > 100) {  // Max 100 metadata fields
    return absl::InvalidArgumentError("Invalid metadata count");
  }

  Metadata metadata;

  // Read each entry
  for (uint64_t i = 0; i < count; ++i) {
    auto entry_result = DeserializeEntry(is);
    if (!entry_result.ok()) {
      return entry_result.status();
    }
    auto [key, value] = std::move(entry_result.value());
    metadata[key] = value;
  }

  return metadata;
}

}  // namespace core
}  // namespace gvdb