// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <map>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace gvdb {
namespace core {

/**
 * @brief Metadata value types supported by GVDB
 *
 * Supports common data types for filtering and indexing:
 * - int64_t: Integer values (prices, counts, IDs, etc.)
 * - double: Floating point values (ratings, scores, etc.)
 * - std::string: Text values (names, categories, tags, etc.)
 * - bool: Boolean flags (in_stock, featured, active, etc.)
 */
using MetadataValue = std::variant<int64_t, double, std::string, bool>;

/**
 * @brief Map of metadata key-value pairs
 *
 * Example:
 * {
 *   "price": 120.0,
 *   "brand": "Nike",
 *   "in_stock": true,
 *   "category": "running_shoes"
 * }
 */
using Metadata = std::map<std::string, MetadataValue>;

/**
 * @brief Metadata value type enumeration for type checking
 */
enum class MetadataType {
  INT64,
  DOUBLE,
  STRING,
  BOOL
};

/**
 * @brief Get the type of a metadata value
 */
inline MetadataType get_metadata_type(const MetadataValue& value) {
  if (std::holds_alternative<int64_t>(value)) {
    return MetadataType::INT64;
  } else if (std::holds_alternative<double>(value)) {
    return MetadataType::DOUBLE;
  } else if (std::holds_alternative<std::string>(value)) {
    return MetadataType::STRING;
  } else {
    return MetadataType::BOOL;
  }
}

/**
 * @brief Convert metadata type to string for debugging
 */
inline std::string metadata_type_to_string(MetadataType type) {
  switch (type) {
    case MetadataType::INT64:
      return "int64";
    case MetadataType::DOUBLE:
      return "double";
    case MetadataType::STRING:
      return "string";
    case MetadataType::BOOL:
      return "bool";
  }
  return "unknown";
}

/**
 * @brief Comparison operators for metadata filtering
 */
enum class ComparisonOp {
  EQUAL,              // =
  NOT_EQUAL,          // !=
  LESS_THAN,          // <
  LESS_EQUAL,         // <=
  GREATER_THAN,       // >
  GREATER_EQUAL,      // >=
  IN,                 // IN (value1, value2, ...)
  NOT_IN,             // NOT IN (value1, value2, ...)
  LIKE,               // LIKE 'pattern%'
  NOT_LIKE            // NOT LIKE 'pattern%'
};

/**
 * @brief Logical operators for combining filters
 */
enum class LogicalOp {
  AND,
  OR,
  NOT
};

/**
 * @brief Compare two metadata values
 *
 * @param left Left operand
 * @param op Comparison operator
 * @param right Right operand
 * @return true if comparison succeeds, false otherwise
 *
 * Note: Type checking is performed at runtime
 */
bool compare_metadata_values(
    const MetadataValue& left,
    ComparisonOp op,
    const MetadataValue& right);

/**
 * @brief Compare metadata value against a list (for IN/NOT_IN operators)
 */
bool compare_metadata_value_in_list(
    const MetadataValue& value,
    bool negate,
    const std::vector<MetadataValue>& list);

/**
 * @brief Check if a string matches a LIKE pattern
 *
 * Supports:
 * - % : matches any sequence of characters
 * - _ : matches any single character
 *
 * Examples:
 * - "Nike%" matches "Nike Air Max", "Nike Pegasus"
 * - "%running%" matches "best running shoes", "running gear"
 * - "Air_Max" matches "Air Max", "Air-Max"
 */
bool match_like_pattern(const std::string& value, const std::string& pattern);

/**
 * @brief Validate metadata key names
 *
 * Rules:
 * - Must start with letter or underscore
 * - Can contain letters, digits, underscores
 * - Cannot be empty
 * - Cannot exceed 255 characters
 */
absl::Status validate_metadata_key(const std::string& key);

/**
 * @brief Validate metadata value
 *
 * Rules:
 * - Strings cannot exceed 64KB
 * - Numbers must be finite (no NaN, Inf)
 */
absl::Status validate_metadata_value(const MetadataValue& value);

/**
 * @brief Validate entire metadata map
 *
 * Validates all keys and values, enforces limits:
 * - Maximum 100 metadata fields per vector
 */
absl::Status validate_metadata(const Metadata& metadata);

/**
 * @brief Binary serialization for Metadata (for network transfer)
 *
 * Format per entry:
 * - [key_len:4 bytes][key:string][type_tag:1 byte][value:varies]
 *
 * Type tags:
 * - 0: int64_t (8 bytes)
 * - 1: double (8 bytes)
 * - 2: string ([len:4 bytes][data:string])
 * - 3: bool (1 byte)
 */
class MetadataSerializer {
 public:
  // Serialize single metadata entry to output stream
  static void SerializeEntry(
      const std::string& key,
      const MetadataValue& value,
      std::ostream& os);

  // Deserialize single metadata entry from input stream
  static absl::StatusOr<std::pair<std::string, MetadataValue>> DeserializeEntry(
      std::istream& is);

  // Serialize entire metadata map to output stream
  static void Serialize(const Metadata& metadata, std::ostream& os);

  // Deserialize entire metadata map from input stream
  static absl::StatusOr<Metadata> Deserialize(std::istream& is);

 private:
  enum class TypeTag : uint8_t {
    INT64 = 0,
    DOUBLE = 1,
    STRING = 2,
    BOOL = 3
  };

  static TypeTag GetTypeTag(const MetadataValue& value);
};

}  // namespace core
}  // namespace gvdb