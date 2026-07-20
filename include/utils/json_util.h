// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_UTILS_JSON_UTIL_H_
#define GVDB_UTILS_JSON_UTIL_H_

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace gvdb {
namespace utils {
namespace json {

// Minimal helpers for the hand-rolled JSON documents GVDB reads and writes
// (segment manifests, backup manifests, the coordinator registry). They are
// not a general JSON parser: fields are located by their quoted name, which
// is sufficient for documents this codebase itself produced.

// Escape a string for embedding inside a JSON string literal.
std::string EscapeJson(const std::string& s);

// Parse a scalar field ("field":value) out of a JSON object string.
// Return false when the field is absent or malformed.
bool ParseUint32(const std::string& json, std::string_view field,
                 uint32_t& out);
bool ParseInt32(const std::string& json, std::string_view field, int32_t& out);
bool ParseUint64(const std::string& json, std::string_view field,
                 uint64_t& out);
bool ParseInt64(const std::string& json, std::string_view field, int64_t& out);

// Parse a string field ("field":"value"). Handles the escapes EscapeJson
// produces. Return false when absent.
bool ParseString(const std::string& json, std::string_view field,
                 std::string& out);

// Locate the body of a JSON array following "field":[ with depth tracking;
// returns the substring from '[' to its matching ']' inclusive, or empty if
// not found. Bracket characters inside strings are skipped.
std::string_view ExtractArrayBody(const std::string& json,
                                  std::string_view field);

// Split a JSON array body "[{...},{...}]" into the inner object strings.
// Handles nested objects, nested arrays, and strings containing braces.
std::vector<std::string> SplitObjects(std::string_view array_body);

}  // namespace json
}  // namespace utils
}  // namespace gvdb

#endif  // GVDB_UTILS_JSON_UTIL_H_
