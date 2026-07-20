// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "utils/json_util.h"

#include <sstream>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"

namespace gvdb {
namespace utils {
namespace json {

std::string EscapeJson(const std::string& s) {
  std::string result;
  result.reserve(s.size());
  for (char c : s) {
    switch (c) {
      case '"': result += "\\\""; break;
      case '\\': result += "\\\\"; break;
      case '\n': result += "\\n"; break;
      case '\r': result += "\\r"; break;
      case '\t': result += "\\t"; break;
      default: result += c;
    }
  }
  return result;
}

// ============================================================================
// JSON parsing helpers
// ============================================================================

// Parse "field":N where N is decimal. Returns true on success.
bool ParseUint32(const std::string& json, std::string_view field,
                 uint32_t& out) {
  std::string needle = absl::StrCat("\"", field, "\":");
  auto pos = json.find(needle);
  if (pos == std::string::npos) return false;
  pos += needle.size();
  auto end = json.find_first_of(",}", pos);
  if (end == std::string::npos) return false;
  return absl::SimpleAtoi(
      absl::string_view(json.data() + pos, end - pos), &out);
}

bool ParseInt32(const std::string& json, std::string_view field, int32_t& out) {
  std::string needle = absl::StrCat("\"", field, "\":");
  auto pos = json.find(needle);
  if (pos == std::string::npos) return false;
  pos += needle.size();
  auto end = json.find_first_of(",}", pos);
  if (end == std::string::npos) return false;
  return absl::SimpleAtoi(
      absl::string_view(json.data() + pos, end - pos), &out);
}

bool ParseUint64(const std::string& json, std::string_view field,
                 uint64_t& out) {
  std::string needle = absl::StrCat("\"", field, "\":");
  auto pos = json.find(needle);
  if (pos == std::string::npos) return false;
  pos += needle.size();
  auto end = json.find_first_of(",}", pos);
  if (end == std::string::npos) return false;
  return absl::SimpleAtoi(
      absl::string_view(json.data() + pos, end - pos), &out);
}

bool ParseInt64(const std::string& json, std::string_view field, int64_t& out) {
  std::string needle = absl::StrCat("\"", field, "\":");
  auto pos = json.find(needle);
  if (pos == std::string::npos) return false;
  pos += needle.size();
  auto end = json.find_first_of(",}", pos);
  if (end == std::string::npos) return false;
  return absl::SimpleAtoi(
      absl::string_view(json.data() + pos, end - pos), &out);
}

bool ParseString(const std::string& json, std::string_view field,
                 std::string& out) {
  std::string needle = absl::StrCat("\"", field, "\":\"");
  auto pos = json.find(needle);
  if (pos == std::string::npos) return false;
  pos += needle.size();
  // String content runs until the next unescaped quote. Since we only
  // escape \", \\, \n, \r, \t, we can find the next quote that isn't
  // preceded by an odd number of backslashes.
  size_t i = pos;
  while (i < json.size()) {
    if (json[i] == '"') {
      size_t backslashes = 0;
      size_t j = i;
      while (j > pos && json[j - 1] == '\\') {
        ++backslashes;
        --j;
      }
      if (backslashes % 2 == 0) break;  // unescaped quote
    }
    ++i;
  }
  if (i >= json.size()) return false;
  out = json.substr(pos, i - pos);
  // Reverse the escape pass.
  std::string unescaped;
  unescaped.reserve(out.size());
  for (size_t k = 0; k < out.size(); ++k) {
    if (out[k] == '\\' && k + 1 < out.size()) {
      char next = out[k + 1];
      switch (next) {
        case '"': unescaped += '"'; break;
        case '\\': unescaped += '\\'; break;
        case 'n': unescaped += '\n'; break;
        case 'r': unescaped += '\r'; break;
        case 't': unescaped += '\t'; break;
        default: unescaped += next; break;
      }
      ++k;
    } else {
      unescaped += out[k];
    }
  }
  out = std::move(unescaped);
  return true;
}

// Locate the body of a JSON array following "field":[, with proper depth
// tracking on []/{}/""; returns the substring from '[' to its matching ']'
// inclusive, or empty if not found. Skips bracket characters inside strings.
std::string_view ExtractArrayBody(const std::string& json,
                                  std::string_view field) {
  std::string needle = absl::StrCat("\"", field, "\":[");
  auto pos = json.find(needle);
  if (pos == std::string::npos) return {};
  auto start = pos + needle.size() - 1;  // index of the '['
  // We only track nested array depth (`[]`); braces inside the array
  // belong to JSON objects that the caller will parse separately, and
  // brackets inside strings are skipped via `in_string`.
  int square = 0;
  bool in_string = false;
  bool escape = false;
  for (size_t i = start; i < json.size(); ++i) {
    char c = json[i];
    if (in_string) {
      if (escape) {
        escape = false;
      } else if (c == '\\') {
        escape = true;
      } else if (c == '"') {
        in_string = false;
      }
      continue;
    }
    if (c == '"') {
      in_string = true;
    } else if (c == '[') {
      ++square;
    } else if (c == ']') {
      --square;
      if (square == 0) {
        return std::string_view(json.data() + start, i - start + 1);
      }
    }
  }
  return {};
}

// Split a JSON array body "[{...},{...}]" into the inner object strings.
// Handles nested objects, nested arrays, and strings containing braces.
std::vector<std::string> SplitObjects(std::string_view array_body) {
  std::vector<std::string> objects;
  if (array_body.size() < 2) return objects;
  int curly = 0;
  int square = 0;
  bool in_string = false;
  bool escape = false;
  size_t obj_start = 0;
  // Skip the leading '['.
  for (size_t i = 1; i + 1 < array_body.size(); ++i) {
    char c = array_body[i];
    if (in_string) {
      if (escape) {
        escape = false;
      } else if (c == '\\') {
        escape = true;
      } else if (c == '"') {
        in_string = false;
      }
      continue;
    }
    if (c == '"') {
      in_string = true;
    } else if (c == '{') {
      if (curly == 0 && square == 0) obj_start = i;
      ++curly;
    } else if (c == '}') {
      --curly;
      if (curly == 0 && square == 0) {
        objects.emplace_back(array_body.substr(obj_start, i - obj_start + 1));
      }
    } else if (c == '[') {
      ++square;
    } else if (c == ']') {
      --square;
    }
  }
  return objects;
}

}  // namespace json
}  // namespace utils
}  // namespace gvdb
