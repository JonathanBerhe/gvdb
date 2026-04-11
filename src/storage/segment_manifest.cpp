// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/segment_manifest.h"

#include <sstream>
#include <regex>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/numbers.h"
#include "absl/strings/strip.h"

namespace gvdb {
namespace storage {

namespace {

// Minimal JSON serialization (avoids adding nlohmann/json dependency).
// Manifest is a simple flat array of objects with fixed fields.

std::string EscapeJson(const std::string& s) {
  std::string result;
  result.reserve(s.size());
  for (char c : s) {
    switch (c) {
      case '"': result += "\\\""; break;
      case '\\': result += "\\\\"; break;
      case '\n': result += "\\n"; break;
      default: result += c;
    }
  }
  return result;
}

std::string EntryToJson(const ManifestEntry& e) {
  std::ostringstream ss;
  ss << "{"
     << "\"segment_id\":" << e.segment_id
     << ",\"collection_id\":" << e.collection_id
     << ",\"dimension\":" << e.dimension
     << ",\"metric\":" << e.metric
     << ",\"index_type\":" << e.index_type
     << ",\"vector_count\":" << e.vector_count
     << ",\"size_bytes\":" << e.size_bytes
     << ",\"uploaded_at\":\"" << EscapeJson(e.uploaded_at) << "\""
     << "}";
  return ss.str();
}

// Parse a single integer field from a JSON-like string
bool ParseUint32(const std::string& json, const std::string& field,
                 uint32_t& out) {
  auto pos = json.find("\"" + field + "\":");
  if (pos == std::string::npos) return false;
  pos += field.size() + 3;  // skip "field":
  auto end = json.find_first_of(",}", pos);
  if (end == std::string::npos) return false;
  return absl::SimpleAtoi(absl::string_view(json.data() + pos, end - pos),
                          &out);
}

bool ParseInt32(const std::string& json, const std::string& field,
                int32_t& out) {
  auto pos = json.find("\"" + field + "\":");
  if (pos == std::string::npos) return false;
  pos += field.size() + 3;
  auto end = json.find_first_of(",}", pos);
  if (end == std::string::npos) return false;
  return absl::SimpleAtoi(absl::string_view(json.data() + pos, end - pos),
                          &out);
}

bool ParseUint64(const std::string& json, const std::string& field,
                 uint64_t& out) {
  auto pos = json.find("\"" + field + "\":");
  if (pos == std::string::npos) return false;
  pos += field.size() + 3;
  auto end = json.find_first_of(",}", pos);
  if (end == std::string::npos) return false;
  return absl::SimpleAtoi(absl::string_view(json.data() + pos, end - pos),
                          &out);
}

bool ParseString(const std::string& json, const std::string& field,
                 std::string& out) {
  auto needle = "\"" + field + "\":\"";
  auto pos = json.find(needle);
  if (pos == std::string::npos) return false;
  pos += needle.size();
  auto end = json.find('"', pos);
  if (end == std::string::npos) return false;
  out = json.substr(pos, end - pos);
  return true;
}

core::StatusOr<ManifestEntry> ParseEntry(const std::string& json) {
  ManifestEntry e;
  if (!ParseUint32(json, "segment_id", e.segment_id) ||
      !ParseUint32(json, "collection_id", e.collection_id) ||
      !ParseInt32(json, "dimension", e.dimension) ||
      !ParseInt32(json, "metric", e.metric) ||
      !ParseInt32(json, "index_type", e.index_type) ||
      !ParseUint64(json, "vector_count", e.vector_count) ||
      !ParseUint64(json, "size_bytes", e.size_bytes)) {
    return core::InvalidArgumentError("Failed to parse manifest entry");
  }
  ParseString(json, "uploaded_at", e.uploaded_at);  // optional
  return e;
}

// Split JSON array into individual object strings
std::vector<std::string> SplitJsonArray(const std::string& json) {
  std::vector<std::string> objects;
  int depth = 0;
  size_t start = 0;
  for (size_t i = 0; i < json.size(); ++i) {
    if (json[i] == '{') {
      if (depth == 0) start = i;
      ++depth;
    } else if (json[i] == '}') {
      --depth;
      if (depth == 0) {
        objects.push_back(json.substr(start, i - start + 1));
      }
    }
  }
  return objects;
}

}  // namespace

std::string SegmentManifest::Serialize(
    const std::vector<ManifestEntry>& entries) {
  std::ostringstream ss;
  ss << "{\"version\":1,\"segments\":[";
  for (size_t i = 0; i < entries.size(); ++i) {
    if (i > 0) ss << ",";
    ss << EntryToJson(entries[i]);
  }
  ss << "]}";
  return ss.str();
}

core::StatusOr<std::vector<ManifestEntry>> SegmentManifest::Deserialize(
    const std::string& json) {
  if (json.empty()) {
    return std::vector<ManifestEntry>{};
  }

  // Find the "segments" array
  auto pos = json.find("\"segments\":[");
  if (pos == std::string::npos) {
    return core::InvalidArgumentError("Manifest missing 'segments' array");
  }

  // Extract the array portion
  auto array_start = json.find('[', pos);
  auto array_end = json.rfind(']');
  if (array_start == std::string::npos || array_end == std::string::npos ||
      array_end <= array_start) {
    return core::InvalidArgumentError("Malformed segments array");
  }
  auto array_str = json.substr(array_start, array_end - array_start + 1);

  // Parse individual entries
  auto object_strs = SplitJsonArray(array_str);
  std::vector<ManifestEntry> entries;
  entries.reserve(object_strs.size());
  for (const auto& obj : object_strs) {
    auto entry = ParseEntry(obj);
    if (!entry.ok()) return entry.status();
    entries.push_back(std::move(*entry));
  }
  return entries;
}

std::string SegmentManifest::AddEntry(
    const std::string& existing_json,
    const ManifestEntry& entry) {
  auto result = Deserialize(existing_json);
  std::vector<ManifestEntry> entries;
  if (result.ok()) {
    entries = std::move(*result);
  }

  // Replace if segment_id already exists, otherwise append
  bool found = false;
  for (auto& e : entries) {
    if (e.segment_id == entry.segment_id) {
      e = entry;
      found = true;
      break;
    }
  }
  if (!found) {
    entries.push_back(entry);
  }
  return Serialize(entries);
}

std::string SegmentManifest::RemoveEntry(
    const std::string& existing_json,
    uint32_t segment_id) {
  auto result = Deserialize(existing_json);
  if (!result.ok()) return existing_json;

  std::vector<ManifestEntry> filtered;
  for (const auto& e : *result) {
    if (e.segment_id != segment_id) {
      filtered.push_back(e);
    }
  }
  return Serialize(filtered);
}

}  // namespace storage
}  // namespace gvdb
