// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/backup_manifest.h"

#include "utils/json_util.h"

#include <sstream>
#include <string_view>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"

namespace gvdb {
namespace storage {

namespace {

// ============================================================================
// JSON serialization helpers (hand-rolled to match segment_manifest.cpp
// conventions; the manifest schema is flat enough not to warrant pulling
// in a third-party JSON library).
// ============================================================================

using utils::json::EscapeJson;
using utils::json::ExtractArrayBody;
using utils::json::ParseInt32;
using utils::json::ParseInt64;
using utils::json::ParseString;
using utils::json::ParseUint32;
using utils::json::ParseUint64;
using utils::json::SplitObjects;

// ============================================================================
// Object writers
// ============================================================================

std::string CollectionToJson(const BackupCollectionMeta& c) {
  std::ostringstream ss;
  ss << "{"
     << "\"collection_id\":" << c.collection_id
     << ",\"collection_name\":\"" << EscapeJson(c.collection_name) << "\""
     << ",\"dimension\":" << c.dimension
     << ",\"metric\":" << c.metric
     << ",\"index_type\":" << c.index_type
     << ",\"num_shards\":" << c.num_shards
     << ",\"replication_factor\":" << c.replication_factor
     << "}";
  return ss.str();
}

std::string ShardRefToJson(const BackupShardRef& s) {
  std::ostringstream ss;
  ss << "{"
     << "\"shard_id\":" << s.shard_id
     << ",\"manifest_key\":\"" << EscapeJson(s.manifest_key) << "\""
     << "}";
  return ss.str();
}

std::string FileToJson(const ShardBackupFile& f) {
  std::ostringstream ss;
  ss << "{"
     << "\"name\":\"" << EscapeJson(f.name) << "\""
     << ",\"key\":\"" << EscapeJson(f.key) << "\""
     << ",\"size\":" << f.size
     << "}";
  return ss.str();
}

std::string SegmentToJson(const ShardBackupSegment& s) {
  std::ostringstream ss;
  ss << "{"
     << "\"segment_id\":" << s.segment_id
     << ",\"state\":\"" << EscapeJson(s.state) << "\""
     << ",\"vector_count\":" << s.vector_count
     << ",\"size_bytes\":" << s.size_bytes
     << ",\"files\":[";
  for (size_t i = 0; i < s.files.size(); ++i) {
    if (i > 0) ss << ",";
    ss << FileToJson(s.files[i]);
  }
  ss << "]}";
  return ss.str();
}

// ============================================================================
// Object readers
// ============================================================================

core::StatusOr<BackupCollectionMeta> ParseCollectionMeta(
    const std::string& json) {
  BackupCollectionMeta c;
  if (!ParseUint32(json, "collection_id", c.collection_id) ||
      !ParseString(json, "collection_name", c.collection_name) ||
      !ParseInt32(json, "dimension", c.dimension) ||
      !ParseInt32(json, "metric", c.metric) ||
      !ParseInt32(json, "index_type", c.index_type) ||
      !ParseUint32(json, "num_shards", c.num_shards) ||
      !ParseUint32(json, "replication_factor", c.replication_factor)) {
    return core::InvalidArgumentError(
        "Failed to parse backup manifest collection metadata");
  }
  return c;
}

core::StatusOr<BackupShardRef> ParseShardRef(const std::string& json) {
  BackupShardRef s;
  if (!ParseUint32(json, "shard_id", s.shard_id) ||
      !ParseString(json, "manifest_key", s.manifest_key)) {
    return core::InvalidArgumentError(
        "Failed to parse backup manifest shard reference");
  }
  return s;
}

core::StatusOr<ShardBackupFile> ParseFile(const std::string& json) {
  ShardBackupFile f;
  if (!ParseString(json, "name", f.name) ||
      !ParseString(json, "key", f.key) ||
      !ParseUint64(json, "size", f.size)) {
    return core::InvalidArgumentError(
        "Failed to parse shard manifest file entry");
  }
  return f;
}

core::StatusOr<ShardBackupSegment> ParseSegment(const std::string& json) {
  ShardBackupSegment s;
  if (!ParseUint64(json, "segment_id", s.segment_id) ||
      !ParseString(json, "state", s.state) ||
      !ParseUint64(json, "vector_count", s.vector_count) ||
      !ParseUint64(json, "size_bytes", s.size_bytes)) {
    return core::InvalidArgumentError(
        "Failed to parse shard manifest segment entry");
  }
  auto files_body = ExtractArrayBody(json, "files");
  if (files_body.empty()) {
    return core::InvalidArgumentError(
        absl::StrCat("Segment ", s.segment_id, " missing 'files' array"));
  }
  for (const auto& obj : SplitObjects(files_body)) {
    auto f = ParseFile(obj);
    if (!f.ok()) return f.status();
    s.files.push_back(std::move(*f));
  }
  return s;
}

}  // namespace

// ============================================================================
// Top-level manifest
// ============================================================================

std::string BackupManifest::SerializeTop(const BackupManifestV1& m) {
  std::ostringstream ss;
  ss << "{"
     << "\"manifest_version\":" << m.manifest_version
     << ",\"backup_id\":\"" << EscapeJson(m.backup_id) << "\""
     << ",\"collection\":" << CollectionToJson(m.collection)
     << ",\"created_at_unix_ms\":" << m.created_at_unix_ms
     << ",\"vector_count\":" << m.vector_count
     << ",\"size_bytes\":" << m.size_bytes
     << ",\"shards\":[";
  for (size_t i = 0; i < m.shards.size(); ++i) {
    if (i > 0) ss << ",";
    ss << ShardRefToJson(m.shards[i]);
  }
  ss << "]"
     << ",\"incremental_from\":\"" << EscapeJson(m.incremental_from) << "\""
     << ",\"gvdb_version\":\"" << EscapeJson(m.gvdb_version) << "\""
     << "}";
  return ss.str();
}

core::StatusOr<BackupManifestV1> BackupManifest::DeserializeTop(
    const std::string& json) {
  if (json.empty()) {
    return core::InvalidArgumentError("Empty backup manifest");
  }
  BackupManifestV1 m;
  if (!ParseUint32(json, "manifest_version", m.manifest_version)) {
    return core::InvalidArgumentError(
        "Backup manifest missing 'manifest_version'");
  }
  if (m.manifest_version != 1) {
    return core::InvalidArgumentError(absl::StrCat(
        "Unsupported backup manifest version: ", m.manifest_version));
  }
  if (!ParseString(json, "backup_id", m.backup_id)) {
    return core::InvalidArgumentError("Backup manifest missing 'backup_id'");
  }
  // Collection sub-object lives between "collection":{ ... } — extract by
  // matching braces because the field parsers stop at the first comma or
  // closing brace, which would clip inside the sub-object.
  std::string coll_needle = "\"collection\":{";
  auto coll_pos = json.find(coll_needle);
  if (coll_pos == std::string::npos) {
    return core::InvalidArgumentError(
        "Backup manifest missing 'collection' object");
  }
  size_t coll_start = coll_pos + coll_needle.size() - 1;  // at '{'
  int curly = 0;
  size_t coll_end = std::string::npos;
  bool in_string = false;
  bool escape = false;
  for (size_t i = coll_start; i < json.size(); ++i) {
    char c = json[i];
    if (in_string) {
      if (escape) { escape = false; }
      else if (c == '\\') { escape = true; }
      else if (c == '"') { in_string = false; }
      continue;
    }
    if (c == '"') { in_string = true; }
    else if (c == '{') { ++curly; }
    else if (c == '}') {
      --curly;
      if (curly == 0) { coll_end = i; break; }
    }
  }
  if (coll_end == std::string::npos) {
    return core::InvalidArgumentError("Malformed 'collection' object");
  }
  auto coll = ParseCollectionMeta(
      json.substr(coll_start, coll_end - coll_start + 1));
  if (!coll.ok()) return coll.status();
  m.collection = std::move(*coll);

  if (!ParseInt64(json, "created_at_unix_ms", m.created_at_unix_ms) ||
      !ParseUint64(json, "vector_count", m.vector_count) ||
      !ParseUint64(json, "size_bytes", m.size_bytes)) {
    return core::InvalidArgumentError(
        "Backup manifest missing required numeric fields");
  }
  auto shards_body = ExtractArrayBody(json, "shards");
  if (shards_body.empty()) {
    return core::InvalidArgumentError(
        "Backup manifest missing 'shards' array");
  }
  for (const auto& obj : SplitObjects(shards_body)) {
    auto s = ParseShardRef(obj);
    if (!s.ok()) return s.status();
    m.shards.push_back(std::move(*s));
  }
  ParseString(json, "incremental_from", m.incremental_from);  // optional
  ParseString(json, "gvdb_version", m.gvdb_version);          // optional
  return m;
}

// ============================================================================
// Per-shard manifest
// ============================================================================

std::string BackupManifest::SerializeShard(const ShardManifestV1& m) {
  std::ostringstream ss;
  ss << "{"
     << "\"manifest_version\":" << m.manifest_version
     << ",\"shard_id\":" << m.shard_id
     << ",\"node_id\":" << m.node_id
     << ",\"primary_term\":" << m.primary_term
     << ",\"segments\":[";
  for (size_t i = 0; i < m.segments.size(); ++i) {
    if (i > 0) ss << ",";
    ss << SegmentToJson(m.segments[i]);
  }
  ss << "]}";
  return ss.str();
}

core::StatusOr<ShardManifestV1> BackupManifest::DeserializeShard(
    const std::string& json) {
  if (json.empty()) {
    return core::InvalidArgumentError("Empty shard manifest");
  }
  ShardManifestV1 m;
  if (!ParseUint32(json, "manifest_version", m.manifest_version)) {
    return core::InvalidArgumentError(
        "Shard manifest missing 'manifest_version'");
  }
  if (m.manifest_version != 1) {
    return core::InvalidArgumentError(absl::StrCat(
        "Unsupported shard manifest version: ", m.manifest_version));
  }
  if (!ParseUint32(json, "shard_id", m.shard_id) ||
      !ParseUint32(json, "node_id", m.node_id) ||
      !ParseUint64(json, "primary_term", m.primary_term)) {
    return core::InvalidArgumentError(
        "Shard manifest missing required numeric fields");
  }
  auto segments_body = ExtractArrayBody(json, "segments");
  if (segments_body.empty()) {
    return core::InvalidArgumentError(
        "Shard manifest missing 'segments' array");
  }
  for (const auto& obj : SplitObjects(segments_body)) {
    auto s = ParseSegment(obj);
    if (!s.ok()) return s.status();
    m.segments.push_back(std::move(*s));
  }
  return m;
}

// ============================================================================
// Key conventions
// ============================================================================

std::string BackupManifest::BackupRootPrefix(const std::string& backup_id) {
  return absl::StrCat("backups/", backup_id);
}

std::string BackupManifest::TopManifestKey(const std::string& backup_id) {
  return absl::StrCat(BackupRootPrefix(backup_id), "/backup.manifest.json");
}

std::string BackupManifest::ShardManifestKey(const std::string& backup_id,
                                             uint32_t shard_id) {
  return absl::StrCat(BackupRootPrefix(backup_id), "/shards/", shard_id,
                      "/shard.manifest.json");
}

std::string BackupManifest::SegmentFileKey(const std::string& backup_id,
                                           uint32_t shard_id,
                                           uint64_t segment_id,
                                           const std::string& filename) {
  return absl::StrCat(BackupRootPrefix(backup_id), "/shards/", shard_id,
                      "/segments/", segment_id, "/", filename);
}

}  // namespace storage
}  // namespace gvdb
