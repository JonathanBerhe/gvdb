// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_SEGMENT_MANIFEST_H_
#define GVDB_STORAGE_SEGMENT_MANIFEST_H_

#include <string>
#include <vector>

#include "core/status.h"
#include "core/types.h"

namespace gvdb {
namespace storage {

// Entry describing a segment stored in object storage
struct ManifestEntry {
  uint32_t segment_id = 0;
  uint32_t collection_id = 0;
  int32_t dimension = 0;
  int32_t metric = 0;       // MetricType as int
  int32_t index_type = 0;   // IndexType as int
  uint64_t vector_count = 0;
  uint64_t size_bytes = 0;
  std::string uploaded_at;   // ISO 8601
};

// ============================================================================
// SegmentManifest - JSON manifest for tracking segments in object storage
// ============================================================================
// The manifest is stored at {prefix}/manifest.json in the object store.
// It provides fast startup discovery without ListObjects calls.
class SegmentManifest {
 public:
  // Serialize a list of entries to JSON string
  [[nodiscard]] static std::string Serialize(
      const std::vector<ManifestEntry>& entries);

  // Deserialize JSON string to a list of entries
  [[nodiscard]] static core::StatusOr<std::vector<ManifestEntry>> Deserialize(
      const std::string& json);

  // Add an entry to existing manifest JSON, return updated JSON
  [[nodiscard]] static std::string AddEntry(
      const std::string& existing_json,
      const ManifestEntry& entry);

  // Remove an entry by segment ID, return updated JSON
  [[nodiscard]] static std::string RemoveEntry(
      const std::string& existing_json,
      uint32_t segment_id);
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_SEGMENT_MANIFEST_H_
