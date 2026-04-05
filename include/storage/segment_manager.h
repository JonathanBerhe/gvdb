// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_SEGMENT_MANAGER_H_
#define GVDB_STORAGE_SEGMENT_MANAGER_H_

#include <functional>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "core/interfaces.h"
#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"
#include "storage/segment.h"

// Hash specialization for SegmentId and CollectionId (needed for unordered_map)
// Must come BEFORE use in SegmentManager class
namespace std {
template <>
struct hash<gvdb::core::SegmentId> {
  size_t operator()(const gvdb::core::SegmentId& id) const {
    return std::hash<uint32_t>()(gvdb::core::ToUInt32(id));
  }
};

template <>
struct hash<gvdb::core::CollectionId> {
  size_t operator()(const gvdb::core::CollectionId& id) const {
    return std::hash<uint32_t>()(gvdb::core::ToUInt32(id));
  }
};
}  // namespace std

namespace gvdb {
namespace storage {

// ============================================================================
// SegmentManager - Manages lifecycle of multiple segments
// ============================================================================
// Coordinates segment creation, sealing, flushing, and garbage collection
// Thread-safety: All operations are thread-safe using shared_mutex
class SegmentManager {
 public:
  // Constructor
  explicit SegmentManager(const std::string& base_path,
                          core::IIndexFactory* index_factory,
                          size_t max_segment_size = Segment::kMaxSegmentSize);

  // Disable copy and move (contains mutex and unique_ptr)
  SegmentManager(const SegmentManager&) = delete;
  SegmentManager& operator=(const SegmentManager&) = delete;
  SegmentManager(SegmentManager&&) = delete;
  SegmentManager& operator=(SegmentManager&&) = delete;

  ~SegmentManager() = default;

  // ========== Segment Lifecycle ==========

  // Create a new growing segment (auto-assigns segment_id)
  [[nodiscard]] core::StatusOr<core::SegmentId> CreateSegment(
      core::CollectionId collection_id,
      core::Dimension dimension,
      core::MetricType metric);

  // Create segment with specific ID (for distributed mode - called via RPC)
  [[nodiscard]] core::Status CreateSegmentWithId(
      core::SegmentId segment_id,
      core::CollectionId collection_id,
      core::Dimension dimension,
      core::MetricType metric,
      core::IndexType index_type);

  // Get a segment by ID (returns nullptr if not found)
  [[nodiscard]] Segment* GetSegment(core::SegmentId id);
  [[nodiscard]] const Segment* GetSegment(core::SegmentId id) const;

  // Seal a segment (build index, make immutable)
  [[nodiscard]] core::Status SealSegment(core::SegmentId id,
                                          const core::IndexConfig& index_config);

  // Flush a segment to persistent storage
  [[nodiscard]] core::Status FlushSegment(core::SegmentId id);

  // Drop a segment (remove from memory and optionally delete from disk)
  [[nodiscard]] core::Status DropSegment(core::SegmentId id, bool delete_files = false);

  // Load segment from disk
  [[nodiscard]] core::Status LoadSegment(core::SegmentId id);

  // Add replicated segment (from network transfer)
  [[nodiscard]] core::Status AddReplicatedSegment(std::unique_ptr<Segment> segment);

  // ========== Data Operations ==========

  // Write vectors to a segment
  [[nodiscard]] core::Status WriteVectors(
      core::SegmentId segment_id,
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids);

  // Read vectors from a segment
  [[nodiscard]] core::StatusOr<std::vector<core::Vector>> ReadVectors(
      core::SegmentId segment_id,
      const std::vector<core::VectorId>& ids) const;

  // Search in a specific segment
  [[nodiscard]] core::StatusOr<core::SearchResult> SearchSegment(
      core::SegmentId segment_id,
      const core::Vector& query,
      int k) const;

  // Search across all segments in a collection
  [[nodiscard]] core::StatusOr<core::SearchResult> SearchCollection(
      core::CollectionId collection_id,
      const core::Vector& query,
      int k) const;

  // ========== Active Segment Rotation ==========

  // Callback invoked when a segment is rotated out (full).
  // Parameters: (old_segment_id, index_type_to_build)
  using SealCallback = std::function<void(core::SegmentId, core::IndexType)>;

  // Register a callback for segment rotation events.
  // Typically wired to DataNode::ScheduleBuildTask.
  void SetSealCallback(SealCallback callback);

  // Get a writable GROWING segment for a collection.
  // If the active segment is full (or can't fit required_bytes), rotates:
  // creates a new segment, invokes the seal callback for the old one,
  // and returns the new one.
  [[nodiscard]] Segment* GetWritableSegment(core::CollectionId collection_id,
                                            size_t required_bytes = 0);

  // Get all queryable segments for a collection (GROWING + SEALED).
  [[nodiscard]] std::vector<Segment*> GetQueryableSegments(
      core::CollectionId collection_id) const;

  // ========== Management ==========

  // Load all previously-flushed segments from disk (startup recovery)
  absl::Status LoadAllSegments();

  // Get all segment IDs for a collection
  [[nodiscard]] std::vector<core::SegmentId> GetCollectionSegments(
      core::CollectionId collection_id) const;

  // Get segment count
  [[nodiscard]] size_t GetSegmentCount() const;

  // Get total memory usage
  [[nodiscard]] size_t GetTotalMemoryUsage() const;

  // Clear all segments (for testing)
  void Clear();

 private:
  // Storage path
  std::string base_path_;

  // Index factory for creating indexes when sealing
  core::IIndexFactory* index_factory_;

  // Segment storage (keyed by SegmentId)
  std::unordered_map<core::SegmentId, std::unique_ptr<Segment>> segments_;

  // Collection to segments mapping (for efficient collection queries)
  std::unordered_map<core::CollectionId, std::vector<core::SegmentId>>
      collection_segments_;

  // Thread safety
  mutable std::shared_mutex mutex_;

  // Segment ID counter
  uint32_t next_segment_id_;

  // Maximum segment size (passed to new segments)
  size_t max_segment_size_;

  // Seal callback (invoked on rotation)
  SealCallback seal_callback_;

  // Helper methods
  [[nodiscard]] core::SegmentId AllocateSegmentId();
  [[nodiscard]] std::string GetSegmentPath(core::SegmentId id) const;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_SEGMENT_MANAGER_H_