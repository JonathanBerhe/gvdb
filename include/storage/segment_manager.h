// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_SEGMENT_MANAGER_H_
#define GVDB_STORAGE_SEGMENT_MANAGER_H_

#include <atomic>
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
#include "storage/segment_store.h"

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
class SegmentManager : public ISegmentStore {
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

  ~SegmentManager() override = default;

  // ========== Segment Lifecycle ==========

  [[nodiscard]] core::StatusOr<core::SegmentId> CreateSegment(
      core::CollectionId collection_id,
      core::Dimension dimension,
      core::MetricType metric) override;

  [[nodiscard]] core::Status CreateSegmentWithId(
      core::SegmentId segment_id,
      core::CollectionId collection_id,
      core::Dimension dimension,
      core::MetricType metric,
      core::IndexType index_type) override;

  [[nodiscard]] Segment* GetSegment(core::SegmentId id) override;
  [[nodiscard]] const Segment* GetSegment(core::SegmentId id) const override;

  [[nodiscard]] core::Status SealSegment(
      core::SegmentId id,
      const core::IndexConfig& index_config) override;

  [[nodiscard]] core::Status FlushSegment(core::SegmentId id) override;

  [[nodiscard]] core::Status DropSegment(
      core::SegmentId id, bool delete_files = false) override;

  [[nodiscard]] core::Status LoadSegment(core::SegmentId id) override;

  [[nodiscard]] core::Status AddReplicatedSegment(
      std::unique_ptr<Segment> segment) override;

  // ========== Data Operations ==========

  [[nodiscard]] core::Status WriteVectors(
      core::SegmentId segment_id,
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids) override;

  [[nodiscard]] core::StatusOr<std::vector<core::Vector>> ReadVectors(
      core::SegmentId segment_id,
      const std::vector<core::VectorId>& ids) const override;

  [[nodiscard]] core::StatusOr<core::SearchResult> SearchSegment(
      core::SegmentId segment_id,
      const core::Vector& query,
      int k) const override;

  [[nodiscard]] core::StatusOr<core::SearchResult> SearchCollection(
      core::CollectionId collection_id,
      const core::Vector& query,
      int k) const override;

  // ========== Active Segment Rotation ==========

  void SetSealCallback(SealCallback callback) override;

  [[nodiscard]] Segment* GetWritableSegment(
      core::CollectionId collection_id,
      size_t required_bytes = 0) override;

  [[nodiscard]] std::vector<Segment*> GetQueryableSegments(
      core::CollectionId collection_id) const override;

  [[nodiscard]] std::vector<core::SegmentId> GetAllSegmentIds() const override;

  void RunTTLSweepLoop(const std::atomic<bool>& shutdown) override;

  // ========== Management ==========

  [[nodiscard]] absl::Status LoadAllSegments() override;

  [[nodiscard]] std::vector<core::SegmentId> GetCollectionSegments(
      core::CollectionId collection_id) const override;

  [[nodiscard]] size_t GetSegmentCount() const override;

  [[nodiscard]] size_t GetTotalMemoryUsage() const override;

  void Clear() override;

  // Get the base path for segment storage on local disk
  [[nodiscard]] const std::string& GetBasePath() const { return base_path_; }

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