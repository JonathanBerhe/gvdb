// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_LOCAL_STORAGE_H_
#define GVDB_STORAGE_LOCAL_STORAGE_H_

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "core/config.h"
#include "core/interfaces.h"
#include "core/status.h"
#include "core/types.h"
#include "storage/segment_manager.h"

namespace gvdb {
namespace storage {

// ============================================================================
// LocalStorage - Local disk-based implementation of IStorage interface
// ============================================================================
// Provides unified storage layer combining segment management and metadata
// Thread-safety: All operations are thread-safe
class LocalStorage : public core::IStorage {
 public:
  explicit LocalStorage(const core::StorageConfig& config,
                        core::IIndexFactory* index_factory);

  ~LocalStorage() override = default;

  // Disable copy and move
  LocalStorage(const LocalStorage&) = delete;
  LocalStorage& operator=(const LocalStorage&) = delete;
  LocalStorage(LocalStorage&&) = delete;
  LocalStorage& operator=(LocalStorage&&) = delete;

  // ========== Segment operations (from IStorage) ==========

  [[nodiscard]] core::StatusOr<core::SegmentId> CreateSegment(
      core::CollectionId collection_id) override;

  [[nodiscard]] core::Status WriteVectors(
      core::SegmentId segment_id,
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids) override;

  [[nodiscard]] core::StatusOr<std::vector<core::Vector>> ReadVectors(
      core::SegmentId segment_id,
      const std::vector<core::VectorId>& ids) override;

  [[nodiscard]] core::Status SealSegment(core::SegmentId segment_id) override;

  [[nodiscard]] core::Status FlushSegment(core::SegmentId segment_id) override;

  [[nodiscard]] core::Status DropSegment(core::SegmentId segment_id) override;

  [[nodiscard]] core::StatusOr<core::SegmentState> GetSegmentState(
      core::SegmentId segment_id) const override;

  // ========== Metadata operations (from IStorage) ==========

  [[nodiscard]] core::Status PutMetadata(
      const std::string& key, const std::string& value) override;

  [[nodiscard]] core::StatusOr<std::string> GetMetadata(
      const std::string& key) const override;

  [[nodiscard]] core::Status DeleteMetadata(const std::string& key) override;

  // ========== Storage management (from IStorage) ==========

  [[nodiscard]] size_t GetStorageSize() const override;

  [[nodiscard]] core::Status Compact() override;

  [[nodiscard]] core::Status Close() override;

 private:
  // Configuration
  core::StorageConfig config_;

  // Segment manager
  std::unique_ptr<SegmentManager> segment_manager_;

  // Metadata store (simple in-memory map for now)
  std::unordered_map<std::string, std::string> metadata_;
  mutable std::shared_mutex metadata_mutex_;

  // Collection metadata (dimension and metric per collection)
  struct CollectionMeta {
    core::Dimension dimension;
    core::MetricType metric;
  };
  std::unordered_map<core::CollectionId, CollectionMeta> collections_;
  mutable std::shared_mutex collections_mutex_;

  // Helper methods
  [[nodiscard]] core::StatusOr<CollectionMeta> GetCollectionMeta(
      core::CollectionId collection_id) const;
  [[nodiscard]] core::Status RegisterCollection(
      core::CollectionId collection_id,
      core::Dimension dimension,
      core::MetricType metric);

  // Compaction helpers
  [[nodiscard]] core::Status CompactCollection(core::CollectionId collection_id);
  [[nodiscard]] core::Status MergeSegments(core::SegmentId seg1_id,
                                            core::SegmentId seg2_id,
                                            core::CollectionId collection_id);
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_LOCAL_STORAGE_H_