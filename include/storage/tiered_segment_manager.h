// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_TIERED_SEGMENT_MANAGER_H_
#define GVDB_STORAGE_TIERED_SEGMENT_MANAGER_H_

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "storage/segment_store.h"
#include "storage/segment_manager.h"
#include "storage/object_store.h"
#include "storage/segment_cache.h"
#include "storage/segment_manifest.h"
#include "utils/thread_pool.h"

namespace gvdb {
namespace storage {

// ============================================================================
// TieredSegmentManager - Local disk + object storage tiered backend
// ============================================================================
// Composes a local SegmentManager with an IObjectStore backend. GROWING
// segments live on local disk. Sealed segments are uploaded to object storage
// asynchronously after local flush. Remote segments are discovered via manifest
// on startup and lazily downloaded on first access.
//
// All ISegmentStore methods are implemented via delegation to the local
// SegmentManager, with S3 upload/download/discovery added on top.
class TieredSegmentManager : public ISegmentStore {
 public:
  TieredSegmentManager(
      std::unique_ptr<SegmentManager> local,
      std::unique_ptr<IObjectStore> object_store,
      std::unique_ptr<SegmentCache> cache,
      const std::string& prefix,
      int upload_threads = 2);

  ~TieredSegmentManager() override;

  // ── ISegmentStore implementation ──

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

  [[nodiscard]] core::Status WriteVectors(
      core::SegmentId segment_id,
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids) override;

  [[nodiscard]] core::StatusOr<std::vector<core::Vector>> ReadVectors(
      core::SegmentId segment_id,
      const std::vector<core::VectorId>& ids) const override;

  [[nodiscard]] core::StatusOr<core::SearchResult> SearchSegment(
      core::SegmentId segment_id,
      const core::Vector& query, int k) const override;

  [[nodiscard]] core::StatusOr<core::SearchResult> SearchCollection(
      core::CollectionId collection_id,
      const core::Vector& query, int k) const override;

  void SetSealCallback(SealCallback callback) override;

  [[nodiscard]] Segment* GetWritableSegment(
      core::CollectionId collection_id,
      size_t required_bytes = 0) override;

  [[nodiscard]] std::vector<Segment*> GetQueryableSegments(
      core::CollectionId collection_id) const override;

  [[nodiscard]] std::vector<core::SegmentId> GetAllSegmentIds() const override;

  void RunTTLSweepLoop(const std::atomic<bool>& shutdown) override;

  [[nodiscard]] absl::Status LoadAllSegments() override;

  [[nodiscard]] std::vector<core::SegmentId> GetCollectionSegments(
      core::CollectionId collection_id) const override;

  [[nodiscard]] size_t GetSegmentCount() const override;
  [[nodiscard]] size_t GetTotalMemoryUsage() const override;

  void Clear() override;

  // ── S3-specific accessors (for testing / diagnostics) ──

  // Number of segments known to exist in object storage
  [[nodiscard]] size_t GetRemoteSegmentCount() const;

  // Whether a segment is currently being uploaded
  [[nodiscard]] bool IsUploading(core::SegmentId id) const;

 private:
  std::unique_ptr<SegmentManager> local_;
  std::unique_ptr<IObjectStore> object_store_;
  std::unique_ptr<SegmentCache> cache_;
  std::string prefix_;

  // Async upload pool
  std::unique_ptr<utils::ThreadPool> upload_pool_;

  // Track in-flight uploads
  mutable std::mutex upload_mutex_;
  std::unordered_set<uint32_t> uploading_;

  // Remote segment metadata (discovered from manifest, not yet loaded)
  struct RemoteSegmentInfo {
    core::CollectionId collection_id;
    core::Dimension dimension;
    core::MetricType metric;
    core::IndexType index_type;
    uint64_t vector_count;
    uint64_t size_bytes;
  };
  mutable std::shared_mutex remote_mutex_;
  std::unordered_map<uint32_t, RemoteSegmentInfo> remote_segments_;

  // Manifest JSON (cached in memory)
  mutable std::mutex manifest_mutex_;
  std::string manifest_json_;

  // ── Internal methods ──

  // Upload a flushed segment's files to object storage
  core::Status UploadSegmentToS3(core::SegmentId id);

  // Download a segment's files from object storage to local cache
  core::Status DownloadSegmentFromS3(core::SegmentId id) const;

  // Discover segments from manifest (or fallback to ListObjects)
  core::Status DiscoverSegmentsFromS3();

  // Update manifest in object storage after upload/delete
  core::Status UpdateManifest(const ManifestEntry& entry, bool remove = false);

  // Ensure a remote segment is loaded locally (lazy load)
  core::Status EnsureSegmentLoaded(core::SegmentId id) const;

  // Build S3 key for a segment file
  [[nodiscard]] std::string MakeS3Key(
      uint32_t collection_id, uint32_t segment_id,
      const std::string& filename) const;

  // Get the local base path from the underlying SegmentManager
  [[nodiscard]] const std::string& GetLocalBasePath() const;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_TIERED_SEGMENT_MANAGER_H_
