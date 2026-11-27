// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// TieredSegmentManager: segment lifecycle and data operations, delegated
// to the local segment manager. tiered_segment_sync.cpp holds the object
// store side: upload with retry, download and discovery.

#include "storage/segment/tiered_segment_manager.h"
#include "core/status.h"
#include <algorithm>
#include <array>
#include <mutex>
#include <vector>
#include "absl/container/flat_hash_set.h"

namespace gvdb {
namespace storage {

TieredSegmentManager::TieredSegmentManager(
    std::unique_ptr<SegmentManager> local,
    std::unique_ptr<IObjectStore> object_store,
    std::unique_ptr<SegmentCache> cache,
    const std::string& prefix,
    int upload_threads,
    std::chrono::milliseconds retry_base_delay)
    : local_(std::move(local)),
      object_store_(std::move(object_store)),
      cache_(std::move(cache)),
      prefix_(prefix),
      upload_pool_(std::make_unique<common::ThreadPool>(
          std::max(1, upload_threads))),
      retry_base_delay_(retry_base_delay) {
  retry_thread_ = std::thread([this]() { RetryLoop(); });
}

TieredSegmentManager::~TieredSegmentManager() {
  // Stop the retry thread before draining the pool: signal, wake, join. Once it
  // has exited no new upload tasks can be enqueued, so the pool can drain any
  // in-flight upload cleanly (ThreadPool destructor waits for pending tasks).
  {
    std::lock_guard lock(upload_mutex_);
    retry_stop_ = true;
  }
  retry_cv_.notify_all();
  if (retry_thread_.joinable()) retry_thread_.join();
  upload_pool_.reset();
}

// ============================================================================
// Segment Lifecycle: delegate to local, add S3 on top
// ============================================================================

absl::StatusOr<core::SegmentId> TieredSegmentManager::CreateSegment(
    core::CollectionId collection_id,
    core::Dimension dimension,
    core::MetricType metric) {
  return local_->CreateSegment(collection_id, dimension, metric);
}

absl::Status TieredSegmentManager::CreateSegmentWithId(
    core::SegmentId segment_id,
    core::CollectionId collection_id,
    core::Dimension dimension,
    core::MetricType metric,
    core::IndexType index_type) {
  return local_->CreateSegmentWithId(
      segment_id, collection_id, dimension, metric, index_type);
}

Segment* TieredSegmentManager::GetSegment(core::SegmentId id) {
  // Try local first
  auto* seg = local_->GetSegment(id);
  if (seg != nullptr) return seg;

  // Try lazy load from S3
  auto status = EnsureSegmentLoaded(id);
  if (!status.ok()) return nullptr;

  return local_->GetSegment(id);
}

const Segment* TieredSegmentManager::GetSegment(core::SegmentId id) const {
  const auto* seg = local_->GetSegment(id);
  if (seg != nullptr) return seg;

  auto status = EnsureSegmentLoaded(id);
  if (!status.ok()) return nullptr;

  return local_->GetSegment(id);
}

absl::Status TieredSegmentManager::SealSegment(
    core::SegmentId id, const core::IndexConfig& index_config) {
  return local_->SealSegment(id, index_config);
}

absl::Status TieredSegmentManager::FlushSegment(core::SegmentId id) {
  // 1. Flush to local disk
  RETURN_IF_ERROR(local_->FlushSegment(id));

  // 2. Async upload to object storage. A failed upload is not dropped: RunUpload
  //    records it in failed_uploads_ and the retry thread re-drives it.
  auto seg_id = id;
  {
    std::lock_guard lock(upload_mutex_);
    uploading_.insert(core::ToUInt32(seg_id));
  }

  upload_pool_->enqueue([this, seg_id]() { RunUpload(seg_id, 0); });

  return absl::OkStatus();
}

absl::Status TieredSegmentManager::DropSegment(
    core::SegmentId id, bool delete_files) {
  auto status = local_->DropSegment(id, delete_files);

  // Copy remote info under lock, then do S3 I/O without holding the lock
  // to avoid blocking concurrent readers during potentially slow network calls.
  auto seg_key = core::ToUInt32(id);
  RemoteSegmentInfo info;
  bool is_remote = false;
  {
    std::shared_lock lock(remote_mutex_);
    auto it = remote_segments_.find(seg_key);
    if (it != remote_segments_.end()) {
      info = it->second;
      is_remote = true;
    }
  }

  if (is_remote) {
    auto col_id = core::ToUInt32(info.collection_id);

    // Delete files from S3 (best-effort, don't block on failure)
    for (const auto& filename : kSegmentFiles) {
      (void)object_store_->DeleteObject(MakeS3Key(col_id, seg_key, filename));
    }

    // Update manifest (best-effort)
    ManifestEntry dummy;
    dummy.segment_id = seg_key;
    (void)UpdateManifest(dummy, /*remove=*/true);
  }

  // Remove from remote tracking
  {
    std::unique_lock lock(remote_mutex_);
    remote_segments_.erase(seg_key);
  }

  return status;
}

absl::Status TieredSegmentManager::AddReplicatedSegment(
    std::unique_ptr<Segment> segment) {
  return local_->AddReplicatedSegment(std::move(segment));
}

// ============================================================================
// Data Operations: delegate with lazy load fallback
// ============================================================================

absl::Status TieredSegmentManager::WriteVectors(
    core::SegmentId segment_id,
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {
  return local_->WriteVectors(segment_id, vectors, ids);
}

absl::StatusOr<core::SearchResult> TieredSegmentManager::SearchCollection(
    core::CollectionId collection_id,
    const core::Vector& query, int k) const {
  // Collect remote segment IDs under lock, then load outside the lock
  // to avoid recursive shared_mutex acquisition (UB per C++ standard).
  std::vector<core::SegmentId> to_load;
  {
    std::shared_lock lock(remote_mutex_);
    for (const auto& [seg_key, info] : remote_segments_) {
      if (info.collection_id == collection_id) {
        auto seg_id = static_cast<core::SegmentId>(seg_key);
        if (local_->GetSegment(seg_id) == nullptr) {
          to_load.push_back(seg_id);
        }
      }
    }
  }
  for (auto seg_id : to_load) {
    (void)EnsureSegmentLoaded(seg_id);
  }

  return local_->SearchCollection(collection_id, query, k);
}

// ============================================================================
// Active Segment Rotation: pure delegation
// ============================================================================

void TieredSegmentManager::SetSealCallback(SealCallback callback) {
  local_->SetSealCallback(std::move(callback));
}

Segment* TieredSegmentManager::GetWritableSegment(
    core::CollectionId collection_id, size_t required_bytes) {
  return local_->GetWritableSegment(collection_id, required_bytes);
}

void TieredSegmentManager::RunTTLSweepLoop(const std::atomic<bool>& shutdown) {
  local_->RunTTLSweepLoop(shutdown);
}

// ============================================================================
// Management
// ============================================================================

absl::Status TieredSegmentManager::LoadAllSegments() {
  // 1. Load local segments
  RETURN_IF_ERROR(local_->LoadAllSegments());

  // 2. Discover remote segments from S3 manifest
  auto s3_status = DiscoverSegmentsFromS3();
  if (!s3_status.ok()) {
    (void)s3_status;
    // Non-fatal, local segments still work
  }

  // 3. Re-drive segments that are FLUSHED locally but missing from the remote
  //    manifest: their original upload never reached the cold tier.
  RedriveUnuploadedSegments();

  return absl::OkStatus();
}

std::vector<core::SegmentId> TieredSegmentManager::GetCollectionSegments(
    core::CollectionId collection_id) const {
  auto local_segs = local_->GetCollectionSegments(collection_id);

  // Add remote segments for this collection (O(1) lookup via set)
  absl::flat_hash_set<uint32_t> local_set;
  local_set.reserve(local_segs.size());
  for (auto id : local_segs) local_set.insert(core::ToUInt32(id));

  std::shared_lock lock(remote_mutex_);
  for (const auto& [seg_key, info] : remote_segments_) {
    if (info.collection_id == collection_id && !local_set.contains(seg_key)) {
      local_segs.push_back(static_cast<core::SegmentId>(seg_key));
    }
  }
  return local_segs;
}

size_t TieredSegmentManager::GetSegmentCount() const {
  auto local_count = local_->GetSegmentCount();
  std::shared_lock lock(remote_mutex_);
  // Count remote segments not loaded locally
  size_t remote_only = 0;
  for (const auto& [seg_key, _] : remote_segments_) {
    if (local_->GetSegment(static_cast<core::SegmentId>(seg_key)) == nullptr) {
      ++remote_only;
    }
  }
  return local_count + remote_only;
}

void TieredSegmentManager::Clear() {
  local_->Clear();
  std::unique_lock lock(remote_mutex_);
  remote_segments_.clear();
}

}  // namespace storage
}  // namespace gvdb
