#include "storage/local_storage.h"

#include <filesystem>
#include <fstream>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace storage {

LocalStorage::LocalStorage(const core::StorageConfig& config,
                           core::IIndexFactory* index_factory)
    : config_(config) {
  // Create base directory
  std::filesystem::create_directories(config_.base_path);

  // Initialize segment manager
  segment_manager_ =
      std::make_unique<SegmentManager>(config_.base_path, index_factory);
}

// ========== Segment operations ==========

core::StatusOr<core::SegmentId> LocalStorage::CreateSegment(
    core::CollectionId collection_id) {
  // Get collection metadata to create segment with correct dimension/metric
  auto meta_result = GetCollectionMeta(collection_id);
  if (!meta_result.ok()) {
    return meta_result.status();
  }

  auto meta = meta_result.value();
  return segment_manager_->CreateSegment(collection_id, meta.dimension,
                                         meta.metric);
}

core::Status LocalStorage::WriteVectors(
    core::SegmentId segment_id, const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {
  // Get segment to extract collection metadata
  auto* segment = segment_manager_->GetSegment(segment_id);
  if (segment == nullptr) {
    return core::NotFoundError(
        absl::StrCat("Segment not found: ", core::ToUInt32(segment_id)));
  }

  // Register collection if not already registered
  auto register_status = RegisterCollection(
      segment->GetCollectionId(), segment->GetDimension(), segment->GetMetric());
  if (!register_status.ok() &&
      !absl::IsAlreadyExists(register_status)) {
    return register_status;
  }

  return segment_manager_->WriteVectors(segment_id, vectors, ids);
}

core::StatusOr<std::vector<core::Vector>> LocalStorage::ReadVectors(
    core::SegmentId segment_id, const std::vector<core::VectorId>& ids) {
  return segment_manager_->ReadVectors(segment_id, ids);
}

core::Status LocalStorage::SealSegment(core::SegmentId segment_id) {
  // Get segment info
  auto* segment = segment_manager_->GetSegment(segment_id);
  if (segment == nullptr) {
    return core::NotFoundError(
        absl::StrCat("Segment not found: ", core::ToUInt32(segment_id)));
  }

  // Create default index config based on segment properties
  core::IndexConfig index_config;
  index_config.index_type = core::IndexType::FLAT;  // Default to FLAT
  index_config.dimension = segment->GetDimension();
  index_config.metric_type = segment->GetMetric();

  return segment_manager_->SealSegment(segment_id, index_config);
}

core::Status LocalStorage::FlushSegment(core::SegmentId segment_id) {
  return segment_manager_->FlushSegment(segment_id);
}

core::Status LocalStorage::DropSegment(core::SegmentId segment_id) {
  return segment_manager_->DropSegment(segment_id, true);  // Delete files
}

core::StatusOr<core::SegmentState> LocalStorage::GetSegmentState(
    core::SegmentId segment_id) const {
  auto* segment = segment_manager_->GetSegment(segment_id);
  if (segment == nullptr) {
    return core::NotFoundError(
        absl::StrCat("Segment not found: ", core::ToUInt32(segment_id)));
  }

  return segment->GetState();
}

// ========== Metadata operations ==========

core::Status LocalStorage::PutMetadata(const std::string& key,
                                        const std::string& value) {
  std::unique_lock lock(metadata_mutex_);
  metadata_[key] = value;
  return core::OkStatus();
}

core::StatusOr<std::string> LocalStorage::GetMetadata(
    const std::string& key) const {
  std::shared_lock lock(metadata_mutex_);

  auto it = metadata_.find(key);
  if (it == metadata_.end()) {
    return core::NotFoundError(absl::StrCat("Metadata key not found: ", key));
  }

  return it->second;
}

core::Status LocalStorage::DeleteMetadata(const std::string& key) {
  std::unique_lock lock(metadata_mutex_);

  auto it = metadata_.find(key);
  if (it == metadata_.end()) {
    return core::NotFoundError(absl::StrCat("Metadata key not found: ", key));
  }

  metadata_.erase(it);
  return core::OkStatus();
}

// ========== Storage management ==========

size_t LocalStorage::GetStorageSize() const {
  size_t total_size = 0;

  // Add segment memory usage
  total_size += segment_manager_->GetTotalMemoryUsage();

  // Add metadata size (approximate)
  {
    std::shared_lock lock(metadata_mutex_);
    for (const auto& [key, value] : metadata_) {
      total_size += key.size() + value.size();
    }
  }

  return total_size;
}

core::Status LocalStorage::Compact() {
  // Get all collections
  std::unordered_map<core::CollectionId, CollectionMeta> collections_snapshot;
  {
    std::shared_lock lock(collections_mutex_);
    collections_snapshot = collections_;
  }

  // Compact each collection
  for (const auto& [collection_id, meta] : collections_snapshot) {
    auto compact_status = CompactCollection(collection_id);
    if (!compact_status.ok()) {
      // Log error but continue with other collections
      // In production, this would use proper logging
      continue;
    }
  }

  return core::OkStatus();
}

core::Status LocalStorage::Close() {
  // Clear in-memory structures
  segment_manager_->Clear();

  {
    std::unique_lock lock(metadata_mutex_);
    metadata_.clear();
  }

  {
    std::unique_lock lock(collections_mutex_);
    collections_.clear();
  }

  return core::OkStatus();
}

// ========== Private helper methods ==========

core::StatusOr<LocalStorage::CollectionMeta> LocalStorage::GetCollectionMeta(
    core::CollectionId collection_id) const {
  std::shared_lock lock(collections_mutex_);

  auto it = collections_.find(collection_id);
  if (it == collections_.end()) {
    return core::NotFoundError(absl::StrCat(
        "Collection not registered: ", core::ToUInt32(collection_id)));
  }

  return it->second;
}

core::Status LocalStorage::RegisterCollection(core::CollectionId collection_id,
                                                core::Dimension dimension,
                                                core::MetricType metric) {
  std::unique_lock lock(collections_mutex_);

  auto it = collections_.find(collection_id);
  if (it != collections_.end()) {
    // Verify metadata matches
    if (it->second.dimension != dimension || it->second.metric != metric) {
      return core::FailedPreconditionError(absl::StrCat(
          "Collection ", core::ToUInt32(collection_id),
          " already registered with different metadata"));
    }
    return core::AlreadyExistsError(absl::StrCat(
        "Collection already registered: ", core::ToUInt32(collection_id)));
  }

  collections_[collection_id] = CollectionMeta{dimension, metric};
  return core::OkStatus();
}

core::Status LocalStorage::CompactCollection(core::CollectionId collection_id) {
  // Get all segments for this collection
  auto segment_ids = segment_manager_->GetCollectionSegments(collection_id);

  if (segment_ids.size() < 2) {
    // Need at least 2 segments to compact
    return core::OkStatus();
  }

  // Find sealed segments that are candidates for compaction
  struct SegmentInfo {
    core::SegmentId id;
    size_t vector_count;
    size_t memory_usage;
  };

  std::vector<SegmentInfo> compaction_candidates;

  for (const auto& seg_id : segment_ids) {
    auto* segment = segment_manager_->GetSegment(seg_id);
    if (segment == nullptr) continue;

    auto state = segment->GetState();
    // Only compact sealed or flushed segments
    if (state != core::SegmentState::SEALED &&
        state != core::SegmentState::FLUSHED) {
      continue;
    }

    // Only compact small segments (less than threshold)
    size_t memory = segment->GetMemoryUsage();
    if (memory < config_.compaction_threshold) {
      compaction_candidates.push_back({
        seg_id,
        segment->GetVectorCount(),
        memory
      });
    }
  }

  // Need at least 2 small segments to merge
  if (compaction_candidates.size() < 2) {
    return core::OkStatus();
  }

  // Sort by size (merge smallest first)
  std::sort(compaction_candidates.begin(), compaction_candidates.end(),
            [](const SegmentInfo& a, const SegmentInfo& b) {
              return a.memory_usage < b.memory_usage;
            });

  // Merge segments in pairs to avoid creating too-large segments
  for (size_t i = 0; i + 1 < compaction_candidates.size(); i += 2) {
    auto status = MergeSegments(compaction_candidates[i].id,
                                 compaction_candidates[i + 1].id,
                                 collection_id);
    if (!status.ok()) {
      // Log error but continue
      continue;
    }
  }

  return core::OkStatus();
}

core::Status LocalStorage::MergeSegments(core::SegmentId seg1_id,
                                          core::SegmentId seg2_id,
                                          core::CollectionId collection_id) {
  // Verify collection exists
  auto meta_result = GetCollectionMeta(collection_id);
  if (!meta_result.ok()) {
    return meta_result.status();
  }
  // Note: meta would be used to validate segment compatibility in full implementation

  // Get both segments
  auto* seg1 = segment_manager_->GetSegment(seg1_id);
  auto* seg2 = segment_manager_->GetSegment(seg2_id);

  if (seg1 == nullptr || seg2 == nullptr) {
    return core::NotFoundError("One or both segments not found for merge");
  }

  // Verify both segments are sealed (have vectors in memory)
  if (seg1->GetState() != core::SegmentState::SEALED &&
      seg1->GetState() != core::SegmentState::FLUSHED) {
    return core::FailedPreconditionError("Segment 1 must be sealed");
  }
  if (seg2->GetState() != core::SegmentState::SEALED &&
      seg2->GetState() != core::SegmentState::FLUSHED) {
    return core::FailedPreconditionError("Segment 2 must be sealed");
  }

  // OPTION 1 (CURRENT): Read vectors from memory
  // Since we keep vectors in memory after sealing, we can access them directly
  // by reading all vector IDs and then calling ReadVectors

  // Note: We need to get all vector IDs from the segments
  // For now, this is a limitation - we don't track all IDs in a sealed segment
  // TODO: Add GetAllVectorIds() method to Segment class

  return core::UnimplementedError(
      "Segment merging requires GetAllVectorIds() method. "
      "TODO: Add Segment::GetAllVectorIds() to retrieve IDs from sealed segments, "
      "then use Segment::ReadVectors() to get vectors (which are now in memory).");

  // OPTION 2 (FUTURE): Disk-based compaction
  // When we implement disk-based vector storage:
  // 1. Load vectors from disk files (segment_1/vectors.bin)
  // 2. Merge vectors from both segments
  // 3. Create new merged segment
  // 4. Write merged vectors to disk
  // 5. Build new index
  // 6. Delete old segments
  //
  // See STORAGE_COMPACTION_OPTIONS.md for implementation details
}

}  // namespace storage
}  // namespace gvdb
