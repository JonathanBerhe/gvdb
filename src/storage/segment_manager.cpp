#include "storage/segment_manager.h"

#include <algorithm>
#include <filesystem>

#include "absl/strings/str_cat.h"
#include "utils/logger.h"

namespace gvdb {
namespace storage {

SegmentManager::SegmentManager(const std::string& base_path,
                               core::IIndexFactory* index_factory)
    : base_path_(base_path),
      index_factory_(index_factory),
      next_segment_id_(1) {
  // Create base directory if it doesn't exist
  std::filesystem::create_directories(base_path_);
}

// ========== Segment Lifecycle ==========

core::StatusOr<core::SegmentId> SegmentManager::CreateSegment(
    core::CollectionId collection_id, core::Dimension dimension,
    core::MetricType metric) {
  std::unique_lock lock(mutex_);

  // Allocate segment ID
  auto segment_id = AllocateSegmentId();

  // Create segment
  auto segment =
      std::make_unique<Segment>(segment_id, collection_id, dimension, metric);

  // Store segment
  segments_[segment_id] = std::move(segment);

  // Add to collection mapping
  collection_segments_[collection_id].push_back(segment_id);

  return segment_id;
}

Segment* SegmentManager::GetSegment(core::SegmentId id) {
  std::shared_lock lock(mutex_);

  auto it = segments_.find(id);
  if (it == segments_.end()) {
    return nullptr;
  }
  return it->second.get();
}

const Segment* SegmentManager::GetSegment(core::SegmentId id) const {
  std::shared_lock lock(mutex_);

  auto it = segments_.find(id);
  if (it == segments_.end()) {
    return nullptr;
  }
  return it->second.get();
}

core::Status SegmentManager::SealSegment(
    core::SegmentId id, const core::IndexConfig& index_config) {
  // Get segment (read lock first)
  Segment* segment = nullptr;
  {
    std::shared_lock lock(mutex_);
    auto it = segments_.find(id);
    if (it == segments_.end()) {
      return core::NotFoundError(
          absl::StrCat("Segment not found: ", core::ToUInt32(id)));
    }
    segment = it->second.get();
  }

  // Create index
  auto index_result = index_factory_->CreateIndex(index_config);
  if (!index_result.ok()) {
    return index_result.status();
  }

  // Seal segment (transfers ownership of index to segment)
  return segment->Seal(index_result.value().release());
}

core::Status SegmentManager::FlushSegment(core::SegmentId id) {
  std::shared_lock lock(mutex_);

  auto it = segments_.find(id);
  if (it == segments_.end()) {
    return core::NotFoundError(
        absl::StrCat("Segment not found: ", core::ToUInt32(id)));
  }

  std::string segment_path = GetSegmentPath(id);
  std::filesystem::create_directories(segment_path);

  return it->second->Flush(base_path_);
}

core::Status SegmentManager::DropSegment(core::SegmentId id,
                                          bool delete_files) {
  std::unique_lock lock(mutex_);

  auto it = segments_.find(id);
  if (it == segments_.end()) {
    return core::NotFoundError(
        absl::StrCat("Segment not found: ", core::ToUInt32(id)));
  }

  // Get collection ID for cleanup
  auto collection_id = it->second->GetCollectionId();

  // Remove from segments map
  segments_.erase(it);

  // Remove from collection mapping
  auto& coll_segments = collection_segments_[collection_id];
  coll_segments.erase(
      std::remove(coll_segments.begin(), coll_segments.end(), id),
      coll_segments.end());

  // Delete files if requested
  if (delete_files) {
    std::string segment_path = GetSegmentPath(id);
    std::error_code ec;
    std::filesystem::remove_all(segment_path, ec);
    if (ec) {
      return core::InternalError(
          absl::StrCat("Failed to delete segment files: ", ec.message()));
    }
  }

  return core::OkStatus();
}

core::Status SegmentManager::LoadSegment(core::SegmentId id) {
  // Load segment from disk
  auto segment_result = Segment::Load(base_path_, id);
  if (!segment_result.ok()) {
    return segment_result.status();
  }

  std::unique_lock lock(mutex_);

  // Check if already loaded
  if (segments_.find(id) != segments_.end()) {
    return core::AlreadyExistsError(
        absl::StrCat("Segment already loaded: ", core::ToUInt32(id)));
  }

  auto segment = std::move(segment_result.value());
  auto collection_id = segment->GetCollectionId();

  // Store segment
  segments_[id] = std::move(segment);

  // Add to collection mapping
  collection_segments_[collection_id].push_back(id);

  return core::OkStatus();
}

core::Status SegmentManager::AddReplicatedSegment(std::unique_ptr<Segment> segment) {
  if (!segment) {
    return core::InvalidArgumentError("Segment is null");
  }

  auto id = segment->GetId();
  auto collection_id = segment->GetCollectionId();

  std::unique_lock lock(mutex_);

  // Check if segment already exists
  if (segments_.find(id) != segments_.end()) {
    return core::AlreadyExistsError(
        absl::StrCat("Segment already exists: ", core::ToUInt32(id)));
  }

  // Store segment
  segments_[id] = std::move(segment);

  // Add to collection mapping
  collection_segments_[collection_id].push_back(id);

  utils::Logger::Instance().Info("Added replicated segment {} to collection {}",
                                  core::ToUInt32(id), core::ToUInt32(collection_id));

  return core::OkStatus();
}

// ========== Data Operations ==========

core::Status SegmentManager::WriteVectors(
    core::SegmentId segment_id, const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {
  std::shared_lock lock(mutex_);

  auto it = segments_.find(segment_id);
  if (it == segments_.end()) {
    return core::NotFoundError(
        absl::StrCat("Segment not found: ", core::ToUInt32(segment_id)));
  }

  return it->second->AddVectors(vectors, ids);
}

core::StatusOr<std::vector<core::Vector>> SegmentManager::ReadVectors(
    core::SegmentId segment_id,
    const std::vector<core::VectorId>& ids) const {
  std::shared_lock lock(mutex_);

  auto it = segments_.find(segment_id);
  if (it == segments_.end()) {
    return core::NotFoundError(
        absl::StrCat("Segment not found: ", core::ToUInt32(segment_id)));
  }

  return it->second->ReadVectors(ids);
}

core::StatusOr<core::SearchResult> SegmentManager::SearchSegment(
    core::SegmentId segment_id, const core::Vector& query, int k) const {
  std::shared_lock lock(mutex_);

  auto it = segments_.find(segment_id);
  if (it == segments_.end()) {
    return core::NotFoundError(
        absl::StrCat("Segment not found: ", core::ToUInt32(segment_id)));
  }

  return it->second->Search(query, k);
}

core::StatusOr<core::SearchResult> SegmentManager::SearchCollection(
    core::CollectionId collection_id, const core::Vector& query, int k) const {
  std::shared_lock lock(mutex_);

  // Get all segments for collection
  auto coll_it = collection_segments_.find(collection_id);
  if (coll_it == collection_segments_.end()) {
    return core::NotFoundError(absl::StrCat(
        "Collection not found: ", core::ToUInt32(collection_id)));
  }

  const auto& segment_ids = coll_it->second;
  if (segment_ids.empty()) {
    return core::SearchResult{};  // Empty result
  }

  // Search each segment and merge results
  std::vector<core::SearchResultEntry> all_results;

  for (const auto& segment_id : segment_ids) {
    auto it = segments_.find(segment_id);
    if (it == segments_.end()) {
      continue;  // Skip missing segments
    }

    // Search sealed, flushed, and growing segments (for real-time queries)
    // Skip only DROPPED segments
    if (it->second->GetState() == core::SegmentState::DROPPED) {
      continue;
    }

    auto search_result = it->second->Search(query, k);
    if (search_result.ok()) {
      all_results.insert(all_results.end(),
                         search_result.value().entries.begin(),
                         search_result.value().entries.end());
    }
  }

  // Sort by distance and take top k
  std::partial_sort(all_results.begin(),
                    all_results.begin() + std::min(k, (int)all_results.size()),
                    all_results.end(),
                    [](const core::SearchResultEntry& a,
                       const core::SearchResultEntry& b) {
                      return a.distance < b.distance;
                    });

  // Keep only top k
  if (all_results.size() > static_cast<size_t>(k)) {
    all_results.resize(k);
  }

  core::SearchResult result;
  result.entries = std::move(all_results);
  return result;
}

// ========== Management ==========

std::vector<core::SegmentId> SegmentManager::GetCollectionSegments(
    core::CollectionId collection_id) const {
  std::shared_lock lock(mutex_);

  auto it = collection_segments_.find(collection_id);
  if (it == collection_segments_.end()) {
    return {};
  }
  return it->second;
}

size_t SegmentManager::GetSegmentCount() const {
  std::shared_lock lock(mutex_);
  return segments_.size();
}

size_t SegmentManager::GetTotalMemoryUsage() const {
  std::shared_lock lock(mutex_);

  size_t total = 0;
  for (const auto& [id, segment] : segments_) {
    total += segment->GetMemoryUsage();
  }
  return total;
}

void SegmentManager::Clear() {
  std::unique_lock lock(mutex_);
  segments_.clear();
  collection_segments_.clear();
  next_segment_id_ = 1;
}

// ========== Private Methods ==========

core::SegmentId SegmentManager::AllocateSegmentId() {
  // Must be called with lock held
  return core::MakeSegmentId(next_segment_id_++);
}

std::string SegmentManager::GetSegmentPath(core::SegmentId id) const {
  return absl::StrCat(base_path_, "/segment_", core::ToUInt32(id));
}

}  // namespace storage
}  // namespace gvdb
