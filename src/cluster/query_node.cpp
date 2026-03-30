// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/query_node.h"
#include "utils/logger.h"
#include "absl/strings/str_cat.h"

namespace gvdb {
namespace cluster {

QueryNode::QueryNode(std::shared_ptr<storage::SegmentManager> segment_manager,
                     std::shared_ptr<compute::QueryExecutor> query_executor,
                     size_t memory_limit_bytes)
    : segment_manager_(std::move(segment_manager)),
      query_executor_(std::move(query_executor)),
      memory_limit_bytes_(memory_limit_bytes),
      memory_used_bytes_(0) {
  utils::Logger::Instance().Info("QueryNode initialized with {} MB memory limit",
                                 memory_limit_bytes / (1024 * 1024));
}

absl::Status QueryNode::LoadSegment(core::SegmentId segment_id) {
  // Get segment from storage manager
  auto* segment = segment_manager_->GetSegment(segment_id);
  if (!segment) {
    return absl::NotFoundError(
        absl::StrCat("Segment not found: ", core::ToUInt32(segment_id)));
  }

  size_t segment_size = segment->GetMemoryUsage();

  // Check if we need to evict segments
  auto evict_status = EvictSegmentsIfNeeded(segment_size);
  if (!evict_status.ok()) {
    return evict_status;
  }

  std::unique_lock lock(segment_mutex_);

  // Check if already loaded
  if (loaded_segments_.count(segment_id) > 0) {
    return absl::AlreadyExistsError(
        absl::StrCat("Segment already loaded: ", core::ToUInt32(segment_id)));
  }

  // Load segment (shared_ptr to segment)
  loaded_segments_[segment_id] = std::shared_ptr<storage::Segment>(
      segment, [](storage::Segment*) {});  // Non-owning shared_ptr
  memory_used_bytes_ += segment_size;

  utils::Logger::Instance().Info("Loaded segment {}, memory usage: {} MB / {} MB",
                                 core::ToUInt32(segment_id),
                                 memory_used_bytes_ / (1024 * 1024),
                                 memory_limit_bytes_ / (1024 * 1024));

  return absl::OkStatus();
}

absl::Status QueryNode::UnloadSegment(core::SegmentId segment_id) {
  std::unique_lock lock(segment_mutex_);

  auto it = loaded_segments_.find(segment_id);
  if (it == loaded_segments_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Segment not loaded: ", core::ToUInt32(segment_id)));
  }

  size_t segment_size = it->second->GetMemoryUsage();
  loaded_segments_.erase(it);
  memory_used_bytes_ -= segment_size;

  utils::Logger::Instance().Info("Unloaded segment {}, memory usage: {} MB / {} MB",
                                 core::ToUInt32(segment_id),
                                 memory_used_bytes_ / (1024 * 1024),
                                 memory_limit_bytes_ / (1024 * 1024));

  return absl::OkStatus();
}

bool QueryNode::IsSegmentLoaded(core::SegmentId segment_id) const {
  std::shared_lock lock(segment_mutex_);
  return loaded_segments_.count(segment_id) > 0;
}

std::vector<core::SegmentId> QueryNode::GetLoadedSegments() const {
  std::shared_lock lock(segment_mutex_);
  std::vector<core::SegmentId> result;
  result.reserve(loaded_segments_.size());

  for (const auto& [segment_id, segment] : loaded_segments_) {
    result.push_back(segment_id);
  }

  return result;
}

absl::StatusOr<core::SearchResult> QueryNode::ExecuteSearch(
    core::CollectionId collection_id,
    const core::Vector& query_vector,
    size_t top_k) {

  // Execute search using query executor
  auto result = query_executor_->Search(collection_id, query_vector, top_k);
  if (!result.ok()) {
    return result.status();
  }

  return result;
}

absl::Status QueryNode::EvictSegmentsIfNeeded(size_t required_bytes) {
  if (memory_used_bytes_ + required_bytes <= memory_limit_bytes_) {
    return absl::OkStatus();  // No eviction needed
  }

  // Simple LRU eviction: remove oldest segments until we have enough space
  // TODO: Implement proper LRU tracking with access timestamps

  std::unique_lock lock(segment_mutex_);

  while (memory_used_bytes_ + required_bytes > memory_limit_bytes_ &&
         !loaded_segments_.empty()) {
    // Evict the first segment (simple strategy)
    auto it = loaded_segments_.begin();
    size_t segment_size = it->second->GetMemoryUsage();
    core::SegmentId evicted_id = it->first;

    loaded_segments_.erase(it);
    memory_used_bytes_ -= segment_size;

    utils::Logger::Instance().Info("Evicted segment {} to free memory",
                                   core::ToUInt32(evicted_id));
  }

  if (memory_used_bytes_ + required_bytes > memory_limit_bytes_) {
    return absl::ResourceExhaustedError(
        absl::StrCat("Not enough memory to load segment. Required: ",
                    required_bytes / (1024 * 1024), " MB, Available: ",
                    (memory_limit_bytes_ - memory_used_bytes_) / (1024 * 1024), " MB"));
  }

  return absl::OkStatus();
}

}  // namespace cluster
}  // namespace gvdb