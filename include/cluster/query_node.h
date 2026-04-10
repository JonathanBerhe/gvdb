// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/types.h"
#include "core/vector.h"
#include "storage/segment_store.h"
#include "compute/query_executor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include <memory>
#include <map>
#include <shared_mutex>

namespace gvdb {
namespace cluster {

// Query node serves read requests with cached segments
class QueryNode {
 public:
  QueryNode(std::shared_ptr<storage::ISegmentStore> segment_store,
            std::shared_ptr<compute::QueryExecutor> query_executor,
            size_t memory_limit_bytes);
  ~QueryNode() = default;

  // Segment loading/unloading
  absl::Status LoadSegment(core::SegmentId segment_id);
  absl::Status UnloadSegment(core::SegmentId segment_id);
  bool IsSegmentLoaded(core::SegmentId segment_id) const;
  std::vector<core::SegmentId> GetLoadedSegments() const;

  // Query execution
  absl::StatusOr<core::SearchResult> ExecuteSearch(
      core::CollectionId collection_id,
      const core::Vector& query_vector,
      size_t top_k);

  // Resource management
  size_t GetMemoryUsage() const { return memory_used_bytes_; }
  size_t GetMemoryLimit() const { return memory_limit_bytes_; }
  float GetMemoryUtilization() const {
    return static_cast<float>(memory_used_bytes_) / memory_limit_bytes_;
  }

 private:
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  size_t memory_limit_bytes_;
  size_t memory_used_bytes_;

  mutable std::shared_mutex segment_mutex_;
  std::map<core::SegmentId, std::shared_ptr<storage::Segment>> loaded_segments_;

  // Evict least recently used segments if memory is full
  absl::Status EvictSegmentsIfNeeded(size_t required_bytes);
};

}  // namespace cluster
}  // namespace gvdb