// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "compute/query_executor.h"

#include <algorithm>
#include <queue>
#include <vector>

#include "absl/strings/str_format.h"

#include "common/logger.h"
#include "common/timer.h"

namespace gvdb {
namespace compute {

QueryExecutor::QueryExecutor(storage::ISegmentStore* segment_store,
                             common::ThreadPool* thread_pool)
    : segment_store_(segment_store), thread_pool_(thread_pool) {
  if (segment_store_ == nullptr) {
    throw std::invalid_argument("ISegmentStore cannot be null");
  }

  // Create thread pool if not provided
  if (thread_pool_ == nullptr) {
    owned_thread_pool_ = std::make_unique<common::ThreadPool>();
    thread_pool_ = owned_thread_pool_.get();
  }
}

QueryExecutor::~QueryExecutor() = default;

absl::StatusOr<core::SearchResult> QueryExecutor::Search(
    core::CollectionId collection_id,
    const core::Vector& query,
    int top_k,
    const std::string& filter) {
  common::Timer timer;

  if (top_k <= 0) {
    return absl::InvalidArgumentError("top_k must be positive");
  }

  // Validate dimension against collection
  auto seg_ids = segment_store_->GetCollectionSegments(collection_id);
  if (!seg_ids.empty()) {
    auto* first_seg = segment_store_->GetSegment(seg_ids[0]);
    if (first_seg && query.dimension() != first_seg->GetDimension()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("Query vector dimension mismatch: expected %d, got %d",
                          first_seg->GetDimension(), query.dimension()));
    }
  }

  // Check cache
  if (cache_) {
    auto cache_key = compute::MakeCacheKey(
        collection_id, query.data(), query.dimension(), top_k, filter);
    auto cached = cache_->Get(cache_key, core::ToUInt32(collection_id));
    if (cached.has_value()) {
      common::Logger::Instance().Debug(
          "Cache hit for collection {} (top_k={}, filter={})",
          core::ToUInt32(collection_id), top_k, filter.empty() ? "none" : "yes");
      return *cached;
    }
  }

  // Search via SegmentManager
  absl::StatusOr<core::SearchResult> result;
  if (filter.empty()) {
    result = segment_store_->SearchCollection(collection_id, query, top_k);
  } else {
    // Filter-aware search: iterate segments and merge
    auto seg_ids = segment_store_->GetCollectionSegments(collection_id);
    std::vector<core::SearchResult> partial_results;
    for (const auto& seg_id : seg_ids) {
      auto* segment = segment_store_->GetSegment(seg_id);
      if (!segment) continue;
      auto seg_result = segment->SearchWithFilter(query, top_k, filter);
      if (seg_result.ok()) {
        partial_results.push_back(std::move(*seg_result));
      }
    }
    if (partial_results.empty()) {
      result = core::SearchResult{};
    } else {
      result = MergeResults(partial_results, top_k);
    }
  }

  if (result.ok()) {
    common::Logger::Instance().Debug(
        "Search completed in {} us for collection {} (top_k={}, results={})",
        timer.ElapsedMicros(),
        core::ToUInt32(collection_id),
        top_k,
        result->entries.size());

    // Store in cache
    if (cache_) {
      auto cache_key = compute::MakeCacheKey(
          collection_id, query.data(), query.dimension(), top_k, filter);
      cache_->Put(cache_key, *result, core::ToUInt32(collection_id));
    }
  }

  return result;
}

core::SearchResult QueryExecutor::MergeResults(
    const std::vector<core::SearchResult>& partial_results,
    int top_k) {
  // Use min-heap to efficiently maintain top-k results
  // We use std::greater to create a min-heap (smallest distance at top)
  auto compare = [](const core::SearchResultEntry& a,
                   const core::SearchResultEntry& b) {
    return a.distance > b.distance;  // Min-heap based on distance
  };

  std::priority_queue<core::SearchResultEntry,
                     std::vector<core::SearchResultEntry>,
                     decltype(compare)> min_heap(compare);

  // Process all partial results
  for (const auto& partial : partial_results) {
    for (const auto& entry : partial.entries) {
      if (static_cast<int>(min_heap.size()) < top_k) {
        // Haven't reached k elements yet, just add
        min_heap.push(entry);
      } else if (entry.distance < min_heap.top().distance) {
        // Found a closer result, replace the farthest one
        min_heap.pop();
        min_heap.push(entry);
      }
    }
  }

  // Extract results from heap (will be in reverse order)
  core::SearchResult final_result;
  final_result.entries.reserve(min_heap.size());

  while (!min_heap.empty()) {
    final_result.entries.push_back(min_heap.top());
    min_heap.pop();
  }

  // Reverse to get ascending order by distance (closest first)
  std::reverse(final_result.entries.begin(), final_result.entries.end());

  return final_result;
}

}  // namespace compute
}  // namespace gvdb