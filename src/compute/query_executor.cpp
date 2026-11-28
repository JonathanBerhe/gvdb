// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "compute/query_executor.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"

#include "common/logger.h"
#include "common/timer.h"
#include "core/filter.h"
#include "core/status.h"

namespace gvdb::compute {

QueryExecutor::QueryExecutor(storage::ISegmentStore& segment_store, common::ThreadPool* thread_pool,
                             std::shared_ptr<compute::QueryCache> cache)
    : segment_store_(&segment_store), thread_pool_(thread_pool), cache_(std::move(cache)) {
  // Create thread pool if not provided
  if (thread_pool_ == nullptr) {
    owned_thread_pool_ = std::make_unique<common::ThreadPool>();
    thread_pool_ = owned_thread_pool_.get();
  }
}

QueryExecutor::~QueryExecutor() = default;

absl::StatusOr<core::SearchResult> QueryExecutor::Search(core::CollectionId collection_id,
                                                         const core::Vector& query, int top_k,
                                                         const std::string& filter) {
  common::Timer timer;

  if (top_k <= 0) {
    return absl::InvalidArgumentError("top_k must be positive");
  }

  // Parse the filter once for every segment. A bad filter is an
  // InvalidArgument error, not an empty result.
  std::unique_ptr<core::FilterNode> parsed_filter;
  if (!filter.empty()) {
    ASSIGN_OR_RETURN(parsed_filter, core::FilterParser::parse(filter));
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
    auto cache_key =
        compute::MakeCacheKey(collection_id, query.data(), query.dimension(), top_k, filter);
    auto cached = cache_->Get(cache_key, core::ToUInt32(collection_id));
    if (cached.has_value()) {
      common::Logger::Instance().Debug("Cache hit for collection {} (top_k={}, filter={})",
                                       core::ToUInt32(collection_id), top_k,
                                       filter.empty() ? "none" : "yes");
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
      auto seg_result = segment->SearchWithFilter(query, top_k, *parsed_filter);
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
        "Search completed in {} us for collection {} (top_k={}, results={})", timer.ElapsedMicros(),
        core::ToUInt32(collection_id), top_k, result->entries.size());

    // Store in cache
    if (cache_) {
      auto cache_key =
          compute::MakeCacheKey(collection_id, query.data(), query.dimension(), top_k, filter);
      cache_->Put(cache_key, *result, core::ToUInt32(collection_id));
    }
  }

  return result;
}

core::SearchResult QueryExecutor::MergeResults(
    const std::vector<core::SearchResult>& partial_results, int top_k) {
  // Every partial result holds canonical distances, where lower is closer.
  // Keep the top_k closest entries of all of them, closest first.
  core::SearchResult merged;
  for (const auto& partial : partial_results) {
    merged.entries.insert(merged.entries.end(), partial.entries.begin(), partial.entries.end());
  }
  const auto keep = std::min(merged.entries.size(), static_cast<size_t>(std::max(top_k, 0)));
  std::ranges::partial_sort(merged.entries,
                            merged.entries.begin() + static_cast<std::ptrdiff_t>(keep),
                            [](const auto& a, const auto& b) { return a.distance < b.distance; });
  merged.entries.resize(keep);
  return merged;
}

}  // namespace gvdb::compute
