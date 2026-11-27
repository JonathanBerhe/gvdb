// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <vector>

#include "common/thread_pool.h"
#include "compute/query_cache.h"
#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"
#include "storage/segment/segment_store.h"

namespace gvdb {
namespace compute {

// ============================================================================
// QueryExecutor - Executes vector search queries across segments
// ============================================================================
class QueryExecutor {
 public:
  // thread_pool and cache are optional. A null thread_pool makes the
  // executor create its own pool with one thread per hardware thread. A
  // null cache turns off result caching.
  explicit QueryExecutor(storage::ISegmentStore& segment_store,
                         common::ThreadPool* thread_pool = nullptr,
                         std::shared_ptr<compute::QueryCache> cache = nullptr);

  ~QueryExecutor();

  // Disable copy and move
  QueryExecutor(const QueryExecutor&) = delete;
  QueryExecutor& operator=(const QueryExecutor&) = delete;
  QueryExecutor(QueryExecutor&&) = delete;
  QueryExecutor& operator=(QueryExecutor&&) = delete;

  // Execute a vector search query
  // Searches across all segments in the collection and merges results
  // Optional filter expression for metadata filtering
  [[nodiscard]] absl::StatusOr<core::SearchResult> Search(core::CollectionId collection_id,
                                                          const core::Vector& query, int top_k,
                                                          const std::string& filter = "");

 private:
  // Merge multiple partial results into final top-k
  static core::SearchResult MergeResults(const std::vector<core::SearchResult>& partial_results,
                                         int top_k);

  storage::ISegmentStore* segment_store_;
  std::unique_ptr<common::ThreadPool> owned_thread_pool_;
  common::ThreadPool* thread_pool_;  // Points to owned or external pool
  const std::shared_ptr<compute::QueryCache> cache_;

 public:
  // The query cache, or null when caching is off.
  compute::QueryCache* GetCache() const { return cache_.get(); }
};

}  // namespace compute
}  // namespace gvdb
