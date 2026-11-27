// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <vector>

#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"
#include "storage/segment/segment_store.h"
#include "utils/thread_pool.h"
#include "utils/query_cache.h"

namespace gvdb {
namespace compute {

// ============================================================================
// QueryExecutor - Executes vector search queries across segments
// ============================================================================
class QueryExecutor {
 public:
  // Create query executor with segment manager and optional thread pool
  // If thread_pool is nullptr, creates a default pool with hardware concurrency
  explicit QueryExecutor(storage::ISegmentStore* segment_store,
                        utils::ThreadPool* thread_pool = nullptr);

  ~QueryExecutor();

  // Disable copy and move
  QueryExecutor(const QueryExecutor&) = delete;
  QueryExecutor& operator=(const QueryExecutor&) = delete;
  QueryExecutor(QueryExecutor&&) = delete;
  QueryExecutor& operator=(QueryExecutor&&) = delete;

  // Execute a vector search query
  // Searches across all segments in the collection and merges results
  // Optional filter expression for metadata filtering
  [[nodiscard]] core::StatusOr<core::SearchResult> Search(
      core::CollectionId collection_id,
      const core::Vector& query,
      int top_k,
      const std::string& filter = "");

 private:
  // Merge multiple partial results into final top-k
  static core::SearchResult MergeResults(
      const std::vector<core::SearchResult>& partial_results,
      int top_k);

  storage::ISegmentStore* segment_store_;
  std::unique_ptr<utils::ThreadPool> owned_thread_pool_;
  utils::ThreadPool* thread_pool_;  // Points to owned or external pool
  std::shared_ptr<utils::QueryCache> cache_;

 public:
  // Set or get the query cache (optional, nullptr disables caching)
  void SetCache(std::shared_ptr<utils::QueryCache> cache) { cache_ = std::move(cache); }
  utils::QueryCache* GetCache() const { return cache_.get(); }
};

}  // namespace compute
}  // namespace gvdb

