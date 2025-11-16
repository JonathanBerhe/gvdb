#include "compute/query_executor.h"

#include <algorithm>
#include <future>
#include <queue>
#include <vector>

#include "utils/logger.h"
#include "utils/timer.h"

namespace gvdb {
namespace compute {

QueryExecutor::QueryExecutor(storage::SegmentManager* segment_manager,
                             utils::ThreadPool* thread_pool)
    : segment_manager_(segment_manager), thread_pool_(thread_pool) {
  if (segment_manager_ == nullptr) {
    throw std::invalid_argument("SegmentManager cannot be null");
  }

  // Create thread pool if not provided
  if (thread_pool_ == nullptr) {
    owned_thread_pool_ = std::make_unique<utils::ThreadPool>();
    thread_pool_ = owned_thread_pool_.get();
  }
}

QueryExecutor::~QueryExecutor() = default;

core::StatusOr<core::SearchResult> QueryExecutor::Search(
    core::CollectionId collection_id,
    const core::Vector& query,
    int top_k) {
  utils::Timer timer;

  if (top_k <= 0) {
    return core::InvalidArgumentError("top_k must be positive");
  }

  // Use SegmentManager's search (which already searches all segments)
  auto result = segment_manager_->SearchCollection(collection_id, query, top_k);

  if (result.ok()) {
    LOG_DEBUG("Search completed in {} us for collection {} (top_k={})",
              timer.elapsed_micros(),
              core::ToUInt32(collection_id),
              top_k);
  }

  return result;
}

core::StatusOr<std::vector<core::SearchResult>> QueryExecutor::SearchBatch(
    core::CollectionId collection_id,
    const std::vector<core::Vector>& queries,
    int top_k) {
  utils::Timer timer;

  if (queries.empty()) {
    return std::vector<core::SearchResult>{};
  }

  if (top_k <= 0) {
    return core::InvalidArgumentError("top_k must be positive");
  }

  // Execute searches in parallel
  std::vector<std::future<core::StatusOr<core::SearchResult>>> futures;
  futures.reserve(queries.size());

  for (const auto& query : queries) {
    futures.push_back(thread_pool_->enqueue([this, collection_id, &query, top_k]() {
      return segment_manager_->SearchCollection(collection_id, query, top_k);
    }));
  }

  // Collect results
  std::vector<core::SearchResult> results;
  results.reserve(queries.size());

  for (auto& future : futures) {
    auto result = future.get();
    if (!result.ok()) {
      return result.status();
    }
    results.push_back(std::move(result.value()));
  }

  LOG_DEBUG("Batch search completed in {} us ({} queries, top_k={})",
            timer.elapsed_micros(),
            queries.size(),
            top_k);

  return results;
}

size_t QueryExecutor::thread_count() const {
  return thread_pool_->size();
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
