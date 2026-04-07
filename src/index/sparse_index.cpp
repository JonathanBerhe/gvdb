// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "index/sparse_index.h"
#include <algorithm>
#include <functional>

namespace gvdb {
namespace index {

core::Status SparseIndex::AddVector(core::VectorId id,
                                    const core::SparseVector& sparse) {
  auto status = sparse.Validate();
  if (!status.ok()) return status;

  for (const auto& [dim_idx, value] : sparse.entries()) {
    posting_lists_[dim_idx].push_back({id, value});
  }

  doc_count_++;
  return core::OkStatus();
}

core::StatusOr<core::SearchResult> SparseIndex::Search(
    const core::SparseVector& query, int k) {
  if (k <= 0) {
    return core::InvalidArgumentError("k must be positive");
  }
  if (doc_count_ == 0 || query.empty()) {
    return core::SearchResult{};
  }

  // Accumulate dot-product scores per document
  std::unordered_map<uint64_t, float> scores;

  for (const auto& [query_dim, query_val] : query.entries()) {
    auto it = posting_lists_.find(query_dim);
    if (it == posting_lists_.end()) continue;

    for (const auto& posting : it->second) {
      scores[core::ToUInt64(posting.id)] += query_val * posting.value;
    }
  }

  // Extract top-k using min-heap (same pattern as BM25Index)
  using ScorePair = std::pair<float, uint64_t>;
  std::priority_queue<ScorePair, std::vector<ScorePair>, std::greater<>> heap;

  for (const auto& [id_val, score] : scores) {
    if (static_cast<int>(heap.size()) < k) {
      heap.push({score, id_val});
    } else if (score > heap.top().first) {
      heap.pop();
      heap.push({score, id_val});
    }
  }

  // Build result (highest score first)
  core::SearchResult result;
  while (!heap.empty()) {
    auto [score, id_val] = heap.top();
    heap.pop();
    result.entries.emplace_back(core::MakeVectorId(id_val), score);
  }
  std::reverse(result.entries.begin(), result.entries.end());

  return result;
}

size_t SparseIndex::GetDocumentCount() const {
  return doc_count_;
}

size_t SparseIndex::GetMemoryUsage() const {
  size_t usage = sizeof(*this);
  for (const auto& [dim, postings] : posting_lists_) {
    usage += sizeof(dim) + postings.size() * sizeof(Posting);
  }
  return usage;
}

}  // namespace index
}  // namespace gvdb
