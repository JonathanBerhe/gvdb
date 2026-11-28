// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Segment searches: brute force, filtered, range and hybrid (text and
// sparse), and the text and sparse indexes they use.

#include <algorithm>
#include <mutex>
#include <utility>
#include "absl/functional/function_ref.h"
#include "absl/strings/str_cat.h"
#include "common/logger.h"
#include "core/filter.h"
#include "core/status.h"
#include "storage/segment/segment.h"

namespace gvdb::storage {

namespace {

// Runs `search` with a growing fetch size and keeps the entries whose id
// `keep` accepts, in the order of the search, until k entries pass. The first
// round fetches `first_fetch` entries, and each later round fetches four times
// more. The rounds stop when the search returns fewer entries than it was
// asked for, or when the fetch size reaches `total`, the number of vectors of
// the segment. So a filter that few vectors pass still finds all of them.
absl::StatusOr<core::SearchResult> SearchKeeping(
    int k, int first_fetch, int total,
    absl::FunctionRef<absl::StatusOr<core::SearchResult>(int)> search,
    absl::FunctionRef<bool(uint64_t)> keep) {
  if (k <= 0 || total <= 0) return core::SearchResult{};
  int fetch = std::min(std::max(first_fetch, k), total);
  while (true) {
    auto searched = search(fetch);
    if (!searched.ok()) return searched.status();

    core::SearchResult kept(static_cast<size_t>(k));
    for (const auto& entry : searched->entries) {
      if (keep(core::ToUInt64(entry.id))) {
        kept.entries.push_back(entry);
        if (std::cmp_equal(kept.Size(), k)) break;
      }
    }
    const bool exhausted = std::cmp_less(searched->entries.size(), fetch) || fetch >= total;
    if (std::cmp_equal(kept.Size(), k) || exhausted) return kept;
    fetch = fetch > total / 4 ? total : fetch * 4;
  }
}

}  // namespace

float Segment::ComputeDistance(const core::Vector& a, const core::Vector& b) const {
  switch (metric_) {
    case core::MetricType::L2:
      return a.L2Distance(b);
    case core::MetricType::INNER_PRODUCT:
      return -a.InnerProduct(b);
    case core::MetricType::COSINE:
      return a.CosineDistance(b);
    default:
      return a.L2Distance(b);
  }
}

absl::StatusOr<core::SearchResult> Segment::BruteForceSearchUnlocked(const core::Vector& query,
                                                                     int k) const {
  if (vectors_.empty()) {
    return core::SearchResult{};
  }

  std::vector<core::SearchResultEntry> results;
  results.reserve(vectors_.size());

  int64_t now = NowEpochSeconds();
  for (size_t i = 0; i < vectors_.size(); ++i) {
    if (IsExpired(core::ToUInt64(vector_ids_[i]), now)) continue;
    float distance = ComputeDistance(query, vectors_[i]);
    results.emplace_back(vector_ids_[i], distance);
  }

  int actual_k = std::min(k, static_cast<int>(results.size()));
  std::partial_sort(results.begin(), results.begin() + actual_k, results.end(),
                    [](const core::SearchResultEntry& a, const core::SearchResultEntry& b) {
                      return a.distance < b.distance;
                    });
  results.resize(actual_k);

  core::SearchResult result;
  result.entries = std::move(results);
  return result;
}

absl::StatusOr<core::SearchResult> Segment::Search(const core::Vector& query, int k) const {
  std::shared_lock lock(mutex_);

  if (state_ == core::SegmentState::GROWING) {
    return BruteForceSearchUnlocked(query, k);
  }

  if (state_ != core::SegmentState::SEALED && state_ != core::SegmentState::FLUSHED) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot search in segment state: ", static_cast<int>(state_)));
  }

  if (!index_) {
    return absl::FailedPreconditionError(
        absl::StrCat("Segment ", core::ToUInt32(id_), " has no index"));
  }

  // Over-fetch to make up for the expired entries that the search drops.
  const int64_t now = NowEpochSeconds();
  return SearchKeeping(
      k, k * 2, static_cast<int>(index_->GetVectorCount()),
      [&](int fetch) { return index_->Search(query, fetch); },
      [this, now](uint64_t id) { return !IsExpired(id, now); });
}

// ========== Build Sparse Index ==========

absl::Status Segment::BuildSparseIndex(std::unique_ptr<index::SparseIndex> sparse_index) {
  std::unique_lock lock(mutex_);
  if (!sparse_index) {
    return absl::InvalidArgumentError("Sparse index pointer is null");
  }

  for (const auto& [id_val, sparse] : sparse_vectors_) {
    RETURN_IF_ERROR(sparse_index->AddVector(core::MakeVectorId(id_val), sparse));
  }

  sparse_index_ = std::move(sparse_index);
  return absl::OkStatus();
}

// ========== Range Search ==========

absl::StatusOr<core::SearchResult> Segment::SearchRange(const core::Vector& query, float radius,
                                                        int max_results) const {
  std::shared_lock lock(mutex_);

  core::SearchResult result;

  if (state_ == core::SegmentState::GROWING) {
    // Brute-force range search for GROWING segments
    for (size_t i = 0; i < vectors_.size(); ++i) {
      float distance = ComputeDistance(query, vectors_[i]);
      if (distance <= radius) {
        result.entries.emplace_back(vector_ids_[i], distance);
      }
    }
  } else if (state_ == core::SegmentState::SEALED || state_ == core::SegmentState::FLUSHED) {
    if (!index_) {
      return absl::FailedPreconditionError("Segment has no index");
    }
    // Use index range search if available, otherwise brute-force over vectors
    auto index_result = index_->SearchRange(query, radius);
    if (index_result.ok()) {
      result = std::move(*index_result);
    } else {
      // Fallback: brute-force using stored vectors
      for (size_t i = 0; i < vectors_.size(); ++i) {
        float distance = ComputeDistance(query, vectors_[i]);
        if (distance <= radius) {
          result.entries.emplace_back(vector_ids_[i], distance);
        }
      }
    }
  } else {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot search in segment state: ", static_cast<int>(state_)));
  }

  // Drop the expired vectors, then keep the nearest max_results entries,
  // closest first.
  const int64_t now = NowEpochSeconds();
  std::erase_if(result.entries, [this, now](const core::SearchResultEntry& e) {
    return IsExpired(core::ToUInt64(e.id), now);
  });
  std::ranges::sort(result.entries,
                    [](const auto& a, const auto& b) { return a.distance < b.distance; });
  if (max_results > 0 && std::cmp_greater(result.entries.size(), max_results)) {
    result.entries.resize(static_cast<size_t>(max_results));
  }

  return result;
}

absl::StatusOr<core::SearchResult> Segment::SearchRangeWithFilter(const core::Vector& query,
                                                                  float radius,
                                                                  const core::FilterNode& filter,
                                                                  int max_results) const {
  // Get range results first
  auto range_result = SearchRange(query, radius, 0);  // No limit for pre-filter
  if (!range_result.ok()) {
    return range_result.status();
  }

  // Post-filter
  std::shared_lock lock(mutex_);
  core::SearchResult filtered;
  for (const auto& entry : range_result->entries) {
    auto it = metadata_map_.find(core::ToUInt64(entry.id));
    if (it != metadata_map_.end() && filter.Evaluate(it->second)) {
      filtered.entries.push_back(entry);
      if (max_results > 0 && static_cast<int>(filtered.entries.size()) >= max_results) {
        break;
      }
    }
  }

  return filtered;
}

absl::StatusOr<core::SearchResult> Segment::SearchWithFilter(const core::Vector& query, int k,
                                                             const core::FilterNode& filter) const {
  std::shared_lock lock(mutex_);

  // Try to use scalar indexes to narrow the candidate set
  std::optional<VectorIdSet> indexed_candidates;
  if (scalar_indexes_.HasIndexes()) {
    indexed_candidates = scalar_indexes_.Evaluate(filter);
  }

  int64_t now = NowEpochSeconds();

  // For GROWING segments with brute-force search
  if (state_ == core::SegmentState::GROWING) {
    if (vectors_.empty()) {
      return core::SearchResult{};
    }

    std::vector<core::SearchResultEntry> results;
    results.reserve(vectors_.size());

    for (size_t i = 0; i < vectors_.size(); ++i) {
      uint64_t id_uint = core::ToUInt64(vector_ids_[i]);

      if (IsExpired(id_uint, now)) continue;

      // Fast path: scalar index pre-filter
      if (indexed_candidates) {
        if (!indexed_candidates->contains(id_uint)) {
          continue;  // Not in candidate set from scalar index
        }
        // Still need to evaluate full filter for partial index coverage
        auto it = metadata_map_.find(id_uint);
        if (it != metadata_map_.end() && !filter.Evaluate(it->second)) {
          continue;
        }
      } else {
        // Slow path: full scan with filter evaluation
        auto it = metadata_map_.find(id_uint);
        if (it == metadata_map_.end() || !filter.Evaluate(it->second)) {
          continue;
        }
      }

      float distance = ComputeDistance(query, vectors_[i]);
      results.emplace_back(vector_ids_[i], distance);
    }

    std::partial_sort(
        results.begin(), results.begin() + std::min(k, static_cast<int>(results.size())),
        results.end(), [](const auto& a, const auto& b) { return a.distance < b.distance; });

    core::SearchResult result(k);
    for (int i = 0; i < std::min(k, static_cast<int>(results.size())); ++i) {
      result.AddEntry(results[i].id, results[i].distance);
    }
    return result;
  }

  // For SEALED segments with index
  if (state_ != core::SegmentState::SEALED && state_ != core::SegmentState::FLUSHED) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot search in segment state: ", static_cast<int>(state_)));
  }

  if (!index_) {
    return absl::FailedPreconditionError(
        absl::StrCat("Segment ", core::ToUInt32(id_), " has no index"));
  }

  // A scalar index can show that no vector matches.
  if (indexed_candidates && indexed_candidates->empty()) {
    return core::SearchResult{};
  }

  // The first round fetches 10 times k. When the filter passes fewer than k
  // of them, the search fetches more, so a selective filter still finds its
  // matches.
  return SearchKeeping(
      k, k * 10, static_cast<int>(index_->GetVectorCount()),
      [&](int fetch) { return index_->Search(query, fetch); },
      [&](uint64_t id) {
        if (IsExpired(id, now)) return false;
        // Fast path: check scalar index first
        if (indexed_candidates && !indexed_candidates->contains(id)) return false;
        // Full filter evaluation (for partial index coverage or unindexed predicates)
        auto it = metadata_map_.find(id);
        return it != metadata_map_.end() && filter.Evaluate(it->second);
      });
}

absl::StatusOr<core::SearchResult> Segment::SearchHybrid(
    const core::Vector& query_vector, const std::string& text_query, int k, float vector_weight,
    float text_weight, const std::string& /*text_field*/, const core::SparseVector* sparse_query,
    float sparse_weight, const core::FilterNode* filter) const {
  std::shared_lock lock(mutex_);

  if (vectors_.empty() && !index_) {
    return core::SearchResult{};
  }

  // Each signal collects up to 3 x k live candidates that pass the filter.
  // Dropping the others before the fusion makes the ranks count only live,
  // matching candidates. When the filter passes few of the first candidates,
  // the signal searches further, so a selective filter still gets 3 x k.
  const int64_t now = NowEpochSeconds();
  auto keep = [&](uint64_t id) {
    if (IsExpired(id, now)) return false;
    if (!filter) return true;
    auto it = metadata_map_.find(id);
    return it != metadata_map_.end() && filter->Evaluate(it->second);
  };
  const int total =
      index_ ? static_cast<int>(index_->GetVectorCount()) : static_cast<int>(vectors_.size());
  const int candidates = k * 3;

  // 1. Dense vector search (index if sealed, brute-force if growing)
  auto vector_result = SearchKeeping(
      candidates, candidates, total,
      [&](int fetch) {
        return index_ ? index_->Search(query_vector, fetch)
                      : BruteForceSearchUnlocked(query_vector, fetch);
      },
      keep);
  if (!vector_result.ok()) {
    return vector_result.status();
  }

  // 2. BM25 text search (if text index exists and text query is non-empty)
  core::SearchResult text_result;
  if (text_index_ && !text_query.empty()) {
    auto bm25_result = SearchKeeping(
        candidates, candidates, total,
        [&](int fetch) { return text_index_->Search(text_query, fetch); }, keep);
    if (bm25_result.ok()) {
      text_result = std::move(*bm25_result);
    }
  }

  // 2b. Sparse search (if sparse index exists and sparse query provided)
  core::SearchResult sparse_result;
  if (sparse_index_ && sparse_query && !sparse_query->empty()) {
    auto sparse_search = SearchKeeping(
        candidates, candidates, total,
        [&](int fetch) { return sparse_index_->Search(*sparse_query, fetch); }, keep);
    if (sparse_search.ok()) {
      sparse_result = std::move(*sparse_search);
    }
  }

  // 3. Reciprocal Rank Fusion (RRF)
  constexpr float kRrf = 60.0f;

  // Build rank maps
  std::unordered_map<uint64_t, int> vector_ranks;
  for (int i = 0; i < static_cast<int>(vector_result->entries.size()); ++i) {
    vector_ranks[core::ToUInt64(vector_result->entries[i].id)] = i + 1;
  }

  std::unordered_map<uint64_t, int> text_ranks;
  for (int i = 0; i < static_cast<int>(text_result.entries.size()); ++i) {
    text_ranks[core::ToUInt64(text_result.entries[i].id)] = i + 1;
  }

  std::unordered_map<uint64_t, int> sparse_ranks;
  for (int i = 0; i < static_cast<int>(sparse_result.entries.size()); ++i) {
    sparse_ranks[core::ToUInt64(sparse_result.entries[i].id)] = i + 1;
  }

  // Collect all candidate IDs
  std::unordered_map<uint64_t, float> fused_scores;
  for (const auto& [id_val, rank] : vector_ranks) {
    fused_scores[id_val] += vector_weight / (kRrf + rank);
  }
  for (const auto& [id_val, rank] : text_ranks) {
    fused_scores[id_val] += text_weight / (kRrf + rank);
  }
  for (const auto& [id_val, rank] : sparse_ranks) {
    fused_scores[id_val] += sparse_weight / (kRrf + rank);
  }

  // 4. Sort by fused score and take top-k
  std::vector<std::pair<uint64_t, float>> scored_ids(fused_scores.begin(), fused_scores.end());
  std::ranges::sort(scored_ids, [](const auto& a, const auto& b) { return a.second > b.second; });

  core::SearchResult result;
  int count = std::min(k, static_cast<int>(scored_ids.size()));
  for (int i = 0; i < count; ++i) {
    // Use RRF score as the "distance" (higher = better)
    result.entries.emplace_back(core::MakeVectorId(scored_ids[i].first), scored_ids[i].second);
  }

  return result;
}

absl::Status Segment::BuildTextIndex(std::unique_ptr<index::ITextIndex> text_index,
                                     const std::string& text_field) {
  std::unique_lock lock(mutex_);

  if (!text_index) {
    return absl::InvalidArgumentError("Text index pointer is null");
  }

  // Build from metadata: extract the text field from each vector's metadata
  size_t indexed = 0;
  for (const auto& [id_val, metadata] : metadata_map_) {
    auto it = metadata.find(text_field);
    if (it != metadata.end()) {
      // Check if the value is a string
      if (auto* str_val = std::get_if<std::string>(&it->second)) {
        RETURN_IF_ERROR(text_index->AddDocument(core::MakeVectorId(id_val), *str_val));
        indexed++;
      }
    }
  }

  text_index_ = std::move(text_index);

  common::Logger::Instance().Info("Built text index for segment {} ({} documents from '{}' field)",
                                  core::ToUInt32(id_), indexed, text_field);

  return absl::OkStatus();
}

}  // namespace gvdb::storage
