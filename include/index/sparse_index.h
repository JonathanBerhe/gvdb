// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/sparse_vector.h"
#include "core/types.h"
#include <queue>
#include <unordered_map>
#include <vector>

namespace gvdb {
namespace index {

// Inverted-index based sparse vector search.
// Computes dot-product scores by iterating posting lists for each query
// dimension, then extracts top-k via min-heap. Follows the same design
// pattern as BM25Index (inverted posting lists + score accumulation).
class SparseIndex {
 public:
  SparseIndex() = default;
  ~SparseIndex() = default;

  // Add a sparse vector to the index
  core::Status AddVector(core::VectorId id, const core::SparseVector& sparse);

  // Search for top-k vectors by dot product with query (higher = better)
  [[nodiscard]] core::StatusOr<core::SearchResult> Search(
      const core::SparseVector& query, int k);

  [[nodiscard]] size_t GetDocumentCount() const;
  [[nodiscard]] size_t GetMemoryUsage() const;

 private:
  struct Posting {
    core::VectorId id;
    float value;  // Feature weight at this dimension
  };

  // Inverted index: dimension_index → posting list
  std::unordered_map<uint32_t, std::vector<Posting>> posting_lists_;

  size_t doc_count_ = 0;
};

}  // namespace index
}  // namespace gvdb
