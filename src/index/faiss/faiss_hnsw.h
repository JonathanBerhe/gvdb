// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "index/faiss/faiss_base.h"

namespace gvdb::index {

// HNSW index: Hierarchical Navigable Small World
// Best for medium to large datasets with high recall requirements
// Memory intensive but very fast searches
class FaissHNSWIndex : public FaissIndexBase {
 public:
  FaissHNSWIndex(core::Dimension dimension, core::MetricType metric, int M = 32,
                 int ef_construction = 200, int ef_search = 50);
  ~FaissHNSWIndex() override = default;

  [[nodiscard]] core::IndexType GetIndexType() const override;

  // Set search parameter (applies to the underlying HNSW index)
  void SetEfSearch(int ef);

  // Read efSearch from the underlying Faiss HNSW index (returns -1 if the
  // internal cast chain is broken, for test diagnosis).
  [[nodiscard]] int GetEfSearch() const;

 private:
  static std::unique_ptr<faiss::Index> CreateHNSWIndex(core::Dimension dimension,
                                                       core::MetricType metric, int M,
                                                       int ef_construction);

  int ef_search_;
};

}  // namespace gvdb::index
