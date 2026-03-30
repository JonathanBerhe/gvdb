// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_INDEX_FAISS_HNSW_H_
#define GVDB_INDEX_FAISS_HNSW_H_

#include "faiss_base.h"

namespace gvdb {
namespace index {

// HNSW index: Hierarchical Navigable Small World
// Best for medium to large datasets with high recall requirements
// Memory intensive but very fast searches
class FaissHNSWIndex : public FaissIndexBase {
 public:
  FaissHNSWIndex(core::Dimension dimension, core::MetricType metric, int M = 32,
                 int ef_construction = 200);
  ~FaissHNSWIndex() override = default;

  [[nodiscard]] core::IndexType GetIndexType() const override;

  // Set search parameter
  void SetEfSearch(int ef);

 private:
  static std::unique_ptr<faiss::Index> CreateHNSWIndex(
      core::Dimension dimension, core::MetricType metric, int M,
      int ef_construction);

  int M_;
  int ef_construction_;
  int ef_search_;
};

}  // namespace index
}  // namespace gvdb

#endif  // GVDB_INDEX_FAISS_HNSW_H_