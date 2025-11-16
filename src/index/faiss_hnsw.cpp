#include "faiss_hnsw.h"

#include <faiss/IndexHNSW.h>
#include <faiss/IndexIDMap.h>

namespace gvdb {
namespace index {

FaissHNSWIndex::FaissHNSWIndex(core::Dimension dimension,
                               core::MetricType metric, int M,
                               int ef_construction)
    : FaissIndexBase(CreateHNSWIndex(dimension, metric, M, ef_construction),
                     metric),
      M_(M),
      ef_construction_(ef_construction),
      ef_search_(50) {}

std::unique_ptr<faiss::Index> FaissHNSWIndex::CreateHNSWIndex(
    core::Dimension dimension, core::MetricType metric, int M,
    int ef_construction) {
  faiss::MetricType faiss_metric = (metric == core::MetricType::INNER_PRODUCT)
                                       ? faiss::METRIC_INNER_PRODUCT
                                       : faiss::METRIC_L2;

  auto hnsw_index = new faiss::IndexHNSWFlat(dimension, M, faiss_metric);
  hnsw_index->hnsw.efConstruction = ef_construction;

  // Wrap with IDMap to support add_with_ids
  return std::make_unique<faiss::IndexIDMap>(hnsw_index);
}

void FaissHNSWIndex::SetEfSearch(int ef) {
  std::unique_lock lock(mutex_);
  ef_search_ = ef;
  auto* hnsw_index = dynamic_cast<faiss::IndexHNSW*>(index_.get());
  if (hnsw_index) {
    hnsw_index->hnsw.efSearch = ef;
  }
}

core::IndexType FaissHNSWIndex::GetIndexType() const {
  return core::IndexType::HNSW;
}

}  // namespace index
}  // namespace gvdb
