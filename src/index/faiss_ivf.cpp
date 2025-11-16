#include "faiss_ivf.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexIVFPQ.h>

namespace gvdb {
namespace index {

FaissIVFIndex::FaissIVFIndex(core::Dimension dimension, core::MetricType metric,
                             int nlist, int nprobe, bool use_pq, int pq_m)
    : FaissIndexBase(CreateIVFIndex(dimension, metric, nlist, use_pq, pq_m),
                     metric),
      nlist_(nlist),
      nprobe_(nprobe),
      use_pq_(use_pq) {
  SetNProbe(nprobe);
}

std::unique_ptr<faiss::Index> FaissIVFIndex::CreateIVFIndex(
    core::Dimension dimension, core::MetricType metric, int nlist, bool use_pq,
    int pq_m) {
  faiss::MetricType faiss_metric = (metric == core::MetricType::INNER_PRODUCT)
                                       ? faiss::METRIC_INNER_PRODUCT
                                       : faiss::METRIC_L2;

  // Create quantizer (flat index for cluster centers)
  auto quantizer = new faiss::IndexFlat(dimension, faiss_metric);

  if (use_pq) {
    // IVF with Product Quantization (more memory efficient)
    return std::make_unique<faiss::IndexIVFPQ>(quantizer, dimension, nlist,
                                                pq_m, 8, faiss_metric);
  } else {
    // IVF with flat storage (exact distances within clusters)
    return std::make_unique<faiss::IndexIVFFlat>(quantizer, dimension, nlist,
                                                  faiss_metric);
  }
}

void FaissIVFIndex::SetNProbe(int nprobe) {
  std::unique_lock lock(mutex_);
  nprobe_ = nprobe;

  // Set nprobe on the index
  if (use_pq_) {
    auto* ivf_pq = dynamic_cast<faiss::IndexIVFPQ*>(index_.get());
    if (ivf_pq) {
      ivf_pq->nprobe = nprobe;
    }
  } else {
    auto* ivf_flat = dynamic_cast<faiss::IndexIVFFlat*>(index_.get());
    if (ivf_flat) {
      ivf_flat->nprobe = nprobe;
    }
  }
}

core::IndexType FaissIVFIndex::GetIndexType() const {
  return use_pq_ ? core::IndexType::IVF_PQ : core::IndexType::IVF_FLAT;
}

}  // namespace index
}  // namespace gvdb
