#include "faiss_ivf.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexScalarQuantizer.h>

namespace gvdb {
namespace index {

FaissIVFIndex::FaissIVFIndex(core::Dimension dimension, core::MetricType metric,
                             int nlist, int nprobe,
                             IVFQuantizationType quantization, int pq_m,
                             int pq_nbits)
    : FaissIndexBase(CreateIVFIndex(dimension, metric, nlist, quantization, pq_m,
                                    pq_nbits),
                     metric),
      nlist_(nlist),
      nprobe_(nprobe),
      quantization_(quantization) {
  SetNProbe(nprobe);
}

std::unique_ptr<faiss::Index> FaissIVFIndex::CreateIVFIndex(
    core::Dimension dimension, core::MetricType metric, int nlist,
    IVFQuantizationType quantization, int pq_m, int pq_nbits) {
  faiss::MetricType faiss_metric = (metric == core::MetricType::INNER_PRODUCT)
                                       ? faiss::METRIC_INNER_PRODUCT
                                       : faiss::METRIC_L2;

  // Create quantizer (flat index for cluster centers)
  auto quantizer = new faiss::IndexFlat(dimension, faiss_metric);

  switch (quantization) {
    case IVFQuantizationType::PQ:
      // IVF with Product Quantization (more memory efficient)
      // nbits controls centroids per subquantizer: 2^nbits centroids
      // Training requires at least 39 * (2^nbits) points
      return std::make_unique<faiss::IndexIVFPQ>(quantizer, dimension, nlist,
                                                  pq_m, pq_nbits, faiss_metric);

    case IVFQuantizationType::SQ:
      // IVF with Scalar Quantization (fast and memory efficient)
      return std::make_unique<faiss::IndexIVFScalarQuantizer>(
          quantizer, dimension, nlist, faiss::ScalarQuantizer::QT_8bit,
          faiss_metric);

    case IVFQuantizationType::NONE:
    default:
      // IVF with flat storage (exact distances within clusters)
      return std::make_unique<faiss::IndexIVFFlat>(quantizer, dimension, nlist,
                                                    faiss_metric);
  }
}

void FaissIVFIndex::SetNProbe(int nprobe) {
  std::unique_lock lock(mutex_);
  nprobe_ = nprobe;

  // Set nprobe on the index based on quantization type
  switch (quantization_) {
    case IVFQuantizationType::PQ: {
      auto* ivf_pq = dynamic_cast<faiss::IndexIVFPQ*>(index_.get());
      if (ivf_pq) {
        ivf_pq->nprobe = nprobe;
      }
      break;
    }
    case IVFQuantizationType::SQ: {
      auto* ivf_sq = dynamic_cast<faiss::IndexIVFScalarQuantizer*>(index_.get());
      if (ivf_sq) {
        ivf_sq->nprobe = nprobe;
      }
      break;
    }
    case IVFQuantizationType::NONE: {
      auto* ivf_flat = dynamic_cast<faiss::IndexIVFFlat*>(index_.get());
      if (ivf_flat) {
        ivf_flat->nprobe = nprobe;
      }
      break;
    }
  }
}

core::IndexType FaissIVFIndex::GetIndexType() const {
  switch (quantization_) {
    case IVFQuantizationType::PQ:
      return core::IndexType::IVF_PQ;
    case IVFQuantizationType::SQ:
      return core::IndexType::IVF_SQ;
    case IVFQuantizationType::NONE:
    default:
      return core::IndexType::IVF_FLAT;
  }
}

}  // namespace index
}  // namespace gvdb
