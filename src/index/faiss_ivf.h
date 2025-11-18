#ifndef GVDB_INDEX_FAISS_IVF_H_
#define GVDB_INDEX_FAISS_IVF_H_

#include "faiss_base.h"
#include "index/index_factory.h"

namespace gvdb {
namespace index {

// IVF (Inverted File) index: partitions vectors into clusters
// Best for large datasets where memory is a concern
// Supports Product Quantization (PQ) and Scalar Quantization (SQ)
class FaissIVFIndex : public FaissIndexBase {
 public:
  FaissIVFIndex(core::Dimension dimension, core::MetricType metric,
                int nlist = 100, int nprobe = 10,
                IVFQuantizationType quantization = IVFQuantizationType::NONE,
                int pq_m = 8, int pq_nbits = 8);
  ~FaissIVFIndex() override = default;

  [[nodiscard]] core::IndexType GetIndexType() const override;

  // Set number of clusters to probe during search
  void SetNProbe(int nprobe);

 private:
  static std::unique_ptr<faiss::Index> CreateIVFIndex(
      core::Dimension dimension, core::MetricType metric, int nlist,
      IVFQuantizationType quantization, int pq_m, int pq_nbits);

  int nlist_;
  int nprobe_;
  IVFQuantizationType quantization_;
};

}  // namespace index
}  // namespace gvdb

#endif  // GVDB_INDEX_FAISS_IVF_H_
