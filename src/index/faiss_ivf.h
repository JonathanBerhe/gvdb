#ifndef GVDB_INDEX_FAISS_IVF_H_
#define GVDB_INDEX_FAISS_IVF_H_

#include "faiss_base.h"

namespace gvdb {
namespace index {

// IVF (Inverted File) index: partitions vectors into clusters
// Best for large datasets where memory is a concern
// Supports Product Quantization (PQ) and Scalar Quantization (SQ)
class FaissIVFIndex : public FaissIndexBase {
 public:
  FaissIVFIndex(core::Dimension dimension, core::MetricType metric,
                int nlist = 100, int nprobe = 10, bool use_pq = false,
                int pq_m = 8);
  ~FaissIVFIndex() override = default;

  [[nodiscard]] core::IndexType GetIndexType() const override;

  // Set number of clusters to probe during search
  void SetNProbe(int nprobe);

 private:
  static std::unique_ptr<faiss::Index> CreateIVFIndex(
      core::Dimension dimension, core::MetricType metric, int nlist,
      bool use_pq, int pq_m);

  int nlist_;
  int nprobe_;
  bool use_pq_;
};

}  // namespace index
}  // namespace gvdb

#endif  // GVDB_INDEX_FAISS_IVF_H_
