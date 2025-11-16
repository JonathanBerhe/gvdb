#include "faiss_base.h"

#include <fstream>

#include <faiss/index_io.h>
#include <faiss/impl/AuxIndexStructures.h>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace index {

FaissIndexBase::FaissIndexBase(std::unique_ptr<faiss::Index> index,
                               core::MetricType metric)
    : index_(std::move(index)), metric_type_(metric), is_trained_(false) {
  // Check if index is already trained (FLAT doesn't need training)
  is_trained_ = index_->is_trained;
}

core::Status FaissIndexBase::Build(const std::vector<core::Vector>& vectors,
                                    const std::vector<core::VectorId>& ids) {
  if (vectors.empty()) {
    return core::InvalidArgumentError("Cannot build index with empty vectors");
  }

  if (vectors.size() != ids.size()) {
    return core::InvalidArgumentError(
        absl::StrCat("Vector and ID count mismatch: ", vectors.size(), " vs ",
                     ids.size()));
  }

  std::unique_lock lock(mutex_);

  // Train if needed
  if (!index_->is_trained) {
    auto training_data = VectorsToFloatArray(vectors);
    auto train_status = ExecuteFaissOperation(
        [&]() { index_->train(vectors.size(), training_data.data()); },
        "Train");
    if (!train_status.ok()) {
      return train_status;
    }
  }

  // Convert to Faiss format
  auto float_data = VectorsToFloatArray(vectors);
  std::vector<faiss::idx_t> faiss_ids(ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    faiss_ids[i] = VectorIdToIdx(ids[i]);
  }

  return ExecuteFaissOperation(
      [&]() { index_->add_with_ids(vectors.size(), float_data.data(),
                                   faiss_ids.data()); },
      "Build");
}

core::Status FaissIndexBase::Add(const core::Vector& vector,
                                  core::VectorId id) {
  std::unique_lock lock(mutex_);

  if (!index_->is_trained) {
    return core::FailedPreconditionError("Index must be trained before adding vectors");
  }

  auto float_data = VectorToFloatArray(vector);
  faiss::idx_t faiss_id = VectorIdToIdx(id);

  return ExecuteFaissOperation(
      [&]() { index_->add_with_ids(1, float_data.data(), &faiss_id); }, "Add");
}

core::Status FaissIndexBase::AddBatch(
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {
  if (vectors.empty()) {
    return core::OkStatus();
  }

  if (vectors.size() != ids.size()) {
    return core::InvalidArgumentError("Vector and ID count mismatch");
  }

  std::unique_lock lock(mutex_);

  if (!index_->is_trained) {
    return core::FailedPreconditionError("Index must be trained before adding vectors");
  }

  auto float_data = VectorsToFloatArray(vectors);
  std::vector<faiss::idx_t> faiss_ids(ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    faiss_ids[i] = VectorIdToIdx(ids[i]);
  }

  return ExecuteFaissOperation(
      [&]() { index_->add_with_ids(vectors.size(), float_data.data(),
                                   faiss_ids.data()); },
      "AddBatch");
}

core::Status FaissIndexBase::Remove(core::VectorId id) {
  std::unique_lock lock(mutex_);

  // Faiss doesn't support direct removal in all index types
  // This would need to be implemented per-index-type
  return core::UnimplementedError("Remove not supported for this index type");
}

core::StatusOr<core::SearchResult> FaissIndexBase::Search(
    const core::Vector& query, int k) {
  if (k <= 0) {
    return core::InvalidArgumentError(
        absl::StrCat("k must be positive, got ", k));
  }

  std::shared_lock lock(mutex_);

  if (index_->ntotal == 0) {
    return core::FailedPreconditionError("Cannot search empty index");
  }

  // Clamp k to available vectors
  k = std::min(k, static_cast<int>(index_->ntotal));

  auto float_query = VectorToFloatArray(query);
  std::vector<float> distances(k);
  std::vector<faiss::idx_t> labels(k);

  return ExecuteFaissOperationWithReturn<core::SearchResult>(
      [&]() {
        index_->search(1, float_query.data(), k, distances.data(),
                      labels.data());

        core::SearchResult result(k);
        for (int i = 0; i < k; ++i) {
          if (labels[i] >= 0) {  // Valid result
            result.AddEntry(IdxToVectorId(labels[i]), distances[i]);
          }
        }
        return result;
      },
      "Search");
}

core::StatusOr<core::SearchResult> FaissIndexBase::SearchRange(
    const core::Vector& query, float radius) {
  std::shared_lock lock(mutex_);

  if (index_->ntotal == 0) {
    return core::FailedPreconditionError("Cannot search empty index");
  }

  // Faiss range_search API
  auto float_query = VectorToFloatArray(query);
  faiss::RangeSearchResult range_result(1);

  return ExecuteFaissOperationWithReturn<core::SearchResult>(
      [&]() {
        index_->range_search(1, float_query.data(), radius, &range_result);

        core::SearchResult result;
        for (size_t i = range_result.lims[0]; i < range_result.lims[1]; ++i) {
          result.AddEntry(IdxToVectorId(range_result.labels[i]),
                         range_result.distances[i]);
        }
        return result;
      },
      "SearchRange");
}

core::StatusOr<std::vector<core::SearchResult>> FaissIndexBase::SearchBatch(
    const std::vector<core::Vector>& queries, int k) {
  if (queries.empty()) {
    return std::vector<core::SearchResult>();
  }

  if (k <= 0) {
    return core::InvalidArgumentError("k must be positive");
  }

  std::shared_lock lock(mutex_);

  if (index_->ntotal == 0) {
    return core::FailedPreconditionError("Cannot search empty index");
  }

  k = std::min(k, static_cast<int>(index_->ntotal));

  auto float_queries = VectorsToFloatArray(queries);
  size_t nq = queries.size();
  std::vector<float> distances(nq * k);
  std::vector<faiss::idx_t> labels(nq * k);

  return ExecuteFaissOperationWithReturn<std::vector<core::SearchResult>>(
      [&]() {
        index_->search(nq, float_queries.data(), k, distances.data(),
                      labels.data());

        std::vector<core::SearchResult> results(nq);
        for (size_t i = 0; i < nq; ++i) {
          results[i] = core::SearchResult(k);
          for (int j = 0; j < k; ++j) {
            size_t idx = i * k + j;
            if (labels[idx] >= 0) {
              results[i].AddEntry(IdxToVectorId(labels[idx]), distances[idx]);
            }
          }
        }
        return results;
      },
      "SearchBatch");
}

size_t FaissIndexBase::GetMemoryUsage() const {
  std::shared_lock lock(mutex_);
  // Rough estimation based on index type
  return index_->ntotal * index_->d * sizeof(float);
}

size_t FaissIndexBase::GetVectorCount() const {
  std::shared_lock lock(mutex_);
  return index_->ntotal;
}

core::Dimension FaissIndexBase::GetDimension() const {
  std::shared_lock lock(mutex_);
  return static_cast<core::Dimension>(index_->d);
}

core::MetricType FaissIndexBase::GetMetricType() const { return metric_type_; }

core::Status FaissIndexBase::Train(
    const std::vector<core::Vector>& training_data) {
  if (training_data.empty()) {
    return core::InvalidArgumentError("Training data cannot be empty");
  }

  std::unique_lock lock(mutex_);

  if (index_->is_trained) {
    return core::OkStatus();  // Already trained
  }

  auto float_data = VectorsToFloatArray(training_data);
  return ExecuteFaissOperation(
      [&]() { index_->train(training_data.size(), float_data.data()); },
      "Train");
}

bool FaissIndexBase::IsTrained() const {
  std::shared_lock lock(mutex_);
  return index_->is_trained;
}

core::Status FaissIndexBase::Serialize(const std::string& path) const {
  std::shared_lock lock(mutex_);

  return ExecuteFaissOperation(
      [&]() { faiss::write_index(index_.get(), path.c_str()); }, "Serialize");
}

core::Status FaissIndexBase::Deserialize(const std::string& path) {
  std::unique_lock lock(mutex_);

  return ExecuteFaissOperation(
      [&]() {
        auto* loaded_index = faiss::read_index(path.c_str());
        index_.reset(loaded_index);
      },
      "Deserialize");
}

// Helper implementations

std::vector<float> FaissIndexBase::VectorToFloatArray(
    const core::Vector& vec) {
  std::vector<float> result(vec.dimension());
  std::memcpy(result.data(), vec.data(), vec.dimension() * sizeof(float));
  return result;
}

std::vector<float> FaissIndexBase::VectorsToFloatArray(
    const std::vector<core::Vector>& vectors) {
  if (vectors.empty()) {
    return {};
  }

  size_t dim = vectors[0].dimension();
  std::vector<float> result(vectors.size() * dim);

  for (size_t i = 0; i < vectors.size(); ++i) {
    std::memcpy(result.data() + i * dim, vectors[i].data(),
                dim * sizeof(float));
  }

  return result;
}

faiss::idx_t FaissIndexBase::VectorIdToIdx(core::VectorId id) {
  return static_cast<faiss::idx_t>(core::ToUInt64(id));
}

core::VectorId FaissIndexBase::IdxToVectorId(faiss::idx_t idx) {
  return core::MakeVectorId(static_cast<uint64_t>(idx));
}

}  // namespace index
}  // namespace gvdb
