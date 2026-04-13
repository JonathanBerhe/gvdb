// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifdef GVDB_HAS_METAL

#include "metal_flat_index.h"
#include "metal_compute.h"

#include <Metal/Metal.hpp>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <numeric>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace index {
namespace metal {

MetalFlatIndex::MetalFlatIndex(core::Dimension dimension,
                               core::MetricType metric)
    : dimension_(dimension), metric_type_(metric) {}

void MetalFlatIndex::RebuildGpuBuffer() {
  if (gpu_vectors_) {
    gpu_vectors_->release();
    gpu_vectors_ = nullptr;
  }
  if (!vectors_.empty()) {
    gpu_vectors_ = MetalCompute::Instance().GetDevice()->newBuffer(
        vectors_.data(), vectors_.size() * sizeof(float),
        MTL::ResourceStorageModeShared);
  }
}

std::string MetalFlatIndex::KernelName() const {
  switch (metric_type_) {
    case core::MetricType::L2:             return "l2_distance";
    case core::MetricType::INNER_PRODUCT:  return "inner_product";
    case core::MetricType::COSINE:         return "cosine_distance";
  }
  return "l2_distance";
}

core::SearchResult MetalFlatIndex::TopK(const float* distances, size_t nb,
                                         int k) const {
  // L2/Cosine: smaller = more similar (ascending).
  // Inner product: larger = more similar (descending).
  bool descending = (metric_type_ == core::MetricType::INNER_PRODUCT);

  std::vector<size_t> indices(nb);
  std::iota(indices.begin(), indices.end(), 0);

  size_t actual_k = std::min(static_cast<size_t>(k), nb);
  std::partial_sort(indices.begin(), indices.begin() + actual_k, indices.end(),
                    [distances, descending](size_t a, size_t b) {
                      return descending ? distances[a] > distances[b]
                                        : distances[a] < distances[b];
                    });

  core::SearchResult result(actual_k);
  for (size_t i = 0; i < actual_k; ++i) {
    result.AddEntry(ids_[indices[i]], distances[indices[i]]);
  }
  return result;
}

core::Status MetalFlatIndex::Build(
    const std::vector<core::Vector>& vectors,
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

  vectors_.resize(vectors.size() * dimension_);
  ids_ = ids;

  for (size_t i = 0; i < vectors.size(); ++i) {
    std::memcpy(vectors_.data() + i * dimension_, vectors[i].data(),
                dimension_ * sizeof(float));
  }

  RebuildGpuBuffer();
  return core::OkStatus();
}

core::Status MetalFlatIndex::Add(const core::Vector& vector,
                                  core::VectorId id) {
  std::unique_lock lock(mutex_);

  size_t old_size = vectors_.size();
  vectors_.resize(old_size + dimension_);
  std::memcpy(vectors_.data() + old_size, vector.data(),
              dimension_ * sizeof(float));
  ids_.push_back(id);

  RebuildGpuBuffer();
  return core::OkStatus();
}

core::Status MetalFlatIndex::AddBatch(
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {
  if (vectors.empty()) return core::OkStatus();
  if (vectors.size() != ids.size()) {
    return core::InvalidArgumentError("Vector and ID count mismatch");
  }

  std::unique_lock lock(mutex_);

  size_t old_size = vectors_.size();
  vectors_.resize(old_size + vectors.size() * dimension_);
  for (size_t i = 0; i < vectors.size(); ++i) {
    std::memcpy(vectors_.data() + old_size + i * dimension_, vectors[i].data(),
                dimension_ * sizeof(float));
  }
  ids_.insert(ids_.end(), ids.begin(), ids.end());

  RebuildGpuBuffer();
  return core::OkStatus();
}

core::Status MetalFlatIndex::Remove(core::VectorId id) {
  return core::UnimplementedError("Remove not supported for MetalFlatIndex");
}

core::StatusOr<core::SearchResult> MetalFlatIndex::Search(
    const core::Vector& query, int k) {
  if (k <= 0) {
    return core::InvalidArgumentError(
        absl::StrCat("k must be positive, got ", k));
  }

  std::shared_lock lock(mutex_);

  size_t nb = ids_.size();
  if (nb == 0) {
    return core::FailedPreconditionError("Cannot search empty index");
  }

  k = std::min(k, static_cast<int>(nb));

  // Dispatch distance computation to Metal GPU
  std::vector<float> distances(nb);
  MetalCompute::Instance().ComputeDistances(
      KernelName(), query.data(), 1, gpu_vectors_, nb, dimension_,
      distances.data());

  return TopK(distances.data(), nb, k);
}

core::StatusOr<core::SearchResult> MetalFlatIndex::SearchRange(
    const core::Vector& query, float radius) {
  std::shared_lock lock(mutex_);

  size_t nb = ids_.size();
  if (nb == 0) {
    return core::FailedPreconditionError("Cannot search empty index");
  }

  std::vector<float> distances(nb);
  MetalCompute::Instance().ComputeDistances(
      KernelName(), query.data(), 1, gpu_vectors_, nb, dimension_,
      distances.data());

  core::SearchResult result;
  for (size_t i = 0; i < nb; ++i) {
    if (distances[i] <= radius) {
      result.AddEntry(ids_[i], distances[i]);
    }
  }
  return result;
}

core::StatusOr<std::vector<core::SearchResult>> MetalFlatIndex::SearchBatch(
    const std::vector<core::Vector>& queries, int k) {
  if (queries.empty()) return std::vector<core::SearchResult>();
  if (k <= 0) return core::InvalidArgumentError("k must be positive");

  std::shared_lock lock(mutex_);

  size_t nb = ids_.size();
  if (nb == 0) {
    return core::FailedPreconditionError("Cannot search empty index");
  }

  k = std::min(k, static_cast<int>(nb));
  size_t nq = queries.size();

  // Flatten queries into contiguous array
  std::vector<float> q_flat(nq * dimension_);
  for (size_t i = 0; i < nq; ++i) {
    std::memcpy(q_flat.data() + i * dimension_, queries[i].data(),
                dimension_ * sizeof(float));
  }

  // Batch distance computation on GPU
  std::vector<float> distances(nq * nb);
  MetalCompute::Instance().ComputeDistances(
      KernelName(), q_flat.data(), nq, gpu_vectors_, nb, dimension_,
      distances.data());

  // Extract top-k per query
  std::vector<core::SearchResult> results(nq);
  for (size_t i = 0; i < nq; ++i) {
    results[i] = TopK(distances.data() + i * nb, nb, k);
  }
  return results;
}

size_t MetalFlatIndex::GetMemoryUsage() const {
  std::shared_lock lock(mutex_);
  return vectors_.size() * sizeof(float) + ids_.size() * sizeof(core::VectorId);
}

size_t MetalFlatIndex::GetVectorCount() const {
  std::shared_lock lock(mutex_);
  return ids_.size();
}

core::Dimension MetalFlatIndex::GetDimension() const {
  return dimension_;
}

core::MetricType MetalFlatIndex::GetMetricType() const {
  return metric_type_;
}

core::Status MetalFlatIndex::Train(const std::vector<core::Vector>&) {
  return core::OkStatus();  // FLAT doesn't need training
}

bool MetalFlatIndex::IsTrained() const {
  return true;  // Always trained
}

core::Status MetalFlatIndex::Serialize(const std::string& path) const {
  std::shared_lock lock(mutex_);

  std::ofstream file(path, std::ios::binary);
  if (!file.is_open()) {
    return core::InternalError(absl::StrCat("Cannot open file: ", path));
  }

  // Header: dimension, metric, count
  uint32_t dim = static_cast<uint32_t>(dimension_);
  uint32_t metric = static_cast<uint32_t>(metric_type_);
  uint64_t count = ids_.size();

  file.write(reinterpret_cast<const char*>(&dim), sizeof(dim));
  file.write(reinterpret_cast<const char*>(&metric), sizeof(metric));
  file.write(reinterpret_cast<const char*>(&count), sizeof(count));
  file.write(reinterpret_cast<const char*>(vectors_.data()),
             vectors_.size() * sizeof(float));
  file.write(reinterpret_cast<const char*>(ids_.data()),
             ids_.size() * sizeof(core::VectorId));

  return core::OkStatus();
}

core::Status MetalFlatIndex::Deserialize(const std::string& path) {
  std::unique_lock lock(mutex_);

  std::ifstream file(path, std::ios::binary);
  if (!file.is_open()) {
    return core::InternalError(absl::StrCat("Cannot open file: ", path));
  }

  uint32_t dim, metric;
  uint64_t count;

  file.read(reinterpret_cast<char*>(&dim), sizeof(dim));
  file.read(reinterpret_cast<char*>(&metric), sizeof(metric));
  file.read(reinterpret_cast<char*>(&count), sizeof(count));

  dimension_ = static_cast<core::Dimension>(dim);
  metric_type_ = static_cast<core::MetricType>(metric);
  vectors_.resize(count * dimension_);
  ids_.resize(count);

  file.read(reinterpret_cast<char*>(vectors_.data()),
            vectors_.size() * sizeof(float));
  file.read(reinterpret_cast<char*>(ids_.data()),
            ids_.size() * sizeof(core::VectorId));

  return core::OkStatus();
}

core::IndexType MetalFlatIndex::GetIndexType() const {
  return core::IndexType::FLAT;  // Drop-in for FLAT — same type
}

}  // namespace metal
}  // namespace index
}  // namespace gvdb

#endif  // GVDB_HAS_METAL
