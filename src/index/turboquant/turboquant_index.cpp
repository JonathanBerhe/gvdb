// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "turboquant_index.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <limits>
#include <queue>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace index {
namespace turboquant {

namespace {

constexpr uint32_t kMagic = 0x54425154;  // "TBQT"
constexpr uint32_t kVersion = 1;

template <typename T>
void WriteValue(std::ofstream& out, const T& value) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
bool ReadValue(std::ifstream& in, T& value) {
  in.read(reinterpret_cast<char*>(&value), sizeof(T));
  return in.good();
}

template <typename T>
void WriteVector(std::ofstream& out, const std::vector<T>& vec) {
  out.write(reinterpret_cast<const char*>(vec.data()),
            static_cast<std::streamsize>(vec.size() * sizeof(T)));
}

template <typename T>
bool ReadVector(std::ifstream& in, std::vector<T>& vec, size_t count) {
  vec.resize(count);
  in.read(reinterpret_cast<char*>(vec.data()),
          static_cast<std::streamsize>(count * sizeof(T)));
  return in.good();
}

}  // namespace

// ============================================================================
// Construction
// ============================================================================

TurboQuantIndex::TurboQuantIndex(core::Dimension dimension,
                                   core::MetricType metric,
                                   int bit_width, bool use_qjl, int qjl_dim)
    : codec_(dimension, bit_width, use_qjl, qjl_dim),
      metric_type_(metric) {}

// ============================================================================
// Building and modification
// ============================================================================

core::Status TurboQuantIndex::Build(
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {
  if (vectors.size() != ids.size()) {
    return core::InvalidArgumentError("vectors and ids size mismatch");
  }

  std::unique_lock lock(mutex_);

  // Clear existing data
  norms_.clear();
  residual_norms_.clear();
  codes_.clear();
  qjl_bits_.clear();
  ids_.clear();
  deleted_.clear();
  num_vectors_ = 0;

  // Reserve storage
  size_t n = vectors.size();
  norms_.reserve(n);
  residual_norms_.reserve(n);
  codes_.reserve(n * codec_.CodeBytesPerVector());
  if (codec_.use_qjl()) {
    qjl_bits_.reserve(n * codec_.QJLBytesPerVector());
  }
  ids_.reserve(n);
  deleted_.reserve(n);

  lock.unlock();

  // Encode each vector (Build replaces all data)
  for (size_t i = 0; i < n; i++) {
    if (vectors[i].dimension() != codec_.dimension()) {
      return core::InvalidArgumentError(
          absl::StrCat("vector ", i, " dimension mismatch: expected ",
                       codec_.dimension(), ", got ", vectors[i].dimension()));
    }
    auto status = EncodeAndStore(vectors[i], ids[i]);
    if (!status.ok()) return status;
  }

  return core::OkStatus();
}

core::Status TurboQuantIndex::Add(const core::Vector& vector,
                                    core::VectorId id) {
  if (vector.dimension() != codec_.dimension()) {
    return core::InvalidArgumentError("dimension mismatch");
  }
  return EncodeAndStore(vector, id);
}

core::Status TurboQuantIndex::AddBatch(
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {
  if (vectors.size() != ids.size()) {
    return core::InvalidArgumentError("vectors and ids size mismatch");
  }
  for (size_t i = 0; i < vectors.size(); i++) {
    if (vectors[i].dimension() != codec_.dimension()) {
      return core::InvalidArgumentError(
          absl::StrCat("vector ", i, " dimension mismatch"));
    }
    auto status = EncodeAndStore(vectors[i], ids[i]);
    if (!status.ok()) return status;
  }
  return core::OkStatus();
}

core::Status TurboQuantIndex::Remove(core::VectorId id) {
  std::unique_lock lock(mutex_);
  for (size_t i = 0; i < num_vectors_; i++) {
    if (ids_[i] == id && !deleted_[i]) {
      deleted_[i] = true;
      return core::OkStatus();
    }
  }
  return core::NotFoundError("vector id not found");
}

// ============================================================================
// Search
// ============================================================================

core::StatusOr<core::SearchResult> TurboQuantIndex::Search(
    const core::Vector& query, int k) {
  if (query.dimension() != codec_.dimension()) {
    return core::InvalidArgumentError("query dimension mismatch");
  }
  if (k <= 0) {
    return core::InvalidArgumentError("k must be positive");
  }

  std::shared_lock lock(mutex_);

  if (num_vectors_ == 0) {
    return core::SearchResult();
  }

  // Preprocess query once
  auto pq = codec_.PreprocessQuery(query.data(), query.dimension());

  // Max-heap: (distance, index) — largest distance on top for eviction
  using Entry = std::pair<float, size_t>;
  std::priority_queue<Entry> heap;

  size_t code_stride = codec_.CodeBytesPerVector();
  size_t qjl_stride = codec_.QJLBytesPerVector();

  for (size_t i = 0; i < num_vectors_; i++) {
    if (deleted_[i]) continue;

    const uint8_t* vec_codes = codes_.data() + i * code_stride;
    const uint8_t* vec_qjl =
        codec_.use_qjl() ? qjl_bits_.data() + i * qjl_stride : nullptr;

    float ip = codec_.EstimateIP(pq, vec_codes, norms_[i],
                                  residual_norms_[i], vec_qjl);
    float dist = IPToDistance(ip, norms_[i], pq);

    if (static_cast<int>(heap.size()) < k) {
      heap.push({dist, i});
    } else if (dist < heap.top().first) {
      heap.pop();
      heap.push({dist, i});
    }
  }

  // Extract results in ascending distance order
  core::SearchResult result(static_cast<size_t>(k));
  std::vector<Entry> sorted;
  sorted.reserve(heap.size());
  while (!heap.empty()) {
    sorted.push_back(heap.top());
    heap.pop();
  }
  std::reverse(sorted.begin(), sorted.end());

  for (auto& [dist, idx] : sorted) {
    result.AddEntry(ids_[idx], dist);
  }

  return result;
}

core::StatusOr<core::SearchResult> TurboQuantIndex::SearchRange(
    const core::Vector& query, float radius) {
  if (query.dimension() != codec_.dimension()) {
    return core::InvalidArgumentError("query dimension mismatch");
  }

  std::shared_lock lock(mutex_);

  auto pq = codec_.PreprocessQuery(query.data(), query.dimension());
  core::SearchResult result;

  size_t code_stride = codec_.CodeBytesPerVector();
  size_t qjl_stride = codec_.QJLBytesPerVector();

  for (size_t i = 0; i < num_vectors_; i++) {
    if (deleted_[i]) continue;

    const uint8_t* vec_codes = codes_.data() + i * code_stride;
    const uint8_t* vec_qjl =
        codec_.use_qjl() ? qjl_bits_.data() + i * qjl_stride : nullptr;

    float ip = codec_.EstimateIP(pq, vec_codes, norms_[i],
                                  residual_norms_[i], vec_qjl);
    float dist = IPToDistance(ip, norms_[i], pq);

    if (dist <= radius) {
      result.AddEntry(ids_[i], dist);
    }
  }

  return result;
}

core::StatusOr<std::vector<core::SearchResult>> TurboQuantIndex::SearchBatch(
    const std::vector<core::Vector>& queries, int k) {
  std::vector<core::SearchResult> results;
  results.reserve(queries.size());
  for (const auto& query : queries) {
    auto result = Search(query, k);
    if (!result.ok()) return result.status();
    results.push_back(std::move(result.value()));
  }
  return results;
}

// ============================================================================
// Index management
// ============================================================================

size_t TurboQuantIndex::GetMemoryUsage() const {
  std::shared_lock lock(mutex_);
  return codes_.size() + qjl_bits_.size() +
         norms_.size() * sizeof(float) +
         residual_norms_.size() * sizeof(float) +
         ids_.size() * sizeof(core::VectorId) +
         num_vectors_;  // deleted_ bitmap
}

size_t TurboQuantIndex::GetVectorCount() const {
  std::shared_lock lock(mutex_);
  size_t count = 0;
  for (size_t i = 0; i < num_vectors_; i++) {
    if (!deleted_[i]) count++;
  }
  return count;
}

core::Dimension TurboQuantIndex::GetDimension() const {
  return codec_.dimension();
}

core::MetricType TurboQuantIndex::GetMetricType() const {
  return metric_type_;
}

core::Status TurboQuantIndex::Train(
    const std::vector<core::Vector>& /*training_data*/) {
  // TurboQuant is data-oblivious — no training required
  return core::OkStatus();
}

bool TurboQuantIndex::IsTrained() const {
  return true;  // Always trained (data-oblivious)
}

core::IndexType TurboQuantIndex::GetIndexType() const {
  return core::IndexType::TURBOQUANT;
}

// ============================================================================
// Serialization
// ============================================================================

core::Status TurboQuantIndex::Serialize(const std::string& path) const {
  std::shared_lock lock(mutex_);

  std::ofstream out(path, std::ios::binary);
  if (!out.is_open()) {
    return core::InternalError(absl::StrCat("cannot open file: ", path));
  }

  // Header
  WriteValue(out, kMagic);
  WriteValue(out, kVersion);
  WriteValue(out, codec_.dimension());
  WriteValue(out, codec_.bit_width());
  uint8_t use_qjl = codec_.use_qjl() ? 1 : 0;
  WriteValue(out, use_qjl);
  WriteValue(out, codec_.qjl_dim());
  WriteValue(out, codec_.seed());
  auto metric = static_cast<int32_t>(metric_type_);
  WriteValue(out, metric);
  WriteValue(out, num_vectors_);

  // Data arrays
  WriteVector(out, norms_);
  WriteVector(out, residual_norms_);
  WriteVector(out, codes_);
  WriteVector(out, qjl_bits_);

  // IDs (as uint64_t)
  for (size_t i = 0; i < num_vectors_; i++) {
    auto raw_id = core::ToUInt64(ids_[i]);
    WriteValue(out, raw_id);
  }

  // Deleted flags
  for (size_t i = 0; i < num_vectors_; i++) {
    uint8_t d = deleted_[i] ? 1 : 0;
    WriteValue(out, d);
  }

  if (!out.good()) {
    return core::InternalError("write error during serialization");
  }
  return core::OkStatus();
}

core::Status TurboQuantIndex::Deserialize(const std::string& path) {
  std::unique_lock lock(mutex_);

  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    return core::NotFoundError(absl::StrCat("cannot open file: ", path));
  }

  // Read and validate header
  uint32_t magic, version;
  if (!ReadValue(in, magic) || magic != kMagic) {
    return core::InvalidArgumentError("invalid TurboQuant file magic");
  }
  if (!ReadValue(in, version) || version != kVersion) {
    return core::InvalidArgumentError("unsupported TurboQuant file version");
  }

  core::Dimension dim;
  int32_t bit_width;
  uint8_t use_qjl;
  int32_t qjl_dim;
  uint64_t seed;
  int32_t metric;
  size_t num_vecs;

  ReadValue(in, dim);
  ReadValue(in, bit_width);
  ReadValue(in, use_qjl);
  ReadValue(in, qjl_dim);
  ReadValue(in, seed);
  ReadValue(in, metric);
  ReadValue(in, num_vecs);

  if (!in.good()) {
    return core::InternalError("failed to read header");
  }

  // Reconstruct codec with saved parameters
  codec_ = TurboQuantCodec(dim, bit_width, use_qjl != 0, qjl_dim, seed);
  metric_type_ = static_cast<core::MetricType>(metric);
  num_vectors_ = num_vecs;

  // Read data arrays
  if (!ReadVector(in, norms_, num_vecs)) {
    return core::InternalError("failed to read norms");
  }
  if (!ReadVector(in, residual_norms_, num_vecs)) {
    return core::InternalError("failed to read residual_norms");
  }

  size_t total_code_bytes = num_vecs * codec_.CodeBytesPerVector();
  if (!ReadVector(in, codes_, total_code_bytes)) {
    return core::InternalError("failed to read codes");
  }

  size_t total_qjl_bytes = num_vecs * codec_.QJLBytesPerVector();
  if (total_qjl_bytes > 0) {
    if (!ReadVector(in, qjl_bits_, total_qjl_bytes)) {
      return core::InternalError("failed to read qjl_bits");
    }
  } else {
    qjl_bits_.clear();
  }

  // Read IDs
  ids_.resize(num_vecs);
  for (size_t i = 0; i < num_vecs; i++) {
    uint64_t raw_id;
    ReadValue(in, raw_id);
    ids_[i] = core::MakeVectorId(raw_id);
  }

  // Read deleted flags
  deleted_.resize(num_vecs);
  for (size_t i = 0; i < num_vecs; i++) {
    uint8_t d;
    ReadValue(in, d);
    deleted_[i] = (d != 0);
  }

  if (!in.good()) {
    return core::InternalError("read error during deserialization");
  }
  return core::OkStatus();
}

// ============================================================================
// Private helpers
// ============================================================================

core::Status TurboQuantIndex::EncodeAndStore(const core::Vector& vector,
                                               core::VectorId id) {
  std::vector<uint8_t> vec_codes;
  float norm = 0.0f;
  float residual_norm = 0.0f;
  std::vector<uint8_t> vec_qjl;

  codec_.Encode(vector.data(), vector.dimension(),
                vec_codes, norm, residual_norm, vec_qjl);

  std::unique_lock lock(mutex_);

  norms_.push_back(norm);
  residual_norms_.push_back(residual_norm);
  codes_.insert(codes_.end(), vec_codes.begin(), vec_codes.end());
  if (codec_.use_qjl()) {
    qjl_bits_.insert(qjl_bits_.end(), vec_qjl.begin(), vec_qjl.end());
  }
  ids_.push_back(id);
  deleted_.push_back(false);
  num_vectors_++;

  return core::OkStatus();
}

float TurboQuantIndex::IPToDistance(float estimated_ip, float x_norm,
                                     const PreprocessedQuery& query) const {
  switch (metric_type_) {
    case core::MetricType::L2:
      // L2^2 = ||x||^2 + ||q||^2 - 2*IP
      return x_norm * x_norm + query.norm * query.norm - 2.0f * estimated_ip;

    case core::MetricType::INNER_PRODUCT:
      // Lower = more similar (negate IP)
      return -estimated_ip;

    case core::MetricType::COSINE: {
      // cosine distance = 1 - cosine_similarity
      float denom = x_norm * query.norm;
      if (denom < 1e-30f) return 1.0f;
      return 1.0f - estimated_ip / denom;
    }
  }
  return -estimated_ip;  // fallback
}

}  // namespace turboquant
}  // namespace index
}  // namespace gvdb
