// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "ivf_turboquant_index.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <limits>
#include <queue>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace index {
namespace turboquant {

IVFTurboQuantIndex::IVFTurboQuantIndex(
    core::Dimension dimension, core::MetricType metric,
    int nlist, int nprobe, int bit_width,
    bool use_qjl, int qjl_dim)
    : dimension_(dimension),
      metric_type_(metric),
      nlist_(nlist),
      nprobe_(std::min(nprobe, nlist)) {
  // Create Faiss coarse quantizer
  faiss::MetricType faiss_metric = (metric == core::MetricType::INNER_PRODUCT)
      ? faiss::METRIC_INNER_PRODUCT : faiss::METRIC_L2;
  coarse_quantizer_ = std::make_unique<faiss::IndexFlat>(dimension, faiss_metric);

  // Create TurboQuant codec
  codec_ = std::make_unique<TurboQuantCodec>(dimension, bit_width, use_qjl, qjl_dim);

  // Initialize cluster storage
  clusters_.resize(nlist);
}

// ============================================================================
// Training
// ============================================================================

core::Status IVFTurboQuantIndex::Train(
    const std::vector<core::Vector>& training_data) {
  if (training_data.empty()) {
    return core::InvalidArgumentError("Training data is empty");
  }

  // Flatten training vectors for Faiss
  size_t n = training_data.size();
  std::vector<float> flat_data(n * dimension_);
  for (size_t i = 0; i < n; ++i) {
    std::memcpy(flat_data.data() + i * dimension_,
                training_data[i].data(), dimension_ * sizeof(float));
  }

  // Train k-means clustering
  coarse_quantizer_->reset();
  coarse_quantizer_->train(n, flat_data.data());
  coarse_quantizer_->add(n, flat_data.data());

  // Actually, IndexFlat doesn't need training — we need to compute centroids.
  // Use Faiss Clustering directly.
  faiss::MetricType faiss_metric = (metric_type_ == core::MetricType::INNER_PRODUCT)
      ? faiss::METRIC_INNER_PRODUCT : faiss::METRIC_L2;

  // Reset and use k-means
  coarse_quantizer_ = std::make_unique<faiss::IndexFlat>(dimension_, faiss_metric);

  faiss::Clustering clus(dimension_, nlist_);
  clus.niter = 20;
  clus.verbose = false;

  faiss::IndexFlat quantizer_for_training(dimension_, faiss_metric);
  clus.train(n, flat_data.data(), quantizer_for_training);

  // Add centroids to our coarse quantizer
  coarse_quantizer_->add(nlist_, clus.centroids.data());

  trained_ = true;
  return core::OkStatus();
}

bool IVFTurboQuantIndex::IsTrained() const {
  return trained_;
}

// ============================================================================
// Build
// ============================================================================

core::Status IVFTurboQuantIndex::Build(
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {
  if (vectors.size() != ids.size()) {
    return core::InvalidArgumentError("Vectors and IDs size mismatch");
  }
  if (vectors.empty()) {
    return core::InvalidArgumentError("Cannot build from empty vectors");
  }

  // Train if needed
  if (!trained_) {
    auto status = Train(vectors);
    if (!status.ok()) return status;
  }

  std::unique_lock lock(mutex_);

  // Clear existing data
  for (auto& cluster : clusters_) {
    cluster = ClusterData{};
  }
  total_vectors_ = 0;

  // Assign each vector to nearest cluster and encode
  for (size_t i = 0; i < vectors.size(); ++i) {
    // Find nearest centroid
    float dist;
    faiss::idx_t cluster_id;
    coarse_quantizer_->search(1, vectors[i].data(), 1, &dist, &cluster_id);

    if (cluster_id < 0 || cluster_id >= nlist_) {
      continue;  // Skip invalid assignments
    }

    EncodeAndStore(clusters_[cluster_id], vectors[i], ids[i]);
    total_vectors_++;
  }

  return core::OkStatus();
}

// ============================================================================
// Add / AddBatch
// ============================================================================

core::Status IVFTurboQuantIndex::Add(const core::Vector& vector, core::VectorId id) {
  if (!trained_) {
    return core::FailedPreconditionError("Index must be trained before adding vectors");
  }

  float dist;
  faiss::idx_t cluster_id;
  coarse_quantizer_->search(1, vector.data(), 1, &dist, &cluster_id);

  if (cluster_id < 0 || cluster_id >= nlist_) {
    return core::InternalError("Failed to assign vector to cluster");
  }

  std::unique_lock lock(mutex_);
  EncodeAndStore(clusters_[cluster_id], vector, id);
  total_vectors_++;

  return core::OkStatus();
}

core::Status IVFTurboQuantIndex::AddBatch(
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {
  if (vectors.size() != ids.size()) {
    return core::InvalidArgumentError("Vectors and IDs size mismatch");
  }
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto status = Add(vectors[i], ids[i]);
    if (!status.ok()) return status;
  }
  return core::OkStatus();
}

// ============================================================================
// Remove
// ============================================================================

core::Status IVFTurboQuantIndex::Remove(core::VectorId id) {
  std::unique_lock lock(mutex_);

  uint64_t target = core::ToUInt64(id);
  for (auto& cluster : clusters_) {
    for (size_t i = 0; i < cluster.ids.size(); ++i) {
      if (core::ToUInt64(cluster.ids[i]) == target) {
        // Swap-and-pop removal
        size_t last = cluster.num_vectors - 1;
        if (i != last) {
          // Swap codes
          size_t code_bytes = codec_->CodeBytesPerVector();
          std::memcpy(cluster.codes.data() + i * code_bytes,
                      cluster.codes.data() + last * code_bytes, code_bytes);
          cluster.norms[i] = cluster.norms[last];
          cluster.residual_norms[i] = cluster.residual_norms[last];
          if (codec_->use_qjl()) {
            size_t qjl_bytes = codec_->QJLBytesPerVector();
            std::memcpy(cluster.qjl_bits.data() + i * qjl_bytes,
                        cluster.qjl_bits.data() + last * qjl_bytes, qjl_bytes);
          }
          cluster.ids[i] = cluster.ids[last];
        }

        // Pop last
        cluster.codes.resize(last * codec_->CodeBytesPerVector());
        cluster.norms.pop_back();
        cluster.residual_norms.pop_back();
        if (codec_->use_qjl()) {
          cluster.qjl_bits.resize(last * codec_->QJLBytesPerVector());
        }
        cluster.ids.pop_back();
        cluster.num_vectors--;
        total_vectors_--;
        return core::OkStatus();
      }
    }
  }

  return core::NotFoundError(
      absl::StrCat("Vector ID ", core::ToUInt64(id), " not found in index"));
}

// ============================================================================
// Search
// ============================================================================

core::StatusOr<core::SearchResult> IVFTurboQuantIndex::Search(
    const core::Vector& query, int k) {
  if (!trained_) {
    return core::FailedPreconditionError("Index not trained");
  }

  std::shared_lock lock(mutex_);

  if (total_vectors_ == 0) {
    return core::SearchResult{};
  }

  // Find nprobe nearest clusters
  std::vector<float> dists(nprobe_);
  std::vector<faiss::idx_t> labels(nprobe_);
  coarse_quantizer_->search(1, query.data(), nprobe_, dists.data(), labels.data());

  // Preprocess query once
  auto pq = codec_->PreprocessQuery(query.data(), dimension_);

  // Max-heap for top-k (largest distance on top)
  using Entry = std::pair<float, core::VectorId>;
  auto cmp = [](const Entry& a, const Entry& b) { return a.first < b.first; };
  std::priority_queue<Entry, std::vector<Entry>, decltype(cmp)> heap(cmp);

  size_t code_bytes = codec_->CodeBytesPerVector();
  size_t qjl_bytes = codec_->QJLBytesPerVector();

  // Scan selected clusters
  for (int p = 0; p < nprobe_; ++p) {
    faiss::idx_t cid = labels[p];
    if (cid < 0 || cid >= nlist_) continue;

    const auto& cluster = clusters_[cid];
    for (size_t i = 0; i < cluster.num_vectors; ++i) {
      const uint8_t* codes_ptr = cluster.codes.data() + i * code_bytes;
      const uint8_t* qjl_ptr = codec_->use_qjl()
          ? cluster.qjl_bits.data() + i * qjl_bytes : nullptr;

      float ip = codec_->EstimateIP(pq, codes_ptr, cluster.norms[i],
                                     cluster.residual_norms[i], qjl_ptr);
      float dist = IPToDistance(ip, cluster.norms[i], pq);

      if (static_cast<int>(heap.size()) < k) {
        heap.push({dist, cluster.ids[i]});
      } else if (dist < heap.top().first) {
        heap.pop();
        heap.push({dist, cluster.ids[i]});
      }
    }
  }

  // Extract results sorted by distance
  core::SearchResult result;
  result.entries.resize(heap.size());
  for (int i = static_cast<int>(heap.size()) - 1; i >= 0; --i) {
    result.entries[i] = {heap.top().second, heap.top().first};
    heap.pop();
  }

  return result;
}

core::StatusOr<core::SearchResult> IVFTurboQuantIndex::SearchRange(
    const core::Vector& query, float radius) {
  if (!trained_) {
    return core::FailedPreconditionError("Index not trained");
  }

  std::shared_lock lock(mutex_);

  if (total_vectors_ == 0) {
    return core::SearchResult{};
  }

  std::vector<float> dists(nprobe_);
  std::vector<faiss::idx_t> labels(nprobe_);
  coarse_quantizer_->search(1, query.data(), nprobe_, dists.data(), labels.data());

  auto pq = codec_->PreprocessQuery(query.data(), dimension_);

  size_t code_bytes = codec_->CodeBytesPerVector();
  size_t qjl_bytes = codec_->QJLBytesPerVector();

  std::vector<core::SearchResultEntry> results;

  for (int p = 0; p < nprobe_; ++p) {
    faiss::idx_t cid = labels[p];
    if (cid < 0 || cid >= nlist_) continue;

    const auto& cluster = clusters_[cid];
    for (size_t i = 0; i < cluster.num_vectors; ++i) {
      const uint8_t* codes_ptr = cluster.codes.data() + i * code_bytes;
      const uint8_t* qjl_ptr = codec_->use_qjl()
          ? cluster.qjl_bits.data() + i * qjl_bytes : nullptr;

      float ip = codec_->EstimateIP(pq, codes_ptr, cluster.norms[i],
                                     cluster.residual_norms[i], qjl_ptr);
      float dist = IPToDistance(ip, cluster.norms[i], pq);

      if (dist <= radius) {
        results.push_back({cluster.ids[i], dist});
      }
    }
  }

  std::sort(results.begin(), results.end(),
            [](const auto& a, const auto& b) { return a.distance < b.distance; });

  core::SearchResult result;
  result.entries = std::move(results);
  return result;
}

core::StatusOr<std::vector<core::SearchResult>> IVFTurboQuantIndex::SearchBatch(
    const std::vector<core::Vector>& queries, int k) {
  std::vector<core::SearchResult> results;
  results.reserve(queries.size());
  for (const auto& query : queries) {
    auto result = Search(query, k);
    if (!result.ok()) return result.status();
    results.push_back(std::move(*result));
  }
  return results;
}

// ============================================================================
// Serialization
// ============================================================================

core::Status IVFTurboQuantIndex::Serialize(const std::string& path) const {
  std::shared_lock lock(mutex_);

  std::ofstream out(path, std::ios::binary);
  if (!out) {
    return core::InternalError(absl::StrCat("Cannot open file: ", path));
  }

  // Write header
  uint32_t magic = 0x49545151;  // "ITQQ"
  out.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
  uint32_t dim = dimension_;
  out.write(reinterpret_cast<const char*>(&dim), sizeof(dim));
  int32_t metric = static_cast<int32_t>(metric_type_);
  out.write(reinterpret_cast<const char*>(&metric), sizeof(metric));
  out.write(reinterpret_cast<const char*>(&nlist_), sizeof(nlist_));
  out.write(reinterpret_cast<const char*>(&nprobe_), sizeof(nprobe_));
  int32_t bw = codec_->bit_width();
  out.write(reinterpret_cast<const char*>(&bw), sizeof(bw));
  int32_t qjl = codec_->use_qjl() ? 1 : 0;
  out.write(reinterpret_cast<const char*>(&qjl), sizeof(qjl));
  int32_t qjl_d = codec_->qjl_dim();
  out.write(reinterpret_cast<const char*>(&qjl_d), sizeof(qjl_d));
  uint8_t tr = trained_ ? 1 : 0;
  out.write(reinterpret_cast<const char*>(&tr), sizeof(tr));

  // Write centroids
  if (trained_) {
    size_t centroid_bytes = nlist_ * dimension_ * sizeof(float);
    auto* centroid_data = reinterpret_cast<const char*>(
        coarse_quantizer_->get_xb());
    out.write(centroid_data, centroid_bytes);
  }

  // Write clusters
  uint64_t total = total_vectors_;
  out.write(reinterpret_cast<const char*>(&total), sizeof(total));

  for (int c = 0; c < nlist_; ++c) {
    const auto& cluster = clusters_[c];
    uint64_t n = cluster.num_vectors;
    out.write(reinterpret_cast<const char*>(&n), sizeof(n));
    if (n == 0) continue;

    out.write(reinterpret_cast<const char*>(cluster.codes.data()), cluster.codes.size());
    out.write(reinterpret_cast<const char*>(cluster.norms.data()), n * sizeof(float));
    out.write(reinterpret_cast<const char*>(cluster.residual_norms.data()), n * sizeof(float));
    if (codec_->use_qjl()) {
      out.write(reinterpret_cast<const char*>(cluster.qjl_bits.data()), cluster.qjl_bits.size());
    }
    for (size_t i = 0; i < n; ++i) {
      uint64_t id = core::ToUInt64(cluster.ids[i]);
      out.write(reinterpret_cast<const char*>(&id), sizeof(id));
    }
  }

  return core::OkStatus();
}

core::Status IVFTurboQuantIndex::Deserialize(const std::string& path) {
  std::unique_lock lock(mutex_);

  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return core::InternalError(absl::StrCat("Cannot open file: ", path));
  }

  uint32_t magic;
  in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
  if (magic != 0x49545151) {
    return core::InternalError("Invalid IVF_TURBOQUANT file magic");
  }

  uint32_t dim;
  in.read(reinterpret_cast<char*>(&dim), sizeof(dim));
  int32_t metric;
  in.read(reinterpret_cast<char*>(&metric), sizeof(metric));
  in.read(reinterpret_cast<char*>(&nlist_), sizeof(nlist_));
  in.read(reinterpret_cast<char*>(&nprobe_), sizeof(nprobe_));
  int32_t bw;
  in.read(reinterpret_cast<char*>(&bw), sizeof(bw));
  int32_t qjl;
  in.read(reinterpret_cast<char*>(&qjl), sizeof(qjl));
  int32_t qjl_d;
  in.read(reinterpret_cast<char*>(&qjl_d), sizeof(qjl_d));

  dimension_ = dim;
  metric_type_ = static_cast<core::MetricType>(metric);

  codec_ = std::make_unique<TurboQuantCodec>(dimension_, bw, qjl != 0, qjl_d);

  uint8_t tr;
  in.read(reinterpret_cast<char*>(&tr), sizeof(tr));
  trained_ = tr != 0;

  // Read centroids
  if (trained_) {
    faiss::MetricType faiss_metric = (metric_type_ == core::MetricType::INNER_PRODUCT)
        ? faiss::METRIC_INNER_PRODUCT : faiss::METRIC_L2;
    coarse_quantizer_ = std::make_unique<faiss::IndexFlat>(dimension_, faiss_metric);

    std::vector<float> centroids(nlist_ * dimension_);
    in.read(reinterpret_cast<char*>(centroids.data()),
            centroids.size() * sizeof(float));
    coarse_quantizer_->add(nlist_, centroids.data());
  }

  // Read clusters
  uint64_t total;
  in.read(reinterpret_cast<char*>(&total), sizeof(total));

  clusters_.resize(nlist_);
  total_vectors_ = 0;

  size_t code_bytes = codec_->CodeBytesPerVector();
  size_t qjl_bytes = codec_->QJLBytesPerVector();

  for (int c = 0; c < nlist_; ++c) {
    auto& cluster = clusters_[c];
    uint64_t n;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    cluster.num_vectors = n;
    if (n == 0) continue;

    cluster.codes.resize(n * code_bytes);
    in.read(reinterpret_cast<char*>(cluster.codes.data()), cluster.codes.size());

    cluster.norms.resize(n);
    in.read(reinterpret_cast<char*>(cluster.norms.data()), n * sizeof(float));

    cluster.residual_norms.resize(n);
    in.read(reinterpret_cast<char*>(cluster.residual_norms.data()), n * sizeof(float));

    if (codec_->use_qjl()) {
      cluster.qjl_bits.resize(n * qjl_bytes);
      in.read(reinterpret_cast<char*>(cluster.qjl_bits.data()), cluster.qjl_bits.size());
    }

    cluster.ids.resize(n);
    for (size_t i = 0; i < n; ++i) {
      uint64_t id;
      in.read(reinterpret_cast<char*>(&id), sizeof(id));
      cluster.ids[i] = core::MakeVectorId(id);
    }

    total_vectors_ += n;
  }

  return core::OkStatus();
}

// ============================================================================
// Helpers
// ============================================================================

void IVFTurboQuantIndex::EncodeAndStore(
    ClusterData& cluster, const core::Vector& vector, core::VectorId id) {
  std::vector<uint8_t> vec_codes;
  float norm = 0.0f;
  float residual_norm = 0.0f;
  std::vector<uint8_t> vec_qjl;

  codec_->Encode(vector.data(), vector.dimension(),
                 vec_codes, norm, residual_norm, vec_qjl);

  cluster.norms.push_back(norm);
  cluster.residual_norms.push_back(residual_norm);
  cluster.codes.insert(cluster.codes.end(), vec_codes.begin(), vec_codes.end());
  if (codec_->use_qjl()) {
    cluster.qjl_bits.insert(cluster.qjl_bits.end(), vec_qjl.begin(), vec_qjl.end());
  }
  cluster.ids.push_back(id);
  cluster.num_vectors++;
}

float IVFTurboQuantIndex::IPToDistance(
    float estimated_ip, float x_norm, const PreprocessedQuery& query) const {
  switch (metric_type_) {
    case core::MetricType::L2:
      return x_norm * x_norm + query.norm * query.norm - 2.0f * estimated_ip;
    case core::MetricType::INNER_PRODUCT:
      return -estimated_ip;
    case core::MetricType::COSINE: {
      float denom = x_norm * query.norm;
      if (denom < 1e-30f) return 1.0f;
      return 1.0f - estimated_ip / denom;
    }
  }
  return -estimated_ip;
}

size_t IVFTurboQuantIndex::GetMemoryUsage() const {
  std::shared_lock lock(mutex_);
  size_t usage = sizeof(*this);
  // Coarse quantizer: nlist * dim * 4 bytes
  usage += nlist_ * dimension_ * sizeof(float);
  // Per-cluster data
  for (const auto& cluster : clusters_) {
    usage += cluster.codes.capacity();
    usage += cluster.norms.capacity() * sizeof(float);
    usage += cluster.residual_norms.capacity() * sizeof(float);
    usage += cluster.qjl_bits.capacity();
    usage += cluster.ids.capacity() * sizeof(core::VectorId);
  }
  return usage;
}

size_t IVFTurboQuantIndex::GetVectorCount() const {
  std::shared_lock lock(mutex_);
  return total_vectors_;
}

}  // namespace turboquant
}  // namespace index
}  // namespace gvdb
