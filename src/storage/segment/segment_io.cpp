// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Segment flush to disk, load from disk, and the byte format for
// replication.

#include <fstream>
#include <sstream>
#include "absl/strings/str_cat.h"
#include "common/logger.h"
#include "core/status.h"
#include "storage/segment/segment.h"

namespace gvdb {
namespace storage {

absl::Status Segment::Flush(const std::string& base_path) {
  std::shared_lock lock(mutex_);

  // Validate state
  if (state_ != core::SegmentState::SEALED) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot flush segment in state: ", static_cast<int>(state_)));
  }

  if (!index_) {
    return absl::FailedPreconditionError(
        absl::StrCat("Segment ", core::ToUInt32(id_), " has no index"));
  }

  // Segment directory (must be created by SegmentManager before calling Flush)
  std::string segment_path = absl::StrCat(base_path, "/segment_", core::ToUInt32(id_));

  // Serialize index to disk
  std::string index_path = absl::StrCat(segment_path, "/index.faiss");
  RETURN_IF_ERROR(index_->Serialize(index_path));

  // Write metadata
  std::string metadata_path = absl::StrCat(segment_path, "/metadata.txt");
  std::ofstream metadata_file(metadata_path);
  if (!metadata_file.is_open()) {
    return absl::InternalError(absl::StrCat("Failed to open metadata file: ", metadata_path));
  }

  metadata_file << "segment_id=" << core::ToUInt32(id_) << "\n";
  metadata_file << "collection_id=" << core::ToUInt32(collection_id_) << "\n";
  metadata_file << "dimension=" << dimension_ << "\n";
  metadata_file << "metric=" << static_cast<int>(metric_) << "\n";
  metadata_file << "vector_count=" << index_->GetVectorCount() << "\n";
  metadata_file << "index_type=" << static_cast<int>(index_->GetIndexType()) << "\n";

  metadata_file.close();

  // Persist vectors, IDs, and per-vector metadata to vectors.bin
  std::string vectors_path = absl::StrCat(segment_path, "/vectors.bin");
  std::ofstream vectors_file(vectors_path, std::ios::binary);
  if (!vectors_file.is_open()) {
    return absl::InternalError(absl::StrCat("Failed to open vectors file: ", vectors_path));
  }

  // Write vector count
  uint64_t vector_count = vectors_.size();
  vectors_file.write(reinterpret_cast<const char*>(&vector_count), sizeof(vector_count));

  // Write vector IDs
  for (const auto& vid : vector_ids_) {
    uint64_t id_val = core::ToUInt64(vid);
    vectors_file.write(reinterpret_cast<const char*>(&id_val), sizeof(id_val));
  }

  // Write vector data
  for (const auto& vec : vectors_) {
    vectors_file.write(reinterpret_cast<const char*>(vec.data()), vec.size() * sizeof(float));
  }

  // Write per-vector metadata
  uint64_t metadata_count = metadata_map_.size();
  vectors_file.write(reinterpret_cast<const char*>(&metadata_count), sizeof(metadata_count));

  for (const auto& [vid_raw, metadata] : metadata_map_) {
    vectors_file.write(reinterpret_cast<const char*>(&vid_raw), sizeof(vid_raw));
    std::stringstream meta_ss;
    core::MetadataSerializer::Serialize(metadata, meta_ss);
    std::string meta_bytes = meta_ss.str();
    uint64_t meta_size = meta_bytes.size();
    vectors_file.write(reinterpret_cast<const char*>(&meta_size), sizeof(meta_size));
    vectors_file.write(meta_bytes.data(), meta_size);
  }

  vectors_file.close();

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<Segment>> Segment::Load(const std::string& base_path,
                                                       core::SegmentId id) {
  // Read metadata
  std::string segment_path = absl::StrCat(base_path, "/segment_", core::ToUInt32(id));
  std::string metadata_path = absl::StrCat(segment_path, "/metadata.txt");

  std::ifstream metadata_file(metadata_path);
  if (!metadata_file.is_open()) {
    return absl::NotFoundError(absl::StrCat("Metadata file not found: ", metadata_path));
  }

  // Parse metadata
  uint32_t collection_id_raw = 0;
  core::Dimension dimension = 0;
  int metric_raw = 0;
  int index_type_raw = 0;

  std::string line;
  while (std::getline(metadata_file, line)) {
    size_t pos = line.find('=');
    if (pos == std::string::npos) continue;

    std::string key = line.substr(0, pos);
    std::string value = line.substr(pos + 1);

    if (key == "collection_id") {
      collection_id_raw = std::stoul(value);
    } else if (key == "dimension") {
      dimension = std::stoul(value);
    } else if (key == "metric") {
      metric_raw = std::stoi(value);
    } else if (key == "index_type") {
      index_type_raw = std::stoi(value);
    }
  }

  metadata_file.close();

  // Create segment
  auto segment = std::make_unique<Segment>(id, core::MakeCollectionId(collection_id_raw), dimension,
                                           static_cast<core::MetricType>(metric_raw));
  segment->index_type_ = static_cast<core::IndexType>(index_type_raw);

  // Load vectors from vectors.bin if it exists
  std::string vectors_path = absl::StrCat(segment_path, "/vectors.bin");
  std::ifstream vectors_file(vectors_path, std::ios::binary);
  if (vectors_file.is_open()) {
    // Read vector count
    uint64_t vector_count = 0;
    vectors_file.read(reinterpret_cast<char*>(&vector_count), sizeof(vector_count));

    // Read vector IDs
    segment->vector_ids_.reserve(vector_count);
    for (uint64_t i = 0; i < vector_count; ++i) {
      uint64_t id_val = 0;
      vectors_file.read(reinterpret_cast<char*>(&id_val), sizeof(id_val));
      segment->vector_ids_.push_back(core::MakeVectorId(id_val));
    }

    // Read vector data
    segment->vectors_.reserve(vector_count);
    for (uint64_t i = 0; i < vector_count; ++i) {
      std::vector<float> data(dimension);
      vectors_file.read(reinterpret_cast<char*>(data.data()), dimension * sizeof(float));
      segment->vectors_.push_back(core::Vector(std::move(data)));
    }

    // Read per-vector metadata
    uint64_t metadata_count = 0;
    vectors_file.read(reinterpret_cast<char*>(&metadata_count), sizeof(metadata_count));

    for (uint64_t i = 0; i < metadata_count; ++i) {
      uint64_t vid_raw = 0;
      vectors_file.read(reinterpret_cast<char*>(&vid_raw), sizeof(vid_raw));

      uint64_t meta_size = 0;
      vectors_file.read(reinterpret_cast<char*>(&meta_size), sizeof(meta_size));

      std::string meta_bytes(meta_size, '\0');
      vectors_file.read(meta_bytes.data(), meta_size);

      std::stringstream meta_ss(meta_bytes);
      auto meta_result = core::MetadataSerializer::Deserialize(meta_ss);
      if (meta_result.ok()) {
        segment->metadata_map_[vid_raw] = std::move(*meta_result);
      }
    }

    vectors_file.close();
  }

  segment->state_ = core::SegmentState::FLUSHED;

  return segment;
}

// ========== Serialization for Replication ==========

absl::StatusOr<std::string> Segment::SerializeToBytes() const {
  std::shared_lock lock(mutex_);

  // Simple binary format for segment data
  // Format: [header][vector_count][vectors_data][ids_data][metadata_data]

  std::stringstream ss;

  // Header: segment_id, collection_id, dimension, metric, state
  uint32_t seg_id = core::ToUInt32(id_);
  uint32_t coll_id = core::ToUInt32(collection_id_);
  uint32_t dim = dimension_;
  int32_t metric = static_cast<int32_t>(metric_);
  int32_t state = static_cast<int32_t>(state_);

  ss.write(reinterpret_cast<const char*>(&seg_id), sizeof(seg_id));
  ss.write(reinterpret_cast<const char*>(&coll_id), sizeof(coll_id));
  ss.write(reinterpret_cast<const char*>(&dim), sizeof(dim));
  ss.write(reinterpret_cast<const char*>(&metric), sizeof(metric));
  ss.write(reinterpret_cast<const char*>(&state), sizeof(state));

  // Vector count
  uint64_t vector_count = vectors_.size();
  ss.write(reinterpret_cast<const char*>(&vector_count), sizeof(vector_count));

  // Vector IDs
  for (const auto& vid : vector_ids_) {
    uint64_t id_val = core::ToUInt64(vid);
    ss.write(reinterpret_cast<const char*>(&id_val), sizeof(id_val));
  }

  // Vector data
  for (const auto& vec : vectors_) {
    ss.write(reinterpret_cast<const char*>(vec.data()), vec.size() * sizeof(float));
  }

  // Metadata: write count then serialize each metadata map
  uint64_t metadata_count = metadata_map_.size();
  ss.write(reinterpret_cast<const char*>(&metadata_count), sizeof(metadata_count));

  for (const auto& [vid_raw, metadata] : metadata_map_) {
    // Write vector ID
    ss.write(reinterpret_cast<const char*>(&vid_raw), sizeof(vid_raw));

    // Serialize metadata using MetadataSerializer
    core::MetadataSerializer::Serialize(metadata, ss);
  }

  // Sparse vectors
  uint64_t sparse_count = sparse_vectors_.size();
  ss.write(reinterpret_cast<const char*>(&sparse_count), sizeof(sparse_count));
  for (const auto& [vid_raw, sparse] : sparse_vectors_) {
    ss.write(reinterpret_cast<const char*>(&vid_raw), sizeof(vid_raw));
    uint32_t nnz = static_cast<uint32_t>(sparse.nnz());
    ss.write(reinterpret_cast<const char*>(&nnz), sizeof(nnz));
    for (const auto& [idx, val] : sparse.entries()) {
      ss.write(reinterpret_cast<const char*>(&idx), sizeof(idx));
      ss.write(reinterpret_cast<const char*>(&val), sizeof(val));
    }
  }

  // Expiry map (TTL)
  uint64_t expiry_count = expiry_map_.size();
  ss.write(reinterpret_cast<const char*>(&expiry_count), sizeof(expiry_count));
  for (const auto& [vid, expiry] : expiry_map_) {
    ss.write(reinterpret_cast<const char*>(&vid), sizeof(vid));
    ss.write(reinterpret_cast<const char*>(&expiry), sizeof(expiry));
  }

  common::Logger::Instance().Info("Serialized segment {} ({} vectors, {} bytes)",
                                  core::ToUInt32(id_), vector_count, ss.str().size());

  return ss.str();
}

absl::StatusOr<std::unique_ptr<Segment>> Segment::DeserializeFromBytes(
    const std::string& bytes_data) {
  // Check minimum size for header
  const size_t kMinHeaderSize = sizeof(uint32_t) * 3 + sizeof(int32_t) * 2 + sizeof(uint64_t);
  if (bytes_data.size() < kMinHeaderSize) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid segment data: too small (", bytes_data.size(), " bytes)"));
  }

  std::stringstream ss(bytes_data);

  // Read header
  uint32_t seg_id, coll_id, dim;
  int32_t metric, state;

  ss.read(reinterpret_cast<char*>(&seg_id), sizeof(seg_id));
  ss.read(reinterpret_cast<char*>(&coll_id), sizeof(coll_id));
  ss.read(reinterpret_cast<char*>(&dim), sizeof(dim));
  ss.read(reinterpret_cast<char*>(&metric), sizeof(metric));
  ss.read(reinterpret_cast<char*>(&state), sizeof(state));

  if (ss.fail()) {
    return absl::InvalidArgumentError("Failed to read segment header");
  }

  // Validate dimension
  if (dim == 0 || dim > 100000) {
    return absl::InvalidArgumentError(absl::StrCat("Invalid dimension: ", dim));
  }

  // Create segment
  auto segment =
      std::make_unique<Segment>(core::MakeSegmentId(seg_id), core::MakeCollectionId(coll_id), dim,
                                static_cast<core::MetricType>(metric));

  segment->state_ = static_cast<core::SegmentState>(state);

  // Read vector count
  uint64_t vector_count;
  ss.read(reinterpret_cast<char*>(&vector_count), sizeof(vector_count));

  if (ss.fail()) {
    return absl::InvalidArgumentError("Failed to read vector count");
  }

  // Validate vector count (max 100M vectors)
  if (vector_count > 100000000) {
    return absl::InvalidArgumentError(absl::StrCat("Invalid vector count: ", vector_count));
  }

  // Read vector IDs
  std::vector<core::VectorId> ids;
  ids.reserve(vector_count);
  for (uint64_t i = 0; i < vector_count; ++i) {
    uint64_t id_val;
    ss.read(reinterpret_cast<char*>(&id_val), sizeof(id_val));
    if (ss.fail()) {
      return absl::InvalidArgumentError(absl::StrCat("Failed to read vector ID at index ", i));
    }
    ids.push_back(core::MakeVectorId(id_val));
  }

  // Read vector data
  std::vector<core::Vector> vectors;
  vectors.reserve(vector_count);
  for (uint64_t i = 0; i < vector_count; ++i) {
    std::vector<float> values(dim);
    ss.read(reinterpret_cast<char*>(values.data()), dim * sizeof(float));
    if (ss.fail()) {
      return absl::InvalidArgumentError(absl::StrCat("Failed to read vector data at index ", i));
    }
    vectors.emplace_back(std::move(values));
  }

  // Set vectors directly (bypass state checks for deserialization)
  segment->vectors_ = std::move(vectors);
  segment->vector_ids_ = std::move(ids);
  segment->memory_usage_ = vector_count * dim * sizeof(float);

  // Read metadata count
  uint64_t metadata_count;
  ss.read(reinterpret_cast<char*>(&metadata_count), sizeof(metadata_count));

  if (ss.fail() || ss.eof()) {
    return absl::InvalidArgumentError("Failed to read metadata count");
  }

  // Validate metadata count (max 100M entries)
  if (metadata_count > 100000000) {
    return absl::InvalidArgumentError(absl::StrCat("Invalid metadata count: ", metadata_count));
  }

  // Check if stream has enough remaining data for metadata entries
  // Each entry needs at minimum: vid (8 bytes) + count (8 bytes) = 16 bytes
  std::streampos current_pos = ss.tellg();
  ss.seekg(0, std::ios::end);
  std::streampos end_pos = ss.tellg();
  ss.seekg(current_pos);  // Restore position

  size_t bytes_remaining = end_pos - current_pos;
  size_t min_bytes_needed = metadata_count * 16;  // Minimum per entry

  if (bytes_remaining < min_bytes_needed) {
    return absl::InvalidArgumentError(absl::StrCat("Insufficient data for metadata: need at least ",
                                                   min_bytes_needed, " bytes, have ",
                                                   bytes_remaining));
  }

  // Read metadata entries
  for (uint64_t i = 0; i < metadata_count; ++i) {
    uint64_t vid_raw;
    ss.read(reinterpret_cast<char*>(&vid_raw), sizeof(vid_raw));
    if (ss.fail()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Failed to read metadata vector ID at index ", i));
    }

    // Deserialize metadata using MetadataSerializer
    auto metadata_result = core::MetadataSerializer::Deserialize(ss);
    if (!metadata_result.ok()) {
      return metadata_result.status();
    }

    segment->metadata_map_[vid_raw] = std::move(metadata_result.value());
  }

  // Sparse vectors (optional section, old segments may not have it)
  if (!ss.eof() && ss.good()) {
    uint64_t sparse_count = 0;
    ss.read(reinterpret_cast<char*>(&sparse_count), sizeof(sparse_count));
    if (ss.good()) {
      for (uint64_t i = 0; i < sparse_count && ss.good(); ++i) {
        uint64_t vid_raw = 0;
        ss.read(reinterpret_cast<char*>(&vid_raw), sizeof(vid_raw));
        uint32_t nnz = 0;
        ss.read(reinterpret_cast<char*>(&nnz), sizeof(nnz));

        std::vector<core::SparseVector::Entry> entries;
        entries.reserve(nnz);
        for (uint32_t j = 0; j < nnz && ss.good(); ++j) {
          uint32_t idx = 0;
          float val = 0.0f;
          ss.read(reinterpret_cast<char*>(&idx), sizeof(idx));
          ss.read(reinterpret_cast<char*>(&val), sizeof(val));
          entries.emplace_back(idx, val);
        }
        segment->sparse_vectors_[vid_raw] = core::SparseVector(std::move(entries));
      }
    }
  }

  // Expiry map (optional, old segments may not have it)
  if (!ss.eof() && ss.good()) {
    uint64_t expiry_count = 0;
    ss.read(reinterpret_cast<char*>(&expiry_count), sizeof(expiry_count));
    if (ss.good()) {
      for (uint64_t i = 0; i < expiry_count && ss.good(); ++i) {
        uint64_t vid = 0;
        int64_t expiry = 0;
        ss.read(reinterpret_cast<char*>(&vid), sizeof(vid));
        ss.read(reinterpret_cast<char*>(&expiry), sizeof(expiry));
        segment->expiry_map_[vid] = expiry;
      }
    }
  }

  common::Logger::Instance().Info("Deserialized segment {} ({} vectors, {} bytes)", seg_id,
                                  vector_count, bytes_data.size());

  return segment;
}

}  // namespace storage
}  // namespace gvdb
