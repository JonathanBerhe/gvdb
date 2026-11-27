// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Segment: construction, writes, deletes, metadata, TTL, upsert, sealing
// and accessors. segment_search.cpp holds the searches and
// segment_io.cpp the disk and wire formats.

#include "storage/segment/segment.h"
#include <algorithm>
#include <chrono>
#include <mutex>
#include "absl/strings/str_cat.h"
#include "common/logger.h"
#include "core/status.h"

namespace gvdb::storage {

Segment::Segment(core::SegmentId id, core::CollectionId collection_id, core::Dimension dimension,
                 core::MetricType metric, size_t max_segment_size)
    : id_(id),
      collection_id_(collection_id),
      dimension_(dimension),
      metric_(metric),

      max_segment_size_(max_segment_size) {}

// ========== Data Operations ==========

int64_t Segment::NowEpochSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

absl::Status Segment::AddVectors(const std::vector<core::Vector>& vectors,
                                 const std::vector<core::VectorId>& ids,
                                 const std::unordered_map<uint64_t, int64_t>& expiry_entries) {
  std::unique_lock lock(mutex_);

  // Validate state
  if (state_ != core::SegmentState::GROWING) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot add vectors to segment in state: ", static_cast<int>(state_)));
  }

  // Validate inputs
  RETURN_IF_ERROR(ValidateVectors(vectors, ids));

  // Check if segment would exceed size limit
  size_t additional_size = 0;
  for (const auto& vec : vectors) {
    additional_size += vec.byte_size();
  }

  if (memory_usage_ + additional_size > max_segment_size_) {
    return absl::ResourceExhaustedError(absl::StrCat("Segment ", core::ToUInt32(id_),
                                                     " would exceed max size ", max_segment_size_));
  }

  // Add vectors
  vectors_.insert(vectors_.end(), vectors.begin(), vectors.end());
  vector_ids_.insert(vector_ids_.end(), ids.begin(), ids.end());
  memory_usage_ += additional_size;

  // Apply TTL expiry entries atomically with the insert
  for (const auto& [vid, expiry] : expiry_entries) {
    expiry_map_[vid] = expiry;
  }

  return absl::OkStatus();
}

absl::StatusOr<std::vector<core::Vector>> Segment::ReadVectors(
    const std::vector<core::VectorId>& ids) const {
  std::shared_lock lock(mutex_);

  std::vector<core::Vector> result;
  result.reserve(ids.size());

  for (const auto& query_id : ids) {
    // Find vector with matching ID
    bool found = false;
    for (size_t i = 0; i < vector_ids_.size(); ++i) {
      if (vector_ids_[i] == query_id) {
        result.push_back(vectors_[i]);
        found = true;
        break;
      }
    }

    if (!found) {
      return absl::NotFoundError(absl::StrCat("Vector ID ", core::ToUInt64(query_id),
                                              " not found in segment ", core::ToUInt32(id_)));
    }
  }

  return result;
}

Segment::GetVectorsResult Segment::GetVectors(const std::vector<core::VectorId>& ids,
                                              bool include_metadata) const {
  std::shared_lock lock(mutex_);

  GetVectorsResult result;
  result.found_ids.reserve(ids.size());
  result.found_vectors.reserve(ids.size());
  if (include_metadata) {
    result.found_metadata.reserve(ids.size());
  }

  int64_t now = NowEpochSeconds();

  for (const auto& query_id : ids) {
    // Find vector with matching ID
    bool found = false;
    for (size_t i = 0; i < vector_ids_.size(); ++i) {
      if (vector_ids_[i] == query_id) {
        if (IsExpired(core::ToUInt64(query_id), now)) {
          result.not_found_ids.push_back(query_id);
          found = true;
          break;
        }
        result.found_ids.push_back(query_id);
        result.found_vectors.push_back(vectors_[i]);

        if (include_metadata) {
          // Try to get metadata for this vector
          auto it = metadata_map_.find(core::ToUInt64(query_id));
          if (it != metadata_map_.end()) {
            result.found_metadata.push_back(it->second);
          } else {
            // No metadata for this vector, push empty metadata
            result.found_metadata.emplace_back();
          }
        }

        found = true;
        break;
      }
    }

    if (!found) {
      result.not_found_ids.push_back(query_id);
    }
  }

  return result;
}

absl::StatusOr<Segment::DeleteVectorsResult> Segment::DeleteVectorsUnlocked(
    const std::vector<core::VectorId>& ids) {
  // Caller must hold unique_lock on mutex_

  // Validate state - can only delete from GROWING segments
  if (state_ != core::SegmentState::GROWING) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot delete vectors from segment in state: ", static_cast<int>(state_)));
  }

  DeleteVectorsResult result;
  result.deleted_count = 0;

  // Create a set of indices to delete for efficient removal
  std::vector<size_t> indices_to_delete;
  indices_to_delete.reserve(ids.size());

  for (const auto& query_id : ids) {
    // Find vector with matching ID
    bool found = false;
    for (size_t i = 0; i < vector_ids_.size(); ++i) {
      if (vector_ids_[i] == query_id) {
        indices_to_delete.push_back(i);
        found = true;
        break;
      }
    }

    if (!found) {
      result.not_found_ids.push_back(query_id);
    }
  }

  // Sort indices in descending order to delete from back to front
  // This prevents index invalidation during deletion
  std::ranges::sort(indices_to_delete, std::greater<>());

  // Delete vectors, IDs, and metadata
  for (size_t idx : indices_to_delete) {
    // Update memory usage
    memory_usage_ -= vectors_[idx].byte_size();

    // Remove metadata and scalar index entries
    uint64_t id_uint = core::ToUInt64(vector_ids_[idx]);
    auto md_it = metadata_map_.find(id_uint);
    if (md_it != metadata_map_.end()) {
      scalar_indexes_.RemoveVector(id_uint, md_it->second);
      metadata_map_.erase(md_it);
    }
    sparse_vectors_.erase(id_uint);
    expiry_map_.erase(id_uint);

    // Remove vector and ID using swap-and-pop for efficiency
    if (idx != vectors_.size() - 1) {
      std::swap(vectors_[idx], vectors_.back());
      std::swap(vector_ids_[idx], vector_ids_.back());
    }
    vectors_.pop_back();
    vector_ids_.pop_back();

    result.deleted_count++;
  }

  return result;
}

absl::StatusOr<Segment::DeleteVectorsResult> Segment::DeleteVectors(
    const std::vector<core::VectorId>& ids) {
  std::unique_lock lock(mutex_);
  return DeleteVectorsUnlocked(ids);
}

absl::Status Segment::UpdateMetadata(core::VectorId id, const core::Metadata& metadata,
                                     bool merge) {
  std::unique_lock lock(mutex_);

  // Validate state - can only update metadata in GROWING segments
  if (state_ != core::SegmentState::GROWING) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot update metadata in segment state: ", static_cast<int>(state_)));
  }

  // Find vector with matching ID
  bool found = false;
  for (auto& vector_id : vector_ids_) {
    if (vector_id == id) {
      found = true;
      break;
    }
  }

  if (!found) {
    return absl::NotFoundError(absl::StrCat("Vector ID ", core::ToUInt64(id),
                                            " not found in segment ", core::ToUInt32(id_)));
  }

  uint64_t id_uint = core::ToUInt64(id);

  if (merge) {
    // Merge with existing metadata
    auto it = metadata_map_.find(id_uint);
    if (it != metadata_map_.end()) {
      // Merge: update existing fields and add new ones
      for (const auto& [key, value] : metadata) {
        it->second[key] = value;
      }
    } else {
      // No existing metadata, just insert new
      metadata_map_[id_uint] = metadata;
    }
  } else {
    // Replace existing metadata completely
    metadata_map_[id_uint] = metadata;
  }

  return absl::OkStatus();
}

absl::Status Segment::AddVectorsWithMetadata(
    const std::vector<core::Vector>& vectors, const std::vector<core::VectorId>& ids,
    const std::vector<core::Metadata>& metadata,
    const std::unordered_map<uint64_t, int64_t>& expiry_entries) {
  std::unique_lock lock(mutex_);

  // Validate state
  if (state_ != core::SegmentState::GROWING) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot add vectors to segment in state: ", static_cast<int>(state_)));
  }

  // Validate inputs
  RETURN_IF_ERROR(ValidateVectors(vectors, ids));

  if (metadata.size() != vectors.size()) {
    return absl::InvalidArgumentError("Metadata size must match vector count");
  }

  // Validate all metadata
  for (size_t i = 0; i < metadata.size(); ++i) {
    auto validation = core::ValidateMetadata(metadata[i]);
    if (!validation.ok()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid metadata at index ", i, ": ", validation.message()));
    }
  }

  // Check if segment would exceed size limit
  size_t additional_size = 0;
  for (const auto& vec : vectors) {
    additional_size += vec.byte_size();
  }
  // Rough estimate for metadata size (conservative)
  additional_size += metadata.size() * kMetadataBytesEstimate;

  if (memory_usage_ + additional_size > max_segment_size_) {
    return absl::ResourceExhaustedError(absl::StrCat("Segment ", core::ToUInt32(id_),
                                                     " would exceed max size ", max_segment_size_));
  }

  // Add vectors and metadata
  vectors_.insert(vectors_.end(), vectors.begin(), vectors.end());
  vector_ids_.insert(vector_ids_.end(), ids.begin(), ids.end());

  for (size_t i = 0; i < ids.size(); ++i) {
    uint64_t id_uint = core::ToUInt64(ids[i]);
    metadata_map_[id_uint] = metadata[i];
    scalar_indexes_.IndexVector(id_uint, metadata[i]);
  }

  memory_usage_ += additional_size;

  // Apply TTL expiry entries atomically with the insert
  for (const auto& [vid, expiry] : expiry_entries) {
    expiry_map_[vid] = expiry;
  }

  return absl::OkStatus();
}

// ========== Add Vectors with Sparse ==========

absl::Status Segment::AddVectorsWithSparse(
    const std::vector<core::Vector>& vectors, const std::vector<core::VectorId>& ids,
    const std::vector<core::Metadata>& metadata,
    const std::unordered_map<uint64_t, core::SparseVector>& sparse,
    const std::unordered_map<uint64_t, int64_t>& expiry_entries) {
  // Delegate to AddVectorsWithMetadata for dense + metadata + expiry
  RETURN_IF_ERROR(AddVectorsWithMetadata(vectors, ids, metadata, expiry_entries));

  // Store sparse vectors
  std::unique_lock lock(mutex_);
  for (const auto& [vid, sv] : sparse) {
    sparse_vectors_[vid] = sv;
    memory_usage_ += sv.byte_size();
  }

  return absl::OkStatus();
}

// ========== TTL ==========

void Segment::SetExpiry(core::VectorId id, int64_t expiry_timestamp) {
  std::unique_lock lock(mutex_);
  expiry_map_[core::ToUInt64(id)] = expiry_timestamp;
}

size_t Segment::SweepExpired() {
  std::unique_lock lock(mutex_);
  if (state_ != core::SegmentState::GROWING) return 0;

  int64_t now = NowEpochSeconds();

  std::vector<core::VectorId> to_delete;
  for (const auto& [vid, expiry] : expiry_map_) {
    if (now >= expiry) {
      to_delete.push_back(core::MakeVectorId(vid));
    }
  }

  if (to_delete.empty()) return 0;

  auto result = DeleteVectorsUnlocked(to_delete);
  if (!result.ok()) {
    common::Logger::Instance().Error("TTL sweep delete failed for segment {}: {}",
                                     core::ToUInt32(id_), result.status().message());
    return 0;
  }
  return result->deleted_count;
}

// ========== Upsert ==========

absl::StatusOr<Segment::UpsertResult> Segment::UpsertVectors(
    const std::vector<core::Vector>& vectors, const std::vector<core::VectorId>& ids,
    const std::vector<core::Metadata>& metadata,
    const std::unordered_map<uint64_t, int64_t>& expiry_entries) {
  std::unique_lock lock(mutex_);

  if (state_ != core::SegmentState::GROWING) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot upsert in segment state: ", static_cast<int>(state_)));
  }

  RETURN_IF_ERROR(ValidateVectors(vectors, ids));

  if (!metadata.empty() && metadata.size() != vectors.size()) {
    return absl::InvalidArgumentError("Metadata size must match vector count");
  }

  UpsertResult result{0, 0};

  for (size_t i = 0; i < ids.size(); ++i) {
    uint64_t id_uint = core::ToUInt64(ids[i]);
    bool existed = false;

    // Check if vector ID already exists: delete old entry
    for (size_t j = 0; j < vector_ids_.size(); ++j) {
      if (vector_ids_[j] == ids[i]) {
        // Remove old metadata from scalar index
        auto md_it = metadata_map_.find(id_uint);
        if (md_it != metadata_map_.end()) {
          scalar_indexes_.RemoveVector(id_uint, md_it->second);
        }

        memory_usage_ -= vectors_[j].byte_size();
        metadata_map_.erase(id_uint);
        sparse_vectors_.erase(id_uint);
        expiry_map_.erase(id_uint);

        // Swap-and-pop
        if (j != vectors_.size() - 1) {
          std::swap(vectors_[j], vectors_.back());
          std::swap(vector_ids_[j], vector_ids_.back());
        }
        vectors_.pop_back();
        vector_ids_.pop_back();
        existed = true;
        break;
      }
    }

    // Insert new vector
    vectors_.push_back(vectors[i]);
    vector_ids_.push_back(ids[i]);
    memory_usage_ += vectors[i].byte_size();

    if (!metadata.empty()) {
      metadata_map_[id_uint] = metadata[i];
      scalar_indexes_.IndexVector(id_uint, metadata[i]);
    }

    // Apply TTL for this vector if provided
    auto expiry_it = expiry_entries.find(id_uint);
    if (expiry_it != expiry_entries.end()) {
      expiry_map_[id_uint] = expiry_it->second;
    }

    if (existed) {
      result.updated_count++;
    } else {
      result.inserted_count++;
    }
  }

  return result;
}

absl::StatusOr<core::Metadata> Segment::GetMetadata(core::VectorId id) const {
  std::shared_lock lock(mutex_);

  auto it = metadata_map_.find(core::ToUInt64(id));
  if (it == metadata_map_.end()) {
    return absl::NotFoundError(absl::StrCat("Metadata for vector ", core::ToUInt64(id),
                                            " not found in segment ", core::ToUInt32(id_)));
  }

  return it->second;
}

// ========== State Management ==========

absl::Status Segment::Seal(core::IVectorIndex* index) {
  std::unique_lock lock(mutex_);

  // Validate state transition
  if (state_ != core::SegmentState::GROWING) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot seal segment in state: ", static_cast<int>(state_)));
  }

  if (vectors_.empty()) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot seal empty segment ", core::ToUInt32(id_)));
  }

  if (index == nullptr) {
    return absl::InvalidArgumentError("Index pointer is null");
  }

  // Build the index
  RETURN_IF_ERROR(index->Build(vectors_, vector_ids_));

  // Take ownership of the index
  index_.reset(index);

  // COMPACTION STRATEGY: Keep vectors in memory for fast compaction
  // Memory overhead: 2x (vectors + index both in RAM)
  // Trade-off: Optimized for 4GB pods with fast compaction requirements

  // Build scalar indexes from all metadata (ensures full coverage after seal)
  if (!metadata_map_.empty()) {
    scalar_indexes_.BuildFromMetadata(metadata_map_);
  }

  // Update state
  state_ = core::SegmentState::SEALED;

  return absl::OkStatus();
}

std::vector<core::VectorId> Segment::GetAllVectorIds() const {
  std::shared_lock lock(mutex_);
  return vector_ids_;
}

// ========== Accessors ==========

void Segment::SetCollectionIdForRestore(core::CollectionId collection_id) {
  // Mutates a member that's normally immutable post-construction. Only
  // the restore path uses this: see the header for the precondition.
  std::unique_lock lock(mutex_);
  collection_id_ = collection_id;
}

void Segment::SetSegmentIdForRestore(core::SegmentId segment_id) {
  std::unique_lock lock(mutex_);
  id_ = segment_id;
}

core::SegmentState Segment::GetState() const {
  std::shared_lock lock(mutex_);
  return state_;
}

size_t Segment::GetVectorCount() const {
  std::shared_lock lock(mutex_);

  if (index_) {
    return index_->GetVectorCount();
  }
  return vectors_.size();
}

size_t Segment::GetMemoryUsage() const {
  std::shared_lock lock(mutex_);

  if (state_ == core::SegmentState::GROWING) {
    return memory_usage_;
  } else if (index_) {
    return index_->GetMemoryUsage();
  }
  return 0;
}

bool Segment::CanAcceptWrites() const {
  std::shared_lock lock(mutex_);
  return state_ == core::SegmentState::GROWING && !IsFull();
}

bool Segment::CanFit(size_t additional_bytes) const {
  std::shared_lock lock(mutex_);
  return state_ == core::SegmentState::GROWING &&
         memory_usage_ + additional_bytes <= max_segment_size_;
}

bool Segment::HasIndex() const {
  std::shared_lock lock(mutex_);
  return index_ != nullptr;
}

// ========== Private Methods ==========

bool Segment::IsFull() const {
  // Must be called with lock held
  return memory_usage_ >= max_segment_size_;
}

absl::Status Segment::ValidateVectors(const std::vector<core::Vector>& vectors,
                                      const std::vector<core::VectorId>& ids) const {
  // Must be called with lock held

  if (vectors.empty()) {
    return absl::InvalidArgumentError("Empty vectors array");
  }

  if (vectors.size() != ids.size()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Vector and ID array size mismatch: ", vectors.size(), " vs ", ids.size()));
  }

  // Validate dimension
  for (const auto& vec : vectors) {
    if (vec.dimension() != dimension_) {
      return absl::InvalidArgumentError(absl::StrCat("Vector dimension mismatch: expected ",
                                                     dimension_, " got ", vec.dimension()));
    }
  }

  return absl::OkStatus();
}

}  // namespace gvdb::storage
