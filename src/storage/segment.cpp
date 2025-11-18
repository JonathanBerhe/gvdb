#include "storage/segment.h"

#include <algorithm>
#include <fstream>
#include <sstream>

#include "absl/strings/str_cat.h"
#include "core/filter.h"
#include "utils/logger.h"

namespace gvdb {
namespace storage {

Segment::Segment(core::SegmentId id, core::CollectionId collection_id,
                 core::Dimension dimension, core::MetricType metric)
    : id_(id),
      collection_id_(collection_id),
      dimension_(dimension),
      metric_(metric),
      state_(core::SegmentState::GROWING),
      memory_usage_(0) {}

// ========== Data Operations ==========

core::Status Segment::AddVectors(const std::vector<core::Vector>& vectors,
                                  const std::vector<core::VectorId>& ids) {
  std::unique_lock lock(mutex_);

  // Validate state
  if (state_ != core::SegmentState::GROWING) {
    return core::FailedPreconditionError(
        absl::StrCat("Cannot add vectors to segment in state: ",
                     static_cast<int>(state_)));
  }

  // Validate inputs
  auto status = ValidateVectors(vectors, ids);
  if (!status.ok()) {
    return status;
  }

  // Check if segment would exceed size limit
  size_t additional_size = 0;
  for (const auto& vec : vectors) {
    additional_size += vec.byte_size();
  }

  if (memory_usage_ + additional_size > kMaxSegmentSize) {
    return core::ResourceExhaustedError(
        absl::StrCat("Segment ", core::ToUInt32(id_),
                     " would exceed max size ", kMaxSegmentSize));
  }

  // Add vectors
  vectors_.insert(vectors_.end(), vectors.begin(), vectors.end());
  vector_ids_.insert(vector_ids_.end(), ids.begin(), ids.end());
  memory_usage_ += additional_size;

  return core::OkStatus();
}

core::StatusOr<std::vector<core::Vector>> Segment::ReadVectors(
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
      return core::NotFoundError(
          absl::StrCat("Vector ID ", core::ToUInt64(query_id),
                       " not found in segment ", core::ToUInt32(id_)));
    }
  }

  return result;
}

Segment::GetVectorsResult Segment::GetVectors(
    const std::vector<core::VectorId>& ids, bool include_metadata) const {
  std::shared_lock lock(mutex_);

  GetVectorsResult result;
  result.found_ids.reserve(ids.size());
  result.found_vectors.reserve(ids.size());
  if (include_metadata) {
    result.found_metadata.reserve(ids.size());
  }

  for (const auto& query_id : ids) {
    // Find vector with matching ID
    bool found = false;
    for (size_t i = 0; i < vector_ids_.size(); ++i) {
      if (vector_ids_[i] == query_id) {
        result.found_ids.push_back(query_id);
        result.found_vectors.push_back(vectors_[i]);

        if (include_metadata) {
          // Try to get metadata for this vector
          auto it = metadata_map_.find(core::ToUInt64(query_id));
          if (it != metadata_map_.end()) {
            result.found_metadata.push_back(it->second);
          } else {
            // No metadata for this vector, push empty metadata
            result.found_metadata.push_back(core::Metadata{});
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

core::StatusOr<Segment::DeleteVectorsResult> Segment::DeleteVectors(
    const std::vector<core::VectorId>& ids) {
  std::unique_lock lock(mutex_);

  // Validate state - can only delete from GROWING segments
  if (state_ != core::SegmentState::GROWING) {
    return core::FailedPreconditionError(
        absl::StrCat("Cannot delete vectors from segment in state: ",
                     static_cast<int>(state_)));
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
  std::sort(indices_to_delete.begin(), indices_to_delete.end(),
            std::greater<size_t>());

  // Delete vectors, IDs, and metadata
  for (size_t idx : indices_to_delete) {
    // Update memory usage
    memory_usage_ -= vectors_[idx].byte_size();

    // Remove metadata if present
    uint64_t id_uint = core::ToUInt64(vector_ids_[idx]);
    metadata_map_.erase(id_uint);

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

core::Status Segment::UpdateMetadata(
    core::VectorId id, const core::Metadata& metadata, bool merge) {
  std::unique_lock lock(mutex_);

  // Validate state - can only update metadata in GROWING segments
  if (state_ != core::SegmentState::GROWING) {
    return core::FailedPreconditionError(
        absl::StrCat("Cannot update metadata in segment state: ",
                     static_cast<int>(state_)));
  }

  // Find vector with matching ID
  bool found = false;
  for (size_t i = 0; i < vector_ids_.size(); ++i) {
    if (vector_ids_[i] == id) {
      found = true;
      break;
    }
  }

  if (!found) {
    return core::NotFoundError(
        absl::StrCat("Vector ID ", core::ToUInt64(id),
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

  return core::OkStatus();
}

core::StatusOr<core::SearchResult> Segment::Search(const core::Vector& query,
                                                     int k) const {
  std::shared_lock lock(mutex_);

  // For GROWING segments, do brute-force search (real-time search capability)
  if (state_ == core::SegmentState::GROWING) {
    if (vectors_.empty()) {
      return core::SearchResult{};  // Empty result for empty segment
    }

    // Brute-force search: compute distance to all vectors
    std::vector<core::SearchResultEntry> results;
    results.reserve(vectors_.size());

    for (size_t i = 0; i < vectors_.size(); ++i) {
      float distance;
      switch (metric_) {
        case core::MetricType::L2:
          distance = query.L2Distance(vectors_[i]);
          break;
        case core::MetricType::INNER_PRODUCT:
          distance = query.InnerProduct(vectors_[i]);
          break;
        case core::MetricType::COSINE:
          distance = query.CosineDistance(vectors_[i]);
          break;
        default:
          distance = query.L2Distance(vectors_[i]);
      }
      results.push_back({vector_ids_[i], distance});
    }

    // Sort by distance and take top k
    int actual_k = std::min(k, static_cast<int>(results.size()));
    std::partial_sort(results.begin(),
                      results.begin() + actual_k,
                      results.end(),
                      [](const core::SearchResultEntry& a,
                         const core::SearchResultEntry& b) {
                        return a.distance < b.distance;
                      });

    results.resize(actual_k);

    core::SearchResult result;
    result.entries = std::move(results);
    return result;
  }

  // For SEALED/FLUSHED segments, use the index
  if (state_ != core::SegmentState::SEALED &&
      state_ != core::SegmentState::FLUSHED) {
    return core::FailedPreconditionError(
        absl::StrCat("Cannot search in segment state: ",
                     static_cast<int>(state_)));
  }

  if (!index_) {
    return core::FailedPreconditionError(
        absl::StrCat("Segment ", core::ToUInt32(id_), " has no index"));
  }

  // Perform search using the index
  return index_->Search(query, k);
}

core::Status Segment::AddVectorsWithMetadata(
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids,
    const std::vector<core::Metadata>& metadata) {
  std::unique_lock lock(mutex_);

  // Validate state
  if (state_ != core::SegmentState::GROWING) {
    return core::FailedPreconditionError(
        absl::StrCat("Cannot add vectors to segment in state: ",
                     static_cast<int>(state_)));
  }

  // Validate inputs
  auto status = ValidateVectors(vectors, ids);
  if (!status.ok()) {
    return status;
  }

  if (metadata.size() != vectors.size()) {
    return core::InvalidArgumentError(
        "Metadata size must match vector count");
  }

  // Validate all metadata
  for (size_t i = 0; i < metadata.size(); ++i) {
    auto validation = core::validate_metadata(metadata[i]);
    if (!validation.ok()) {
      return core::InvalidArgumentError(
          absl::StrCat("Invalid metadata at index ", i, ": ",
                       validation.message()));
    }
  }

  // Check if segment would exceed size limit
  size_t additional_size = 0;
  for (const auto& vec : vectors) {
    additional_size += vec.byte_size();
  }
  // Rough estimate for metadata size (conservative)
  additional_size += metadata.size() * 1024;  // ~1KB per metadata

  if (memory_usage_ + additional_size > kMaxSegmentSize) {
    return core::ResourceExhaustedError(
        absl::StrCat("Segment ", core::ToUInt32(id_),
                     " would exceed max size ", kMaxSegmentSize));
  }

  // Add vectors and metadata
  vectors_.insert(vectors_.end(), vectors.begin(), vectors.end());
  vector_ids_.insert(vector_ids_.end(), ids.begin(), ids.end());

  for (size_t i = 0; i < ids.size(); ++i) {
    metadata_map_[core::ToUInt64(ids[i])] = metadata[i];
  }

  memory_usage_ += additional_size;

  return core::OkStatus();
}

core::StatusOr<core::SearchResult> Segment::SearchWithFilter(
    const core::Vector& query, int k, const std::string& filter_expr) const {
  std::shared_lock lock(mutex_);

  // Parse filter
  auto filter = core::FilterParser::parse(filter_expr);
  if (!filter.ok()) {
    return filter.status();
  }

  // For GROWING segments with brute-force search
  if (state_ == core::SegmentState::GROWING) {
    if (vectors_.empty()) {
      return core::SearchResult{};
    }

    // Compute distances for vectors that match the filter
    std::vector<core::SearchResultEntry> results;
    results.reserve(vectors_.size());

    for (size_t i = 0; i < vectors_.size(); ++i) {
      // Check if this vector has metadata and matches filter
      auto it = metadata_map_.find(core::ToUInt64(vector_ids_[i]));
      if (it != metadata_map_.end()) {
        if (!(*filter)->evaluate(it->second)) {
          continue;  // Skip vectors that don't match filter
        }
      } else {
        continue;  // Skip vectors without metadata
      }

      float distance;
      if (metric_ == core::MetricType::L2) {
        distance = query.L2Distance(vectors_[i]);
      } else if (metric_ == core::MetricType::INNER_PRODUCT) {
        distance = -query.InnerProduct(vectors_[i]);
      } else {
        distance = query.CosineDistance(vectors_[i]);
      }

      results.emplace_back(vector_ids_[i], distance);
    }

    // Sort by distance
    std::partial_sort(results.begin(),
                      results.begin() + std::min(k, (int)results.size()),
                      results.end(),
                      [](const auto& a, const auto& b) {
                        return a.distance < b.distance;
                      });

    // Return top-k
    core::SearchResult result(k);
    for (int i = 0; i < std::min(k, (int)results.size()); ++i) {
      result.AddEntry(results[i].id, results[i].distance);
    }
    return result;
  }

  // For SEALED segments with index
  if (state_ != core::SegmentState::SEALED &&
      state_ != core::SegmentState::FLUSHED) {
    return core::FailedPreconditionError(
        absl::StrCat("Cannot search in segment state: ",
                     static_cast<int>(state_)));
  }

  if (!index_) {
    return core::FailedPreconditionError(
        absl::StrCat("Segment ", core::ToUInt32(id_), " has no index"));
  }

  // Search with over-fetching and post-filtering
  // Fetch more results than needed to account for filtering
  int fetch_k = std::min(k * 10, static_cast<int>(GetVectorCount()));
  auto search_result = index_->Search(query, fetch_k);
  if (!search_result.ok()) {
    return search_result.status();
  }

  // Filter results
  core::SearchResult filtered_result(k);
  for (const auto& entry : search_result->entries) {
    if (filtered_result.Size() >= static_cast<size_t>(k)) {
      break;
    }

    // Check metadata
    auto it = metadata_map_.find(core::ToUInt64(entry.id));
    if (it != metadata_map_.end()) {
      if ((*filter)->evaluate(it->second)) {
        filtered_result.AddEntry(entry.id, entry.distance);
      }
    }
  }

  return filtered_result;
}

core::StatusOr<core::Metadata> Segment::GetMetadata(core::VectorId id) const {
  std::shared_lock lock(mutex_);

  auto it = metadata_map_.find(core::ToUInt64(id));
  if (it == metadata_map_.end()) {
    return core::NotFoundError(
        absl::StrCat("Metadata for vector ", core::ToUInt64(id),
                     " not found in segment ", core::ToUInt32(id_)));
  }

  return it->second;
}

// ========== State Management ==========

core::Status Segment::Seal(core::IVectorIndex* index) {
  std::unique_lock lock(mutex_);

  // Validate state transition
  if (state_ != core::SegmentState::GROWING) {
    return core::FailedPreconditionError(
        absl::StrCat("Cannot seal segment in state: ",
                     static_cast<int>(state_)));
  }

  if (vectors_.empty()) {
    return core::FailedPreconditionError(
        absl::StrCat("Cannot seal empty segment ", core::ToUInt32(id_)));
  }

  if (index == nullptr) {
    return core::InvalidArgumentError("Index pointer is null");
  }

  // Build the index
  auto build_status = index->Build(vectors_, vector_ids_);
  if (!build_status.ok()) {
    return build_status;
  }

  // Take ownership of the index
  index_.reset(index);

  // COMPACTION STRATEGY: Keep vectors in memory for fast compaction
  // Memory overhead: 2x (vectors + index both in RAM)
  // Trade-off: Optimized for 4GB pods with fast compaction requirements
  //
  // NOTE: For disk-based compaction (Option 2), uncomment these lines:
  // vectors_.clear();
  // vectors_.shrink_to_fit();
  // vector_ids_.clear();
  // vector_ids_.shrink_to_fit();
  //
  // See STORAGE_COMPACTION_OPTIONS.md for detailed analysis of:
  // - Option 1 (current): In-memory vectors (fast compaction, 2x RAM)
  // - Option 2 (future): Disk-based vectors (slower compaction, 1x RAM)

  // Update state
  state_ = core::SegmentState::SEALED;

  return core::OkStatus();
}

core::Status Segment::Flush(const std::string& base_path) {
  std::shared_lock lock(mutex_);

  // Validate state
  if (state_ != core::SegmentState::SEALED) {
    return core::FailedPreconditionError(
        absl::StrCat("Cannot flush segment in state: ",
                     static_cast<int>(state_)));
  }

  if (!index_) {
    return core::FailedPreconditionError(
        absl::StrCat("Segment ", core::ToUInt32(id_), " has no index"));
  }

  // Create segment directory
  std::string segment_path =
      absl::StrCat(base_path, "/segment_", core::ToUInt32(id_));

  // Serialize index to disk
  std::string index_path = absl::StrCat(segment_path, "/index.faiss");
  auto serialize_status = index_->Serialize(index_path);
  if (!serialize_status.ok()) {
    return serialize_status;
  }

  // Write metadata
  std::string metadata_path = absl::StrCat(segment_path, "/metadata.txt");
  std::ofstream metadata_file(metadata_path);
  if (!metadata_file.is_open()) {
    return core::InternalError(
        absl::StrCat("Failed to open metadata file: ", metadata_path));
  }

  metadata_file << "segment_id=" << core::ToUInt32(id_) << "\n";
  metadata_file << "collection_id=" << core::ToUInt32(collection_id_) << "\n";
  metadata_file << "dimension=" << dimension_ << "\n";
  metadata_file << "metric=" << static_cast<int>(metric_) << "\n";
  metadata_file << "vector_count=" << index_->GetVectorCount() << "\n";
  metadata_file << "index_type=" << static_cast<int>(index_->GetIndexType())
                << "\n";

  metadata_file.close();

  return core::OkStatus();
}

core::StatusOr<std::unique_ptr<Segment>> Segment::Load(
    const std::string& base_path, core::SegmentId id) {
  // Read metadata
  std::string segment_path =
      absl::StrCat(base_path, "/segment_", core::ToUInt32(id));
  std::string metadata_path = absl::StrCat(segment_path, "/metadata.txt");

  std::ifstream metadata_file(metadata_path);
  if (!metadata_file.is_open()) {
    return core::NotFoundError(
        absl::StrCat("Metadata file not found: ", metadata_path));
  }

  // Parse metadata
  uint32_t collection_id_raw = 0;
  core::Dimension dimension = 0;
  int metric_raw = 0;

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
    }
  }

  metadata_file.close();

  // Create segment
  auto segment = std::make_unique<Segment>(
      id, core::MakeCollectionId(collection_id_raw), dimension,
      static_cast<core::MetricType>(metric_raw));

  // Note: Index deserialization would happen here, but we need the index
  // instance This will be handled by SegmentManager which has access to
  // IndexFactory

  segment->state_ = core::SegmentState::FLUSHED;

  return segment;
}

// ========== Serialization for Replication ==========

core::StatusOr<std::string> Segment::SerializeToBytes() const {
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
    ss.write(reinterpret_cast<const char*>(vec.data()),
             vec.size() * sizeof(float));
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

  utils::Logger::Instance().Info("Serialized segment {} ({} vectors, {} bytes)",
                                  core::ToUInt32(id_), vector_count, ss.str().size());

  return ss.str();
}

core::StatusOr<std::unique_ptr<Segment>> Segment::DeserializeFromBytes(
    const std::string& bytes_data) {
  // Check minimum size for header
  const size_t kMinHeaderSize = sizeof(uint32_t) * 3 + sizeof(int32_t) * 2 + sizeof(uint64_t);
  if (bytes_data.size() < kMinHeaderSize) {
    return core::InvalidArgumentError(
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
    return core::InvalidArgumentError("Failed to read segment header");
  }

  // Validate dimension
  if (dim == 0 || dim > 100000) {
    return core::InvalidArgumentError(
        absl::StrCat("Invalid dimension: ", dim));
  }

  // Create segment
  auto segment = std::make_unique<Segment>(
      core::MakeSegmentId(seg_id),
      core::MakeCollectionId(coll_id),
      dim,
      static_cast<core::MetricType>(metric));

  segment->state_ = static_cast<core::SegmentState>(state);

  // Read vector count
  uint64_t vector_count;
  ss.read(reinterpret_cast<char*>(&vector_count), sizeof(vector_count));

  if (ss.fail()) {
    return core::InvalidArgumentError("Failed to read vector count");
  }

  // Validate vector count (max 100M vectors)
  if (vector_count > 100000000) {
    return core::InvalidArgumentError(
        absl::StrCat("Invalid vector count: ", vector_count));
  }

  // Read vector IDs
  std::vector<core::VectorId> ids;
  ids.reserve(vector_count);
  for (uint64_t i = 0; i < vector_count; ++i) {
    uint64_t id_val;
    ss.read(reinterpret_cast<char*>(&id_val), sizeof(id_val));
    if (ss.fail()) {
      return core::InvalidArgumentError(
          absl::StrCat("Failed to read vector ID at index ", i));
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
      return core::InvalidArgumentError(
          absl::StrCat("Failed to read vector data at index ", i));
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
    return core::InvalidArgumentError("Failed to read metadata count");
  }

  // Validate metadata count (max 100M entries)
  if (metadata_count > 100000000) {
    return core::InvalidArgumentError(
        absl::StrCat("Invalid metadata count: ", metadata_count));
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
    return core::InvalidArgumentError(
        absl::StrCat("Insufficient data for metadata: need at least ",
                     min_bytes_needed, " bytes, have ", bytes_remaining));
  }

  // Read metadata entries
  for (uint64_t i = 0; i < metadata_count; ++i) {
    uint64_t vid_raw;
    ss.read(reinterpret_cast<char*>(&vid_raw), sizeof(vid_raw));
    if (ss.fail()) {
      return core::InvalidArgumentError(
          absl::StrCat("Failed to read metadata vector ID at index ", i));
    }

    // Deserialize metadata using MetadataSerializer
    auto metadata_result = core::MetadataSerializer::Deserialize(ss);
    if (!metadata_result.ok()) {
      return metadata_result.status();
    }

    segment->metadata_map_[vid_raw] = std::move(metadata_result.value());
  }

  utils::Logger::Instance().Info("Deserialized segment {} ({} vectors, {} bytes)",
                                  seg_id, vector_count, bytes_data.size());

  return segment;
}

std::vector<core::VectorId> Segment::GetAllVectorIds() const {
  std::shared_lock lock(mutex_);
  return vector_ids_;
}

// ========== Accessors ==========

core::SegmentState Segment::GetState() const {
  std::shared_lock lock(mutex_);
  return state_;
}

size_t Segment::GetVectorCount() const {
  std::shared_lock lock(mutex_);

  if (state_ == core::SegmentState::GROWING) {
    return vectors_.size();
  } else if (index_) {
    return index_->GetVectorCount();
  }
  return 0;
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

// ========== Private Methods ==========

bool Segment::IsFull() const {
  // Must be called with lock held
  return memory_usage_ >= kMaxSegmentSize;
}

core::Status Segment::ValidateVectors(
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) const {
  // Must be called with lock held

  if (vectors.empty()) {
    return core::InvalidArgumentError("Empty vectors array");
  }

  if (vectors.size() != ids.size()) {
    return core::InvalidArgumentError(
        absl::StrCat("Vector and ID array size mismatch: ", vectors.size(),
                     " vs ", ids.size()));
  }

  // Validate dimension
  for (const auto& vec : vectors) {
    if (vec.dimension() != dimension_) {
      return core::InvalidArgumentError(
          absl::StrCat("Vector dimension mismatch: expected ", dimension_,
                       " got ", vec.dimension()));
    }
  }

  return core::OkStatus();
}

}  // namespace storage
}  // namespace gvdb
