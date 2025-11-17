#include "network/vectordb_service.h"
#include "network/proto_conversions.h"
#include "utils/logger.h"
#include "utils/timer.h"
#include "utils/metrics.h"
#include <shared_mutex>
#include <unordered_map>

namespace gvdb {
namespace network {

// Internal collection metadata (name → ID mapping)
// TODO: Move to persistent metadata store in future
struct CollectionMetadata {
  core::CollectionId id;
  std::string name;
  uint32_t dimension;
  core::MetricType metric;
  core::SegmentId segment_id;  // Single segment for Phase 1
};

// Thread-safe collection registry
class CollectionRegistry {
 public:
  core::StatusOr<core::CollectionId> GetCollectionId(const std::string& name) const {
    std::shared_lock lock(mutex_);
    auto it = name_to_metadata_.find(name);
    if (it == name_to_metadata_.end()) {
      return absl::NotFoundError("Collection not found: " + name);
    }
    return it->second.id;
  }

  core::StatusOr<CollectionMetadata> GetMetadata(const std::string& name) const {
    std::shared_lock lock(mutex_);
    auto it = name_to_metadata_.find(name);
    if (it == name_to_metadata_.end()) {
      return absl::NotFoundError("Collection not found: " + name);
    }
    return it->second;
  }

  absl::Status AddCollection(const CollectionMetadata& metadata) {
    std::unique_lock lock(mutex_);
    if (name_to_metadata_.count(metadata.name) > 0) {
      return absl::AlreadyExistsError("Collection already exists: " + metadata.name);
    }
    name_to_metadata_[metadata.name] = metadata;
    return absl::OkStatus();
  }

  absl::Status RemoveCollection(const std::string& name) {
    std::unique_lock lock(mutex_);
    if (name_to_metadata_.erase(name) == 0) {
      return absl::NotFoundError("Collection not found: " + name);
    }
    return absl::OkStatus();
  }

  std::vector<CollectionMetadata> ListAll() const {
    std::shared_lock lock(mutex_);
    std::vector<CollectionMetadata> result;
    result.reserve(name_to_metadata_.size());
    for (const auto& [name, metadata] : name_to_metadata_) {
      result.push_back(metadata);
    }
    return result;
  }

  size_t Count() const {
    std::shared_lock lock(mutex_);
    return name_to_metadata_.size();
  }

  void Clear() {
    std::unique_lock lock(mutex_);
    name_to_metadata_.clear();
  }

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, CollectionMetadata> name_to_metadata_;
};

// ============================================================================
// VectorDBService Implementation
// ============================================================================

VectorDBService::VectorDBService(
    std::shared_ptr<storage::SegmentManager> segment_manager,
    std::shared_ptr<compute::QueryExecutor> query_executor)
    : segment_manager_(std::move(segment_manager)),
      query_executor_(std::move(query_executor)),
      collection_registry_(std::make_unique<CollectionRegistry>()),
      next_collection_id_(1) {
  utils::Logger::Instance().Info("VectorDBService initialized");
}

VectorDBService::~VectorDBService() = default;

// ============================================================================
// Collection Management
// ============================================================================

grpc::Status VectorDBService::CreateCollection(
    grpc::ServerContext* context,
    const proto::CreateCollectionRequest* request,
    proto::CreateCollectionResponse* response) {

  utils::Logger::Instance().Info("CreateCollection: {}", request->collection_name());

  // Business logic limits
  constexpr uint32_t MAX_DIMENSION = 8192;  // Max supported dimension
  constexpr uint32_t MIN_DIMENSION = 1;     // Min valid dimension

  // Validate dimension
  if (request->dimension() < MIN_DIMENSION || request->dimension() > MAX_DIMENSION) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        absl::StrCat("Dimension must be between ", MIN_DIMENSION, " and ", MAX_DIMENSION,
                     ". Requested: ", request->dimension()));
  }

  // Validate collection name
  if (request->collection_name().empty()) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        "Collection name cannot be empty");
  }

  // Convert proto types to core types
  auto metric_result = fromProto(request->metric());
  if (!metric_result.ok()) {
    return toGrpcStatus(metric_result.status());
  }

  auto index_type_result = fromProto(request->index_type());
  if (!index_type_result.ok()) {
    return toGrpcStatus(index_type_result.status());
  }

  // Allocate collection ID
  core::CollectionId collection_id = core::CollectionId(next_collection_id_++);

  // Create segment for this collection
  auto segment_result = segment_manager_->CreateSegment(
      collection_id,
      request->dimension(),
      *metric_result);

  if (!segment_result.ok()) {
    return toGrpcStatus(segment_result.status());
  }

  // Register collection metadata
  CollectionMetadata metadata{
      .id = collection_id,
      .name = request->collection_name(),
      .dimension = request->dimension(),
      .metric = *metric_result,
      .segment_id = *segment_result
  };

  auto status = collection_registry_->AddCollection(metadata);
  if (!status.ok()) {
    // Cleanup: drop the segment we just created
    auto drop_status = segment_manager_->DropSegment(*segment_result, false);
    if (!drop_status.ok()) {
      gvdb::utils::Logger::Instance().Warn(
          "Failed to drop segment during cleanup: {}", drop_status.message());
    }
    return toGrpcStatus(status);
  }

  response->set_collection_id(core::ToUInt32(collection_id));
  response->set_message("Collection created successfully");

  // Update collection count gauge
  utils::MetricsRegistry::Instance().SetCollectionCount(
      collection_registry_->Count());

  return grpc::Status::OK;
}

grpc::Status VectorDBService::DropCollection(
    grpc::ServerContext* context,
    const proto::DropCollectionRequest* request,
    proto::DropCollectionResponse* response) {

  utils::Logger::Instance().Info("DropCollection: {}", request->collection_name());

  // Get collection metadata
  auto metadata_result = collection_registry_->GetMetadata(request->collection_name());
  if (!metadata_result.ok()) {
    return toGrpcStatus(metadata_result.status());
  }

  // Drop the segment
  auto status = segment_manager_->DropSegment(metadata_result->segment_id, true);
  if (!status.ok()) {
    return toGrpcStatus(status);
  }

  // Remove from registry
  status = collection_registry_->RemoveCollection(request->collection_name());
  if (!status.ok()) {
    return toGrpcStatus(status);
  }

  // Update collection count gauge
  utils::MetricsRegistry::Instance().SetCollectionCount(
      collection_registry_->Count());

  response->set_message("Collection dropped successfully");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::ListCollections(
    grpc::ServerContext* context,
    const proto::ListCollectionsRequest* request,
    proto::ListCollectionsResponse* response) {

  auto collections = collection_registry_->ListAll();

  for (const auto& metadata : collections) {
    auto* info = response->add_collections();
    info->set_collection_id(core::ToUInt32(metadata.id));
    info->set_collection_name(metadata.name);
    info->set_dimension(metadata.dimension);
    info->set_metric_type(toString(metadata.metric));

    // Get vector count from segment
    auto* segment = segment_manager_->GetSegment(metadata.segment_id);
    if (segment) {
      info->set_vector_count(segment->GetVectorCount());
    } else {
      info->set_vector_count(0);
    }
  }

  return grpc::Status::OK;
}

// ============================================================================
// Vector Operations
// ============================================================================

grpc::Status VectorDBService::Insert(
    grpc::ServerContext* context,
    const proto::InsertRequest* request,
    proto::InsertResponse* response) {

  utils::Logger::Instance().Info("Insert: {} vectors into {}",
                                 request->vectors().size(),
                                 request->collection_name());

  // Record batch size metrics
  utils::MetricsRegistry::Instance().RecordBatchSize(request->vectors().size());

  // Start latency timer (RAII - records on destruction)
  utils::MetricsTimer timer(
      utils::MetricsRegistry::Instance(),
      utils::MetricsTimer::OperationType::INSERT,
      request->collection_name());

  // Business logic limits to prevent abuse
  constexpr size_t MAX_BATCH_SIZE = 50000;  // Max vectors per request
  constexpr uint32_t MAX_DIMENSION = 8192;   // Max supported dimension

  // Validate batch size
  if (request->vectors().size() > MAX_BATCH_SIZE) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        absl::StrCat("Batch size exceeds maximum (", MAX_BATCH_SIZE, " vectors). "
                     "Current batch: ", request->vectors().size(), ". "
                     "Please split into smaller batches."));
  }

  // Validate empty batch
  if (request->vectors().empty()) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        "Cannot insert empty vector batch");
  }

  // Get collection metadata
  auto metadata_result = collection_registry_->GetMetadata(request->collection_name());
  if (!metadata_result.ok()) {
    return toGrpcStatus(metadata_result.status());
  }

  // Convert proto vectors to core vectors (and optionally metadata)
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  std::vector<core::Metadata> metadata_list;
  vectors.reserve(request->vectors().size());
  ids.reserve(request->vectors().size());

  bool has_metadata = false;

  for (const auto& proto_vec : request->vectors()) {
    auto vec_result = fromProto(proto_vec);
    if (!vec_result.ok()) {
      return toGrpcStatus(vec_result.status());
    }

    ids.push_back(vec_result->first);
    vectors.push_back(std::move(vec_result->second));

    // Extract metadata if present
    if (proto_vec.has_metadata()) {
      has_metadata = true;
      auto meta_result = fromProto(proto_vec.metadata());
      if (!meta_result.ok()) {
        return toGrpcStatus(meta_result.status());
      }
      metadata_list.push_back(std::move(*meta_result));
    } else if (has_metadata) {
      // If some vectors have metadata, all must have it (even if empty)
      metadata_list.push_back(core::Metadata{});
    }
  }

  // Validate dimension
  for (const auto& vec : vectors) {
    if (vec.dimension() != metadata_result->dimension) {
      return toGrpcStatus(absl::InvalidArgumentError(
          absl::StrFormat("Vector dimension mismatch: expected %d, got %d",
                          metadata_result->dimension, vec.dimension())));
    }
  }

  // Write to segment (with or without metadata)
  absl::Status status;
  auto* segment = segment_manager_->GetSegment(metadata_result->segment_id);
  if (!segment) {
    return toGrpcStatus(absl::NotFoundError("Segment not found"));
  }

  if (has_metadata) {
    status = segment->AddVectorsWithMetadata(vectors, ids, metadata_list);
  } else {
    status = segment->AddVectors(vectors, ids);
  }

  if (!status.ok()) {
    // Record failed insert
    utils::MetricsRegistry::Instance().RecordInsert(
        request->collection_name(), false, 0);
    return toGrpcStatus(status);
  }

  // Record successful insert
  utils::MetricsRegistry::Instance().RecordInsert(
      request->collection_name(), true, vectors.size());

  // Update vector count gauge
  utils::MetricsRegistry::Instance().SetVectorCount(
      request->collection_name(), segment->GetVectorCount());

  response->set_inserted_count(vectors.size());
  response->set_message("Vectors inserted successfully");

  return grpc::Status::OK;
}

grpc::Status VectorDBService::Search(
    grpc::ServerContext* context,
    const proto::SearchRequest* request,
    proto::SearchResponse* response) {

  utils::Timer timer;

  // Start metrics timer (RAII - records on destruction)
  utils::MetricsTimer metrics_timer(
      utils::MetricsRegistry::Instance(),
      utils::MetricsTimer::OperationType::SEARCH,
      request->collection_name());

  // Get collection ID
  auto collection_id_result = collection_registry_->GetCollectionId(request->collection_name());
  if (!collection_id_result.ok()) {
    // Record failed search
    utils::MetricsRegistry::Instance().RecordSearch(
        request->collection_name(), false);
    return toGrpcStatus(collection_id_result.status());
  }

  // Convert query vector
  auto query_result = fromProto(request->query_vector());
  if (!query_result.ok()) {
    // Record failed search
    utils::MetricsRegistry::Instance().RecordSearch(
        request->collection_name(), false);
    return toGrpcStatus(query_result.status());
  }

  // Get collection metadata to find segment
  auto metadata_result = collection_registry_->GetMetadata(request->collection_name());
  if (!metadata_result.ok()) {
    utils::MetricsRegistry::Instance().RecordSearch(
        request->collection_name(), false);
    return toGrpcStatus(metadata_result.status());
  }

  auto* segment = segment_manager_->GetSegment(metadata_result->segment_id);
  if (!segment) {
    utils::MetricsRegistry::Instance().RecordSearch(
        request->collection_name(), false);
    return toGrpcStatus(absl::NotFoundError("Segment not found"));
  }

  // Execute search (with or without filter)
  core::StatusOr<core::SearchResult> search_result;

  if (!request->filter().empty()) {
    // Search with metadata filtering
    search_result = segment->SearchWithFilter(
        *query_result,
        request->top_k(),
        request->filter());
  } else {
    // Normal search without filtering
    search_result = segment->Search(*query_result, request->top_k());
  }

  if (!search_result.ok()) {
    // Record failed search
    utils::MetricsRegistry::Instance().RecordSearch(
        request->collection_name(), false);
    return toGrpcStatus(search_result.status());
  }

  // Record successful search
  utils::MetricsRegistry::Instance().RecordSearch(
      request->collection_name(), true);

  // Convert results to proto
  for (const auto& entry : search_result->entries) {
    auto* proto_entry = response->add_results();
    toProto(entry, proto_entry);

    // Optionally include metadata in results
    if (request->return_metadata()) {
      auto meta_result = segment->GetMetadata(entry.id);
      if (meta_result.ok()) {
        toProto(*meta_result, proto_entry->mutable_metadata());
      }
    }
  }

  response->set_query_time_ms(timer.elapsed_millis());

  // Update statistics
  total_queries_.fetch_add(1, std::memory_order_relaxed);
  total_query_time_ms_.fetch_add(static_cast<uint64_t>(timer.elapsed_millis()),
                                  std::memory_order_relaxed);

  return grpc::Status::OK;
}

grpc::Status VectorDBService::Get(
    grpc::ServerContext* context,
    const proto::GetRequest* request,
    proto::GetResponse* response) {

  utils::Logger::Instance().Info("Get: {} IDs from {}",
                                 request->ids().size(),
                                 request->collection_name());

  // Validate request
  if (request->ids().empty()) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        "IDs list cannot be empty");
  }

  // Limit batch size for safety
  constexpr size_t MAX_GET_BATCH_SIZE = 10000;
  if (request->ids().size() > MAX_GET_BATCH_SIZE) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        absl::StrCat("Cannot get more than ", MAX_GET_BATCH_SIZE,
                     " vectors in one request. Requested: ", request->ids().size()));
  }

  // Get collection metadata
  auto metadata_result = collection_registry_->GetMetadata(request->collection_name());
  if (!metadata_result.ok()) {
    return toGrpcStatus(metadata_result.status());
  }

  auto* segment = segment_manager_->GetSegment(metadata_result->segment_id);
  if (!segment) {
    return toGrpcStatus(absl::NotFoundError("Segment not found"));
  }

  // Convert proto IDs to VectorIds
  std::vector<core::VectorId> ids;
  ids.reserve(request->ids().size());
  for (uint64_t id : request->ids()) {
    ids.push_back(core::MakeVectorId(id));
  }

  // Get vectors from segment
  auto result = segment->GetVectors(ids, request->return_metadata());

  // Convert found vectors to proto
  for (size_t i = 0; i < result.found_ids.size(); ++i) {
    auto* proto_vec = response->add_vectors();
    proto_vec->set_id(core::ToUInt64(result.found_ids[i]));

    // Convert vector
    toProto(result.found_vectors[i], proto_vec->mutable_vector());

    // Add metadata if requested and available
    if (request->return_metadata() && i < result.found_metadata.size()) {
      toProto(result.found_metadata[i], proto_vec->mutable_metadata());
    }
  }

  // Add not found IDs
  for (const auto& id : result.not_found_ids) {
    response->add_not_found_ids(core::ToUInt64(id));
  }

  return grpc::Status::OK;
}

grpc::Status VectorDBService::Delete(
    grpc::ServerContext* context,
    const proto::DeleteRequest* request,
    proto::DeleteResponse* response) {

  utils::Logger::Instance().Info("Delete: {} IDs from {}",
                                 request->ids().size(),
                                 request->collection_name());

  // Validate request
  if (request->ids().empty()) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        "IDs list cannot be empty");
  }

  // Limit batch size for safety
  constexpr size_t MAX_DELETE_BATCH_SIZE = 10000;
  if (request->ids().size() > MAX_DELETE_BATCH_SIZE) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        absl::StrCat("Cannot delete more than ", MAX_DELETE_BATCH_SIZE,
                     " vectors in one request. Requested: ", request->ids().size()));
  }

  // Get collection metadata
  auto metadata_result = collection_registry_->GetMetadata(request->collection_name());
  if (!metadata_result.ok()) {
    return toGrpcStatus(metadata_result.status());
  }

  auto* segment = segment_manager_->GetSegment(metadata_result->segment_id);
  if (!segment) {
    return toGrpcStatus(absl::NotFoundError("Segment not found"));
  }

  // Convert proto IDs to VectorIds
  std::vector<core::VectorId> ids;
  ids.reserve(request->ids().size());
  for (uint64_t id : request->ids()) {
    ids.push_back(core::MakeVectorId(id));
  }

  // Delete vectors from segment
  auto result = segment->DeleteVectors(ids);
  if (!result.ok()) {
    return toGrpcStatus(result.status());
  }

  // Build response
  response->set_deleted_count(result->deleted_count);

  for (const auto& id : result->not_found_ids) {
    response->add_not_found_ids(core::ToUInt64(id));
  }

  response->set_message(absl::StrCat(
      "Deleted ", result->deleted_count, " vector(s) from collection '",
      request->collection_name(), "'"));

  return grpc::Status::OK;
}

grpc::Status VectorDBService::UpdateMetadata(
    grpc::ServerContext* context,
    const proto::UpdateMetadataRequest* request,
    proto::UpdateMetadataResponse* response) {

  utils::Logger::Instance().Info("UpdateMetadata: ID {} in {}",
                                 request->id(),
                                 request->collection_name());

  // Validate request
  if (request->id() == 0) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        "Vector ID cannot be 0");
  }

  if (request->metadata().fields().empty()) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        "Metadata cannot be empty");
  }

  // Get collection metadata
  auto collection_meta = collection_registry_->GetMetadata(request->collection_name());
  if (!collection_meta.ok()) {
    return toGrpcStatus(collection_meta.status());
  }

  auto* segment = segment_manager_->GetSegment(collection_meta->segment_id);
  if (!segment) {
    return toGrpcStatus(absl::NotFoundError("Segment not found"));
  }

  // Convert proto metadata to core::Metadata
  auto metadata_result = fromProto(request->metadata());
  if (!metadata_result.ok()) {
    return toGrpcStatus(metadata_result.status());
  }

  // Update metadata in segment
  auto vector_id = core::MakeVectorId(request->id());
  auto status = segment->UpdateMetadata(vector_id, *metadata_result, request->merge());

  if (!status.ok()) {
    response->set_updated(false);
    response->set_message(std::string(status.message()));
    return toGrpcStatus(status);
  }

  response->set_updated(true);
  response->set_message(absl::StrCat(
      "Updated metadata for vector ID ", request->id(),
      " in collection '", request->collection_name(), "'"));

  return grpc::Status::OK;
}

// ============================================================================
// Health and Stats
// ============================================================================

grpc::Status VectorDBService::HealthCheck(
    grpc::ServerContext* context,
    const proto::HealthCheckRequest* request,
    proto::HealthCheckResponse* response) {

  response->set_status(proto::HealthCheckResponse::SERVING);
  response->set_message("Server is healthy");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::GetStats(
    grpc::ServerContext* context,
    const proto::GetStatsRequest* request,
    proto::GetStatsResponse* response) {

  // Get total vector count across all collections
  uint64_t total_vectors = 0;
  auto collections = collection_registry_->ListAll();

  for (const auto& metadata : collections) {
    auto* segment = segment_manager_->GetSegment(metadata.segment_id);
    if (segment) {
      total_vectors += segment->GetVectorCount();
    }
  }

  response->set_total_vectors(total_vectors);
  response->set_total_collections(collection_registry_->Count());

  uint64_t queries = total_queries_.load(std::memory_order_relaxed);
  uint64_t total_time = total_query_time_ms_.load(std::memory_order_relaxed);

  response->set_total_queries(queries);
  if (queries > 0) {
    response->set_avg_query_time_ms(static_cast<float>(total_time) / queries);
  } else {
    response->set_avg_query_time_ms(0.0f);
  }

  return grpc::Status::OK;
}

} // namespace network
} // namespace gvdb
