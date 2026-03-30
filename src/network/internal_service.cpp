// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/internal_service.h"
#include "network/proto_conversions.h"
#include "cluster/node_registry.h"
#include "cluster/coordinator.h"
#include "consensus/timestamp_oracle.h"
#include "utils/logger.h"
#include "utils/timer.h"
#include "core/types.h"
#include <chrono>

namespace gvdb {
namespace network {

InternalService::InternalService(
    std::shared_ptr<cluster::ShardManager> shard_manager,
    std::shared_ptr<storage::SegmentManager> segment_manager,
    std::shared_ptr<compute::QueryExecutor> query_executor,
    std::shared_ptr<cluster::NodeRegistry> node_registry,
    std::shared_ptr<consensus::TimestampOracle> timestamp_oracle,
    std::shared_ptr<cluster::Coordinator> coordinator)
    : shard_manager_(shard_manager),
      segment_manager_(segment_manager),
      query_executor_(query_executor),
      node_registry_(node_registry),
      timestamp_oracle_(timestamp_oracle),
      coordinator_(coordinator) {
  utils::Logger::Instance().Info("InternalService initialized (node_registry={}, timestamp_oracle={}, coordinator={})",
                                  node_registry_ != nullptr ? "yes" : "no",
                                  timestamp_oracle_ != nullptr ? "yes" : "no",
                                  coordinator_ != nullptr ? "yes" : "no");
}

InternalService::~InternalService() {
  utils::Logger::Instance().Info("InternalService shutting down");
}

// =============================================================================
// Shard Management
// =============================================================================

grpc::Status InternalService::AssignShard(
    grpc::ServerContext* context,
    const proto::internal::AssignShardRequest* request,
    proto::internal::AssignShardResponse* response) {
  total_requests_++;

  try {
    uint32_t shard_id = request->shard_id();
    uint32_t node_id = request->node_id();
    bool is_primary = request->is_primary();

    utils::Logger::Instance().Info("AssignShard: shard={}, node={}, primary={}",
                                    shard_id, node_id, is_primary);

    core::ShardId sid = core::MakeShardId(shard_id);
    core::NodeId nid = core::MakeNodeId(node_id);

    absl::Status status;
    if (is_primary) {
      status = shard_manager_->SetPrimaryNode(sid, nid);
    } else {
      status = shard_manager_->AddReplica(sid, nid);
    }

    if (!status.ok()) {
      response->set_success(false);
      response->set_message(std::string(status.message()));
      return grpc::Status::OK;
    }

    response->set_success(true);
    response->set_message(absl::StrFormat("Shard %d assigned to node %d (primary=%d)",
                                           shard_id, node_id, is_primary));
    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("AssignShard failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::GetShardAssignments(
    grpc::ServerContext* context,
    const proto::internal::GetShardAssignmentsRequest* request,
    proto::internal::GetShardAssignmentsResponse* response) {
  total_requests_++;

  try {
    uint32_t collection_id = request->collection_id();

    utils::Logger::Instance().Debug("GetShardAssignments: collection={}", collection_id);

    // Return shard assignments from ShardManager
    auto all_shards = shard_manager_->GetAllShards();
    for (const auto& shard_info : all_shards) {
      // Filter by collection_id if specified (0 = all)
      auto* assignment = response->add_assignments();
      assignment->set_shard_id(core::ToUInt16(shard_info.shard_id));
      assignment->set_primary_node_id(core::ToUInt32(shard_info.primary_node));
      for (const auto& replica : shard_info.replica_nodes) {
        assignment->add_node_ids(core::ToUInt32(replica));
      }
    }

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("GetShardAssignments failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::RebalanceShards(
    grpc::ServerContext* context,
    const proto::internal::RebalanceShardsRequest* request,
    proto::internal::RebalanceShardsResponse* response) {
  total_requests_++;

  try {
    uint32_t collection_id = request->collection_id();

    utils::Logger::Instance().Info("RebalanceShards: collection={}", collection_id);

    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "RebalanceShards not yet implemented");

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("RebalanceShards failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// =============================================================================
// Segment Replication
// =============================================================================

grpc::Status InternalService::ReplicateSegment(
    grpc::ServerContext* context,
    const proto::internal::ReplicateSegmentRequest* request,
    proto::internal::ReplicateSegmentResponse* response) {
  total_requests_++;

  try {
    const auto& segment_info = request->segment_info();
    uint64_t segment_id = segment_info.segment_id();
    uint32_t collection_id = segment_info.collection_id();
    uint32_t shard_id = segment_info.shard_id();
    const auto& segment_data = request->segment_data();

    utils::Logger::Instance().Info("ReplicateSegment: segment={}, collection={}, shard={}, data_size={}",
                                    segment_id, collection_id, shard_id, segment_data.size());

    // Validate segment info
    if (segment_id == 0) {
      response->set_success(false);
      response->set_message("Invalid segment_id");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid segment_id");
    }

    if (segment_data.empty()) {
      response->set_success(false);
      response->set_message("Empty segment_data");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Empty segment_data");
    }

    // Deserialize segment from bytes
    auto segment_result = storage::Segment::DeserializeFromBytes(segment_data);
    if (!segment_result.ok()) {
      response->set_success(false);
      response->set_message(absl::StrFormat("Failed to deserialize segment: %s",
                                             std::string(segment_result.status().message()).c_str()));
      utils::Logger::Instance().Error("Deserialization failed: {}", segment_result.status().message());
      return grpc::Status::OK;
    }

    // Add to segment manager
    auto add_status = segment_manager_->AddReplicatedSegment(std::move(segment_result.value()));
    if (!add_status.ok()) {
      response->set_success(false);
      response->set_message(absl::StrFormat("Failed to add segment: %s", std::string(add_status.message()).c_str()));
      utils::Logger::Instance().Error("AddReplicatedSegment failed: {}", add_status.message());
      return grpc::Status::OK;
    }

    response->set_success(true);
    response->set_message(absl::StrFormat("Segment %lu replicated successfully (%lu bytes)",
                                           segment_id, segment_data.size()));

    utils::Logger::Instance().Info("ReplicateSegment completed: segment={}, {} bytes",
                                    segment_id, segment_data.size());
    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("ReplicateSegment failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::GetSegment(
    grpc::ServerContext* context,
    const proto::internal::GetSegmentRequest* request,
    proto::internal::GetSegmentResponse* response) {
  total_requests_++;

  try {
    uint64_t segment_id = request->segment_id();

    utils::Logger::Instance().Debug("GetSegment: segment={}", segment_id);

    // Validate segment_id
    if (segment_id == 0) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid segment_id");
    }

    // Try to get segment from SegmentManager
    core::SegmentId seg_id = static_cast<core::SegmentId>(segment_id);
    auto segment = segment_manager_->GetSegment(seg_id);
    if (!segment) {
      utils::Logger::Instance().Debug("GetSegment: segment={} not found", segment_id);
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                          absl::StrFormat("Segment %lu not found", segment_id));
    }

    // Fill response with segment info
    auto* segment_info = response->mutable_segment_info();
    segment_info->set_segment_id(static_cast<uint64_t>(segment->GetId()));
    segment_info->set_collection_id(core::ToUInt32(segment->GetCollectionId()));
    segment_info->set_shard_id(0);  // TODO: Track shard_id in segment metadata
    segment_info->set_vector_count(segment->GetVectorCount());
    segment_info->set_is_sealed(segment->GetState() == core::SegmentState::SEALED);

    // Serialize segment data
    auto serialize_result = segment->SerializeToBytes();
    if (!serialize_result.ok()) {
      utils::Logger::Instance().Error("GetSegment: serialization failed: {}",
                                       serialize_result.status().message());
      return grpc::Status(grpc::StatusCode::INTERNAL,
                          absl::StrFormat("Failed to serialize segment: %s",
                                          std::string(serialize_result.status().message()).c_str()));
    }

    // Set segment data and size
    const auto& data = serialize_result.value();
    response->set_segment_data(data);
    segment_info->set_size_bytes(data.size());

    utils::Logger::Instance().Debug("GetSegment: found segment={} with {} vectors ({} bytes)",
                                     segment_id, segment->GetVectorCount(), data.size());
    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("GetSegment failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::ListSegments(
    grpc::ServerContext* context,
    const proto::internal::ListSegmentsRequest* request,
    proto::internal::ListSegmentsResponse* response) {
  total_requests_++;

  try {
    uint32_t collection_id = request->collection_id();
    uint32_t shard_id = request->shard_id();

    utils::Logger::Instance().Debug("ListSegments: collection={}, shard={}", collection_id, shard_id);

    // List segments, optionally filtered by collection_id
    std::vector<core::SegmentId> segment_ids;

    if (collection_id > 0) {
      segment_ids = segment_manager_->GetCollectionSegments(
          core::MakeCollectionId(collection_id));
    } else {
      // Get all segments by iterating all collections
      // SegmentManager doesn't expose a GetAllSegments, so use GetSegment
      // to check known IDs. For now, return segments for all known collections.
      // This is a best-effort approach.
      segment_ids = segment_manager_->GetCollectionSegments(
          core::MakeCollectionId(0));  // Will return empty if no collection 0
    }

    for (const auto& seg_id : segment_ids) {
      auto* segment = segment_manager_->GetSegment(seg_id);
      if (!segment) continue;

      auto* info = response->add_segments();
      info->set_segment_id(static_cast<uint64_t>(core::ToUInt32(seg_id)));
      info->set_collection_id(collection_id);
      info->set_vector_count(segment->GetVectorCount());
      info->set_is_sealed(segment->GetState() == core::SegmentState::SEALED);
    }

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("ListSegments failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::DeleteSegment(
    grpc::ServerContext* context,
    const proto::internal::DeleteSegmentRequest* request,
    proto::internal::DeleteSegmentResponse* response) {
  total_requests_++;

  try {
    uint64_t segment_id = request->segment_id();
    bool force = request->force();

    utils::Logger::Instance().Info("DeleteSegment: segment={}, force={}", segment_id, force);

    // Validate segment_id
    if (segment_id == 0) {
      response->set_success(false);
      response->set_message("Invalid segment_id");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid segment_id");
    }

    // Check if segment exists
    core::SegmentId seg_id = static_cast<core::SegmentId>(segment_id);
    auto segment = segment_manager_->GetSegment(seg_id);
    if (!segment) {
      response->set_success(false);
      response->set_message(absl::StrFormat("Segment %lu not found", segment_id));
      return grpc::Status::OK;
    }

    // Check segment state before deletion
    auto state = segment->GetState();
    utils::Logger::Instance().Info("DeleteSegment: segment={} state={}",
                                    segment_id, static_cast<int>(state));

    // Delete segment from memory and disk
    auto drop_status = segment_manager_->DropSegment(seg_id, true /* delete_files */);
    if (!drop_status.ok()) {
      response->set_success(false);
      response->set_message(absl::StrFormat("Failed to delete segment %lu: %s",
                                             segment_id, std::string(drop_status.message()).c_str()));
      utils::Logger::Instance().Error("DeleteSegment failed: {}", drop_status.message());
      return grpc::Status::OK;
    }

    response->set_success(true);
    response->set_message(absl::StrFormat("Segment %lu deleted successfully (removed from memory and disk)",
                                           segment_id));
    utils::Logger::Instance().Info("DeleteSegment: successfully deleted segment={}", segment_id);

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("DeleteSegment failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::CreateSegment(
    grpc::ServerContext* context,
    const proto::internal::CreateSegmentRequest* request,
    proto::internal::CreateSegmentResponse* response) {
  total_requests_++;

  try {
    uint64_t segment_id = request->segment_id();
    uint32_t collection_id = request->collection_id();
    uint32_t dimension = request->dimension();
    const std::string& metric_type_str = request->metric_type();
    const std::string& index_type_str = request->index_type();

    utils::Logger::Instance().Info(
        "CreateSegment: segment={}, collection={}, dimension={}, metric={}, index={}",
        segment_id, collection_id, dimension, metric_type_str, index_type_str);

    // Validate inputs
    if (segment_id == 0) {
      response->set_success(false);
      response->set_message("Invalid segment_id");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid segment_id");
    }

    if (collection_id == 0) {
      response->set_success(false);
      response->set_message("Invalid collection_id");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid collection_id");
    }

    if (dimension == 0) {
      response->set_success(false);
      response->set_message("Invalid dimension");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid dimension");
    }

    // Convert string types to enums
    auto metric_result = metricTypeFromString(metric_type_str);
    if (!metric_result.ok()) {
      response->set_success(false);
      response->set_message(std::string(metric_result.status().message()));
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          std::string(metric_result.status().message()));
    }

    auto index_result = indexTypeFromString(index_type_str);
    if (!index_result.ok()) {
      response->set_success(false);
      response->set_message(std::string(index_result.status().message()));
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          std::string(index_result.status().message()));
    }

    // Create segment
    core::SegmentId seg_id = static_cast<core::SegmentId>(segment_id);
    core::CollectionId coll_id = core::MakeCollectionId(collection_id);
    core::Dimension dim = static_cast<core::Dimension>(dimension);

    auto create_status = segment_manager_->CreateSegmentWithId(
        seg_id, coll_id, dim, *metric_result, *index_result);

    if (!create_status.ok()) {
      response->set_success(false);
      response->set_message(absl::StrFormat("Failed to create segment: %s",
                                             std::string(create_status.message()).c_str()));
      utils::Logger::Instance().Error("CreateSegment failed: {}", create_status.message());
      return grpc::Status::OK;
    }

    response->set_success(true);
    response->set_message(absl::StrFormat("Segment %lu created successfully", segment_id));
    response->set_segment_id(segment_id);

    utils::Logger::Instance().Info("CreateSegment: successfully created segment={}", segment_id);
    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("CreateSegment failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// =============================================================================
// Metadata Synchronization
// =============================================================================

grpc::Status InternalService::SyncMetadata(
    grpc::ServerContext* context,
    const proto::internal::SyncMetadataRequest* request,
    proto::internal::SyncMetadataResponse* response) {
  total_requests_++;

  try {
    uint32_t node_id = request->node_id();
    int64_t last_sync_timestamp = request->last_sync_timestamp();

    utils::Logger::Instance().Debug("SyncMetadata: node={}, last_sync={}",
                                     node_id, last_sync_timestamp);

    // TODO Phase 4: Implement metadata synchronization
    response->set_current_timestamp(
        std::chrono::system_clock::now().time_since_epoch().count());

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("SyncMetadata failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::GetCollectionMetadata(
    grpc::ServerContext* context,
    const proto::internal::GetCollectionMetadataRequest* request,
    proto::internal::GetCollectionMetadataResponse* response) {
  total_requests_++;

  try {
    // Check if coordinator is available
    if (!coordinator_) {
      utils::Logger::Instance().Warn("GetCollectionMetadata: coordinator not available");
      response->set_found(false);
      return grpc::Status::OK;
    }

    // Get collection metadata from coordinator
    absl::StatusOr<cluster::CollectionMetadata> metadata_result;

    if (request->has_collection_id()) {
      uint32_t collection_id = request->collection_id();
      utils::Logger::Instance().Debug("GetCollectionMetadata: collection_id={}", collection_id);
      metadata_result = coordinator_->GetCollectionMetadata(core::MakeCollectionId(collection_id));
    } else if (request->has_collection_name()) {
      const std::string& collection_name = request->collection_name();
      utils::Logger::Instance().Debug("GetCollectionMetadata: collection_name={}", collection_name);
      metadata_result = coordinator_->GetCollectionMetadata(collection_name);
    } else {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "Either collection_id or collection_name must be provided");
    }

    // Check if collection was found
    if (!metadata_result.ok()) {
      utils::Logger::Instance().Debug("GetCollectionMetadata: collection not found: {}",
                                       metadata_result.status().message());
      response->set_found(false);
      return grpc::Status::OK;
    }

    // Populate response
    const auto& metadata = metadata_result.value();
    response->set_found(true);

    auto* proto_metadata = response->mutable_metadata();
    proto_metadata->set_collection_id(core::ToUInt32(metadata.collection_id));
    proto_metadata->set_collection_name(metadata.collection_name);
    proto_metadata->set_dimension(metadata.dimension);

    // Convert MetricType enum to string
    switch (metadata.metric_type) {
      case core::MetricType::L2:
        proto_metadata->set_metric_type("L2");
        break;
      case core::MetricType::INNER_PRODUCT:
        proto_metadata->set_metric_type("INNER_PRODUCT");
        break;
      case core::MetricType::COSINE:
        proto_metadata->set_metric_type("COSINE");
        break;
      default:
        proto_metadata->set_metric_type("UNKNOWN");
        break;
    }

    // Convert IndexType enum to string
    switch (metadata.index_type) {
      case core::IndexType::FLAT:
        proto_metadata->set_index_type("FLAT");
        break;
      case core::IndexType::HNSW:
        proto_metadata->set_index_type("HNSW");
        break;
      case core::IndexType::IVF_FLAT:
        proto_metadata->set_index_type("IVF_FLAT");
        break;
      case core::IndexType::IVF_PQ:
        proto_metadata->set_index_type("IVF_PQ");
        break;
      case core::IndexType::IVF_SQ:
        proto_metadata->set_index_type("IVF_SQ");
        break;
      default:
        proto_metadata->set_index_type("UNKNOWN");
        break;
    }

    proto_metadata->set_vector_count(metadata.total_vectors);
    proto_metadata->set_created_at(metadata.created_at);
    proto_metadata->set_shard_count(metadata.shard_ids.size());

    utils::Logger::Instance().Debug("GetCollectionMetadata: found collection '{}' (id={}, dim={})",
                                     metadata.collection_name,
                                     core::ToUInt32(metadata.collection_id),
                                     metadata.dimension);

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("GetCollectionMetadata failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// =============================================================================
// Query Routing and Execution
// =============================================================================

grpc::Status InternalService::RouteQuery(
    grpc::ServerContext* context,
    const proto::internal::RouteQueryRequest* request,
    proto::internal::RouteQueryResponse* response) {
  total_requests_++;

  try {
    const std::string& collection_name = request->collection_name();
    uint32_t top_k = request->top_k();

    utils::Logger::Instance().Debug("RouteQuery: collection={}, top_k={}",
                                     collection_name, top_k);

    // Need coordinator to look up collection metadata
    if (!coordinator_) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
          "RouteQuery requires coordinator (only available on coordinator nodes)");
    }

    // Get collection metadata to find shard assignments
    auto metadata_result = coordinator_->GetCollectionMetadata(collection_name);
    if (!metadata_result.ok()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
          std::string(metadata_result.status().message()));
    }

    const auto& metadata = *metadata_result;
    response->set_collection_id(core::ToUInt32(metadata.collection_id));

    // For each shard, find the primary data node
    for (const auto& shard_id : metadata.shard_ids) {
      auto primary_result = shard_manager_->GetPrimaryNode(shard_id);
      if (!primary_result.ok()) continue;

      core::NodeId node_id = *primary_result;
      if (node_id == core::kInvalidNodeId) continue;

      // Get node address from registry
      std::string address;
      if (node_registry_) {
        cluster::RegisteredNode node;
        if (node_registry_->GetNode(core::ToUInt32(node_id), &node)) {
          address = node.info.grpc_address();
        }
      }

      response->add_target_shard_ids(core::ToUInt16(shard_id));
      response->add_target_node_ids(core::ToUInt32(node_id));
      response->add_target_node_addresses(address);
    }

    utils::Logger::Instance().Debug("RouteQuery: {} shards for collection '{}'",
                                     response->target_shard_ids_size(), collection_name);
    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("RouteQuery failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::ExecuteShardQuery(
    grpc::ServerContext* context,
    const proto::internal::ExecuteShardQueryRequest* request,
    proto::internal::ExecuteShardQueryResponse* response) {
  total_requests_++;

  try {
    uint32_t collection_id = request->collection_id();
    uint32_t top_k = request->top_k();

    utils::Logger::Instance().Debug("ExecuteShardQuery: collection={}, top_k={}",
                                     collection_id, top_k);

    uint32_t shard_id = request->shard_id();
    core::SegmentId segment_id = cluster::ShardSegmentId(
        core::MakeCollectionId(collection_id), shard_id);
    auto* segment = segment_manager_->GetSegment(segment_id);
    if (!segment) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
          absl::StrCat("Segment not found for collection ", collection_id));
    }

    // Convert query vector from proto
    if (request->query_vector().empty()) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Query vector is empty");
    }

    std::vector<float> query_data(request->query_vector().begin(),
                                   request->query_vector().end());
    core::Vector query(std::move(query_data));

    // Validate dimension
    if (query.dimension() != segment->GetDimension()) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
          absl::StrCat("Query dimension mismatch: expected ",
                       segment->GetDimension(), ", got ", query.dimension()));
    }

    // Execute search
    utils::Timer timer;
    core::StatusOr<core::SearchResult> search_result;

    if (!request->filter().empty()) {
      search_result = segment->SearchWithFilter(query, top_k, request->filter());
    } else {
      search_result = segment->Search(query, top_k);
    }

    if (!search_result.ok()) {
      return grpc::Status(grpc::StatusCode::INTERNAL,
          std::string(search_result.status().message()));
    }

    // Convert results to proto
    for (const auto& entry : search_result->entries) {
      auto* result = response->add_results();
      result->set_id(core::ToUInt64(entry.id));
      result->set_distance(entry.distance);
    }

    response->set_query_time_ms(static_cast<float>(timer.elapsed_millis()));
    response->set_vectors_scanned(segment->GetVectorCount());

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("ExecuteShardQuery failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// =============================================================================
// Data Transfer
// =============================================================================

grpc::Status InternalService::TransferData(
    grpc::ServerContext* context,
    const proto::internal::TransferDataRequest* request,
    proto::internal::TransferDataResponse* response) {
  total_requests_++;

  try {
    uint32_t collection_id = request->collection_id();
    uint32_t shard_id = request->shard_id();
    uint32_t source_node_id = request->source_node_id();
    uint32_t target_node_id = request->target_node_id();

    utils::Logger::Instance().Info("TransferData: collection={}, shard={}, {} -> {}",
                                    collection_id, shard_id, source_node_id, target_node_id);

    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "TransferData not yet implemented");

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("TransferData failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// =============================================================================
// Health Monitoring
// =============================================================================

grpc::Status InternalService::Heartbeat(
    grpc::ServerContext* context,
    const proto::internal::HeartbeatRequest* request,
    proto::internal::HeartbeatResponse* response) {
  total_requests_++;

  try {
    const auto& node_info = request->node_info();

    utils::Logger::Instance().Debug("Heartbeat: node_id={}, type={}, status={}",
                                     node_info.node_id(),
                                     static_cast<int>(node_info.node_type()),
                                     static_cast<int>(node_info.status()));

    // Update node registry if available
    if (node_registry_) {
      node_registry_->UpdateNode(node_info);
    }

    response->set_acknowledged(true);
    response->set_timestamp(
        std::chrono::system_clock::now().time_since_epoch().count());

    // Send shard assignments back to the node
    if (shard_manager_ && node_info.node_id() > 0) {
      core::NodeId nid = core::MakeNodeId(node_info.node_id());
      auto shards = shard_manager_->GetShardsForNode(nid);
      for (const auto& shard_info : shards) {
        response->add_assigned_shards(core::ToUInt16(shard_info.shard_id));
      }
    }

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("Heartbeat failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::GetClusterHealth(
    grpc::ServerContext* context,
    const proto::internal::GetClusterHealthRequest* request,
    proto::internal::GetClusterHealthResponse* response) {
  total_requests_++;

  try {
    utils::Logger::Instance().Debug("GetClusterHealth");

    // Get cluster stats from node registry
    if (node_registry_) {
      auto stats = node_registry_->GetClusterStats();

      // Add node information
      auto healthy_nodes = node_registry_->GetHealthyNodes();
      for (const auto& node : healthy_nodes) {
        auto* node_info = response->add_nodes();
        *node_info = node.info;
      }

      // Set cluster status
      response->set_total_shards(0);  // TODO: Get from ShardManager
      response->set_healthy_shards(0);
      response->set_degraded_shards(0);

      if (stats.failed_nodes == 0) {
        response->set_cluster_status("healthy");
      } else if (stats.healthy_nodes > stats.failed_nodes) {
        response->set_cluster_status("degraded");
      } else {
        response->set_cluster_status("critical");
      }
    } else {
      // No node registry - single-node mode
      response->set_cluster_status("healthy");
    }

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("GetClusterHealth failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// =============================================================================
// Timestamp Oracle
// =============================================================================

grpc::Status InternalService::GetTimestamp(
    grpc::ServerContext* context,
    const proto::internal::GetTimestampRequest* request,
    proto::internal::GetTimestampResponse* response) {
  total_requests_++;

  try {
    uint32_t count = request->count();
    if (count == 0) count = 1;

    utils::Logger::Instance().Debug("GetTimestamp: count={}", count);

    if (timestamp_oracle_) {
      // Use TimestampOracle for globally unique, monotonic timestamps
      uint64_t start_ts = timestamp_oracle_->GetTimestamp();
      uint64_t end_ts = start_ts;

      // Allocate additional timestamps if needed
      for (uint32_t i = 1; i < count; ++i) {
        end_ts = timestamp_oracle_->GetTimestamp();
      }

      response->set_start_timestamp(start_ts);
      response->set_end_timestamp(end_ts);
    } else {
      // Fallback to system time if no TimestampOracle
      uint64_t start_ts = static_cast<uint64_t>(
          std::chrono::system_clock::now().time_since_epoch().count());
      uint64_t end_ts = start_ts + count - 1;

      response->set_start_timestamp(start_ts);
      response->set_end_timestamp(end_ts);
    }

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("GetTimestamp failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

} // namespace network
} // namespace gvdb