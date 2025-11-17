#include "network/internal_service.h"
#include "cluster/node_registry.h"
#include "consensus/timestamp_oracle.h"
#include "utils/logger.h"
#include "core/types.h"
#include <chrono>

namespace gvdb {
namespace network {

InternalService::InternalService(
    std::shared_ptr<cluster::ShardManager> shard_manager,
    std::shared_ptr<storage::SegmentManager> segment_manager,
    std::shared_ptr<compute::QueryExecutor> query_executor,
    std::shared_ptr<cluster::NodeRegistry> node_registry,
    std::shared_ptr<consensus::TimestampOracle> timestamp_oracle)
    : shard_manager_(shard_manager),
      segment_manager_(segment_manager),
      query_executor_(query_executor),
      node_registry_(node_registry),
      timestamp_oracle_(timestamp_oracle) {
  utils::Logger::Instance().Info("InternalService initialized (node_registry={}, timestamp_oracle={})",
                                  node_registry_ != nullptr ? "yes" : "no",
                                  timestamp_oracle_ != nullptr ? "yes" : "no");
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

    // In Phase 3, this is a stub that just acknowledges the assignment
    // TODO: Actually store shard assignment metadata

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

    // TODO Phase 4: Return actual shard assignments from metadata store
    // For now, return empty response (single-node mode)

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

    // TODO Phase 4: Implement shard rebalancing
    response->set_shards_moved(0);
    response->set_message("Rebalancing not yet implemented (Phase 4)");

    return grpc::Status::OK;

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

    utils::Logger::Instance().Info("ReplicateSegment: segment={}, collection={}, shard={}",
                                    segment_id, collection_id, shard_id);

    // For Phase 4, we implement basic segment metadata replication
    // Actual data transfer would be done via streaming RPC in production

    // Validate segment info
    if (segment_id == 0) {
      response->set_success(false);
      response->set_message("Invalid segment_id");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid segment_id");
    }

    // Note: In production, this would:
    // 1. Stream segment data from source node
    // 2. Write to local storage
    // 3. Build/validate index
    // For now, we just acknowledge receipt

    response->set_success(true);
    response->set_message(absl::StrFormat("Segment %lu metadata received (data transfer pending)",
                                           segment_id));

    utils::Logger::Instance().Info("ReplicateSegment completed for segment={}", segment_id);
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

    // Try to get segment metadata from SegmentManager
    core::SegmentId seg_id = static_cast<core::SegmentId>(segment_id);
    auto segment = segment_manager_->GetSegment(seg_id);
    if (segment) {
      // Fill response with segment info
      auto* segment_info = response->mutable_segment_info();
      segment_info->set_segment_id(static_cast<uint64_t>(segment->GetId()));
      segment_info->set_collection_id(0);  // TODO: Get from segment metadata
      segment_info->set_shard_id(0);       // TODO: Get from segment metadata
      segment_info->set_vector_count(segment->GetVectorCount());
      segment_info->set_size_bytes(0);     // TODO: Calculate actual size
      segment_info->set_is_sealed(segment->GetState() == core::SegmentState::SEALED);

      // Note: Actual vector data would be streamed in production via segment_data field

      utils::Logger::Instance().Debug("GetSegment: found segment={} with {} vectors",
                                       segment_id, segment->GetVectorCount());
      return grpc::Status::OK;
    } else {
      // Return empty response if segment not found
      utils::Logger::Instance().Debug("GetSegment: segment={} not found", segment_id);
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                          absl::StrFormat("Segment %lu not found", segment_id));
    }

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

    // TODO Phase 4: List segments from SegmentManager
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

    // TODO: Implement actual segment deletion
    // For now, this is a stub. Production implementation would:
    // 1. Check if segment can be safely deleted (not in active queries)
    // 2. Remove from memory
    // 3. Delete segment files from disk
    // 4. Update metadata
    response->set_success(false);
    response->set_message(absl::StrFormat("Segment deletion not fully implemented (segment %lu exists but deletion pending)",
                                           segment_id));
    utils::Logger::Instance().Warn("DeleteSegment: deletion not implemented for segment={}", segment_id);

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("DeleteSegment failed: {}", e.what());
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
    // Get collection identifier (either ID or name)
    if (request->has_collection_id()) {
      utils::Logger::Instance().Debug("GetCollectionMetadata: collection_id={}",
                                       request->collection_id());
    } else if (request->has_collection_name()) {
      utils::Logger::Instance().Debug("GetCollectionMetadata: collection_name={}",
                                       request->collection_name());
    }

    // TODO Phase 4: Look up collection metadata from metadata store
    response->set_found(false);

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

    // TODO Phase 5: Implement query routing logic
    // For now, return empty response
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
    uint32_t shard_id = request->shard_id();
    uint32_t top_k = request->top_k();

    utils::Logger::Instance().Debug("ExecuteShardQuery: collection={}, shard={}, top_k={}",
                                     collection_id, shard_id, top_k);

    // TODO Phase 5: Execute query on specific shard using QueryExecutor
    response->set_query_time_ms(0.0f);
    response->set_vectors_scanned(0);

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

    // TODO Phase 4: Implement data transfer for rebalancing
    response->set_success(false);
    response->set_vectors_transferred(0);
    response->set_message("Data transfer not yet implemented (Phase 4)");

    return grpc::Status::OK;

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

    // TODO Phase 4+: Send instructions (shard assignments, rebalance commands)
    // response->set_should_rebalance(false);

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
