// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/internal_service.h"
#include "network/proto_conversions.h"
#include "cluster/node_registry.h"
#include "cluster/coordinator.h"
#include "cluster/primary_term_tracker.h"
#include "consensus/raft_node.h"
#include "consensus/timestamp_oracle.h"
#include "utils/logger.h"
#include "utils/timer.h"
#include "core/types.h"
#include <chrono>

namespace gvdb {
namespace network {

InternalService::InternalService(
    std::shared_ptr<cluster::ShardManager> shard_manager,
    std::shared_ptr<storage::ISegmentStore> segment_store,
    std::shared_ptr<compute::QueryExecutor> query_executor,
    std::shared_ptr<cluster::NodeRegistry> node_registry,
    std::shared_ptr<consensus::TimestampOracle> timestamp_oracle,
    std::shared_ptr<cluster::Coordinator> coordinator,
    std::shared_ptr<consensus::RaftNode> raft_node)
    : shard_manager_(shard_manager),
      segment_store_(segment_store),
      query_executor_(query_executor),
      node_registry_(node_registry),
      timestamp_oracle_(timestamp_oracle),
      coordinator_(coordinator),
      raft_node_(raft_node) {
  utils::Logger::Instance().Info(
      "InternalService initialized (node_registry={}, timestamp_oracle={}, coordinator={}, raft_node={})",
      node_registry_ != nullptr ? "yes" : "no",
      timestamp_oracle_ != nullptr ? "yes" : "no",
      coordinator_ != nullptr ? "yes" : "no",
      raft_node_ != nullptr ? "yes" : "no");
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

    if (!coordinator_) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "RebalanceShards requires coordinator");
    }

    auto cid = core::CollectionId(collection_id);
    auto result = coordinator_->ExecuteRebalancePlan(cid);
    if (!result.ok()) {
      response->set_shards_moved(0);
      response->set_message(std::string(result.status().message()));
      return grpc::Status::OK;
    }

    response->set_shards_moved(*result);
    response->set_message(
        absl::StrFormat("Rebalanced %d shards", *result));
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
    auto add_status = segment_store_->AddReplicatedSegment(std::move(segment_result.value()));
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
    auto segment = segment_store_->GetSegment(seg_id);
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
      segment_ids = segment_store_->GetCollectionSegments(
          core::MakeCollectionId(collection_id));
    } else {
      // Get all segments by iterating all collections
      // SegmentManager doesn't expose a GetAllSegments, so use GetSegment
      // to check known IDs. For now, return segments for all known collections.
      // This is a best-effort approach.
      segment_ids = segment_store_->GetCollectionSegments(
          core::MakeCollectionId(0));  // Will return empty if no collection 0
    }

    for (const auto& seg_id : segment_ids) {
      auto* segment = segment_store_->GetSegment(seg_id);
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
    auto segment = segment_store_->GetSegment(seg_id);
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
    auto drop_status = segment_store_->DropSegment(seg_id, true /* delete_files */);
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

    auto create_status = segment_store_->CreateSegmentWithId(
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

    // Convert IndexType enum to string (reuse proto_conversions toString)
    proto_metadata->set_index_type(toString(metadata.index_type));

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

    const bool prefer_replica = request->prefer_routable_replica();

    // For each shard, build a prioritized list of candidate nodes the caller
    // can try. options[0] becomes the legacy parallel-array entry (so older
    // clients keep working unchanged); options[1..N] are fallbacks the
    // caller can attempt when its first RPC fails with UNAVAILABLE or
    // DEADLINE_EXCEEDED. Ordering rules:
    //
    //   * writes (prefer_replica == false): primary only — replicas can't
    //     accept writes.
    //   * reads with primary routable: [primary, routable-replicas...].
    //     Primary first because it has the freshest data; replicas are
    //     fallbacks for transient primary unreachability.
    //   * reads with primary draining: [routable-replicas..., primary].
    //     Sheds traffic off the draining node immediately while still
    //     leaving the primary as a last-resort fallback if every replica
    //     is also unreachable.
    //
    // If the resulting list is empty (e.g. node_registry_ unset or every
    // candidate has no recorded address), emit the primary as a single
    // option with an empty address so the caller sees a deterministic
    // "no routable target" failure rather than a missing shard entry.
    for (const auto& shard_id : metadata.shard_ids) {
      // Atomic (primary, term) read so we can stamp every option with
      // the same term value. If we read primary and term separately a
      // concurrent SetPrimaryNode could pair an old node with a new
      // term (or vice versa), feeding the proxy a contradiction.
      auto primary_view = shard_manager_->GetPrimaryNodeAndTerm(shard_id);
      if (!primary_view.ok()) continue;

      core::NodeId primary_id = primary_view->node_id;
      const uint64_t primary_term = primary_view->term;
      if (primary_id == core::kInvalidNodeId) continue;

      // Resolve a node-id to (address, routable) using the registry.
      auto resolve = [&](core::NodeId nid) -> std::pair<std::string, bool> {
        if (!node_registry_) return {std::string{}, true};
        cluster::RegisteredNode node;
        if (!node_registry_->GetNode(core::ToUInt32(nid), &node)) {
          return {std::string{}, false};
        }
        return {node.info.grpc_address(),
                node_registry_->IsNodeRoutable(core::ToUInt32(nid))};
      };

      auto [primary_address, primary_routable] = resolve(primary_id);

      std::vector<core::NodeId> replicas;
      if (auto replicas_result = shard_manager_->GetReplicaNodes(shard_id);
          replicas_result.ok()) {
        for (auto replica : *replicas_result) {
          if (replica == primary_id) continue;  // primary handled separately
          replicas.push_back(replica);
        }
      }

      // Build the per-shard options list.
      auto* shard_opts = response->add_per_shard_options();
      shard_opts->set_shard_id(core::ToUInt16(shard_id));

      auto add_option = [&](core::NodeId nid, const std::string& addr,
                            bool is_primary) {
        auto* opt = shard_opts->add_options();
        opt->set_node_id(core::ToUInt32(nid));
        opt->set_node_address(addr);
        opt->set_is_primary(is_primary);
        // Every option carries the same primary_term — the term is a
        // property of the shard's current primary assignment, not of
        // the node that happens to be in the candidate list. The
        // proxy stamps this onto write RPCs as gvdb-shard-term so the
        // data-node can reject term-mismatched writes.
        opt->set_primary_term(primary_term);
      };

      if (!prefer_replica) {
        // Writes: primary only.
        add_option(primary_id, primary_address, /*is_primary=*/true);
      } else if (primary_routable) {
        // Reads, primary healthy: primary first, replicas as fallbacks.
        add_option(primary_id, primary_address, /*is_primary=*/true);
        for (auto replica : replicas) {
          auto [addr, routable] = resolve(replica);
          if (routable && !addr.empty()) {
            add_option(replica, addr, /*is_primary=*/false);
          }
        }
      } else {
        // Reads, primary draining: routable replicas first, primary last.
        for (auto replica : replicas) {
          auto [addr, routable] = resolve(replica);
          if (routable && !addr.empty()) {
            add_option(replica, addr, /*is_primary=*/false);
          }
        }
        // Always include primary as a last-resort even when unrouteable
        // — the caller may have stale heartbeat data and the primary may
        // actually be reachable, and emitting nothing leaves the shard
        // with no target at all.
        add_option(primary_id, primary_address, /*is_primary=*/true);
      }

      // Mirror options[0] into the legacy parallel arrays for clients
      // that haven't been recompiled to read per_shard_options.
      const auto& first = shard_opts->options(0);
      response->add_target_shard_ids(core::ToUInt16(shard_id));
      response->add_target_node_ids(first.node_id());
      response->add_target_node_addresses(first.node_address());
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
    auto* segment = segment_store_->GetSegment(segment_id);
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

      if (request->return_metadata()) {
        auto meta_result = segment->GetMetadata(entry.id);
        if (meta_result.ok()) {
          toProto(*meta_result, result->mutable_metadata());
        }
      }
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

    if (!coordinator_) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "TransferData requires coordinator");
    }

    auto cid = core::CollectionId(collection_id);
    core::SegmentId seg_id = cluster::ShardSegmentId(cid, shard_id);
    auto source = core::MakeNodeId(source_node_id);
    auto target = core::MakeNodeId(target_node_id);

    auto status = coordinator_->ReplicateSegmentData(seg_id, source, target);
    if (!status.ok()) {
      response->set_success(false);
      response->set_message(std::string(status.message()));
      return grpc::Status::OK;
    }

    response->set_success(true);
    response->set_message("Transfer complete");
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

    // Send shard assignments back to the node, plus the authoritative
    // per-shard primary view so the data-node's PrimaryTermTracker
    // can gate writes against term mismatches.
    //
    // Scaling note: GetShardsForNode iterates the full shard table per
    // call, making this loop O(shards) per heartbeat. At the default
    // 10s cadence (heartbeat_sender.cpp:116) and 1k-10k shard scales
    // this is a few hundred microseconds — fine. Beyond ~50k shards
    // we'd want a per-node shard index in ShardManager; not a v1
    // concern.
    if (shard_manager_ && node_info.node_id() > 0) {
      core::NodeId nid = core::MakeNodeId(node_info.node_id());
      auto shards = shard_manager_->GetShardsForNode(nid);
      for (const auto& shard_info : shards) {
        response->add_assigned_shards(core::ToUInt16(shard_info.shard_id));
        // Primary view: shard_id, current primary_node_id, current term.
        // Same payload sent to every node touching this shard (primary
        // or replica) — keeps each data-node's tracker in sync without
        // a separate per-role channel.
        auto* sp = response->add_shard_primaries();
        sp->set_shard_id(core::ToUInt16(shard_info.shard_id));
        sp->set_primary_node_id(core::ToUInt32(shard_info.primary_node));
        sp->set_term(shard_info.primary_term);
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

    // Populate last_rebalance_unix_ms from the coordinator's tracked state
    // so the operator can surface status.lastRebalance (roadmap 0b.6.E).
    // Zero when no rebalance has fired on this coordinator's watch. The
    // value is captured at rebalance completion (wall-clock) rather than
    // reconstructed from a monotonic clock here — that reconstruction was
    // the old approach and broke under NTP steps and process restarts.
    if (coordinator_) {
      auto last = coordinator_->GetLastAutoRebalance();
      if (last && last->completed_unix_ms > 0) {
        response->set_last_rebalance_unix_ms(last->completed_unix_ms);
      }
    }

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    total_errors_++;
    utils::Logger::Instance().Error("GetClusterHealth failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::GetLeaderInfo(
    grpc::ServerContext* /*context*/,
    const proto::internal::GetLeaderInfoRequest* /*request*/,
    proto::internal::GetLeaderInfoResponse* response) {
  total_requests_++;

  // SECURITY NOTE: InternalService is unauthenticated today — any in-cluster
  // client that can reach :50051 can read leader identity and term. The
  // network path is restricted to the gvdb namespace by default (and to
  // NetworkPolicy-ed pods in a hardened install), but this surface should
  // gain a token or mTLS check before Tier 1.Full ships (roadmap).

  if (!raft_node_) {
    // Single-node coordinator: no Raft, so self is the "leader" by fiat.
    response->set_leader_id(1);
    response->set_is_leader_self(true);
    response->set_current_term(0);
    return grpc::Status::OK;
  }

  int leader_id = raft_node_->GetLeaderId();
  response->set_leader_id(leader_id);
  response->set_is_leader_self(raft_node_->IsLeader());
  response->set_current_term(raft_node_->GetCurrentTerm());

  // leader_address is left empty by design: NodeRegistry tracks data-node
  // and query-node endpoints (IDs 101+ / 201+), not coordinator pods
  // (IDs 1..N). The operator reconstructs the coordinator pod name from
  // leader_id via the ordinal convention. Blindly looking up NodeRegistry
  // here was dead code for coordinator IDs and a latent ID-collision
  // hazard for any future scheme that overlapped the ranges.

  return grpc::Status::OK;
}

// =============================================================================
// Raft Membership (roadmap 1.7b)
// =============================================================================
//
// These two RPCs let coordinator pods add/remove themselves from Raft
// membership at runtime. They are leader-only; a non-leader response
// populates current_leader_id so the caller retries against the leader.
// Single-node coordinators respond with success=false because there is
// no Raft cluster to join.

grpc::Status InternalService::JoinCluster(
    grpc::ServerContext* /*context*/,
    const proto::internal::JoinClusterRequest* request,
    proto::internal::JoinClusterResponse* response) {
  total_requests_++;

  if (!raft_node_) {
    response->set_success(false);
    response->set_message("single-node mode; no Raft cluster to join");
    return grpc::Status::OK;
  }

  if (!raft_node_->IsLeader()) {
    response->set_success(false);
    response->set_current_leader_id(raft_node_->GetLeaderId());
    response->set_message("not leader; retry on leader");
    return grpc::Status::OK;
  }

  auto st = raft_node_->AddPeer(
      static_cast<int32_t>(request->node_id()),
      request->raft_advertise_address());
  response->set_success(st.ok());
  if (st.ok()) {
    response->set_message("peer added");
  } else {
    response->set_message(std::string(st.message()));
    response->set_current_leader_id(raft_node_->GetLeaderId());
    total_errors_++;
  }
  return grpc::Status::OK;
}

grpc::Status InternalService::RemovePeer(
    grpc::ServerContext* /*context*/,
    const proto::internal::RemovePeerRequest* request,
    proto::internal::RemovePeerResponse* response) {
  total_requests_++;

  if (!raft_node_) {
    response->set_success(false);
    response->set_message("single-node mode; no Raft cluster to modify");
    return grpc::Status::OK;
  }

  if (!raft_node_->IsLeader()) {
    response->set_success(false);
    response->set_current_leader_id(raft_node_->GetLeaderId());
    response->set_message("not leader; retry on leader");
    return grpc::Status::OK;
  }

  auto st = raft_node_->RemovePeer(static_cast<int32_t>(request->node_id()));
  response->set_success(st.ok());
  if (st.ok()) {
    response->set_message("peer removed");
  } else {
    response->set_message(std::string(st.message()));
    response->set_current_leader_id(raft_node_->GetLeaderId());
    total_errors_++;
  }
  return grpc::Status::OK;
}

// =============================================================================
// Raft Scale Reconciliation (roadmap 1.8)
// =============================================================================
//
// Observes and mutates Raft membership on behalf of the operator's scale
// reconciler. GetRaftMembership is read-only and safe on any pod;
// TransferLeadership is leader-only and returns current_leader_id for
// non-leader redirects (same pattern as RemovePeer above).

grpc::Status InternalService::GetRaftMembership(
    grpc::ServerContext* /*context*/,
    const proto::internal::GetRaftMembershipRequest* /*request*/,
    proto::internal::GetRaftMembershipResponse* response) {
  total_requests_++;

  if (!raft_node_) {
    // Single-node mode: the "cluster" is just self. Report node_id=1
    // (coordinator convention) so the operator's scale logic treats this
    // as a single-member cluster rather than an empty one.
    auto* m = response->add_members();
    m->set_node_id(1);
    m->set_raft_endpoint("");
    m->set_is_learner(false);
    response->set_current_leader_id(1);
    return grpc::Status::OK;
  }

  for (const auto& m : raft_node_->GetClusterMembership()) {
    auto* out = response->add_members();
    out->set_node_id(static_cast<uint32_t>(m.node_id));
    out->set_raft_endpoint(m.endpoint);
    out->set_is_learner(m.is_learner);
  }
  response->set_current_leader_id(raft_node_->GetLeaderId());
  return grpc::Status::OK;
}

grpc::Status InternalService::TransferLeadership(
    grpc::ServerContext* /*context*/,
    const proto::internal::TransferLeadershipRequest* request,
    proto::internal::TransferLeadershipResponse* response) {
  total_requests_++;

  if (!raft_node_) {
    response->set_success(false);
    response->set_message("single-node mode; no Raft leader to transfer");
    return grpc::Status::OK;
  }
  if (!raft_node_->IsLeader()) {
    response->set_success(false);
    response->set_current_leader_id(raft_node_->GetLeaderId());
    response->set_message("not leader; retry on leader");
    return grpc::Status::OK;
  }

  auto st = raft_node_->YieldLeadership(
      static_cast<int32_t>(request->target_node_id()));
  response->set_success(st.ok());
  if (st.ok()) {
    response->set_message("leadership transferred");
    response->set_current_leader_id(raft_node_->GetLeaderId());
  } else {
    response->set_message(std::string(st.message()));
    response->set_current_leader_id(raft_node_->GetLeaderId());
    total_errors_++;
  }
  return grpc::Status::OK;
}

// =============================================================================
// Two-Phase Primary Swap
// =============================================================================
//
// Both handlers are idempotent: a coordinator that retries after a transient
// failure must not corrupt the local PrimaryTermTracker state.
//
//   PausePrimary: stop accepting writes for `shard_id`. Records
//     last_known_term = new_term - 1 so a subsequent stale-routing write
//     tagged with `term ≤ last_known_term` is rejected with the right
//     error. Replay (already-not-primary) returns OK.
//
//   PreparePromote: claim primary status at `new_term`. Strictly
//     monotonic — re-promote at an equal or older term returns
//     success=false so a buggy double-promote surfaces loudly. Replay
//     at the same term is OK because RecordPrimary is idempotent on
//     (shard, term) match.
//
// Both calls are no-ops on a non-data-node host (tracker is null);
// they return FAILED_PRECONDITION rather than crashing so a misrouted
// RPC is surfaced to the caller.

grpc::Status InternalService::PausePrimary(
    grpc::ServerContext* context,
    const proto::internal::PausePrimaryRequest* request,
    proto::internal::PausePrimaryResponse* response) {
  total_requests_++;

  if (!primary_term_tracker_) {
    total_errors_++;
    response->set_success(false);
    response->set_message("PausePrimary received on a non-data-node host");
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "primary_term_tracker not wired on this service");
  }

  const uint32_t shard_id = request->shard_id();
  const uint64_t new_term = request->new_term();
  const uint32_t new_primary = request->new_primary_node_id();

  if (new_term == 0) {
    total_errors_++;
    response->set_success(false);
    response->set_message("new_term must be > 0");
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "PausePrimary: new_term must be > 0");
  }

  // last_known_term is the term we're being demoted FROM, i.e. the
  // cluster's current term before the swap commits. By contract that's
  // exactly new_term - 1.
  const uint64_t last_known_term = new_term - 1;
  const bool ok = primary_term_tracker_->RecordNotPrimary(shard_id, last_known_term);

  utils::Logger::Instance().Info(
      "PausePrimary: shard={} stepping_down_from_term={} new_primary_node={} record_ok={}",
      shard_id, last_known_term, new_primary, ok);

  // RecordNotPrimary returns false only on term regression — a coordinator
  // bug or a stale retry from a swap that already advanced beyond this
  // term. Either way the local view is already correct (or fresher), so
  // surface the regression as success=true with a diagnostic message
  // rather than failing the swap. Idempotency depends on this.
  response->set_success(true);
  if (!ok) {
    response->set_message(
        "term regression detected; local view already at or beyond new_term, idempotent ok");
  } else {
    response->set_message("paused");
  }
  return grpc::Status::OK;
}

grpc::Status InternalService::PreparePromote(
    grpc::ServerContext* context,
    const proto::internal::PreparePromoteRequest* request,
    proto::internal::PreparePromoteResponse* response) {
  total_requests_++;

  if (!primary_term_tracker_) {
    total_errors_++;
    response->set_success(false);
    response->set_message("PreparePromote received on a non-data-node host");
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "primary_term_tracker not wired on this service");
  }

  const uint32_t shard_id = request->shard_id();
  const uint64_t new_term = request->new_term();

  if (new_term == 0) {
    total_errors_++;
    response->set_success(false);
    response->set_message("new_term must be > 0");
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "PreparePromote: new_term must be > 0");
  }

  const auto snap_before = primary_term_tracker_->Get(shard_id);
  const bool ok = primary_term_tracker_->RecordPrimary(shard_id, new_term);

  if (!ok) {
    // Strictly-greater term regression. This is a real coordinator bug —
    // surface it as a hard failure so the orchestrator's reconcile loop
    // logs it and gives up rather than silently overwriting state.
    // Idempotent replay at the SAME term goes through RecordPrimary's
    // ok-on-match path (returns true), so we won't reach this branch
    // for a benign retry.
    total_errors_++;
    response->set_success(false);
    response->set_message(
        "term regression: refusing to promote at term <= currently-recorded term");
    utils::Logger::Instance().Warn(
        "PreparePromote rejected: shard={} requested_term={} known_term={} (regression)",
        shard_id, new_term, snap_before.term);
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "PreparePromote: term must be strictly greater than known term");
  }

  utils::Logger::Instance().Info(
      "PreparePromote: shard={} promoted_to_term={} (was_primary={}, prev_term={})",
      shard_id, new_term, snap_before.is_primary, snap_before.term);

  response->set_success(true);
  response->set_message("promoted");
  return grpc::Status::OK;
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