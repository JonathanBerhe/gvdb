// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// RPCs that the coordinator answers: shard map, collection metadata,
// query routing, heartbeats and cluster health, Raft membership.

#include "service/internal/internal_service.h"
#include "core/type_names.h"
#include "cluster/node_registry.h"
#include "cluster/coordinator.h"
#include "consensus/raft_node.h"
#include "common/logger.h"
#include "core/types.h"

namespace gvdb {
namespace service {

// =============================================================================
// Shard Management
// =============================================================================

grpc::Status InternalService::GetShardAssignments(
    grpc::ServerContext* /*context*/,
    const proto::internal::GetShardAssignmentsRequest* request,
    proto::internal::GetShardAssignmentsResponse* response) {
  try {
    uint32_t collection_id = request->collection_id();

    common::Logger::Instance().Debug("GetShardAssignments: collection={}", collection_id);

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
    common::Logger::Instance().Error("GetShardAssignments failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::RebalanceShards(
    grpc::ServerContext* /*context*/,
    const proto::internal::RebalanceShardsRequest* request,
    proto::internal::RebalanceShardsResponse* response) {
  try {
    uint32_t collection_id = request->collection_id();
    common::Logger::Instance().Info("RebalanceShards: collection={}", collection_id);

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
    common::Logger::Instance().Error("RebalanceShards failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// =============================================================================
// Collection Metadata
// =============================================================================

grpc::Status InternalService::GetCollectionMetadata(
    grpc::ServerContext* /*context*/,
    const proto::internal::GetCollectionMetadataRequest* request,
    proto::internal::GetCollectionMetadataResponse* response) {
  try {
    // Check if coordinator is available
    if (!coordinator_) {
      common::Logger::Instance().Warn("GetCollectionMetadata: coordinator not available");
      response->set_found(false);
      return grpc::Status::OK;
    }

    // Get collection metadata from coordinator
    absl::StatusOr<meta::CollectionMetadata> metadata_result;

    if (request->has_collection_id()) {
      uint32_t collection_id = request->collection_id();
      common::Logger::Instance().Debug("GetCollectionMetadata: collection_id={}", collection_id);
      metadata_result = coordinator_->GetCollectionMetadata(core::MakeCollectionId(collection_id));
    } else if (request->has_collection_name()) {
      const std::string& collection_name = request->collection_name();
      common::Logger::Instance().Debug("GetCollectionMetadata: collection_name={}", collection_name);
      metadata_result = coordinator_->GetCollectionMetadata(collection_name);
    } else {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "Either collection_id or collection_name must be provided");
    }

    // Check if collection was found
    if (!metadata_result.ok()) {
      common::Logger::Instance().Debug("GetCollectionMetadata: collection not found: {}",
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
    proto_metadata->set_index_type(core::toString(metadata.index_type));

    proto_metadata->set_shard_count(metadata.shard_ids.size());

    common::Logger::Instance().Debug("GetCollectionMetadata: found collection '{}' (id={}, dim={})",
                                     metadata.collection_name,
                                     core::ToUInt32(metadata.collection_id),
                                     metadata.dimension);

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    common::Logger::Instance().Error("GetCollectionMetadata failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// =============================================================================
// Query Routing and Execution
// =============================================================================

grpc::Status InternalService::RouteQuery(
    grpc::ServerContext* /*context*/,
    const proto::internal::RouteQueryRequest* request,
    proto::internal::RouteQueryResponse* response) {
  try {
    const std::string& collection_name = request->collection_name();

    common::Logger::Instance().Debug("RouteQuery: collection={}", collection_name);

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
    //   * writes (prefer_replica == false): primary only: replicas can't
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
        // Every option carries the same primary_term: the term is a
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
        // Always include primary as a last-resort even when unrouteable:
        // the caller may have stale heartbeat data and the primary may
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

    common::Logger::Instance().Debug("RouteQuery: {} shards for collection '{}'",
                                     response->target_shard_ids_size(), collection_name);
    return grpc::Status::OK;

  } catch (const std::exception& e) {
    common::Logger::Instance().Error("RouteQuery failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// =============================================================================
// Health Monitoring
// =============================================================================

grpc::Status InternalService::Heartbeat(
    grpc::ServerContext* /*context*/,
    const proto::internal::HeartbeatRequest* request,
    proto::internal::HeartbeatResponse* response) {
  try {
    const auto& node_info = request->node_info();

    common::Logger::Instance().Debug("Heartbeat: node_id={}, type={}, status={}",
                                     node_info.node_id(),
                                     static_cast<int>(node_info.node_type()),
                                     static_cast<int>(node_info.status()));

    // Update node registry if available
    if (node_registry_) {
      node_registry_->UpdateNode(node_info);
    }

    response->set_acknowledged(true);

    // Send the authoritative per-shard primary view back to the node so
    // the data-node's PrimaryTermTracker can gate writes against term
    // mismatches.
    //
    // Scaling note: GetShardsForNode iterates the full shard table per
    // call, making this loop O(shards) per heartbeat. At the default
    // 10s cadence (heartbeat_sender.cpp:116) and 1k-10k shard scales
    // this is a few hundred microseconds: fine. Beyond ~50k shards
    // we'd want a per-node shard index in ShardManager; not a v1
    // concern.
    if (shard_manager_ && node_info.node_id() > 0) {
      core::NodeId nid = core::MakeNodeId(node_info.node_id());
      auto shards = shard_manager_->GetShardsForNode(nid);
      for (const auto& shard_info : shards) {
        // Primary view: shard_id, current primary_node_id, current term.
        // Same payload sent to every node touching this shard (primary
        // or replica): keeps each data-node's tracker in sync without
        // a separate per-role channel.
        auto* sp = response->add_shard_primaries();
        sp->set_shard_id(core::ToUInt16(shard_info.shard_id));
        sp->set_primary_node_id(core::ToUInt32(shard_info.primary_node));
        sp->set_term(shard_info.primary_term);
      }
    }

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    common::Logger::Instance().Error("Heartbeat failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::GetClusterHealth(
    grpc::ServerContext* /*context*/,
    const proto::internal::GetClusterHealthRequest* /*request*/,
    proto::internal::GetClusterHealthResponse* response) {
  try {
    common::Logger::Instance().Debug("GetClusterHealth");

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
    // so the operator can surface status.lastRebalance.
    // Zero when no rebalance has fired on this coordinator's watch. The
    // value is captured at rebalance completion (wall-clock) rather than
    // reconstructed from a monotonic clock here: that reconstruction was
    // the old approach and broke under NTP steps and process restarts.
    if (coordinator_) {
      auto last = coordinator_->GetLastAutoRebalance();
      if (last && last->completed_unix_ms > 0) {
        response->set_last_rebalance_unix_ms(last->completed_unix_ms);
      }
    }

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    common::Logger::Instance().Error("GetClusterHealth failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::GetLeaderInfo(
    grpc::ServerContext* /*context*/,
    const proto::internal::GetLeaderInfoRequest* /*request*/,
    proto::internal::GetLeaderInfoResponse* response) {
  // SECURITY NOTE: InternalService is unauthenticated today: any in-cluster
  // client that can reach :50051 can read leader identity and term. The
  // network path is restricted to the gvdb namespace by default (and to
  // NetworkPolicy-ed pods in a hardened install), but this surface should
  // gain a token or mTLS check.

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
// Raft Membership
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
  }
  return grpc::Status::OK;
}

grpc::Status InternalService::RemovePeer(
    grpc::ServerContext* /*context*/,
    const proto::internal::RemovePeerRequest* request,
    proto::internal::RemovePeerResponse* response) {
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
  }
  return grpc::Status::OK;
}

// =============================================================================
// Raft Scale Reconciliation
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
  }
  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gvdb
