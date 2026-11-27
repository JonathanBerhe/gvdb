// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Collection create, drop, lookup and list, and the shard assignment of a
// new collection.

#include <chrono>
#include <mutex>
#include <ranges>
#include "absl/strings/str_cat.h"
#include "cluster/coordinator/coordinator.h"
#include "cluster/coordinator/internal_client.h"
#include "common/logger.h"
#include "core/status.h"
#include "core/type_names.h"

namespace gvdb::cluster {

absl::StatusOr<core::CollectionId> Coordinator::CreateCollection(
    const std::string& name, core::Dimension dimension, core::MetricType metric_type,
    core::IndexType index_type, size_t replication_factor, size_t num_shards) {
  std::unique_lock lock(collection_mutex_);

  if (num_shards == 0) num_shards = 1;

  if (collection_name_to_id_.contains(name)) {
    return absl::AlreadyExistsError(absl::StrCat("Collection already exists: ", name));
  }

  core::CollectionId collection_id = AllocateCollectionId();

  meta::CollectionMetadata metadata;
  metadata.collection_id = collection_id;
  metadata.collection_name = name;
  metadata.dimension = dimension;
  metadata.metric_type = metric_type;
  metadata.index_type = index_type;
  metadata.num_shards = num_shards;
  metadata.replication_factor = replication_factor;
  metadata.created_at = GetCurrentTimestamp();
  metadata.updated_at = metadata.created_at;

  collections_[collection_id] = metadata;
  collection_name_to_id_[name] = collection_id;

  common::Logger::Instance().Info(
      "Created collection '{}' ID={} (dim={}, shards={}, replication={})", name,
      core::ToUInt32(collection_id), dimension, num_shards, replication_factor);

  // Assign shards to nodes
  auto assign_status = AssignShardsToCollection(collection_id, num_shards, replication_factor);
  if (!assign_status.ok()) {
    collections_.erase(collection_id);
    collection_name_to_id_.erase(name);
    return assign_status;
  }

  PersistRegistryLocked();

  std::string metric_str = core::ToString(metric_type);
  std::string index_str = core::ToString(index_type);

  if (!client_factory_) {
    common::Logger::Instance().Info("Collection '{}' created (distributed mode disabled)", name);
    return collection_id;
  }

  // Create one segment per shard on its primary node
  const auto& shard_ids = collections_[collection_id].shard_ids;

  for (uint32_t i = 0; i < shard_ids.size(); ++i) {
    core::ShardId shard_id = shard_ids[i];
    core::SegmentId seg_id = core::ShardSegmentId(collection_id, i);

    auto primary_result = shard_manager_->GetPrimaryNode(shard_id);
    if (!primary_result.ok() || *primary_result == core::kInvalidNodeId) {
      common::Logger::Instance().Warn("No primary node for shard {}, skipping segment",
                                      core::ToUInt16(shard_id));
      continue;
    }

    auto* client = GetOrCreateDataNodeClient(*primary_result);
    if (!client) continue;

    // Push the initial primary record before creating the segment. The
    // data-node's write gate only accepts a write whose stamped term
    // exactly matches its local record, and without this push the
    // record arrives on the next heartbeat cycle, leaving a multi-
    // second window where create -> insert bounces with "no primary
    // record" (fresh node) or a stale-term ABORTED (warm node, since
    // assignment bumped the term). Best-effort: a failed push is
    // healed by the heartbeat path, so warn and continue rather than
    // failing the create.
    auto view = shard_manager_->GetPrimaryNodeAndTerm(shard_id);
    if (view.ok()) {
      proto::internal::PreparePromoteRequest promote_req;
      promote_req.set_shard_id(core::ToUInt16(shard_id));
      promote_req.set_new_term(view->term);
      proto::internal::PreparePromoteResponse promote_resp;
      grpc::ClientContext promote_ctx;
      promote_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
      auto promote_status = client->PreparePromote(&promote_ctx, promote_req, &promote_resp);
      if (!promote_status.ok() || !promote_resp.success()) {
        common::Logger::Instance().Warn(
            "CreateCollection: initial PreparePromote for shard {} on node {} "
            "did not apply ({}); write gate will sync via heartbeat",
            core::ToUInt16(shard_id), core::ToUInt32(*primary_result),
            promote_status.ok() ? promote_resp.message() : promote_status.error_message());
      }
    }

    proto::internal::CreateSegmentRequest request;
    request.set_segment_id(static_cast<uint64_t>(core::ToUInt32(seg_id)));
    request.set_collection_id(core::ToUInt32(collection_id));
    request.set_dimension(dimension);
    request.set_metric_type(metric_str);
    request.set_index_type(index_str);

    proto::internal::CreateSegmentResponse response;
    grpc::ClientContext context;

    grpc::Status status = client->CreateSegment(&context, request, &response);
    if (status.ok() && response.success()) {
      common::Logger::Instance().Info("Created segment {} (shard {}) on node {}",
                                      core::ToUInt32(seg_id), i, core::ToUInt32(*primary_result));
    } else {
      std::string err = status.ok() ? response.message() : status.error_message();
      common::Logger::Instance().Error("Failed to create segment {} on node {}: {}",
                                       core::ToUInt32(seg_id), core::ToUInt32(*primary_result),
                                       err);
    }
  }

  common::Logger::Instance().Info("Collection '{}' created with {} shards", name, num_shards);

  return collection_id;
}

absl::Status Coordinator::DropCollection(const std::string& name) {
  std::unique_lock lock(collection_mutex_);

  auto name_it = collection_name_to_id_.find(name);
  if (name_it == collection_name_to_id_.end()) {
    return absl::NotFoundError(absl::StrCat("Collection not found: ", name));
  }

  core::CollectionId collection_id = name_it->second;

  // Get collection metadata before erasing
  auto coll_it = collections_.find(collection_id);
  if (coll_it == collections_.end()) {
    return absl::InternalError("Collection metadata inconsistency");
  }

  meta::CollectionMetadata metadata = coll_it->second;  // Copy metadata

  // Distributed cleanup (only if factory is provided)
  if (client_factory_) {
    common::Logger::Instance().Info("Starting distributed cleanup for collection '{}' (ID: {})",
                                    name, core::ToUInt32(collection_id));

    // Delete all shard segments from their respective nodes
    for (uint32_t i = 0; i < metadata.shard_ids.size(); ++i) {
      core::ShardId shard_id = metadata.shard_ids[i];
      core::SegmentId seg_id = core::ShardSegmentId(collection_id, i);

      auto primary_result = shard_manager_->GetPrimaryNode(shard_id);
      if (primary_result.ok() && *primary_result != core::kInvalidNodeId) {
        auto* client = GetOrCreateDataNodeClient(*primary_result);
        if (client) {
          proto::internal::DeleteSegmentRequest del_req;
          del_req.set_segment_id(static_cast<uint64_t>(core::ToUInt32(seg_id)));
          del_req.set_force(true);
          proto::internal::DeleteSegmentResponse del_resp;
          grpc::ClientContext del_ctx;
          client->DeleteSegment(&del_ctx, del_req, &del_resp);
        }
      }
    }

    // Unassign shards from shard manager
    for (core::ShardId shard_id : metadata.shard_ids) {
      // Clear primary assignment
      auto status = shard_manager_->SetPrimaryNode(shard_id, core::kInvalidNodeId);
      if (!status.ok()) {
        common::Logger::Instance().Warn("Failed to clear primary for shard {}: {}",
                                        core::ToUInt16(shard_id), status.message());
      }

      // Remove all replicas
      auto replicas_result = shard_manager_->GetReplicaNodes(shard_id);
      if (replicas_result.ok()) {
        for (core::NodeId replica : replicas_result.value()) {
          if (replica != core::kInvalidNodeId) {
            auto rm_status = shard_manager_->RemoveReplica(shard_id, replica);
            if (!rm_status.ok()) {
              common::Logger::Instance().Warn("Failed to remove replica {} from shard {}: {}",
                                              core::ToUInt32(replica), core::ToUInt16(shard_id),
                                              rm_status.message());
            }
          }
        }
      }
    }
  }

  // Remove from coordinator metadata
  collections_.erase(collection_id);
  collection_name_to_id_.erase(name);

  PersistRegistryLocked();

  common::Logger::Instance().Info("Dropped collection '{}' (ID: {})", name,
                                  core::ToUInt32(collection_id));

  return absl::OkStatus();
}

absl::StatusOr<meta::CollectionMetadata> Coordinator::GetCollectionMetadata(
    const std::string& name) const {
  std::shared_lock lock(collection_mutex_);

  auto name_it = collection_name_to_id_.find(name);
  if (name_it == collection_name_to_id_.end()) {
    return absl::NotFoundError(absl::StrCat("Collection not found: ", name));
  }

  auto it = collections_.find(name_it->second);
  if (it == collections_.end()) {
    return absl::InternalError("Collection metadata inconsistency");
  }

  return it->second;
}

absl::StatusOr<meta::CollectionMetadata> Coordinator::GetCollectionMetadata(
    core::CollectionId id) const {
  std::shared_lock lock(collection_mutex_);

  auto it = collections_.find(id);
  if (it == collections_.end()) {
    return absl::NotFoundError(absl::StrCat("Collection not found: ", core::ToUInt32(id)));
  }

  return it->second;
}

std::vector<meta::CollectionMetadata> Coordinator::ListCollections() const {
  std::shared_lock lock(collection_mutex_);
  return collections_ | std::views::values | std::ranges::to<std::vector>();
}

absl::Status Coordinator::AssignShardsToCollection(core::CollectionId collection_id,
                                                   size_t num_shards, size_t replication_factor) {
  // Get healthy data nodes
  auto data_nodes = GetHealthyNodes(meta::NodeType::DATA_NODE);
  if (data_nodes.empty()) {
    return absl::FailedPreconditionError("No healthy data nodes available");
  }

  if (data_nodes.size() < replication_factor) {
    return absl::FailedPreconditionError(
        absl::StrCat("Not enough data nodes for replication factor ", replication_factor, " (have ",
                     data_nodes.size(), " nodes)"));
  }

  // NOTE: Caller must hold collection_mutex_
  auto it = collections_.find(collection_id);
  if (it == collections_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Collection not found: ", core::ToUInt32(collection_id)));
  }

  // Map per-collection shard index → cluster-wide ShardId. ShardManager
  // pre-allocates the shard pool as ShardId(0)..ShardId(num_shards_-1)
  // in its constructor, so the natural mapping is the identity i→i.
  //
  // The previous implementation used `AssignShard(MakeVectorId(i))`,
  // which is the *vector-routing* function (consistent-hash ring) and
  // does NOT guarantee distinct outputs for distinct inputs: for small
  // num_shards two collection-shard-indices could both map to the same
  // shard_id, silently collapsing a multi-shard collection into one
  // and breaking per-shard fan-out (e.g. backup orchestration).
  const size_t cluster_shards = shard_manager_->GetTotalShards();
  if (num_shards > cluster_shards) {
    return absl::FailedPreconditionError(
        absl::StrCat("Collection requested ", num_shards,
                     " shards but the cluster's "
                     "ShardManager only has ",
                     cluster_shards, " (start the cluster with at least that many shards)"));
  }
  std::vector<core::ShardId> shard_ids;
  shard_ids.reserve(num_shards);
  for (size_t i = 0; i < num_shards; ++i) {
    core::ShardId shard_id = core::MakeShardId(static_cast<uint16_t>(i));
    shard_ids.push_back(shard_id);

    // Set primary node
    core::NodeId primary = data_nodes[i % data_nodes.size()].node_id;
    RETURN_IF_ERROR(shard_manager_->SetPrimaryNode(shard_id, primary));

    // Set replica nodes
    for (size_t r = 1; r < replication_factor; ++r) {
      core::NodeId replica = data_nodes[(i + r) % data_nodes.size()].node_id;
      if (replica != primary) {
        RETURN_IF_ERROR(shard_manager_->AddReplica(shard_id, replica));
      }
    }
  }

  it->second.shard_ids = shard_ids;

  return absl::OkStatus();
}

}  // namespace gvdb::cluster
