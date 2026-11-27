// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Read repair (the consistency check across replicas) and segment copies
// between data nodes.

#include <chrono>
#include <mutex>
#include <set>
#include "absl/strings/str_cat.h"
#include "cluster/coordinator/coordinator.h"
#include "cluster/coordinator/internal_client.h"
#include "common/logger.h"

namespace gvdb::cluster {

// ============================================================================
// Read Repair: Consistency Checking
// ============================================================================

void Coordinator::RunConsistencyCheck() { CheckConsistency(); }

size_t Coordinator::GetRepairQueueSize() const {
  std::scoped_lock lock(repair_mutex_);
  return shards_being_repaired_.size();
}

void Coordinator::CheckConsistency() {
  if (!client_factory_) return;

  std::shared_lock lock(collection_mutex_);
  for (const auto& [coll_id, metadata] : collections_) {
    if (metadata.replication_factor <= 1) continue;

    for (uint32_t i = 0; i < metadata.shard_ids.size(); ++i) {
      core::ShardId shard_id = metadata.shard_ids[i];

      {
        std::scoped_lock repair_lock(repair_mutex_);
        if (shards_being_repaired_.count(shard_id) > 0) continue;
      }

      auto primary_result = shard_manager_->GetPrimaryNode(shard_id);
      if (!primary_result.ok() || *primary_result == core::kInvalidNodeId) continue;

      auto replicas_result = shard_manager_->GetReplicaNodes(shard_id);
      if (!replicas_result.ok() || replicas_result->empty()) continue;

      // Get vector counts from primary
      auto* primary_client = GetOrCreateDataNodeClient(*primary_result);
      if (!primary_client) continue;

      proto::internal::ListSegmentsRequest primary_req;
      primary_req.set_collection_id(core::ToUInt32(coll_id));
      proto::internal::ListSegmentsResponse primary_resp;
      grpc::ClientContext primary_ctx;
      primary_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

      auto primary_status = primary_client->ListSegments(&primary_ctx, primary_req, &primary_resp);
      if (!primary_status.ok()) continue;

      // Sum vector counts across all segments on primary
      uint64_t primary_total = 0;
      for (const auto& seg_info : primary_resp.segments()) {
        primary_total += seg_info.vector_count();
      }

      if (primary_total == 0) continue;

      // Compare with each replica
      for (const auto& replica_node : *replicas_result) {
        if (replica_node == core::kInvalidNodeId) continue;

        auto* replica_client = GetOrCreateDataNodeClient(replica_node);
        if (!replica_client) continue;

        proto::internal::ListSegmentsRequest replica_req;
        replica_req.set_collection_id(core::ToUInt32(coll_id));
        proto::internal::ListSegmentsResponse replica_resp;
        grpc::ClientContext replica_ctx;
        replica_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

        auto replica_status =
            replica_client->ListSegments(&replica_ctx, replica_req, &replica_resp);
        if (!replica_status.ok()) continue;

        uint64_t replica_total = 0;
        for (const auto& seg_info : replica_resp.segments()) {
          replica_total += seg_info.vector_count();
        }

        if (replica_total == primary_total) continue;

        // Divergence detected
        common::Logger::Instance().Warn(
            "ReadRepair: collection {} shard {} diverged: primary node {} "
            "has {} vectors, replica node {} has {} vectors",
            core::ToUInt32(coll_id), core::ToUInt16(shard_id), core::ToUInt32(*primary_result),
            primary_total, core::ToUInt32(replica_node), replica_total);

        {
          std::scoped_lock repair_lock(repair_mutex_);
          shards_being_repaired_.insert(shard_id);
        }

        core::SegmentId seg_id = core::ShardSegmentId(coll_id, i);
        lock.unlock();
        auto repair_status = RepairSegment(seg_id, *primary_result, replica_node);
        lock.lock();

        {
          std::scoped_lock repair_lock(repair_mutex_);
          shards_being_repaired_.erase(shard_id);
        }

        if (repair_status.ok()) {
          common::Logger::Instance().Info("ReadRepair: repaired shard {} on replica node {}",
                                          core::ToUInt16(shard_id), core::ToUInt32(replica_node));
        } else {
          common::Logger::Instance().Error("ReadRepair: failed to repair shard {} on node {}: {}",
                                           core::ToUInt16(shard_id), core::ToUInt32(replica_node),
                                           repair_status.message());
        }

        // Rate limit: one repair per cycle
        return;
      }
    }
  }
}

absl::Status Coordinator::RepairSegment(core::SegmentId segment_id, core::NodeId primary_node,
                                        core::NodeId stale_replica) {
  common::Logger::Instance().Info("ReadRepair: replicating segment {} from node {} to node {}",
                                  core::ToUInt32(segment_id), core::ToUInt32(primary_node),
                                  core::ToUInt32(stale_replica));
  return ReplicateSegmentData(segment_id, primary_node, stale_replica);
}

absl::Status Coordinator::ReplicateSegmentData(core::SegmentId segment_id, core::NodeId source_node,
                                               core::NodeId target_node) {
  if (!client_factory_) {
    return absl::FailedPreconditionError("Distributed mode not enabled");
  }

  // Step 1: Get segment from source node
  auto* source_client = GetOrCreateDataNodeClient(source_node);
  if (!source_client) {
    return absl::UnavailableError(
        absl::StrCat("Cannot reach source node ", core::ToUInt32(source_node)));
  }

  proto::internal::GetSegmentRequest get_req;
  get_req.set_segment_id(static_cast<uint64_t>(core::ToUInt32(segment_id)));
  proto::internal::GetSegmentResponse get_resp;
  grpc::ClientContext get_ctx;

  auto get_status = source_client->GetSegment(&get_ctx, get_req, &get_resp);
  if (!get_status.ok()) {
    return absl::InternalError(absl::StrCat("GetSegment from node ", core::ToUInt32(source_node),
                                            " failed: ", get_status.error_message()));
  }

  if (get_resp.segment_data().empty()) {
    return absl::NotFoundError("Segment data is empty on source node");
  }

  // Step 2: Send segment to target node
  auto* target_client = GetOrCreateDataNodeClient(target_node);
  if (!target_client) {
    return absl::UnavailableError(
        absl::StrCat("Cannot reach target node ", core::ToUInt32(target_node)));
  }

  proto::internal::ReplicateSegmentRequest rep_req;
  auto* seg_info = rep_req.mutable_segment_info();
  if (get_resp.has_segment_info()) {
    *seg_info = get_resp.segment_info();
  }
  rep_req.set_segment_data(get_resp.segment_data());

  proto::internal::ReplicateSegmentResponse rep_resp;
  grpc::ClientContext rep_ctx;

  auto rep_status = target_client->ReplicateSegment(&rep_ctx, rep_req, &rep_resp);
  if (!rep_status.ok()) {
    return absl::InternalError(absl::StrCat("ReplicateSegment to node ",
                                            core::ToUInt32(target_node),
                                            " failed: ", rep_status.error_message()));
  }

  if (!rep_resp.success()) {
    return absl::InternalError(rep_resp.message());
  }

  common::Logger::Instance().Info("Replicated segment {} from node {} to node {}",
                                  core::ToUInt32(segment_id), core::ToUInt32(source_node),
                                  core::ToUInt32(target_node));
  return absl::OkStatus();
}

}  // namespace gvdb::cluster
