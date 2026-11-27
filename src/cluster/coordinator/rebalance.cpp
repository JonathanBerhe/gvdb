// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Shard rebalancing: running a rebalance plan, one shard move, and the
// recovery of moves that a restart interrupted.

#include "cluster/coordinator/coordinator.h"
#include "cluster/coordinator/internal_client.h"
#include "common/logger.h"
#include "absl/strings/str_cat.h"
#include <mutex>
#include <set>

namespace gvdb {
namespace cluster {

namespace {

// RAII scope guard: invokes the stored callable on destruction.
template <typename Fn>
class ScopeGuard {
 public:
  explicit ScopeGuard(Fn fn) : fn_(std::move(fn)) {}
  ~ScopeGuard() { fn_(); }
  ScopeGuard(const ScopeGuard&) = delete;
  ScopeGuard& operator=(const ScopeGuard&) = delete;
 private:
  Fn fn_;
};

}  // namespace

// ============================================================================
// Dynamic Shard Rebalancing
// ============================================================================

absl::StatusOr<uint32_t> Coordinator::ExecuteRebalancePlan(
    core::CollectionId collection_id) {
  if (!client_factory_) {
    return absl::FailedPreconditionError(
        "Distributed mode not enabled (no client factory)");
  }

  auto moves = shard_manager_->CalculateRebalancePlan();
  if (moves.empty()) return uint32_t{0};

  common::Logger::Instance().Info("Rebalance plan: {} moves proposed", moves.size());

  // Cap moves per cycle
  if (moves.size() > kMaxMovesPerCycle) {
    moves.resize(kMaxMovesPerCycle);
  }

  uint32_t success_count = 0;

  for (const auto& move : moves) {
    // Resolve which collection owns this shard and the shard index
    core::CollectionId resolved_cid = core::CollectionId(0);
    uint32_t shard_index = 0;
    bool found = false;

    {
      std::shared_lock lock(collection_mutex_);
      for (const auto& [cid, meta] : collections_) {
        // Filter by collection if specified
        if (core::ToUInt32(collection_id) != 0 && cid != collection_id) continue;

        for (size_t i = 0; i < meta.shard_ids.size(); ++i) {
          if (meta.shard_ids[i] == move.shard_id) {
            resolved_cid = cid;
            shard_index = static_cast<uint32_t>(i);
            found = true;
            break;
          }
        }
        if (found) break;
      }
    }

    if (!found) {
      common::Logger::Instance().Warn(
          "Rebalance: shard {} not found in any collection, skipping",
          core::ToUInt16(move.shard_id));
      continue;
    }

    auto status = ExecuteSingleMove(move, resolved_cid, shard_index);
    if (status.ok()) {
      ++success_count;
    } else {
      common::Logger::Instance().Error(
          "Rebalance: move shard {} failed: {}",
          core::ToUInt16(move.shard_id), status.message());
    }
  }

  common::Logger::Instance().Info(
      "Rebalance complete: {}/{} moves succeeded", success_count, moves.size());
  return success_count;
}

absl::Status Coordinator::ExecuteSingleMove(
    const ShardManager::RebalanceMove& move,
    core::CollectionId collection_id,
    uint32_t shard_index) {

  auto shard_key = core::ToUInt16(move.shard_id);

  // 1. Check and track migration
  {
    std::lock_guard lock(migration_mutex_);
    if (shards_migrating_.count(move.shard_id) > 0) {
      return absl::AlreadyExistsError(
          absl::StrCat("Shard ", shard_key, " already migrating"));
    }
    shards_migrating_.insert(move.shard_id);
  }

  // RAII: remove from migrating set on any exit path (including exceptions)
  ScopeGuard migration_guard([this, shard_id = move.shard_id]() {
    std::lock_guard lock(migration_mutex_);
    shards_migrating_.erase(shard_id);
  });

  // 2. Set shard to MIGRATING
  auto state_status = shard_manager_->SetShardState(
      move.shard_id, ShardState::MIGRATING);
  if (!state_status.ok()) return state_status;

  // 3. Compute segment ID
  core::SegmentId seg_id = core::ShardSegmentId(collection_id, shard_index);

  common::Logger::Instance().Info(
      "Rebalance: migrating shard {} (segment {}) from node {} to node {}",
      shard_key, core::ToUInt32(seg_id),
      core::ToUInt32(move.source_node), core::ToUInt32(move.target_node));

  // 4. Replicate data to target
  auto rep_status = ReplicateSegmentData(
      seg_id, move.source_node, move.target_node);
  if (!rep_status.ok()) {
    common::Logger::Instance().Error(
        "Rebalance: replication failed for shard {}: {}",
        shard_key, rep_status.message());
    shard_manager_->SetShardState(move.shard_id, ShardState::ACTIVE).IgnoreError();
    return rep_status;
  }

  // 5. Update shard mapping. Primary moves go through the two-phase
  //    swap so a write in flight from a stale-routing proxy bounces
  //    with ABORTED instead of committing on the demoted node.
  //    Replica moves just edit the assignment list.
  if (move.is_primary) {
    auto xfer_status = TransferPrimary(move.shard_id, move.source_node,
                                       move.target_node);
    if (!xfer_status.ok()) {
      common::Logger::Instance().Warn(
          "Rebalance: primary swap failed for shard {}: {} (will retry next cycle)",
          core::ToUInt16(move.shard_id), xfer_status.message());
      shard_manager_->SetShardState(move.shard_id, ShardState::ACTIVE).IgnoreError();
      return xfer_status;
    }
  } else {
    shard_manager_->AddReplica(move.shard_id, move.target_node).IgnoreError();
  }

  // 6. Delete segment from source (best-effort)
  auto* client = GetOrCreateDataNodeClient(move.source_node);
  if (client) {
    proto::internal::DeleteSegmentRequest del_req;
    del_req.set_segment_id(
        static_cast<uint64_t>(core::ToUInt32(seg_id)));
    del_req.set_force(true);
    proto::internal::DeleteSegmentResponse del_resp;
    grpc::ClientContext del_ctx;
    auto del_status = client->DeleteSegment(&del_ctx, del_req, &del_resp);
    if (!del_status.ok()) {
      common::Logger::Instance().Warn(
          "Rebalance: failed to delete segment {} from source node {}: {}",
          core::ToUInt32(seg_id), core::ToUInt32(move.source_node),
          del_status.error_message());
    }
  }

  // 7. Remove source from shard mapping (only for replica moves:
  //    for primary moves, SetPrimaryNode already replaced the old primary)
  if (!move.is_primary) {
    shard_manager_->RemoveReplica(move.shard_id, move.source_node).IgnoreError();
  }

  // 8. Set shard back to ACTIVE
  shard_manager_->SetShardState(move.shard_id, ShardState::ACTIVE).IgnoreError();

  common::Logger::Instance().Info(
      "Rebalance: shard {} migration complete", shard_key);
  PersistRegistry();
  return absl::OkStatus();
}

void Coordinator::RecoverMigratingShards() {
  // TODO: A crash between replication and remap leaves orphaned segment data
  // on the target node. Add a background GC that reconciles actual segments
  // against shard mappings to reclaim leaked storage.
  auto all_shards = shard_manager_->GetAllShards();
  for (const auto& shard : all_shards) {
    if (shard.state == ShardState::MIGRATING) {
      common::Logger::Instance().Warn(
          "Recovering shard {} from MIGRATING state (crash recovery)",
          core::ToUInt16(shard.shard_id));
      shard_manager_->SetShardState(shard.shard_id, ShardState::ACTIVE).IgnoreError();
    }
  }
}

}  // namespace cluster
}  // namespace gvdb
