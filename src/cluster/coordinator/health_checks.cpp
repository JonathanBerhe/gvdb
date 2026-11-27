// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// The health check loop: draining, new and failed data nodes, and the
// replication check.

#include "cluster/coordinator/coordinator.h"
#include "common/logger.h"
#include "common/metrics.h"
#include <chrono>
#include <mutex>
#include <set>
#include <thread>

namespace gvdb {
namespace cluster {

namespace {

constexpr uint64_t kHealthCheckIntervalMs = 5000;  // 5 seconds

}  // namespace

void Coordinator::StartHealthCheckLoop() {
  health_check_thread_ = std::make_unique<std::thread>([this] { HealthCheckLoop(); });
  common::Logger::Instance().Info("Health check loop started");
}

void Coordinator::StopHealthCheckLoop() {
  if (health_check_thread_ && health_check_thread_->joinable()) {
    health_check_thread_->join();
    common::Logger::Instance().Info("Health check loop stopped");
  }
}

void Coordinator::HealthCheckLoop() {
  // On startup, recover any shards left in MIGRATING state from a prior crash
  RecoverMigratingShards();

  while (running_.load(std::memory_order_acquire)) {
    // Check draining nodes BEFORE failure detection: a cleanly draining node
    // that finishes migration will be unregistered here; if we ran failure
    // detection first, a draining node whose heartbeat had just timed out
    // would be handled as a failure instead (less graceful).
    DetectDrainingNodes();
    DetectFailedNodes();
    // Fire auto-rebalance if any new data node has joined since last cycle.
    // Ordered after drain/failure so we don't rebalance
    // onto nodes that are actively leaving.
    DetectNewDataNodes();
    CheckReplication();

    health_check_cycle_count_++;
    if (health_check_cycle_count_ % kRepairIntervalCycles == 0) {
      CheckConsistency();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(kHealthCheckIntervalMs));
  }
}

void Coordinator::DetectDrainingNodes() {
  // Ask the registry for nodes that are actively draining (status = DRAINING
  // AND heartbeat still fresh). Stale draining nodes are intentionally
  // excluded: they will be handled as hard failures by DetectFailedNodes
  // below. This also avoids wasted iteration over the long tail of never-
  // pruned dead nodes that accumulate in NodeRegistry.
  auto draining = node_registry_->GetDrainingNodes();
  for (const auto& reg : draining) {
    HandleDrainingNode(core::MakeNodeId(reg.info.node_id()));
  }
}

std::set<core::NodeId> Coordinator::KnownDataNodesForTesting() const {
  std::lock_guard lock(known_nodes_mutex_);
  return known_data_nodes_;
}

std::optional<Coordinator::LastAutoRebalance>
Coordinator::GetLastAutoRebalance() const {
  std::lock_guard lock(rebalance_worker_mutex_);
  return last_auto_rebalance_;
}

bool Coordinator::WaitForAutoRebalanceIdleForTesting(
    std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (!rebalance_in_flight_.load(std::memory_order_acquire)) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return !rebalance_in_flight_.load(std::memory_order_acquire);
}

void Coordinator::DetectNewDataNodes() {
  // Lock ordering: NodeRegistry's own shared_mutex is acquired inside this
  // call and released before we take known_nodes_mutex_ below. Never hold
  // known_nodes_mutex_ while calling back into NodeRegistry.
  auto data_nodes = node_registry_->GetHealthyNodesByType(
      proto::internal::NodeType::NODE_TYPE_DATA_NODE);

  std::vector<core::NodeId> newly_seen;
  bool within_debounce = false;
  {
    std::lock_guard lock(known_nodes_mutex_);
    for (const auto& n : data_nodes) {
      if (n.IsDraining()) continue;  // don't count departing nodes
      auto id = core::MakeNodeId(n.info.node_id());
      auto [it, inserted] = known_data_nodes_.insert(id);
      if (inserted) newly_seen.push_back(id);
    }

    if (newly_seen.empty()) return;

    // In single-node / in-process test mode there is no coordinator client
    // to actually execute the rebalance. Tracking is done above so the
    // baseline is correct if a client_factory_ is later wired in.
    if (!client_factory_) return;

    // Debounce: in a rolling restart scenario, several pods come up in
    // quick succession. Only fire the rebalance once per debounce window
    // regardless of how many nodes newly joined. The timestamp is read and
    // written under the same mutex as known_data_nodes_.
    const auto now = std::chrono::steady_clock::now();
    if (last_auto_rebalance_at_ != std::chrono::steady_clock::time_point{} &&
        now - last_auto_rebalance_at_ < auto_rebalance_config_.debounce) {
      within_debounce = true;
    } else {
      last_auto_rebalance_at_ = now;
    }
  }

  std::string ids_str;
  for (size_t i = 0; i < newly_seen.size(); ++i) {
    if (i > 0) ids_str += ",";
    ids_str += std::to_string(core::ToUInt32(newly_seen[i]));
  }

  if (within_debounce) {
    common::Logger::Instance().Info(
        "Auto-rebalance: new data node(s) [{}] seen; skipping rebalance "
        "(within debounce window)", ids_str);
    common::MetricsRegistry::Instance().IncAutoRebalanceDebounced();
    return;
  }

  // Overlap guard: one rebalance thread at a time. If a worker is still
  // running from a previous trigger, drop this one rather than spawn a
  // concurrent rebalance that would double-count moves and log noise.
  bool expected = false;
  if (!rebalance_in_flight_.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel)) {
    common::Logger::Instance().Info(
        "Auto-rebalance: previous worker still in flight; skipping "
        "trigger for new node(s) [{}]", ids_str);
    common::MetricsRegistry::Instance().IncAutoRebalanceDebounced();
    return;
  }

  common::Logger::Instance().Info(
      "Auto-rebalance: new data node(s) [{}] joined; scheduling "
      "ExecuteRebalancePlan", ids_str);
  common::MetricsRegistry::Instance().IncAutoRebalanceTriggered();

  // Join any previous finished worker so we don't leak the thread handle.
  // Then start a new one.
  {
    std::lock_guard lock(rebalance_worker_mutex_);
    if (rebalance_worker_ && rebalance_worker_->joinable()) {
      rebalance_worker_->join();
    }
    rebalance_worker_ = std::make_unique<std::thread>([this]() {
      auto result = ExecuteRebalancePlan(core::CollectionId(0));
      LastAutoRebalance snapshot;
      snapshot.completed_at = std::chrono::steady_clock::now();
      // Capture wall time at completion so downstream readers (the
      // operator's GVDBCluster.status.lastRebalance) don't have to
      // reconstruct it from a monotonic clock: a conversion that breaks
      // under NTP steps and silently drops data across process restarts.
      snapshot.completed_unix_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count();
      if (result.ok()) {
        snapshot.status = absl::OkStatus();
        snapshot.moves = *result;
        if (*result == 0) {
          common::Logger::Instance().Debug(
              "Auto-rebalance completed: 0 shards moved (already balanced)");
        } else {
          common::Logger::Instance().Info(
              "Auto-rebalance completed: {} shards moved", *result);
          common::MetricsRegistry::Instance().AddAutoRebalanceMovesCompleted(
              *result);
        }
      } else {
        snapshot.status = result.status();
        common::Logger::Instance().Warn(
            "Auto-rebalance failed: {}", result.status().message());
        common::MetricsRegistry::Instance().IncAutoRebalanceFailures();
      }
      {
        std::lock_guard inner(rebalance_worker_mutex_);
        last_auto_rebalance_ = snapshot;
      }
      rebalance_in_flight_.store(false, std::memory_order_release);
    });
  }
}

void Coordinator::HandleDrainingNode(core::NodeId draining_node_id) {
  auto shards = shard_manager_->GetShardsForNode(draining_node_id);
  if (shards.empty()) {
    // No shards left on this node: either already drained or the node
    // never held any. Unregister once from ShardManager (idempotent, we
    // tolerate the NotFound on subsequent cycles).
    (void)shard_manager_->UnregisterNode(draining_node_id, /*graceful=*/true);
    {
      // Same pruning as the post-migration path: let a future rejoin
      // be detected as a scale-up event.
      std::lock_guard lock(known_nodes_mutex_);
      known_data_nodes_.erase(draining_node_id);
    }
    return;
  }

  for (const auto& shard_info : shards) {
    auto primary_result = shard_manager_->GetPrimaryNode(shard_info.shard_id);
    if (!primary_result.ok()) continue;
    auto replicas_result = shard_manager_->GetReplicaNodes(shard_info.shard_id);
    std::vector<core::NodeId> replicas;
    if (replicas_result.ok()) replicas = *replicas_result;

    if (*primary_result == draining_node_id) {
      // Case 1: draining node is PRIMARY. Prefer promoting an existing
      // routable replica (fast, no data movement).
      core::NodeId new_primary = core::kInvalidNodeId;
      for (const auto& r : replicas) {
        if (r == draining_node_id) continue;
        if (node_registry_->IsNodeRoutable(core::ToUInt32(r))) {
          new_primary = r;
          break;
        }
      }

      if (new_primary == core::kInvalidNodeId) {
        // No routable replica to promote. Leave this shard for the next
        // cycle: either another node becomes routable or the draining
        // node's heartbeat eventually times out and HandleFailedNode
        // takes over with its own replica-promotion logic. Proactive
        // segment replication to a fresh node is not done here: it needs
        // the collection context to compute ShardSegmentId and bounded
        // concurrency.
        common::Logger::Instance().Warn(
            "Drain: no routable replica for shard {} on draining node {}; "
            "will retry next cycle",
            core::ToUInt16(shard_info.shard_id),
            core::ToUInt32(draining_node_id));
        continue;
      }

      // Primary swap goes through TransferPrimary so the draining node
      // stops accepting writes the moment we commit, instead of writing
      // to a node we've decided is going away.
      auto status = TransferPrimary(shard_info.shard_id,
                                     draining_node_id, new_primary);
      if (status.ok()) {
        common::Logger::Instance().Info(
            "Drain: promoted replica {} to primary for shard {} "
            "(was on draining node {})",
            core::ToUInt32(new_primary),
            core::ToUInt16(shard_info.shard_id),
            core::ToUInt32(draining_node_id));
      } else {
        common::Logger::Instance().Warn(
            "Drain: TransferPrimary failed for shard {} ({}); will retry next cycle",
            core::ToUInt16(shard_info.shard_id),
            status.message());
      }
    } else {
      // Case 2: draining node is a REPLICA. Primary still serves this
      // shard, so we can drop the replica immediately.
      auto status = shard_manager_->RemoveReplica(
          shard_info.shard_id, draining_node_id);
      if (status.ok()) {
        common::Logger::Instance().Info(
            "Drain: removed draining node {} as replica of shard {}",
            core::ToUInt32(draining_node_id),
            core::ToUInt16(shard_info.shard_id));
      }
    }
  }

  // If all shards migrated, finalize by unregistering.
  auto remaining = shard_manager_->GetShardsForNode(draining_node_id);
  if (remaining.empty()) {
    (void)shard_manager_->UnregisterNode(draining_node_id, /*graceful=*/true);
    {
      // Forget this node so a future rejoin (same id) is treated as a
      // genuine scale-up event and auto-rebalance fires again.
      std::lock_guard lock(known_nodes_mutex_);
      known_data_nodes_.erase(draining_node_id);
    }
    common::Logger::Instance().Info(
        "Drain: node {} fully drained, unregistered from shard manager",
        core::ToUInt32(draining_node_id));
  }
}

void Coordinator::CheckReplication() {
  if (!client_factory_) return;
  bool assignments_changed = false;

  std::shared_lock lock(collection_mutex_);
  for (const auto& [coll_id, metadata] : collections_) {
    if (metadata.replication_factor <= 1) continue;

    for (uint32_t i = 0; i < metadata.shard_ids.size(); ++i) {
      core::ShardId shard_id = metadata.shard_ids[i];

      auto primary_result = shard_manager_->GetPrimaryNode(shard_id);
      if (!primary_result.ok() || *primary_result == core::kInvalidNodeId) continue;

      auto replicas_result = shard_manager_->GetReplicaNodes(shard_id);
      size_t actual_replicas = 1;  // Primary counts as 1
      if (replicas_result.ok()) {
        actual_replicas += replicas_result->size();
      }

      if (actual_replicas < metadata.replication_factor) {
        // Under-replicated: find a healthy node to replicate to
        auto healthy_nodes = GetHealthyNodes(meta::NodeType::DATA_NODE);
        for (const auto& node : healthy_nodes) {
          if (node.node_id == *primary_result) continue;
          bool already_replica = false;
          if (replicas_result.ok()) {
            for (const auto& r : *replicas_result) {
              if (r == node.node_id) { already_replica = true; break; }
            }
          }
          if (already_replica) continue;

          // Found a node that doesn't have this shard: replicate
          core::SegmentId seg_id = core::ShardSegmentId(coll_id, i);
          lock.unlock();  // Release lock during RPC
          auto status = ReplicateSegmentData(seg_id, *primary_result, node.node_id);
          lock.lock();    // Re-acquire (collections_ may have changed, but that's OK for a best-effort check)

          if (status.ok()) {
            (void)shard_manager_->AddReplica(shard_id, node.node_id);
            assignments_changed = true;
            common::Logger::Instance().Info(
                "Auto-replicated shard {} (segment {}) to node {}",
                core::ToUInt16(shard_id), core::ToUInt32(seg_id),
                core::ToUInt32(node.node_id));
          }
          break;  // One replication per check cycle per shard
        }
      }
    }
  }
  if (assignments_changed) PersistRegistry();
}

void Coordinator::DetectFailedNodes() {
  auto failed_ids = node_registry_->DetectFailedNodes();

  for (uint32_t node_id : failed_ids) {
    common::Logger::Instance().Warn("Node {} failed (heartbeat timeout)", node_id);
    HandleFailedNode(core::MakeNodeId(node_id));
  }
}

void Coordinator::HandleFailedNode(core::NodeId failed_node_id) {
  // Get all shards assigned to the failed node
  auto shards = shard_manager_->GetShardsForNode(failed_node_id);

  for (const auto& shard_info : shards) {
    auto primary_result = shard_manager_->GetPrimaryNode(shard_info.shard_id);
    if (!primary_result.ok()) continue;

    if (*primary_result == failed_node_id) {
      // This node was primary: try to promote a replica
      auto replicas_result = shard_manager_->GetReplicaNodes(shard_info.shard_id);
      if (!replicas_result.ok()) continue;
      const auto& replicas = *replicas_result;

      core::NodeId new_primary = core::kInvalidNodeId;
      for (const auto& replica : replicas) {
        if (replica != failed_node_id) {
          // Check if replica is healthy
          RegisteredNode node;
          if (node_registry_->GetNode(core::ToUInt32(replica), &node) &&
              node.info.status() == proto::internal::NodeStatus::NODE_STATUS_READY) {
            new_primary = replica;
            break;
          }
        }
      }

      if (new_primary != core::kInvalidNodeId) {
        // Failed-node promotion: pass kInvalidNodeId for old_node so
        // TransferPrimary skips PausePrimary on the dead node (calling
        // it would block forever on UNAVAILABLE) and just bumps the
        // term on the new primary before committing.
        auto status = TransferPrimary(shard_info.shard_id,
                                       core::kInvalidNodeId, new_primary);
        if (status.ok()) {
          common::Logger::Instance().Info(
              "Promoted node {} to primary for shard {} (was on failed node {})",
              core::ToUInt32(new_primary), core::ToUInt16(shard_info.shard_id),
              core::ToUInt32(failed_node_id));
        } else {
          common::Logger::Instance().Warn(
              "Failed-node promotion: TransferPrimary failed for shard {} ({}); "
              "will retry next health-check cycle",
              core::ToUInt16(shard_info.shard_id), status.message());
        }
      } else {
        common::Logger::Instance().Error(
            "No healthy replica available for shard {} (primary node {} failed)",
            core::ToUInt16(shard_info.shard_id), core::ToUInt32(failed_node_id));
      }
    }
  }

  // Remove failed node from shard manager
  shard_manager_->UnregisterNode(failed_node_id, false).IgnoreError();
  {
    // Same pruning as the drain path: allow a future rejoin to be detected
    // as a scale-up event.
    std::lock_guard lock(known_nodes_mutex_);
    known_data_nodes_.erase(failed_node_id);
  }
  common::Logger::Instance().Info("Removed failed node {} from shard manager",
                                  core::ToUInt32(failed_node_id));
  PersistRegistry();
}

}  // namespace cluster
}  // namespace gvdb
