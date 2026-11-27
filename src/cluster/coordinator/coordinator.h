// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "cluster/coordinator/internal_client.h"
#include "cluster/coordinator/node_registry.h"
#include "cluster/shard_manager.h"
#include "core/types.h"
#include "meta/collection_metadata.h"
#include "meta/node_info.h"
#include "storage/backup/backup_manager.h"
#include "storage/backup/restore_manager.h"

namespace gvdb {
namespace cluster {

// Auto-rebalance configuration. `debounce` caps how often a
// new-node event can trigger ExecuteRebalancePlan: a rolling restart of N
// pods within the window only fires one rebalance. Tests pass a very small
// value to drive the debounce path deterministically.
struct AutoRebalanceConfig {
  std::chrono::seconds debounce{60};
};

// The optional collaborators and settings of a Coordinator. They are
// defined outside the class, so that the constructor can use `= {}` as the
// default argument.
struct CoordinatorOptions {
  // Every member has a default member initializer. Clang then accepts a
  // designated initializer that leaves the member out.

  // Dials the data nodes. Null turns off distributed mode: the coordinator
  // then keeps only the registry and makes no RPC to a data node.
  std::shared_ptr<IInternalServiceClientFactory> client_factory = nullptr;
  AutoRebalanceConfig auto_rebalance{};
  // Run the multi-shard backup and restore. Null makes
  // StartBackupDistributed and StartRestoreDistributed return
  // UNIMPLEMENTED.
  std::shared_ptr<storage::BackupManager> backup_manager = nullptr;
  std::shared_ptr<storage::RestoreManager> restore_manager = nullptr;
  // The folder of the persisted registry. When it is set, every registry
  // change rewrites <registry_dir>/registry.json, and LoadRegistry reads it.
  // Empty keeps the registry in memory only.
  std::string registry_dir{};
};

// Coordinator manages cluster metadata and operations
class Coordinator {
 public:
  explicit Coordinator(std::shared_ptr<ShardManager> shard_manager,
                       std::shared_ptr<NodeRegistry> node_registry,
                       CoordinatorOptions options = {});
  ~Coordinator();

  // Cluster initialization
  absl::Status Start(const std::string& bind_address);
  absl::Status Shutdown();

  // Node registration and health
  absl::Status RegisterNode(const meta::NodeInfo& node_info);
  std::vector<meta::NodeInfo> GetHealthyNodes(meta::NodeType type) const;

  // Collection management
  absl::StatusOr<core::CollectionId> CreateCollection(
      const std::string& name, core::Dimension dimension, core::MetricType metric_type,
      core::IndexType index_type, size_t replication_factor, size_t num_shards = 1);
  absl::Status DropCollection(const std::string& name);
  absl::StatusOr<meta::CollectionMetadata> GetCollectionMetadata(const std::string& name) const;
  absl::StatusOr<meta::CollectionMetadata> GetCollectionMetadata(core::CollectionId id) const;
  std::vector<meta::CollectionMetadata> ListCollections() const;

  // Shard assignment for collections
  absl::Status AssignShardsToCollection(core::CollectionId collection_id, size_t num_shards,
                                        size_t replication_factor);

  // Failure detection and recovery
  void StartHealthCheckLoop();
  void StopHealthCheckLoop();

  // Read repair: run consistency check (public for testing)
  void RunConsistencyCheck();
  size_t GetRepairQueueSize() const;

  // Replicate segment data from source node to target node
  absl::Status ReplicateSegmentData(core::SegmentId segment_id, core::NodeId source_node,
                                    core::NodeId target_node);

  // Handle failed node: reassign shards, promote replicas
  void HandleFailedNode(core::NodeId failed_node_id);

  // Fires a debounced ExecuteRebalancePlan whenever a new healthy data
  // node appears in the registry that the coordinator hasn't seen before.
  // Runs the plan in a detached thread so it does not block the
  // health-check loop (rebalance is bounded at kMaxMovesPerCycle, but each
  // ReplicateSegmentData call can take seconds). Called from the
  // health-check loop; public so tests can drive it directly.
  void DetectNewDataNodes();

  // Public for testing: snapshot the set of data-node IDs the coordinator
  // currently considers "known" for auto-rebalance bookkeeping.
  // Tests use this to assert that new nodes are tracked and that pruning
  // on unregister works.
  std::set<core::NodeId> KnownDataNodesForTesting() const;

  // Observable state for the most recent auto-rebalance attempt.
  // Returns std::nullopt if no auto-rebalance has run yet. The returned
  // pair is (status, steady-clock timestamp of completion). Exposed for
  // dashboards, post-incident forensics, and tests that need to assert
  // a rebalance actually fired without racing on the detached worker.
  struct LastAutoRebalance {
    absl::Status status;
    // Monotonic timestamp: stable across small wall-clock jumps but reset
    // per-process. Use `completed_unix_ms` for anything that needs a wall
    // time or needs to survive (not really; still process-local) an
    // exporter restart.
    std::chrono::steady_clock::time_point completed_at;
    // Wall-clock Unix-ms captured at rebalance completion. Stable against
    // NTP adjustments AFTER the fact but of course reflects whatever time
    // the coordinator believed it was at capture. Zero when unset.
    // Exposed to operators via InternalService.GetClusterHealth so they
    // don't have to reconstruct wall time from steady_clock at read.
    int64_t completed_unix_ms = 0;
    uint32_t moves = 0;
  };
  std::optional<LastAutoRebalance> GetLastAutoRebalance() const;

  // Block until the most recent auto-rebalance worker finishes (or the
  // timeout elapses). Returns true if the worker joined within the
  // deadline. Tests use this to deterministically sequence against the
  // detached rebalance thread.
  bool WaitForAutoRebalanceIdleForTesting(std::chrono::milliseconds timeout);

  // Migrate shards off a node that has signalled NODE_STATUS_DRAINING.
  // Unlike HandleFailedNode, the draining node is still
  // alive and serving reads: we can safely promote replicas and remove
  // the draining node from the shard assignment map before its gRPC server
  // is shut down. Called from the health-check loop; safe to invoke
  // repeatedly (self-limiting: once shards are migrated, GetShardsForNode
  // returns empty and the node is unregistered).
  void HandleDrainingNode(core::NodeId draining_node_id);

  // Dynamic shard rebalancing
  // Returns the number of shards successfully moved. collection_id(0) = all.
  absl::StatusOr<uint32_t> ExecuteRebalancePlan(
      core::CollectionId collection_id = core::CollectionId(0));

  // Reset shards stuck in MIGRATING state (called on startup / after crash)
  void RecoverMigratingShards();

  // =========================================================================
  // Backup / Restore orchestration
  // =========================================================================
  //
  // Registry persistence. The collection registry (metadata, name map, id
  // counter) and the cluster shard assignments (primary, replicas,
  // primary_term) live only in memory; without persistence a coordinator
  // restart orphans every collection even though the segment data is intact
  // on the data nodes and in object storage.
  //
  // CoordinatorOptions::registry_dir enables persistence: after every
  // registry mutation the full state is atomically rewritten to
  // <dir>/registry.json. LoadRegistry restores it on startup (a missing
  // file is not an error: first boot). Call LoadRegistry before serving
  // traffic.
  absl::Status LoadRegistry();

  // Start a distributed backup of the collection. Returns the backup_id
  // immediately; the multi-shard fan-out runs asynchronously and is
  // observable via GetBackupStatus. Each shard is backed up by:
  //   1. FreezeWrites on the shard's primary
  //   2. BackupShard on the shard's primary
  //   3. UnfreezeWrites on the shard's primary
  // On any per-shard failure the partial backup is cleaned up under the
  // target prefix and the job transitions to FAILED.
  absl::StatusOr<std::string> StartBackupDistributed(const std::string& collection_name,
                                                     const storage::BackupTarget& target,
                                                     const std::string& requested_backup_id = "");

  // Start a distributed restore. If `overwrite` is true and a collection
  // with `target_collection_name` already exists, it is dropped before a
  // fresh collection is created from the backup manifest. If `overwrite`
  // is false and the target name exists, the call fails with
  // ALREADY_EXISTS.
  absl::StatusOr<std::string> StartRestoreDistributed(const storage::BackupTarget& source,
                                                      const std::string& backup_id,
                                                      const std::string& target_collection_name,
                                                      bool overwrite);

  // Status queries: pass through to the underlying BackupManager /
  // RestoreManager. Returns NotFound when the id is unknown.
  absl::StatusOr<storage::BackupJobStatus> GetDistributedBackupStatus(
      const std::string& backup_id) const;
  absl::StatusOr<storage::RestoreJobStatus> GetDistributedRestoreStatus(
      const std::string& restore_id) const;

 private:
  // Shard manager
  std::shared_ptr<ShardManager> shard_manager_;

  // Node registry (single source of truth for node state)
  std::shared_ptr<NodeRegistry> node_registry_;

  // Collection registry
  mutable std::shared_mutex collection_mutex_;
  std::map<core::CollectionId, meta::CollectionMetadata> collections_;
  std::map<std::string, core::CollectionId> collection_name_to_id_;

  // Registry persistence. Empty path disables it. PersistRegistryLocked
  // requires collection_mutex_ held (shared or unique); PersistRegistry
  // takes the shared lock itself, for callers that mutate only shard
  // assignments.
  const std::string registry_path_;
  void PersistRegistryLocked() const;
  void PersistRegistry() const;

  // ID generation
  std::atomic<uint32_t> next_collection_id_{1};

  // Health check
  std::atomic<bool> running_{false};
  std::unique_ptr<std::thread> health_check_thread_;
  void HealthCheckLoop();
  void DetectFailedNodes();
  void DetectDrainingNodes();
  void CheckReplication();
  void CheckConsistency();
  absl::Status RepairSegment(core::SegmentId segment_id, core::NodeId primary_node,
                             core::NodeId stale_replica);

  // Repair state
  static constexpr uint32_t kRepairIntervalCycles = 6;  // every ~30s
  uint32_t health_check_cycle_count_{0};
  mutable std::mutex repair_mutex_;
  std::set<core::ShardId> shards_being_repaired_;

  // Rebalancing internals
  absl::Status ExecuteSingleMove(const ShardManager::RebalanceMove& move,
                                 core::CollectionId collection_id, uint32_t shard_index);

  // Two-phase primary swap orchestrator. Drives the protocol that lets
  // a write-in-flight survive a primary change without losing
  // linearizability:
  //   1. Read current term from ShardManager; new_term = current + 1.
  //   2. PausePrimary(old_node, shard, new_term, new_node): old node
  //      stops accepting writes for this shard.
  //   3. PreparePromote(new_node, shard, new_term): new node starts
  //      accepting writes at the bumped term.
  //   4. ShardManager::SetPrimaryNode(shard, new_node, new_term):
  //      RouteQuery now points the proxy at new_node@new_term.
  //
  // On any RPC failure the state is recoverable: each step is
  // idempotent and the ShardManager commit hasn't happened yet, so the
  // next reconcile pass repeats the orchestration. Callers should
  // surface the returned status to their reconcile loop and not retry
  // synchronously: a stuck swap blocks all writes for the shard.
  //
  // `old_node` may equal `kInvalidNodeId` when no current primary
  // exists (shard previously unowned, e.g. a cold-promote during
  // recovery). PausePrimary is then skipped.
  absl::Status TransferPrimary(core::ShardId shard_id, core::NodeId old_node,
                               core::NodeId new_node);
  static constexpr size_t kMaxMovesPerCycle = 4;
  mutable std::mutex migration_mutex_;
  std::set<core::ShardId> shards_migrating_;

  // Auto-rebalance bookkeeping.
  //
  // Lock order (to prevent nested-lock deadlock): NodeRegistry's internal
  // mutex is acquired INSIDE GetHealthyNodesByType()/GetDrainingNodes() and
  // released before we take `known_nodes_mutex_`. NEVER hold
  // `known_nodes_mutex_` while calling back into NodeRegistry.
  //
  // `known_data_nodes_` tracks IDs we've already noticed so DetectNewDataNodes
  // only fires on genuine scale-up events. `last_auto_rebalance_at_` is the
  // debounce timestamp: guarded by `known_nodes_mutex_` so external callers
  // (tests) don't race with the health-check thread. `auto_rebalance_config_`
  // is immutable after construction.
  AutoRebalanceConfig auto_rebalance_config_;
  mutable std::mutex known_nodes_mutex_;
  std::set<core::NodeId> known_data_nodes_;                         // guarded by known_nodes_mutex_
  std::chrono::steady_clock::time_point last_auto_rebalance_at_{};  // guarded by known_nodes_mutex_

  // Serializes access to the detached rebalance worker. Only one rebalance
  // is permitted in flight at a time; see `rebalance_in_flight_`. On
  // shutdown, Shutdown() joins this thread to prevent the worker from
  // touching a destroyed coordinator.
  mutable std::mutex rebalance_worker_mutex_;
  std::unique_ptr<std::thread> rebalance_worker_;
  std::atomic<bool> rebalance_in_flight_{false};

  // Last observable auto-rebalance result. Guarded by `rebalance_worker_mutex_`
  // because it is set by the worker thread and read by observers.
  std::optional<LastAutoRebalance> last_auto_rebalance_;

  // Helper methods
  core::CollectionId AllocateCollectionId();
  core::Timestamp GetCurrentTimestamp() const;

  // Client factory for distributed segment creation
  std::shared_ptr<IInternalServiceClientFactory> client_factory_;

  // Client cache for calling data nodes
  mutable std::shared_mutex client_mutex_;
  std::map<core::NodeId, std::unique_ptr<IInternalServiceClient>> data_node_clients_;

  // Get or create client for a data node
  IInternalServiceClient* GetOrCreateDataNodeClient(core::NodeId node_id);

  // Backup / restore engines. Null on a coordinator that doesn't host
  // backups; the public Start* methods return UNIMPLEMENTED in that case.
  std::shared_ptr<storage::BackupManager> backup_manager_;
  std::shared_ptr<storage::RestoreManager> restore_manager_;
};

}  // namespace cluster
}  // namespace gvdb