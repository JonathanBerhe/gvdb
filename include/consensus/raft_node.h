// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "consensus/raft_config.h"
#include "consensus/metadata_store.h"
#include "consensus/timestamp_oracle.h"
#include "core/status.h"
#include <atomic>
#include <memory>
#include <mutex>

// Forward declarations for NuRaft
namespace nuraft {
  class raft_launcher;
  class raft_server;
  class state_machine;
  class logger;
}

namespace gvdb {
namespace consensus {

// Forward declarations
class MetadataStateMachine;
class GvdbStateManager;

// A Raft peer derived from the "id:host:port" --raft-peers format (roadmap
// 0b.4). Public so the parser can be unit-tested without spinning up a
// real Raft server.
struct RaftPeerSpec {
  int id = 0;
  std::string endpoint;  // "host:port"
};

// Parse one peer spec. Returns an error if the format is wrong, the id
// isn't an integer, the id is non-positive, or the endpoint is missing
// its port separator. Tolerant of leading/trailing whitespace.
core::StatusOr<RaftPeerSpec> ParseRaftPeerSpec(const std::string& spec);

// Result of preparing a declared peer list for NuRaft bootstrap.
// Callers inspect `needs_seed` to decide whether to replace the current
// cluster_config. `peers` contains all declared peers (including self).
struct PeerListPlan {
  std::vector<RaftPeerSpec> peers;
  bool needs_seed = false;
};

// Validate + prepare a peer list for cluster_config seeding (roadmap 0b.4):
//   * every entry parses
//   * peer ids are unique
//   * `self_id` is present as one of the entries (prevents a rogue member
//     voting only for itself when the chart emits a wrong --node-id)
//   * returns needs_seed=true when the current persisted cluster_config
//     holds <= 1 server (fresh boot or single-member upgrade to HA) — the
//     caller should replace the config with these peers. Otherwise trust
//     the persisted multi-member config and leave it alone so runtime
//     add_srv / remove_srv changes aren't clobbered on restart.
core::StatusOr<PeerListPlan> PrepareRaftPeerList(
    int self_id,
    const std::vector<std::string>& declared_peers,
    size_t persisted_cluster_size);

// Raft node for distributed consensus
// Supports both single-node mode (for development) and multi-node mode (using NuRaft)
//
// Thread-safe: All public methods are thread-safe
class RaftNode {
 public:
  explicit RaftNode(const RaftConfig& config);
  ~RaftNode();

  // Disable copy and move
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  // Lifecycle
  core::Status Start();
  core::Status Shutdown();
  bool IsRunning() const;

  // Leadership (in single-node mode, always true after Start())
  bool IsLeader() const;
  int GetLeaderId() const;
  // Current Raft term as observed by this server. Returns 0 before NuRaft
  // has started or in single-node mode. Exposed so operators can detect
  // term stability across reconciles (roadmap 0b.6.C-hardening follow-up).
  uint64_t GetCurrentTerm() const;
  int GetNodeId() const { return config_.node_id; }

  // Metadata operations (go through consensus)
  core::StatusOr<core::CollectionId> CreateCollection(
      const std::string& name,
      core::Dimension dimension,
      core::MetricType metric_type,
      core::IndexType index_type,
      size_t replication_factor);

  core::Status DropCollection(core::CollectionId collection_id);

  core::StatusOr<cluster::CollectionMetadata> GetCollectionMetadata(
      core::CollectionId id) const;

  core::StatusOr<cluster::CollectionMetadata> GetCollectionMetadata(
      const std::string& name) const;

  std::vector<cluster::CollectionMetadata> ListCollections() const;

  // Node operations
  core::Status RegisterNode(const cluster::NodeInfo& node_info);
  core::Status UnregisterNode(core::NodeId node_id);

  core::StatusOr<cluster::NodeInfo> GetNodeInfo(core::NodeId id) const;
  std::vector<cluster::NodeInfo> ListNodes() const;

  // Raft membership changes (roadmap 1.7b). Both methods are leader-only;
  // they return FailedPreconditionError when called on a follower, carrying
  // the current leader id in the status message so callers can retry.
  //
  // AddPeer wraps NuRaft raft_server::add_srv. Used by the leader handler
  // of the JoinCluster RPC when a new coordinator pod announces itself.
  // On success, NuRaft replicates the cluster_config change via Raft log
  // and the joining pod is caught up via snapshot transfer.
  core::Status AddPeer(int32_t node_id, const std::string& raft_endpoint);

  // RemovePeer wraps NuRaft raft_server::remove_srv. Used by the leader
  // handler of the RemovePeer RPC, typically invoked by a coordinator pod
  // self-removing during graceful shutdown. Cannot remove the current
  // leader (NuRaft rejects with CANNOT_REMOVE_LEADER); caller must either
  // transfer leadership first or let NuRaft re-elect after the pod exits.
  core::Status RemovePeer(int32_t node_id);

  // Raft scale reconciliation (roadmap 1.8). Unlike Add/Remove above,
  // these two methods support the operator's scale-down safety net
  // when a coordinator pod was SIGKILLed without running its self-remove
  // handler, leaving a ghost peer in cluster_config.
  //
  // One entry per current cluster_config member (including self).
  struct RaftMemberInfo {
    int32_t node_id;
    std::string endpoint;
    bool is_learner;
  };
  // Read-only; safe on any member (leader or follower). Returns this
  // server's current view of cluster_config. Thread-safe: NuRaft's
  // get_config() returns an immutable snapshot.
  std::vector<RaftMemberInfo> GetClusterMembership() const;

  // Leader-only. Wraps NuRaft raft_server::yield_leadership with a
  // specific successor. Unlike add_srv/remove_srv, yield_leadership
  // returns void — we synthesize a result by polling GetLeaderId()
  // every 100ms up to 3s. Returns Ok when the target_node_id is
  // observed as leader; DeadlineExceeded on timeout. NuRaft aborts the
  // transfer if quorum is lost mid-flight; the caller treats that as
  // transient and retries next reconcile.
  core::Status YieldLeadership(int32_t target_node_id);

  // Timestamp oracle access
  TimestampOracle* GetTimestampOracle() { return &tso_; }
  const TimestampOracle* GetTimestampOracle() const { return &tso_; }

  // Metadata store access (read-only)
  const MetadataStore* GetMetadataStore() const { return &metadata_store_; }

  // Statistics
  size_t GetCommittedOpCount() const;

 private:
  RaftConfig config_;
  std::atomic<bool> running_{false};
  std::atomic<bool> is_leader_{false};

  // Core components
  TimestampOracle tso_;
  MetadataStore metadata_store_;

  // NuRaft components (for multi-node mode)
  std::shared_ptr<MetadataStateMachine> state_machine_;
  std::shared_ptr<GvdbStateManager> state_mgr_;
  std::shared_ptr<nuraft::raft_launcher> launcher_;
  std::shared_ptr<nuraft::raft_server> raft_server_;
  std::shared_ptr<nuraft::logger> nuraft_logger_;

  // Synchronization
  mutable std::mutex mutex_;

  // Statistics
  std::atomic<size_t> committed_ops_{0};

  // Set by InitializeNuRaft when the peer probe found a live leader on
  // startup. Non-empty value triggers a JoinCluster RPC after NuRaft is
  // up (roadmap 1.7b). Cleared once the join has been announced.
  std::string joining_peer_;

  // Helpers
  core::Status ProposeOperation(const MetadataOp& op);
  core::Status InitializeNuRaft();
  // Announce our join to the Raft leader via JoinCluster RPC. Called from
  // Start() after InitializeNuRaft returns when joining_peer_ is set.
  // Retries on NOT_LEADER by following the current_leader_id. Bounded
  // retry budget; returns error if no leader accepts our join.
  core::Status AnnounceJoinToCluster();
};

} // namespace consensus
} // namespace gvdb