// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "consensus/raft_config.h"
#include "consensus/raft_peer_client.h"
#include "consensus/timestamp_oracle.h"
#include "core/status.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

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

// A Raft peer derived from the "id:host:port" --raft-peers format.
// Public so the parser can be unit-tested without spinning up a
// real Raft server.
struct RaftPeerSpec {
  int id = 0;
  std::string endpoint;  // "host:port"
};

// Parse one peer spec. Returns an error if the format is wrong, the id
// isn't an integer, the id is non-positive, or the endpoint is missing
// its port separator. Tolerant of leading/trailing whitespace.
absl::StatusOr<RaftPeerSpec> ParseRaftPeerSpec(const std::string& spec);

// Result of preparing a declared peer list for NuRaft bootstrap.
// Callers inspect `needs_seed` to decide whether to replace the current
// cluster_config. `peers` contains all declared peers (including self).
struct PeerListPlan {
  std::vector<RaftPeerSpec> peers;
  bool needs_seed = false;
};

// Validate + prepare a peer list for cluster_config seeding:
//   * every entry parses
//   * peer ids are unique
//   * `self_id` is present as one of the entries (prevents a rogue member
//     voting only for itself when the chart emits a wrong --node-id)
//   * returns needs_seed=true when the current persisted cluster_config
//     holds <= 1 server (fresh boot or single-member upgrade to HA): the
//     caller should replace the config with these peers. Otherwise trust
//     the persisted multi-member config and leave it alone so runtime
//     add_srv / remove_srv changes aren't clobbered on restart.
absl::StatusOr<PeerListPlan> PrepareRaftPeerList(
    int self_id,
    const std::vector<std::string>& declared_peers,
    size_t persisted_cluster_size);

// Raft node for distributed consensus
// Supports both single-node mode (for development) and multi-node mode (using NuRaft)
//
// Thread-safe: All public methods are thread-safe
class RaftNode {
 public:
  // `peer_client` makes the startup peer probe and the join request. A
  // multi-node config with coordinator_grpc_peers needs it: Start() fails
  // without it. A single node needs none.
  explicit RaftNode(const RaftConfig& config,
                    std::shared_ptr<IRaftPeerClient> peer_client = nullptr);
  ~RaftNode();

  // Disable copy and move
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  // Lifecycle
  absl::Status Start();
  absl::Status Shutdown();
  bool IsRunning() const;

  // Leadership (in single-node mode, always true after Start())
  bool IsLeader() const;
  int GetLeaderId() const;
  // Current Raft term as observed by this server. Returns 0 before NuRaft
  // has started or in single-node mode. Exposed so operators can detect
  // term stability across reconciles.
  uint64_t GetCurrentTerm() const;

  // Raft membership changes. Both methods are leader-only;
  // they return FailedPreconditionError when called on a follower, carrying
  // the current leader id in the status message so callers can retry.
  //
  // AddPeer wraps NuRaft raft_server::add_srv. Used by the leader handler
  // of the JoinCluster RPC when a new coordinator pod announces itself.
  // On success, NuRaft replicates the cluster_config change via Raft log
  // and the joining pod is caught up via snapshot transfer.
  absl::Status AddPeer(int32_t node_id, const std::string& raft_endpoint);

  // RemovePeer wraps NuRaft raft_server::remove_srv. Used by the leader
  // handler of the RemovePeer RPC, typically invoked by a coordinator pod
  // self-removing during graceful shutdown. Cannot remove the current
  // leader (NuRaft rejects with CANNOT_REMOVE_LEADER); caller must either
  // transfer leadership first or let NuRaft re-elect after the pod exits.
  absl::Status RemovePeer(int32_t node_id);

  // Raft scale reconciliation. Unlike Add/Remove above,
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
  // returns void: we synthesize a result by polling GetLeaderId()
  // every 100ms up to 3s. Returns Ok when the target_node_id is
  // observed as leader; DeadlineExceeded on timeout. NuRaft aborts the
  // transfer if quorum is lost mid-flight; the caller treats that as
  // transient and retries next reconcile.
  absl::Status YieldLeadership(int32_t target_node_id);

  // Timestamp oracle access
  TimestampOracle* GetTimestampOracle() { return &tso_; }

 private:
  RaftConfig config_;
  std::shared_ptr<IRaftPeerClient> peer_client_;
  std::atomic<bool> running_{false};
  std::atomic<bool> is_leader_{false};

  // Core components
  TimestampOracle tso_;

  // NuRaft components (for multi-node mode)
  std::shared_ptr<MetadataStateMachine> state_machine_;
  std::shared_ptr<GvdbStateManager> state_mgr_;
  std::shared_ptr<nuraft::raft_launcher> launcher_;
  std::shared_ptr<nuraft::raft_server> raft_server_;
  std::shared_ptr<nuraft::logger> nuraft_logger_;

  // Synchronization
  mutable std::mutex mutex_;

  // Set by InitializeNuRaft when the peer probe found a live leader on
  // startup. Non-empty value triggers a JoinCluster RPC after NuRaft is
  // up. Cleared once the join has been announced.
  std::string joining_peer_;

  // Result of the startup peer probe, when a peer reported a live leader.
  struct LeaderProbeResult {
    int32_t leader_id = 0;
    // The peer that answered: the leader itself, or a follower that knows
    // the leader id. AnnounceJoinToCluster calls JoinCluster on it.
    std::string responding_peer;
  };

  // Helpers
  absl::Status InitializeNuRaft();
  // Asks each coordinator in coordinator_grpc_peers, in order, for the
  // current leader. Returns the first answer with a leader, or nullopt.
  std::optional<LeaderProbeResult> ProbePeersForLeader() const;
  // Announce our join to the Raft leader via JoinCluster RPC. Called from
  // Start() after InitializeNuRaft returns when joining_peer_ is set.
  // Retries on NOT_LEADER by following the current_leader_id. Bounded
  // retry budget; returns error if no leader accepts our join.
  absl::Status AnnounceJoinToCluster();
};

} // namespace consensus
} // namespace gvdb