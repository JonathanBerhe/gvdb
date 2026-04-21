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

  // Helpers
  core::Status ProposeOperation(const MetadataOp& op);
  core::Status InitializeNuRaft();
};

} // namespace consensus
} // namespace gvdb