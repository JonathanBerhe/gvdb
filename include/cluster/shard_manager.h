// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/types.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include <memory>
#include <vector>
#include <map>
#include <set>
#include <shared_mutex>
#include <functional>

namespace gvdb {
namespace cluster {

// Shard state during lifecycle
enum class ShardState {
  ACTIVE,     // Normal operation
  MIGRATING,  // Moving to different node
  SPLITTING,  // Being split into multiple shards
  MERGING     // Being merged with another shard
};

// Information about a single shard assignment
struct ShardInfo {
  core::ShardId shard_id;
  core::NodeId primary_node;
  std::vector<core::NodeId> replica_nodes;
  ShardState state;

  // Metrics for rebalancing decisions
  size_t data_size_bytes;
  size_t vector_count;
  uint64_t query_count;

  // Range: which vectors belong to this shard
  // For simple hash-based sharding, this can be empty
  // For range-based sharding, stores [start, end) ranges
  std::vector<std::pair<uint64_t, uint64_t>> key_ranges;

  ShardInfo()
    : shard_id(core::kInvalidShardId),
      primary_node(core::kInvalidNodeId),
      state(ShardState::ACTIVE),
      data_size_bytes(0),
      vector_count(0),
      query_count(0) {}
};

// Shard assignment strategy
enum class ShardingStrategy {
  HASH,       // Consistent hashing
  RANGE,      // Range-based partitioning
  ROUND_ROBIN // Simple round-robin assignment
};

// Shard manager handles shard distribution and rebalancing
class ShardManager {
 public:
  ShardManager(size_t num_shards, ShardingStrategy strategy);
  ~ShardManager() = default;

  // Shard assignment
  core::ShardId AssignShard(core::VectorId vector_id) const;
  core::ShardId AssignShard(const std::string& key) const;

  // Get shard information
  absl::StatusOr<ShardInfo> GetShardInfo(core::ShardId shard_id) const;
  std::vector<ShardInfo> GetAllShards() const;
  std::vector<ShardInfo> GetShardsForNode(core::NodeId node_id) const;

  // Shard-to-node mapping
  absl::StatusOr<core::NodeId> GetPrimaryNode(core::ShardId shard_id) const;
  absl::StatusOr<std::vector<core::NodeId>> GetReplicaNodes(core::ShardId shard_id) const;
  absl::Status SetPrimaryNode(core::ShardId shard_id, core::NodeId node_id);
  absl::Status AddReplica(core::ShardId shard_id, core::NodeId node_id);
  absl::Status RemoveReplica(core::ShardId shard_id, core::NodeId node_id);

  // Node management
  absl::Status RegisterNode(core::NodeId node_id);
  absl::Status UnregisterNode(core::NodeId node_id, bool graceful);
  std::vector<core::NodeId> GetAllNodes() const;

  // Rebalancing
  struct RebalanceMove {
    core::ShardId shard_id;
    core::NodeId source_node;
    core::NodeId target_node;
    bool is_primary;  // true if moving primary, false if replica
  };

  std::vector<RebalanceMove> CalculateRebalancePlan();
  absl::Status ExecuteRebalanceMove(const RebalanceMove& move);

  // Metrics
  size_t GetTotalShards() const { return num_shards_; }
  float CalculateImbalance() const;

  // Update shard metrics (called by nodes)
  absl::Status UpdateShardMetrics(core::ShardId shard_id,
                                   size_t data_size,
                                   size_t vector_count,
                                   uint64_t query_count);

 private:
  size_t num_shards_;
  ShardingStrategy strategy_;

  // Shard information (shard_id -> ShardInfo)
  mutable std::shared_mutex shard_mutex_;
  std::map<core::ShardId, ShardInfo> shards_;

  // Nodes with shard assignments (not all cluster nodes — see NodeRegistry for that)
  mutable std::shared_mutex node_mutex_;
  std::set<core::NodeId> assigned_nodes_;

  // Hashing
  size_t HashKey(uint64_t key) const;
  size_t HashKey(const std::string& key) const;

  // Consistent hash ring (virtual nodes)
  static constexpr int kVirtualNodesPerShard = 150;
  std::map<uint64_t, core::ShardId> hash_ring_;
  void RebuildHashRing();
  core::ShardId LookupRing(uint64_t hash) const;

  // Helper methods
  void InitializeShards();
  std::vector<core::NodeId> GetLeastLoadedNodes(size_t count) const;
  size_t GetNodeLoad(core::NodeId node_id) const;
};

}  // namespace cluster
}  // namespace gvdb