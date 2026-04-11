// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/types.h"
#include "cluster/shard_manager.h"
#include "cluster/node_registry.h"
#include "cluster/internal_client.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include <memory>
#include <string>
#include <map>
#include <shared_mutex>
#include <atomic>
#include <thread>
#include <set>

namespace gvdb {
namespace cluster {

// Node type in the cluster
enum class NodeType {
  COORDINATOR,  // Manages metadata and coordinates operations
  QUERY_NODE,   // Serves read requests with cached data
  DATA_NODE,    // Handles index building and compaction
  PROXY         // Routes client requests to appropriate nodes
};

// Node status in the cluster
enum class NodeStatus {
  STARTING,    // Node is starting up
  HEALTHY,     // Node is operating normally
  DEGRADED,    // Node is experiencing issues but still functional
  FAILED,      // Node has failed and needs recovery
  LEAVING      // Node is gracefully shutting down
};

// Information about a node in the cluster
struct NodeInfo {
  core::NodeId node_id;
  NodeType type;
  NodeStatus status;
  std::string address;  // host:port
  uint64_t last_heartbeat_ts;

  // Resource information
  size_t memory_capacity_bytes;
  size_t memory_used_bytes;
  size_t disk_capacity_bytes;
  size_t disk_used_bytes;
  float cpu_usage_percent;

  NodeInfo()
    : node_id(core::kInvalidNodeId),
      type(NodeType::QUERY_NODE),
      status(NodeStatus::STARTING),
      last_heartbeat_ts(0),
      memory_capacity_bytes(0),
      memory_used_bytes(0),
      disk_capacity_bytes(0),
      disk_used_bytes(0),
      cpu_usage_percent(0.0f) {}
};

// Collection metadata stored by coordinator
struct CollectionMetadata {
  core::CollectionId collection_id;
  std::string collection_name;
  core::Dimension dimension;
  core::MetricType metric_type;
  core::IndexType index_type;

  // Sharding information
  std::vector<core::ShardId> shard_ids;
  size_t num_shards = 1;
  size_t replication_factor;

  // Stats
  uint64_t total_vectors;
  uint64_t total_size_bytes;
  core::Timestamp created_at;
  core::Timestamp updated_at;

  CollectionMetadata()
    : collection_id(core::kInvalidCollectionId),
      dimension(0),
      metric_type(core::MetricType::L2),
      index_type(core::IndexType::FLAT),
      replication_factor(1),
      total_vectors(0),
      total_size_bytes(0),
      created_at(0),
      updated_at(0) {}

  CollectionMetadata(core::CollectionId id,
                     const std::string& name,
                     core::Dimension dim,
                     core::MetricType metric,
                     core::IndexType index)
    : collection_id(id),
      collection_name(name),
      dimension(dim),
      metric_type(metric),
      index_type(index),
      replication_factor(1),
      total_vectors(0),
      total_size_bytes(0),
      created_at(0),
      updated_at(0) {}
};

// Deterministic segment ID for a shard within a collection.
// Max 1000 shards per collection.
inline core::SegmentId ShardSegmentId(core::CollectionId cid, uint32_t shard_index) {
  return static_cast<core::SegmentId>(core::ToUInt32(cid) * 1000 + shard_index);
}

// Coordinator manages cluster metadata and operations
class Coordinator {
 public:
  explicit Coordinator(
      std::shared_ptr<ShardManager> shard_manager,
      std::shared_ptr<NodeRegistry> node_registry,
      std::shared_ptr<IInternalServiceClientFactory> client_factory = nullptr);
  ~Coordinator();

  // Cluster initialization
  absl::Status Start(const std::string& bind_address);
  absl::Status Shutdown();

  // Node registration and health
  absl::Status RegisterNode(const NodeInfo& node_info);
  absl::Status UnregisterNode(core::NodeId node_id);
  absl::Status UpdateNodeStatus(core::NodeId node_id, NodeStatus status);
  absl::Status ProcessHeartbeat(core::NodeId node_id, const NodeInfo& node_info);
  absl::StatusOr<NodeInfo> GetNodeInfo(core::NodeId node_id) const;
  std::vector<NodeInfo> GetAllNodes() const;
  std::vector<NodeInfo> GetHealthyNodes(NodeType type) const;

  // Collection management
  absl::StatusOr<core::CollectionId> CreateCollection(
      const std::string& name,
      core::Dimension dimension,
      core::MetricType metric_type,
      core::IndexType index_type,
      size_t replication_factor,
      size_t num_shards = 1);
  absl::Status DropCollection(const std::string& name);
  absl::Status DropCollection(core::CollectionId collection_id);
  absl::StatusOr<CollectionMetadata> GetCollectionMetadata(const std::string& name) const;
  absl::StatusOr<CollectionMetadata> GetCollectionMetadata(core::CollectionId id) const;
  std::vector<CollectionMetadata> ListCollections() const;

  // Shard assignment for collections
  absl::Status AssignShardsToCollection(core::CollectionId collection_id,
                                        size_t num_shards,
                                        size_t replication_factor);

  // Cluster health and monitoring
  bool IsHealthy() const;
  size_t GetHealthyNodeCount() const;
  float GetClusterLoad() const;

  // Failure detection and recovery
  void StartHealthCheckLoop();
  void StopHealthCheckLoop();

  // Read repair: run consistency check (public for testing)
  void RunConsistencyCheck();
  size_t GetRepairQueueSize() const;

  // Replicate segment data from source node to target node
  absl::Status ReplicateSegmentData(
      core::SegmentId segment_id,
      core::NodeId source_node,
      core::NodeId target_node);

  // Handle failed node: reassign shards, promote replicas
  void HandleFailedNode(core::NodeId failed_node_id);

  // Dynamic shard rebalancing
  // Returns the number of shards successfully moved. collection_id(0) = all.
  absl::StatusOr<uint32_t> ExecuteRebalancePlan(
      core::CollectionId collection_id = core::CollectionId(0));

  // Reset shards stuck in MIGRATING state (called on startup / after crash)
  void RecoverMigratingShards();

 private:
  // Shard manager
  std::shared_ptr<ShardManager> shard_manager_;

  // Node registry (single source of truth for node state)
  std::shared_ptr<NodeRegistry> node_registry_;

  // Collection registry
  mutable std::shared_mutex collection_mutex_;
  std::map<core::CollectionId, CollectionMetadata> collections_;
  std::map<std::string, core::CollectionId> collection_name_to_id_;

  // ID generation
  std::atomic<uint32_t> next_node_id_{1};
  std::atomic<uint32_t> next_collection_id_{1};

  // Health check
  std::atomic<bool> running_{false};
  std::unique_ptr<std::thread> health_check_thread_;
  void HealthCheckLoop();
  void DetectFailedNodes();
  void CheckReplication();
  void CheckConsistency();
  absl::Status RepairSegment(core::SegmentId segment_id,
                             core::NodeId primary_node,
                             core::NodeId stale_replica);

  // Repair state
  static constexpr uint32_t kRepairIntervalCycles = 6;  // every ~30s
  uint32_t health_check_cycle_count_{0};
  mutable std::mutex repair_mutex_;
  std::set<core::ShardId> shards_being_repaired_;

  // Rebalancing internals
  absl::Status ExecuteSingleMove(
      const ShardManager::RebalanceMove& move,
      core::CollectionId collection_id,
      uint32_t shard_index);
  static constexpr size_t kMaxMovesPerCycle = 4;
  mutable std::mutex migration_mutex_;
  std::set<uint32_t> shards_migrating_;

  // Helper methods
  core::NodeId AllocateNodeId();
  core::CollectionId AllocateCollectionId();
  core::Timestamp GetCurrentTimestamp() const;

  // Client factory for distributed segment creation
  std::shared_ptr<IInternalServiceClientFactory> client_factory_;

  // Client cache for calling data nodes
  mutable std::shared_mutex client_mutex_;
  std::map<core::NodeId, std::unique_ptr<IInternalServiceClient>> data_node_clients_;

  // Get or create client for a data node
  IInternalServiceClient* GetOrCreateDataNodeClient(core::NodeId node_id);
};

}  // namespace cluster
}  // namespace gvdb