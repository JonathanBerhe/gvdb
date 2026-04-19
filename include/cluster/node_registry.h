// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "internal.pb.h"
#include "absl/time/time.h"
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace gvdb {
namespace cluster {

// Information about a registered node
struct RegisteredNode {
  proto::internal::NodeInfo info;
  std::chrono::steady_clock::time_point last_heartbeat;
  std::chrono::steady_clock::time_point registered_at;
  uint64_t total_heartbeats = 0;
  uint64_t missed_heartbeats = 0;

  // Calculated status
  bool IsHealthy(std::chrono::milliseconds timeout) const {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - last_heartbeat);
    return elapsed < timeout;
  }

  bool IsDraining() const {
    return info.status() == proto::internal::NodeStatus::NODE_STATUS_DRAINING;
  }

  // A node is considered "routable" only if it is healthy AND not actively
  // draining. Draining nodes have sent a graceful-shutdown heartbeat and
  // should not receive new work (roadmap 0b.1).
  bool IsRoutable(std::chrono::milliseconds timeout) const {
    return IsHealthy(timeout) && !IsDraining();
  }

  std::chrono::milliseconds TimeSinceLastHeartbeat() const {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        now - last_heartbeat);
  }
};

// Registry for tracking all nodes in the cluster
class NodeRegistry {
 public:
  NodeRegistry(std::chrono::milliseconds heartbeat_timeout = std::chrono::seconds(10));
  ~NodeRegistry();

  // Register or update a node from heartbeat
  void UpdateNode(const proto::internal::NodeInfo& node_info);

  // Get information about a specific node
  bool GetNode(uint32_t node_id, RegisteredNode* node) const;

  // Get all nodes (optionally filtered by type)
  std::vector<RegisteredNode> GetAllNodes() const;
  std::vector<RegisteredNode> GetNodesByType(proto::internal::NodeType type) const;

  // Get only healthy nodes (alive per heartbeat timeout). Includes nodes that
  // have sent NODE_STATUS_DRAINING — use GetRoutableNodes*() if you need to
  // filter those out for routing decisions.
  std::vector<RegisteredNode> GetHealthyNodes() const;
  std::vector<RegisteredNode> GetHealthyNodesByType(proto::internal::NodeType type) const;

  // Get only nodes that are healthy AND not draining. Prefer this when
  // selecting targets for new work (insert, query routing, replica placement).
  std::vector<RegisteredNode> GetRoutableNodes() const;
  std::vector<RegisteredNode> GetRoutableNodesByType(proto::internal::NodeType type) const;

  // Convenience: "is this specific node routable right now?" using the
  // registry's configured heartbeat timeout. Callers that only have a node_id
  // should prefer this over constructing a RegisteredNode and calling
  // IsRoutable(timeout) directly, which risks passing the wrong timeout.
  bool IsNodeRoutable(uint32_t node_id) const;

  // Get failed/unhealthy nodes
  std::vector<RegisteredNode> GetFailedNodes() const;

  // Get nodes that are actively draining AND still within heartbeat timeout.
  // Stale draining nodes (heartbeat expired) are intentionally excluded —
  // they are handled by the failure path rather than the graceful drain
  // path (roadmap 0b.3).
  std::vector<RegisteredNode> GetDrainingNodes() const;

  // Remove a node from the registry
  void RemoveNode(uint32_t node_id);

  // Get cluster statistics
  struct ClusterStats {
    uint32_t total_nodes = 0;
    uint32_t healthy_nodes = 0;
    uint32_t failed_nodes = 0;
    uint32_t coordinators = 0;
    uint32_t data_nodes = 0;
    uint32_t query_nodes = 0;
    uint32_t proxies = 0;
  };
  ClusterStats GetClusterStats() const;

  // Check for failed nodes and return their IDs
  std::vector<uint32_t> DetectFailedNodes();

  // Set heartbeat timeout
  void SetHeartbeatTimeout(std::chrono::milliseconds timeout);

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint32_t, RegisteredNode> nodes_;
  std::chrono::milliseconds heartbeat_timeout_;
};

} // namespace cluster
} // namespace gvdb