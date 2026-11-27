// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include "internal.pb.h"

namespace gvdb {
namespace cluster {

// Information about a registered node
struct RegisteredNode {
  proto::internal::NodeInfo info;
  std::chrono::steady_clock::time_point last_heartbeat;
  std::chrono::steady_clock::time_point registered_at;
  uint64_t total_heartbeats = 0;
  uint64_t missed_heartbeats = 0;

  // Calculated status. `now` is the time to measure the heartbeat age
  // against: the time from the clock of the registry that set
  // last_heartbeat, so that both come from the same clock.
  bool IsHealthy(std::chrono::milliseconds timeout,
                 std::chrono::steady_clock::time_point now) const {
    return TimeSinceLastHeartbeat(now) < timeout;
  }

  bool IsDraining() const {
    return info.status() == proto::internal::NodeStatus::NODE_STATUS_DRAINING;
  }

  // A node is considered "routable" only if it is healthy AND not actively
  // draining. Draining nodes have sent a graceful-shutdown heartbeat and
  // should not receive new work.
  bool IsRoutable(std::chrono::milliseconds timeout,
                  std::chrono::steady_clock::time_point now) const {
    return IsHealthy(timeout, now) && !IsDraining();
  }

  std::chrono::milliseconds TimeSinceLastHeartbeat(
      std::chrono::steady_clock::time_point now) const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat);
  }
};

// Registry for tracking all nodes in the cluster
class NodeRegistry {
 public:
  // Returns the current time. The registry reads it for every heartbeat and
  // every health check, so a test can pass a fake clock and move time
  // forward without sleeping. Readers call it under a shared lock, so it
  // must be safe to call from several threads at once.
  using Clock = std::function<std::chrono::steady_clock::time_point()>;

  NodeRegistry(
      std::chrono::milliseconds heartbeat_timeout = std::chrono::seconds(10),
      Clock clock = [] { return std::chrono::steady_clock::now(); });
  ~NodeRegistry();

  // Register or update a node from heartbeat
  void UpdateNode(const proto::internal::NodeInfo& node_info);

  // Get information about a specific node
  bool GetNode(uint32_t node_id, RegisteredNode* node) const;

  // Get all nodes
  std::vector<RegisteredNode> GetAllNodes() const;

  // Get only healthy nodes (alive per heartbeat timeout). Includes nodes that
  // have sent NODE_STATUS_DRAINING. Use GetRoutableNodesByType() or
  // IsNodeRoutable() if you need to filter those out for routing decisions.
  std::vector<RegisteredNode> GetHealthyNodes() const;
  std::vector<RegisteredNode> GetHealthyNodesByType(proto::internal::NodeType type) const;

  // Get only nodes that are healthy AND not draining. Prefer this when
  // selecting targets for new work (insert, query routing, replica placement).
  std::vector<RegisteredNode> GetRoutableNodesByType(proto::internal::NodeType type) const;

  // Convenience: "is this specific node routable right now?" using the
  // registry's configured heartbeat timeout. Callers that only have a node_id
  // should prefer this over constructing a RegisteredNode and calling
  // IsRoutable(timeout, now) directly, which risks passing the wrong timeout.
  bool IsNodeRoutable(uint32_t node_id) const;

  // Get nodes that are actively draining AND still within heartbeat timeout.
  // Stale draining nodes (heartbeat expired) are intentionally excluded:
  // they are handled by the failure path rather than the graceful drain
  // path.
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

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint32_t, RegisteredNode> nodes_;
  std::chrono::milliseconds heartbeat_timeout_;
  Clock clock_;
};

}  // namespace cluster
}  // namespace gvdb