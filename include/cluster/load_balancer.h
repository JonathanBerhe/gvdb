// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/types.h"
#include "absl/status/statusor.h"
#include <vector>
#include <map>
#include <atomic>
#include <shared_mutex>

namespace gvdb {
namespace cluster {

// Load balancing strategy
enum class LoadBalancingStrategy {
  ROUND_ROBIN,      // Simple round-robin
  LEAST_CONNECTIONS,// Select node with fewest connections
  WEIGHTED,         // Weighted by node capacity
  LATENCY_AWARE     // Select by historical latency
};

// Load balancer distributes requests across nodes
class LoadBalancer {
 public:
  explicit LoadBalancer(LoadBalancingStrategy strategy);
  ~LoadBalancer() = default;

  // Node selection
  absl::StatusOr<core::NodeId> SelectNode(const std::vector<core::NodeId>& nodes);

  // Connection tracking
  void IncrementConnections(core::NodeId node_id);
  void DecrementConnections(core::NodeId node_id);
  size_t GetConnectionCount(core::NodeId node_id) const;

  // Weighted load balancing
  void SetNodeWeight(core::NodeId node_id, float weight);
  float GetNodeWeight(core::NodeId node_id) const;

  // Latency tracking
  void RecordLatency(core::NodeId node_id, uint64_t latency_ms);
  uint64_t GetAverageLatency(core::NodeId node_id) const;

  // Strategy
  LoadBalancingStrategy GetStrategy() const { return strategy_; }
  void SetStrategy(LoadBalancingStrategy strategy) { strategy_ = strategy; }

 private:
  LoadBalancingStrategy strategy_;

  // Round-robin counter
  std::atomic<size_t> round_robin_counter_{0};

  // Connection counts per node
  mutable std::shared_mutex connections_mutex_;
  std::map<core::NodeId, std::atomic<size_t>> connections_;

  // Node weights (for WEIGHTED strategy)
  mutable std::shared_mutex weights_mutex_;
  std::map<core::NodeId, float> weights_;

  // Latency history (for LATENCY_AWARE strategy)
  mutable std::shared_mutex latency_mutex_;
  std::map<core::NodeId, uint64_t> avg_latencies_;

  // Helper methods
  core::NodeId SelectRoundRobin(const std::vector<core::NodeId>& nodes);
  core::NodeId SelectLeastConnections(const std::vector<core::NodeId>& nodes);
  core::NodeId SelectWeighted(const std::vector<core::NodeId>& nodes);
  core::NodeId SelectByLatency(const std::vector<core::NodeId>& nodes);
};

}  // namespace cluster
}  // namespace gvdb