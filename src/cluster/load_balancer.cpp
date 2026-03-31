// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/load_balancer.h"
#include "utils/logger.h"
#include <algorithm>
#include <mutex>
#include <random>

namespace gvdb {
namespace cluster {

LoadBalancer::LoadBalancer(LoadBalancingStrategy strategy)
    : strategy_(strategy) {
  utils::Logger::Instance().Info("LoadBalancer initialized with strategy: {}",
                                 static_cast<int>(strategy));
}

absl::StatusOr<core::NodeId> LoadBalancer::SelectNode(
    const std::vector<core::NodeId>& nodes) {
  if (nodes.empty()) {
    return absl::FailedPreconditionError("No nodes available for load balancing");
  }

  switch (strategy_) {
    case LoadBalancingStrategy::ROUND_ROBIN:
      return SelectRoundRobin(nodes);

    case LoadBalancingStrategy::LEAST_CONNECTIONS:
      return SelectLeastConnections(nodes);

    case LoadBalancingStrategy::WEIGHTED:
      return SelectWeighted(nodes);

    case LoadBalancingStrategy::LATENCY_AWARE:
      return SelectByLatency(nodes);

    default:
      return SelectRoundRobin(nodes);
  }
}

void LoadBalancer::IncrementConnections(core::NodeId node_id) {
  std::shared_lock lock(connections_mutex_);
  connections_[node_id].fetch_add(1, std::memory_order_relaxed);
}

void LoadBalancer::DecrementConnections(core::NodeId node_id) {
  std::shared_lock lock(connections_mutex_);
  auto it = connections_.find(node_id);
  if (it != connections_.end()) {
    it->second.fetch_sub(1, std::memory_order_relaxed);
  }
}

size_t LoadBalancer::GetConnectionCount(core::NodeId node_id) const {
  std::shared_lock lock(connections_mutex_);
  auto it = connections_.find(node_id);
  return (it != connections_.end()) ? it->second.load(std::memory_order_relaxed) : 0;
}

void LoadBalancer::SetNodeWeight(core::NodeId node_id, float weight) {
  std::unique_lock lock(weights_mutex_);
  weights_[node_id] = weight;
}

float LoadBalancer::GetNodeWeight(core::NodeId node_id) const {
  std::shared_lock lock(weights_mutex_);
  auto it = weights_.find(node_id);
  return (it != weights_.end()) ? it->second : 1.0f;
}

void LoadBalancer::RecordLatency(core::NodeId node_id, uint64_t latency_ms) {
  std::unique_lock lock(latency_mutex_);
  // Simple exponential moving average
  auto it = avg_latencies_.find(node_id);
  if (it != avg_latencies_.end()) {
    it->second = static_cast<uint64_t>(0.7 * it->second + 0.3 * latency_ms);
  } else {
    avg_latencies_[node_id] = latency_ms;
  }
}

uint64_t LoadBalancer::GetAverageLatency(core::NodeId node_id) const {
  std::shared_lock lock(latency_mutex_);
  auto it = avg_latencies_.find(node_id);
  return (it != avg_latencies_.end()) ? it->second : 0;
}

core::NodeId LoadBalancer::SelectRoundRobin(const std::vector<core::NodeId>& nodes) {
  size_t index = round_robin_counter_.fetch_add(1, std::memory_order_relaxed) % nodes.size();
  return nodes[index];
}

core::NodeId LoadBalancer::SelectLeastConnections(const std::vector<core::NodeId>& nodes) {
  return *std::min_element(nodes.begin(), nodes.end(),
                           [this](core::NodeId a, core::NodeId b) {
                             return GetConnectionCount(a) < GetConnectionCount(b);
                           });
}

core::NodeId LoadBalancer::SelectWeighted(const std::vector<core::NodeId>& nodes) {
  // Calculate total weight
  float total_weight = 0.0f;
  for (const auto& node : nodes) {
    total_weight += GetNodeWeight(node);
  }

  if (total_weight <= 0.0f) {
    return SelectRoundRobin(nodes);  // Fallback
  }

  // Random selection based on weights
  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_real_distribution<float> dis(0.0f, total_weight);
  float random_value = dis(gen);

  float cumulative_weight = 0.0f;
  for (const auto& node : nodes) {
    cumulative_weight += GetNodeWeight(node);
    if (random_value <= cumulative_weight) {
      return node;
    }
  }

  return nodes.back();  // Fallback
}

core::NodeId LoadBalancer::SelectByLatency(const std::vector<core::NodeId>& nodes) {
  return *std::min_element(nodes.begin(), nodes.end(),
                           [this](core::NodeId a, core::NodeId b) {
                             return GetAverageLatency(a) < GetAverageLatency(b);
                           });
}

}  // namespace cluster
}  // namespace gvdb