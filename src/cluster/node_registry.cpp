// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/node_registry.h"
#include "utils/logger.h"

namespace gvdb {
namespace cluster {

NodeRegistry::NodeRegistry(std::chrono::milliseconds heartbeat_timeout)
    : heartbeat_timeout_(heartbeat_timeout) {
  utils::Logger::Instance().Info("NodeRegistry initialized (timeout={}ms)",
                                  heartbeat_timeout.count());
}

NodeRegistry::~NodeRegistry() {
  utils::Logger::Instance().Info("NodeRegistry shutting down ({} registered nodes)",
                                  nodes_.size());
}

void NodeRegistry::UpdateNode(const proto::internal::NodeInfo& node_info) {
  std::unique_lock lock(mutex_);

  uint32_t node_id = node_info.node_id();
  auto now = std::chrono::steady_clock::now();
  const bool is_draining =
      node_info.status() == proto::internal::NodeStatus::NODE_STATUS_DRAINING;

  auto it = nodes_.find(node_id);
  if (it != nodes_.end()) {
    // Merge incoming heartbeat into the existing record rather than wholesale
    // replacing it. Heartbeats carry a sparse NodeInfo (identity + status
    // only; drain heartbeats especially). We preserve observability fields
    // (memory/disk/perf counters) from the previous heartbeat when the
    // incoming value is zero, so the DRAINING transition does not clobber
    // the last known resource snapshot used by GetClusterHealth.
    const bool was_draining = it->second.IsDraining();
    auto& stored = it->second.info;

    // Identity + status: always overwrite.
    stored.set_node_id(node_info.node_id());
    stored.set_node_type(node_info.node_type());
    stored.set_status(node_info.status());
    if (!node_info.grpc_address().empty()) {
      stored.set_grpc_address(node_info.grpc_address());
    }
    if (!node_info.raft_address().empty()) {
      stored.set_raft_address(node_info.raft_address());
    }

    // Resource fields: only overwrite when the incoming message provides a
    // non-zero value (proto3 cannot distinguish "unset" from "zero").
    if (node_info.memory_total_bytes() > 0) {
      stored.set_memory_total_bytes(node_info.memory_total_bytes());
      stored.set_memory_used_bytes(node_info.memory_used_bytes());
    }
    if (node_info.disk_total_bytes() > 0) {
      stored.set_disk_total_bytes(node_info.disk_total_bytes());
      stored.set_disk_used_bytes(node_info.disk_used_bytes());
    }
    if (node_info.total_requests() > 0) {
      stored.set_total_requests(node_info.total_requests());
      stored.set_avg_latency_ms(node_info.avg_latency_ms());
      stored.set_error_count(node_info.error_count());
    }

    it->second.last_heartbeat = now;
    it->second.total_heartbeats++;

    if (is_draining && !was_draining) {
      utils::Logger::Instance().Info(
          "Node {} entered DRAINING state (type={}, address={})",
          node_id, static_cast<int>(node_info.node_type()),
          node_info.grpc_address());
    } else {
      utils::Logger::Instance().Debug(
          "Updated node {} (type={}, total_heartbeats={})", node_id,
          static_cast<int>(node_info.node_type()),
          it->second.total_heartbeats);
    }
  } else {
    // Register new node
    RegisteredNode node;
    node.info = node_info;
    node.last_heartbeat = now;
    node.registered_at = now;
    node.total_heartbeats = 1;
    node.missed_heartbeats = 0;

    nodes_[node_id] = std::move(node);

    utils::Logger::Instance().Info(
        "Registered new node {} (type={}, address={}{})", node_id,
        static_cast<int>(node_info.node_type()), node_info.grpc_address(),
        is_draining ? ", DRAINING" : "");
  }
}

bool NodeRegistry::GetNode(uint32_t node_id, RegisteredNode* node) const {
  std::shared_lock lock(mutex_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }

  if (node) {
    *node = it->second;
  }
  return true;
}

std::vector<RegisteredNode> NodeRegistry::GetAllNodes() const {
  std::shared_lock lock(mutex_);

  std::vector<RegisteredNode> result;
  result.reserve(nodes_.size());

  for (const auto& [id, node] : nodes_) {
    result.push_back(node);
  }

  return result;
}

std::vector<RegisteredNode> NodeRegistry::GetNodesByType(
    proto::internal::NodeType type) const {
  std::shared_lock lock(mutex_);

  std::vector<RegisteredNode> result;

  for (const auto& [id, node] : nodes_) {
    if (node.info.node_type() == type) {
      result.push_back(node);
    }
  }

  return result;
}

std::vector<RegisteredNode> NodeRegistry::GetHealthyNodes() const {
  std::shared_lock lock(mutex_);

  std::vector<RegisteredNode> result;

  for (const auto& [id, node] : nodes_) {
    if (node.IsHealthy(heartbeat_timeout_)) {
      result.push_back(node);
    }
  }

  return result;
}

std::vector<RegisteredNode> NodeRegistry::GetHealthyNodesByType(
    proto::internal::NodeType type) const {
  std::shared_lock lock(mutex_);

  std::vector<RegisteredNode> result;

  for (const auto& [id, node] : nodes_) {
    if (node.info.node_type() == type && node.IsHealthy(heartbeat_timeout_)) {
      result.push_back(node);
    }
  }

  return result;
}

std::vector<RegisteredNode> NodeRegistry::GetRoutableNodes() const {
  std::shared_lock lock(mutex_);

  std::vector<RegisteredNode> result;
  for (const auto& [id, node] : nodes_) {
    if (node.IsRoutable(heartbeat_timeout_)) {
      result.push_back(node);
    }
  }
  return result;
}

std::vector<RegisteredNode> NodeRegistry::GetRoutableNodesByType(
    proto::internal::NodeType type) const {
  std::shared_lock lock(mutex_);

  std::vector<RegisteredNode> result;
  for (const auto& [id, node] : nodes_) {
    if (node.info.node_type() == type &&
        node.IsRoutable(heartbeat_timeout_)) {
      result.push_back(node);
    }
  }
  return result;
}

bool NodeRegistry::IsNodeRoutable(uint32_t node_id) const {
  std::shared_lock lock(mutex_);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return false;
  return it->second.IsRoutable(heartbeat_timeout_);
}

std::vector<RegisteredNode> NodeRegistry::GetFailedNodes() const {
  std::shared_lock lock(mutex_);

  std::vector<RegisteredNode> result;

  for (const auto& [id, node] : nodes_) {
    if (!node.IsHealthy(heartbeat_timeout_)) {
      result.push_back(node);
    }
  }

  return result;
}

std::vector<RegisteredNode> NodeRegistry::GetDrainingNodes() const {
  std::shared_lock lock(mutex_);

  std::vector<RegisteredNode> result;
  for (const auto& [id, node] : nodes_) {
    // Only include nodes that are *actively* draining: status = DRAINING
    // AND last heartbeat is still within the timeout. A draining node whose
    // heartbeat has expired is stale — the failure-detection path handles it.
    if (node.IsDraining() && node.IsHealthy(heartbeat_timeout_)) {
      result.push_back(node);
    }
  }
  return result;
}

void NodeRegistry::RemoveNode(uint32_t node_id) {
  std::unique_lock lock(mutex_);

  auto it = nodes_.find(node_id);
  if (it != nodes_.end()) {
    utils::Logger::Instance().Info("Removed node {} from registry", node_id);
    nodes_.erase(it);
  }
}

NodeRegistry::ClusterStats NodeRegistry::GetClusterStats() const {
  std::shared_lock lock(mutex_);

  ClusterStats stats;
  stats.total_nodes = nodes_.size();

  for (const auto& [id, node] : nodes_) {
    if (node.IsHealthy(heartbeat_timeout_)) {
      stats.healthy_nodes++;
    } else {
      stats.failed_nodes++;
    }

    switch (node.info.node_type()) {
      case proto::internal::NodeType::NODE_TYPE_COORDINATOR:
        stats.coordinators++;
        break;
      case proto::internal::NodeType::NODE_TYPE_DATA_NODE:
        stats.data_nodes++;
        break;
      case proto::internal::NodeType::NODE_TYPE_QUERY_NODE:
        stats.query_nodes++;
        break;
      case proto::internal::NodeType::NODE_TYPE_PROXY:
        stats.proxies++;
        break;
      default:
        break;
    }
  }

  return stats;
}

std::vector<uint32_t> NodeRegistry::DetectFailedNodes() {
  std::unique_lock lock(mutex_);

  std::vector<uint32_t> failed_node_ids;

  for (auto& [id, node] : nodes_) {
    if (!node.IsHealthy(heartbeat_timeout_)) {
      failed_node_ids.push_back(id);
      node.missed_heartbeats++;

      utils::Logger::Instance().Warn(
          "Node {} appears to have failed (last heartbeat {}ms ago, missed={})",
          id,
          node.TimeSinceLastHeartbeat().count(),
          node.missed_heartbeats);
    }
  }

  return failed_node_ids;
}

void NodeRegistry::SetHeartbeatTimeout(std::chrono::milliseconds timeout) {
  std::unique_lock lock(mutex_);
  heartbeat_timeout_ = timeout;
  utils::Logger::Instance().Info("Heartbeat timeout updated to {}ms", timeout.count());
}

} // namespace cluster
} // namespace gvdb