// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// The cluster's view of one node: its role, its health and its resources.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "core/types.h"

namespace gvdb {
namespace meta {

// Node type in the cluster
enum class NodeType {
  COORDINATOR,  // Manages metadata and coordinates operations
  QUERY_NODE,   // Serves read requests with cached data
  DATA_NODE,    // Handles index building
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

// Information about a node in the cluster, as the coordinator keeps it.
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

}  // namespace meta
}  // namespace gvdb
