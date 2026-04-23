// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <string>
#include <vector>

namespace gvdb {
namespace consensus {

// Configuration for a Raft node
struct RaftConfig {
  // Node identification
  int node_id = 0;                          // Unique node identifier (0-based)
  std::vector<std::string> peers;           // Declared Raft peers in "id:host:port" format
  // listen_address is the BIND address. Today NuRaft's launcher extracts
  // only the port and binds on 0.0.0.0:port; the host portion is historical
  // and ignored for binding. Keep default 0.0.0.0:0 (let the caller pick).
  std::string listen_address = "0.0.0.0:0";
  // advertise_address is the peer-facing endpoint stored in the NuRaft
  // srv_config. On K8s this should be the StatefulSet pod's FQDN so other
  // replicas can reach it regardless of pod IP churn. When empty, falls
  // back to listen_address for backward compatibility (roadmap 0b.4).
  std::string advertise_address;

  // Comma-separated list of coordinator gRPC InternalService endpoints
  // (host:50051 by default) used by the bootstrap-vs-join peer probe on
  // startup (roadmap 1.7b). When a new pod starts with empty persisted
  // state, it probes these endpoints for a live Raft leader; if one is
  // found, it skips self-seeding and calls JoinCluster instead. Empty or
  // single-entry lists fall back to the legacy seed-from-peers path
  // (preserves initial-bootstrap semantics for fresh 3-node deploys).
  std::vector<std::string> coordinator_grpc_peers;

  // Data directory
  std::string data_dir = "/tmp/gvdb/raft";  // Directory for Raft logs and snapshots

  // Timing parameters (milliseconds)
  int election_timeout_ms = 1000;           // Base election timeout (randomized to [1x, 2x])
  int heartbeat_interval_ms = 100;          // Leader heartbeat interval
  int snapshot_interval_ops = 10000;        // Operations between snapshots

  // Batch configuration
  int max_batch_size = 128;                 // Maximum entries per append batch
  int max_batch_bytes = 1024 * 1024;        // Maximum bytes per batch (1MB)

  // Single-node mode (for testing/development)
  bool single_node_mode = false;            // Disable leader election, always be leader

  // Validate configuration
  bool IsValid() const {
    if (node_id < 0) return false;
    if (election_timeout_ms <= 0) return false;
    if (heartbeat_interval_ms <= 0) return false;
    if (heartbeat_interval_ms >= election_timeout_ms) return false;
    if (!single_node_mode && peers.empty()) return false;
    return true;
  }
};

} // namespace consensus
} // namespace gvdb