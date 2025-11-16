#pragma once

#include <string>
#include <vector>

namespace gvdb {
namespace consensus {

// Configuration for a Raft node
struct RaftConfig {
  // Node identification
  int node_id = 0;                          // Unique node identifier (0-based)
  std::vector<std::string> peers;           // Addresses of other Raft nodes (host:port)
  std::string listen_address = "0.0.0.0:0"; // Address to listen on

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
