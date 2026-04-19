// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "internal.grpc.pb.h"
#include <atomic>
#include <memory>
#include <string>
#include <thread>

namespace gvdb {
namespace cluster {

// Sends periodic heartbeats to a coordinator node.
// Used by data nodes and query nodes to register their presence.
class HeartbeatSender {
 public:
  HeartbeatSender(const std::string& coordinator_address,
                  int node_id,
                  const std::string& grpc_address,
                  proto::internal::NodeType node_type,
                  std::atomic<bool>& shutdown_flag);
  ~HeartbeatSender();

  // Start sending heartbeats in a background thread
  void Start();

  // Stop heartbeat thread (called by destructor)
  void Stop();

  // Send a single heartbeat with NODE_STATUS_DRAINING so the coordinator stops
  // routing new work to this node immediately instead of waiting for heartbeat
  // timeout. Thread-safe and independent of the background loop — uses its own
  // short-lived channel so it works even during shutdown.
  // Returns true if the drain heartbeat was acknowledged by the coordinator.
  bool SendDrainHeartbeat();

 private:
  void SendLoop();

  std::string coordinator_address_;
  int node_id_;
  std::string grpc_address_;
  proto::internal::NodeType node_type_;
  std::atomic<bool>& shutdown_flag_;
  std::unique_ptr<std::thread> thread_;
};

}  // namespace cluster
}  // namespace gvdb