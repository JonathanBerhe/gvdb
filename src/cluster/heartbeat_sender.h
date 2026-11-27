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

class PrimaryTermTracker;

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

  // Optional: when set, every successful heartbeat response feeds its
  // shard_primaries field into the tracker, keeping the data-node's
  // local "am I primary for shard X at term Y?" view in sync with the
  // coordinator. Owned by the caller (typically data_node_main); must
  // outlive this HeartbeatSender. No-op when null (e.g. query-nodes,
  // tests).
  void SetPrimaryTermTracker(PrimaryTermTracker* tracker) {
    primary_term_tracker_ = tracker;
  }

  // Start sending heartbeats in a background thread
  void Start();

  // Stop heartbeat thread (called by destructor)
  void Stop();

  // Stop the background loop (if running), then synchronously send a final
  // heartbeat with NODE_STATUS_DRAINING so the coordinator stops routing new
  // work to this node immediately instead of waiting for heartbeat timeout.
  //
  // Order matters: we join the loop first so no concurrent NODE_STATUS_READY
  // heartbeat can race the drain signal and overwrite it at the registry.
  // Retries the drain RPC once on failure with a short backoff.
  //
  // Requires the shutdown_flag passed in the constructor to have been set
  // (normal path: SIGTERM handler in ServerBootstrap); otherwise the loop join
  // would hang indefinitely.
  //
  // Returns true if the drain heartbeat was acknowledged by the coordinator.
  bool DrainAndStop();

 private:
  void SendLoop();
  bool SendDrainHeartbeatOnce();

  std::string coordinator_address_;
  int node_id_;
  std::string grpc_address_;
  proto::internal::NodeType node_type_;
  std::atomic<bool>& shutdown_flag_;
  std::unique_ptr<std::thread> thread_;
  PrimaryTermTracker* primary_term_tracker_ = nullptr;
};

}  // namespace cluster
}  // namespace gvdb