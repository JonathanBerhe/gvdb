// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "absl/status/statusor.h"

namespace gvdb::consensus {

// The calls that a Raft node makes to the other coordinators while it
// starts: the leader probe and the join request. The cluster module
// implements it over the internal gRPC API, so that consensus does not
// depend on gRPC or on the protos.
class IRaftPeerClient {
 public:
  virtual ~IRaftPeerClient() = default;

  struct LeaderInfo {
    // The id of the leader that the peer knows, or 0 or less when it knows
    // no leader.
    int32_t leader_id = 0;
    uint64_t current_term = 0;
  };

  // Asks the coordinator at `address` which node leads the Raft group. An
  // error status means that the call failed, for example because the
  // coordinator is not reachable within `timeout`.
  virtual absl::StatusOr<LeaderInfo> GetLeaderInfo(const std::string& address,
                                                   std::chrono::milliseconds timeout) = 0;

  struct JoinReply {
    bool success = false;
    std::string message;
    // When the peer is not the leader: the id of the leader that it knows,
    // or 0 or less.
    int32_t current_leader_id = 0;
  };

  // Asks the coordinator at `address` to add the node `node_id`, which
  // listens for Raft at `raft_endpoint`, to the group. An error status means
  // that the call failed. A reply with success false means that the peer
  // refused.
  virtual absl::StatusOr<JoinReply> JoinCluster(const std::string& address, int32_t node_id,
                                                const std::string& raft_endpoint,
                                                std::chrono::milliseconds timeout) = 0;
};

}  // namespace gvdb::consensus
