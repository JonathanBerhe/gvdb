// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "absl/status/statusor.h"
#include "consensus/raft_peer_client.h"

namespace gvdb {
namespace cluster {

// IRaftPeerClient over the internal gRPC API: GetLeaderInfo and JoinCluster
// on the InternalService of another coordinator. Each call opens a new
// insecure channel, so the client keeps no state and is thread-safe.
class GrpcRaftPeerClient : public consensus::IRaftPeerClient {
 public:
  absl::StatusOr<LeaderInfo> GetLeaderInfo(const std::string& address,
                                           std::chrono::milliseconds timeout) override;

  absl::StatusOr<JoinReply> JoinCluster(const std::string& address, int32_t node_id,
                                        const std::string& raft_endpoint,
                                        std::chrono::milliseconds timeout) override;
};

}  // namespace cluster
}  // namespace gvdb
