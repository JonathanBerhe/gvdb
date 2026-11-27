// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "cluster/datanode/heartbeat_sender.h"
#include "cluster/datanode/primary_term_tracker.h"
#include "consensus/raft_config.h"
#include "consensus/raft_node.h"
#include "internal.pb.h"

// Startup and shutdown steps of the programs in a cluster: Raft on the
// coordinator, the heartbeat and the drain on the data and query nodes.

namespace gvdb::server {

// Starts a Raft node. The error message starts with "Failed to start Raft: ".
absl::StatusOr<std::unique_ptr<consensus::RaftNode>> StartRaft(const consensus::RaftConfig& config);

// Waits until the Raft group has a leader, for at most `timeout`, and stops
// early on shutdown. At a cold start of a cluster, the other coordinators
// can take tens of seconds to become reachable. So a timeout only logs a
// warning, the program starts its gRPC server, and Raft elects a leader
// when enough peers are up.
void WaitForLeaderElection(const consensus::RaftNode& raft, std::chrono::seconds timeout);

// Asks the Raft leader to remove this coordinator from the group, so that a
// scale-down leaves no dead peer. Does nothing when this node is the leader:
// NuRaft cannot remove the leader, and the group elects a new one after the
// node exits. `grpc_peers` lists the coordinator gRPC addresses in node id
// order (node id 1 is the first). When leadership moved since the node read
// the leader id, the function follows the redirect once. Errors are only
// logged, because a peer that stays in the group is fixed by the next
// reconcile.
void RemoveSelfFromRaft(const consensus::RaftNode& raft, int node_id,
                        const std::vector<std::string>& grpc_peers);

// Starts the heartbeat to the first address in `coordinators`, or returns
// null when the list is empty. The heartbeat advertises `advertise_address`,
// or `bind_address` when `advertise_address` is empty. A non-null `tracker`
// gets the shard primaries from each heartbeat response.
std::unique_ptr<cluster::HeartbeatSender> StartHeartbeat(
    const std::vector<std::string>& coordinators, int node_id, const std::string& bind_address,
    const std::string& advertise_address, proto::internal::NodeType node_type,
    cluster::PrimaryTermTracker* tracker = nullptr);

// Polls the coordinator until no shard is assigned to `node_id` any more,
// or until `timeout` passes. A data node calls it during shutdown and keeps
// its gRPC server up meanwhile, so that the coordinator can copy the
// node's segments to the replicas it promotes. A failed poll is retried
// until the timeout: an early exit would close the server before the
// migration ends.
void WaitForDrainCompletion(const std::string& coordinator_address, int node_id,
                            std::chrono::seconds timeout);

}  // namespace gvdb::server
