// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "server/cluster_steps.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <thread>

#include "cluster/coordinator/raft_peer_client.h"
#include "common/logger.h"
#include "internal.grpc.pb.h"
#include "server/bootstrap.h"

namespace gvdb::server {

absl::StatusOr<std::unique_ptr<consensus::RaftNode>> StartRaft(
    const consensus::RaftConfig& config) {
  auto created =
      consensus::RaftNode::Create(config, std::make_shared<cluster::GrpcRaftPeerClient>());
  if (!created.ok()) return WithPrefix("Failed to start Raft: ", created.status());
  std::unique_ptr<consensus::RaftNode> raft = std::move(*created);
  if (auto status = raft->Start(); !status.ok()) {
    return WithPrefix("Failed to start Raft: ", status);
  }
  return raft;
}

void WaitForLeaderElection(const consensus::RaftNode& raft, std::chrono::seconds timeout) {
  common::Logger::Instance().Info("Waiting up to {}s for leader election...", timeout.count());
  const auto start_time = std::chrono::steady_clock::now();
  while (!raft.IsLeader() && raft.GetLeaderId() < 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    if (ShutdownFlag().load()) break;
    if (std::chrono::steady_clock::now() - start_time > timeout) {
      common::Logger::Instance().Warn(
          "No Raft leader elected after {}s, continuing startup; "
          "election will complete when enough peers are reachable",
          timeout.count());
      break;
    }
  }
}

void RemoveSelfFromRaft(const consensus::RaftNode& raft, int node_id,
                        const std::vector<std::string>& grpc_peers) {
  if (grpc_peers.empty() || raft.IsLeader()) return;

  auto resolve_target = [&](int id) -> std::string {
    if (id <= 0) return "";
    const auto index = static_cast<size_t>(id - 1);
    return index < grpc_peers.size() ? grpc_peers[index] : "";
  };

  // Two attempts with a 3 s deadline each fit in the 30 s grace period that
  // Kubernetes gives a pod by default.
  std::string leader_target = resolve_target(raft.GetLeaderId());
  for (int attempt = 0; attempt < 2 && !leader_target.empty(); ++attempt) {
    common::Logger::Instance().Info("Self-removing node_id={} via leader {} (attempt {})", node_id,
                                    leader_target, attempt);
    auto channel = grpc::CreateChannel(leader_target, grpc::InsecureChannelCredentials());
    auto stub = proto::internal::InternalService::NewStub(channel);
    proto::internal::RemovePeerRequest req;
    req.set_node_id(static_cast<uint32_t>(node_id));
    proto::internal::RemovePeerResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    auto status = stub->RemovePeer(&ctx, req, &resp);
    if (!status.ok()) {
      // A transport error: the same target is not worth a second call.
      common::Logger::Instance().Warn("Self-remove RPC failed: {}", status.error_message());
      break;
    }
    if (resp.success()) {
      common::Logger::Instance().Info("Self-remove succeeded");
      break;
    }
    common::Logger::Instance().Warn("Self-remove denied: {} (leader_id={})", resp.message(),
                                    resp.current_leader_id());
    std::string new_target = resolve_target(static_cast<int>(resp.current_leader_id()));
    if (new_target.empty() || new_target == leader_target) break;
    leader_target = new_target;
  }
}

std::unique_ptr<cluster::HeartbeatSender> StartHeartbeat(
    const std::vector<std::string>& coordinators, int node_id, const std::string& bind_address,
    const std::string& advertise_address, proto::internal::NodeType node_type,
    cluster::PrimaryTermTracker* tracker) {
  if (coordinators.empty()) return nullptr;
  const std::string& address = advertise_address.empty() ? bind_address : advertise_address;
  auto heartbeat = std::make_unique<cluster::HeartbeatSender>(coordinators[0], node_id, address,
                                                              node_type, ShutdownFlag(), tracker);
  heartbeat->Start();
  return heartbeat;
}

void WaitForDrainCompletion(const std::string& coordinator_address, int node_id,
                            std::chrono::seconds timeout) {
  if (coordinator_address.empty() || timeout.count() <= 0) return;

  auto channel = grpc::CreateChannel(coordinator_address, grpc::InsecureChannelCredentials());
  auto stub = proto::internal::InternalService::NewStub(channel);

  const auto deadline = std::chrono::steady_clock::now() + timeout;
  // The coordinator health loop runs once a second, so a faster poll only
  // spins before the coordinator sees the draining node.
  const auto poll_interval = std::chrono::milliseconds(1000);
  int consecutive_rpc_failures = 0;

  while (std::chrono::steady_clock::now() < deadline) {
    proto::internal::GetShardAssignmentsRequest req;
    req.set_collection_id(0);  // All collections.
    proto::internal::GetShardAssignmentsResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

    auto status = stub->GetShardAssignments(&ctx, req, &resp);
    if (!status.ok()) {
      ++consecutive_rpc_failures;
      common::Logger::Instance().Warn("Drain wait: GetShardAssignments failed (attempt {}): {}",
                                      consecutive_rpc_failures, status.error_message());
      // A short exponential backoff, capped so the loop still polls within
      // the budget.
      auto backoff = std::chrono::milliseconds(
          std::min(2000, 250 * (1 << std::min(3, consecutive_rpc_failures - 1))));
      if (std::chrono::steady_clock::now() + backoff >= deadline) break;
      std::this_thread::sleep_for(backoff);
      continue;
    }
    consecutive_rpc_failures = 0;

    const auto self = static_cast<uint32_t>(node_id);
    const bool still_assigned =
        std::any_of(resp.assignments().begin(), resp.assignments().end(), [&](const auto& a) {
          return a.primary_node_id() == self ||
                 std::find(a.node_ids().begin(), a.node_ids().end(), self) != a.node_ids().end();
        });
    if (!still_assigned) {
      common::Logger::Instance().Info("Drain wait: node {} has no remaining shard assignments",
                                      node_id);
      return;
    }
    // Do not sleep past the deadline when it is close.
    if (std::chrono::steady_clock::now() + poll_interval >= deadline) break;
    std::this_thread::sleep_for(poll_interval);
  }
  common::Logger::Instance().Warn(
      "Drain wait: timeout reached with shards still assigned to node {} "
      "(coordinator will fall back to heartbeat-timeout eviction)",
      node_id);
}

}  // namespace gvdb::server
