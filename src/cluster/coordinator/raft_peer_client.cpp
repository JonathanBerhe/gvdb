// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/coordinator/raft_peer_client.h"

#include <grpcpp/grpcpp.h>

#include "internal.grpc.pb.h"
#include "rpc/proto_conversions.h"

namespace gvdb {
namespace cluster {

namespace {

std::unique_ptr<proto::internal::InternalService::Stub> NewStub(const std::string& address) {
  return proto::internal::InternalService::NewStub(
      grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
}

}  // namespace

absl::StatusOr<consensus::IRaftPeerClient::LeaderInfo> GrpcRaftPeerClient::GetLeaderInfo(
    const std::string& address, std::chrono::milliseconds timeout) {
  auto stub = NewStub(address);
  proto::internal::GetLeaderInfoRequest req;
  proto::internal::GetLeaderInfoResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + timeout);

  auto status = stub->GetLeaderInfo(&ctx, req, &resp);
  if (!status.ok()) return rpc::FromGrpcStatus(status);
  return LeaderInfo{resp.leader_id(), resp.current_term()};
}

absl::StatusOr<consensus::IRaftPeerClient::JoinReply> GrpcRaftPeerClient::JoinCluster(
    const std::string& address, int32_t node_id, const std::string& raft_endpoint,
    std::chrono::milliseconds timeout) {
  auto stub = NewStub(address);
  proto::internal::JoinClusterRequest req;
  req.set_node_id(static_cast<uint32_t>(node_id));
  req.set_raft_advertise_address(raft_endpoint);
  proto::internal::JoinClusterResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + timeout);

  auto status = stub->JoinCluster(&ctx, req, &resp);
  if (!status.ok()) return rpc::FromGrpcStatus(status);
  return JoinReply{resp.success(), resp.message(), resp.current_leader_id()};
}

}  // namespace cluster
}  // namespace gvdb
