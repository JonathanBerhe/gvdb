// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "internal.grpc.pb.h"
#include "cluster/coordinator.h"
#include "cluster/data_node.h"
#include "cluster/shard_manager.h"
#include "storage/segment_store.h"
#include "compute/query_executor.h"
#include <memory>
#include <atomic>

namespace gvdb {
namespace cluster { class PrimaryTermTracker; }
}

namespace gvdb {
namespace cluster {
class NodeRegistry;
class Coordinator;
}

namespace consensus {
class TimestampOracle;
class RaftNode;
}

namespace network {

// Implementation of the InternalService gRPC service for node-to-node communication
class InternalService final : public proto::internal::InternalService::Service {
 public:
  InternalService(
      std::shared_ptr<cluster::ShardManager> shard_manager,
      std::shared_ptr<storage::ISegmentStore> segment_store,
      std::shared_ptr<compute::QueryExecutor> query_executor,
      std::shared_ptr<cluster::NodeRegistry> node_registry = nullptr,
      std::shared_ptr<consensus::TimestampOracle> timestamp_oracle = nullptr,
      std::shared_ptr<cluster::Coordinator> coordinator = nullptr,
      std::shared_ptr<consensus::RaftNode> raft_node = nullptr);

  ~InternalService();

  // =========================================================================
  // Shard Management (Coordinator → Data/Query Nodes)
  // =========================================================================

  grpc::Status AssignShard(
      grpc::ServerContext* context,
      const proto::internal::AssignShardRequest* request,
      proto::internal::AssignShardResponse* response) override;

  grpc::Status GetShardAssignments(
      grpc::ServerContext* context,
      const proto::internal::GetShardAssignmentsRequest* request,
      proto::internal::GetShardAssignmentsResponse* response) override;

  grpc::Status RebalanceShards(
      grpc::ServerContext* context,
      const proto::internal::RebalanceShardsRequest* request,
      proto::internal::RebalanceShardsResponse* response) override;

  // =========================================================================
  // Segment Replication (Data Node → Data Node)
  // =========================================================================

  grpc::Status ReplicateSegment(
      grpc::ServerContext* context,
      const proto::internal::ReplicateSegmentRequest* request,
      proto::internal::ReplicateSegmentResponse* response) override;

  grpc::Status GetSegment(
      grpc::ServerContext* context,
      const proto::internal::GetSegmentRequest* request,
      proto::internal::GetSegmentResponse* response) override;

  grpc::Status ListSegments(
      grpc::ServerContext* context,
      const proto::internal::ListSegmentsRequest* request,
      proto::internal::ListSegmentsResponse* response) override;

  grpc::Status DeleteSegment(
      grpc::ServerContext* context,
      const proto::internal::DeleteSegmentRequest* request,
      proto::internal::DeleteSegmentResponse* response) override;

  grpc::Status CreateSegment(
      grpc::ServerContext* context,
      const proto::internal::CreateSegmentRequest* request,
      proto::internal::CreateSegmentResponse* response) override;

  // =========================================================================
  // Metadata Synchronization (Data/Query Nodes → Coordinator)
  // =========================================================================

  grpc::Status SyncMetadata(
      grpc::ServerContext* context,
      const proto::internal::SyncMetadataRequest* request,
      proto::internal::SyncMetadataResponse* response) override;

  grpc::Status GetCollectionMetadata(
      grpc::ServerContext* context,
      const proto::internal::GetCollectionMetadataRequest* request,
      proto::internal::GetCollectionMetadataResponse* response) override;

  // =========================================================================
  // Query Routing and Execution (Proxy → Query Nodes)
  // =========================================================================

  grpc::Status RouteQuery(
      grpc::ServerContext* context,
      const proto::internal::RouteQueryRequest* request,
      proto::internal::RouteQueryResponse* response) override;

  grpc::Status ExecuteShardQuery(
      grpc::ServerContext* context,
      const proto::internal::ExecuteShardQueryRequest* request,
      proto::internal::ExecuteShardQueryResponse* response) override;

  // =========================================================================
  // Data Transfer (for rebalancing)
  // =========================================================================

  grpc::Status TransferData(
      grpc::ServerContext* context,
      const proto::internal::TransferDataRequest* request,
      proto::internal::TransferDataResponse* response) override;

  // =========================================================================
  // Health Monitoring (All Nodes → Coordinator)
  // =========================================================================

  grpc::Status Heartbeat(
      grpc::ServerContext* context,
      const proto::internal::HeartbeatRequest* request,
      proto::internal::HeartbeatResponse* response) override;

  grpc::Status GetClusterHealth(
      grpc::ServerContext* context,
      const proto::internal::GetClusterHealthRequest* request,
      proto::internal::GetClusterHealthResponse* response) override;

  grpc::Status GetLeaderInfo(
      grpc::ServerContext* context,
      const proto::internal::GetLeaderInfoRequest* request,
      proto::internal::GetLeaderInfoResponse* response) override;

  // =========================================================================
  // Raft Membership (Coordinator → Coordinator; roadmap 1.7b)
  // =========================================================================

  grpc::Status JoinCluster(
      grpc::ServerContext* context,
      const proto::internal::JoinClusterRequest* request,
      proto::internal::JoinClusterResponse* response) override;

  grpc::Status RemovePeer(
      grpc::ServerContext* context,
      const proto::internal::RemovePeerRequest* request,
      proto::internal::RemovePeerResponse* response) override;

  // =========================================================================
  // Raft Scale Reconciliation (Operator → Coordinator; roadmap 1.8)
  // =========================================================================

  grpc::Status GetRaftMembership(
      grpc::ServerContext* context,
      const proto::internal::GetRaftMembershipRequest* request,
      proto::internal::GetRaftMembershipResponse* response) override;

  grpc::Status TransferLeadership(
      grpc::ServerContext* context,
      const proto::internal::TransferLeadershipRequest* request,
      proto::internal::TransferLeadershipResponse* response) override;

  // =========================================================================
  // Two-Phase Primary Swap (Coordinator → Data Node)
  // =========================================================================

  grpc::Status PausePrimary(
      grpc::ServerContext* context,
      const proto::internal::PausePrimaryRequest* request,
      proto::internal::PausePrimaryResponse* response) override;

  grpc::Status PreparePromote(
      grpc::ServerContext* context,
      const proto::internal::PreparePromoteRequest* request,
      proto::internal::PreparePromoteResponse* response) override;

  // =========================================================================
  // Timestamp Oracle (All Nodes → Coordinator)
  // =========================================================================

  grpc::Status GetTimestamp(
      grpc::ServerContext* context,
      const proto::internal::GetTimestampRequest* request,
      proto::internal::GetTimestampResponse* response) override;

  // Wire the data-node's local primary-term view so PausePrimary /
  // PreparePromote can update it directly. Optional; null on a coordinator
  // node, query-node, or test fixture (those instantiations don't host
  // the swap RPCs as data-node handlers anyway). Owned by the caller
  // (data_node_main); must outlive this service.
  void SetPrimaryTermTracker(cluster::PrimaryTermTracker* tracker) {
    primary_term_tracker_ = tracker;
  }

 private:
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<consensus::TimestampOracle> timestamp_oracle_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
  std::shared_ptr<consensus::RaftNode> raft_node_;

  // Per-shard primary-term view this data-node is allowed to mutate from
  // PausePrimary / PreparePromote handlers. Null when the service is
  // hosted by a non-data-node binary (coordinator, query-node) — those
  // instances should never see these RPCs in production but we tolerate
  // the call by returning FAILED_PRECONDITION rather than crashing.
  cluster::PrimaryTermTracker* primary_term_tracker_ = nullptr;

  // Statistics
  std::atomic<uint64_t> total_requests_{0};
  std::atomic<uint64_t> total_errors_{0};
};

} // namespace network
} // namespace gvdb