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
namespace cluster {
class NodeRegistry;
class Coordinator;
}

namespace consensus {
class TimestampOracle;
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
      std::shared_ptr<cluster::Coordinator> coordinator = nullptr);

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

  // =========================================================================
  // Timestamp Oracle (All Nodes → Coordinator)
  // =========================================================================

  grpc::Status GetTimestamp(
      grpc::ServerContext* context,
      const proto::internal::GetTimestampRequest* request,
      proto::internal::GetTimestampResponse* response) override;

 private:
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<consensus::TimestampOracle> timestamp_oracle_;
  std::shared_ptr<cluster::Coordinator> coordinator_;

  // Statistics
  std::atomic<uint64_t> total_requests_{0};
  std::atomic<uint64_t> total_errors_{0};
};

} // namespace network
} // namespace gvdb