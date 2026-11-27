// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include "cluster/coordinator/coordinator.h"
#include "cluster/shard_manager.h"
#include "compute/query_executor.h"
#include "internal.grpc.pb.h"
#include "storage/segment/segment_store.h"

namespace gvdb {
namespace cluster {
class PrimaryTermTracker;
class ShardWriteGate;
}  // namespace cluster
namespace storage {
class BackupManager;
class RestoreManager;
}  // namespace storage
}  // namespace gvdb

namespace gvdb {
namespace cluster {
class NodeRegistry;
class Coordinator;
}  // namespace cluster

namespace consensus {
class RaftNode;
}

namespace service {

// The optional collaborators of an InternalService. A null member turns off
// the handlers that need it. The caller owns the objects behind the raw
// pointers, and they must outlive the service.
struct InternalServiceOptions {
  // Every member has a default member initializer. Clang then accepts a
  // designated initializer that leaves the member out.

  // Set on a coordinator. The coordinator RPCs use them: Heartbeat,
  // RouteQuery, GetCollectionMetadata, GetClusterHealth and the Raft
  // membership RPCs.
  std::shared_ptr<cluster::NodeRegistry> node_registry = nullptr;
  std::shared_ptr<cluster::Coordinator> coordinator = nullptr;
  std::shared_ptr<consensus::RaftNode> raft_node = nullptr;
  // Run BackupShard and RestoreShard. Null makes these RPCs return
  // UNIMPLEMENTED.
  std::shared_ptr<storage::BackupManager> backup_manager = nullptr;
  std::shared_ptr<storage::RestoreManager> restore_manager = nullptr;
  // The local view of the shard primaries, which PausePrimary and
  // PreparePromote change. Null makes these RPCs return
  // FAILED_PRECONDITION (coordinator, query node).
  cluster::PrimaryTermTracker* primary_term_tracker = nullptr;
  // The backup write fence of FreezeWrites and UnfreezeWrites. Null makes
  // these RPCs return UNIMPLEMENTED.
  cluster::ShardWriteGate* shard_write_gate = nullptr;
};

// Implementation of the InternalService gRPC service for node-to-node communication
class InternalService final : public proto::internal::InternalService::Service {
 public:
  InternalService(std::shared_ptr<cluster::ShardManager> shard_manager,
                  std::shared_ptr<storage::ISegmentStore> segment_store,
                  std::shared_ptr<compute::QueryExecutor> query_executor,
                  InternalServiceOptions options = {});

  ~InternalService() override;

  // =========================================================================
  // Shard Management (Coordinator → Data/Query Nodes)
  // =========================================================================

  grpc::Status GetShardAssignments(grpc::ServerContext* context,
                                   const proto::internal::GetShardAssignmentsRequest* request,
                                   proto::internal::GetShardAssignmentsResponse* response) override;

  grpc::Status RebalanceShards(grpc::ServerContext* context,
                               const proto::internal::RebalanceShardsRequest* request,
                               proto::internal::RebalanceShardsResponse* response) override;

  // =========================================================================
  // Segment Replication (Data Node → Data Node)
  // =========================================================================

  grpc::Status ReplicateSegment(grpc::ServerContext* context,
                                const proto::internal::ReplicateSegmentRequest* request,
                                proto::internal::ReplicateSegmentResponse* response) override;

  grpc::Status GetSegment(grpc::ServerContext* context,
                          const proto::internal::GetSegmentRequest* request,
                          proto::internal::GetSegmentResponse* response) override;

  grpc::Status ListSegments(grpc::ServerContext* context,
                            const proto::internal::ListSegmentsRequest* request,
                            proto::internal::ListSegmentsResponse* response) override;

  grpc::Status DeleteSegment(grpc::ServerContext* context,
                             const proto::internal::DeleteSegmentRequest* request,
                             proto::internal::DeleteSegmentResponse* response) override;

  grpc::Status CreateSegment(grpc::ServerContext* context,
                             const proto::internal::CreateSegmentRequest* request,
                             proto::internal::CreateSegmentResponse* response) override;

  // =========================================================================
  // Collection Metadata (Data/Query Nodes → Coordinator)
  // =========================================================================

  grpc::Status GetCollectionMetadata(
      grpc::ServerContext* context, const proto::internal::GetCollectionMetadataRequest* request,
      proto::internal::GetCollectionMetadataResponse* response) override;

  // =========================================================================
  // Query Routing and Execution (Proxy → Query Nodes)
  // =========================================================================

  grpc::Status RouteQuery(grpc::ServerContext* context,
                          const proto::internal::RouteQueryRequest* request,
                          proto::internal::RouteQueryResponse* response) override;

  grpc::Status ExecuteShardQuery(grpc::ServerContext* context,
                                 const proto::internal::ExecuteShardQueryRequest* request,
                                 proto::internal::ExecuteShardQueryResponse* response) override;

  // =========================================================================
  // Health Monitoring (All Nodes → Coordinator)
  // =========================================================================

  grpc::Status Heartbeat(grpc::ServerContext* context,
                         const proto::internal::HeartbeatRequest* request,
                         proto::internal::HeartbeatResponse* response) override;

  grpc::Status GetClusterHealth(grpc::ServerContext* context,
                                const proto::internal::GetClusterHealthRequest* request,
                                proto::internal::GetClusterHealthResponse* response) override;

  grpc::Status GetLeaderInfo(grpc::ServerContext* context,
                             const proto::internal::GetLeaderInfoRequest* request,
                             proto::internal::GetLeaderInfoResponse* response) override;

  // =========================================================================
  // Raft Membership (Coordinator → Coordinator)
  // =========================================================================

  grpc::Status JoinCluster(grpc::ServerContext* context,
                           const proto::internal::JoinClusterRequest* request,
                           proto::internal::JoinClusterResponse* response) override;

  grpc::Status RemovePeer(grpc::ServerContext* context,
                          const proto::internal::RemovePeerRequest* request,
                          proto::internal::RemovePeerResponse* response) override;

  // =========================================================================
  // Raft Scale Reconciliation (Operator → Coordinator)
  // =========================================================================

  grpc::Status GetRaftMembership(grpc::ServerContext* context,
                                 const proto::internal::GetRaftMembershipRequest* request,
                                 proto::internal::GetRaftMembershipResponse* response) override;

  grpc::Status TransferLeadership(grpc::ServerContext* context,
                                  const proto::internal::TransferLeadershipRequest* request,
                                  proto::internal::TransferLeadershipResponse* response) override;

  // =========================================================================
  // Two-Phase Primary Swap (Coordinator → Data Node)
  // =========================================================================

  grpc::Status PausePrimary(grpc::ServerContext* context,
                            const proto::internal::PausePrimaryRequest* request,
                            proto::internal::PausePrimaryResponse* response) override;

  grpc::Status PreparePromote(grpc::ServerContext* context,
                              const proto::internal::PreparePromoteRequest* request,
                              proto::internal::PreparePromoteResponse* response) override;

  // =========================================================================
  // Backup / Restore Orchestration (Coordinator → Data Node)
  // =========================================================================

  grpc::Status BackupShard(grpc::ServerContext* context,
                           const proto::internal::BackupShardRequest* request,
                           proto::internal::BackupShardResponse* response) override;

  grpc::Status RestoreShard(grpc::ServerContext* context,
                            const proto::internal::RestoreShardRequest* request,
                            proto::internal::RestoreShardResponse* response) override;

  grpc::Status FreezeWrites(grpc::ServerContext* context,
                            const proto::internal::FreezeWritesRequest* request,
                            proto::internal::FreezeWritesResponse* response) override;

  grpc::Status UnfreezeWrites(grpc::ServerContext* context,
                              const proto::internal::UnfreezeWritesRequest* request,
                              proto::internal::UnfreezeWritesResponse* response) override;

 private:
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  // The optional collaborators. InternalServiceOptions describes each one.
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
  std::shared_ptr<consensus::RaftNode> raft_node_;
  std::shared_ptr<storage::BackupManager> backup_manager_;
  std::shared_ptr<storage::RestoreManager> restore_manager_;
  cluster::PrimaryTermTracker* primary_term_tracker_;
  cluster::ShardWriteGate* shard_write_gate_;
};

}  // namespace service
}  // namespace gvdb