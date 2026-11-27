// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "internal.grpc.pb.h"
#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "storage/segment/segment_store.h"
#include "compute/query_executor.h"
#include <memory>

namespace gvdb {
namespace cluster {
class PrimaryTermTracker;
class ShardWriteGate;
}
namespace storage {
class BackupManager;
class RestoreManager;
}
}

namespace gvdb {
namespace cluster {
class NodeRegistry;
class Coordinator;
}

namespace consensus {
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
      std::shared_ptr<cluster::Coordinator> coordinator = nullptr,
      std::shared_ptr<consensus::RaftNode> raft_node = nullptr);

  ~InternalService();

  // =========================================================================
  // Shard Management (Coordinator → Data/Query Nodes)
  // =========================================================================

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
  // Collection Metadata (Data/Query Nodes → Coordinator)
  // =========================================================================

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
  // Raft Membership (Coordinator → Coordinator)
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
  // Raft Scale Reconciliation (Operator → Coordinator)
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
  // Backup / Restore Orchestration (Coordinator → Data Node)
  // =========================================================================

  grpc::Status BackupShard(
      grpc::ServerContext* context,
      const proto::internal::BackupShardRequest* request,
      proto::internal::BackupShardResponse* response) override;

  grpc::Status RestoreShard(
      grpc::ServerContext* context,
      const proto::internal::RestoreShardRequest* request,
      proto::internal::RestoreShardResponse* response) override;

  grpc::Status FreezeWrites(
      grpc::ServerContext* context,
      const proto::internal::FreezeWritesRequest* request,
      proto::internal::FreezeWritesResponse* response) override;

  grpc::Status UnfreezeWrites(
      grpc::ServerContext* context,
      const proto::internal::UnfreezeWritesRequest* request,
      proto::internal::UnfreezeWritesResponse* response) override;

  // Wire the data-node's local primary-term view so PausePrimary /
  // PreparePromote can update it directly. Optional; null on a coordinator
  // node, query-node, or test fixture (those instantiations don't host
  // the swap RPCs as data-node handlers anyway). Owned by the caller
  // (data_node_main); must outlive this service.
  void SetPrimaryTermTracker(cluster::PrimaryTermTracker* tracker) {
    primary_term_tracker_ = tracker;
  }

  // Wire the per-shard write fence used by FreezeWrites / UnfreezeWrites.
  // Optional; if null, FreezeWrites returns UNIMPLEMENTED. Owned by the
  // caller (data_node_main); must outlive this service.
  void SetShardWriteGate(cluster::ShardWriteGate* gate) {
    shard_write_gate_ = gate;
  }

  // Wire the storage-layer backup/restore engines. When non-null,
  // BackupShard / RestoreShard dispatch into them; when null, those
  // handlers return UNIMPLEMENTED.
  void SetBackupManager(std::shared_ptr<storage::BackupManager> mgr) {
    backup_manager_ = std::move(mgr);
  }
  void SetRestoreManager(std::shared_ptr<storage::RestoreManager> mgr) {
    restore_manager_ = std::move(mgr);
  }

 private:
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
  std::shared_ptr<consensus::RaftNode> raft_node_;

  // Per-shard primary-term view this data-node is allowed to mutate from
  // PausePrimary / PreparePromote handlers. Null when the service is
  // hosted by a non-data-node binary (coordinator, query-node) — those
  // instances should never see these RPCs in production but we tolerate
  // the call by returning FAILED_PRECONDITION rather than crashing.
  cluster::PrimaryTermTracker* primary_term_tracker_ = nullptr;

  // Per-shard pause-writes fence wired by FreezeWrites / UnfreezeWrites.
  // Null on a binary that doesn't host backup orchestration.
  cluster::ShardWriteGate* shard_write_gate_ = nullptr;

  // Storage-layer engines that execute the per-shard backup/restore.
  // Null on a binary that doesn't host them; the handlers return
  // UNIMPLEMENTED in that case.
  std::shared_ptr<storage::BackupManager> backup_manager_;
  std::shared_ptr<storage::RestoreManager> restore_manager_;
};

} // namespace network
} // namespace gvdb