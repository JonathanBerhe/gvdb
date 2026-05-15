// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "vectordb.grpc.pb.h"
#include "internal.grpc.pb.h"
#include "storage/segment_store.h"
#include "storage/bulk_importer.h"
#include "compute/query_executor.h"
#include "network/collection_resolver.h"
#include "auth/rbac.h"
#include <memory>
#include <atomic>
#include <string>

namespace gvdb {
namespace cluster { class PrimaryTermTracker; }
namespace storage {
class BackupManager;
class RestoreManager;
}

namespace network {

// Implementation of the VectorDBService gRPC service.
// Collection management is delegated to an ICollectionResolver,
// which encapsulates the mode-specific behavior (single-node, distributed, coordinator).
class VectorDBService final : public proto::VectorDBService::Service {
 public:
  VectorDBService(
      std::shared_ptr<storage::ISegmentStore> segment_store,
      std::shared_ptr<compute::QueryExecutor> query_executor,
      std::unique_ptr<ICollectionResolver> resolver,
      std::shared_ptr<auth::RbacStore> rbac_store = nullptr,
      std::shared_ptr<storage::BulkImporter> bulk_importer = nullptr,
      std::shared_ptr<storage::BackupManager> backup_manager = nullptr,
      std::shared_ptr<storage::RestoreManager> restore_manager = nullptr);

  ~VectorDBService();

  // Wire the data-node's local primary-term view so writes can be gated
  // against "am I primary for this shard at this term?". Optional;
  // when null (single-node, tests, query-nodes) the gate is skipped
  // and writes accept any term silently. Owned by the caller (typically
  // data_node_main); must outlive this service.
  void SetPrimaryTermTracker(cluster::PrimaryTermTracker* tracker) {
    primary_term_tracker_ = tracker;
  }

  // Collection management
  grpc::Status CreateCollection(
      grpc::ServerContext* context,
      const proto::CreateCollectionRequest* request,
      proto::CreateCollectionResponse* response) override;

  grpc::Status DropCollection(
      grpc::ServerContext* context,
      const proto::DropCollectionRequest* request,
      proto::DropCollectionResponse* response) override;

  grpc::Status ListCollections(
      grpc::ServerContext* context,
      const proto::ListCollectionsRequest* request,
      proto::ListCollectionsResponse* response) override;

  // Vector operations
  grpc::Status Insert(
      grpc::ServerContext* context,
      const proto::InsertRequest* request,
      proto::InsertResponse* response) override;

  grpc::Status StreamInsert(
      grpc::ServerContext* context,
      grpc::ServerReader<proto::InsertRequest>* reader,
      proto::InsertResponse* response) override;

  grpc::Status Search(
      grpc::ServerContext* context,
      const proto::SearchRequest* request,
      proto::SearchResponse* response) override;

  grpc::Status Get(
      grpc::ServerContext* context,
      const proto::GetRequest* request,
      proto::GetResponse* response) override;

  grpc::Status Delete(
      grpc::ServerContext* context,
      const proto::DeleteRequest* request,
      proto::DeleteResponse* response) override;

  grpc::Status UpdateMetadata(
      grpc::ServerContext* context,
      const proto::UpdateMetadataRequest* request,
      proto::UpdateMetadataResponse* response) override;

  grpc::Status Upsert(
      grpc::ServerContext* context,
      const proto::UpsertRequest* request,
      proto::UpsertResponse* response) override;

  grpc::Status RangeSearch(
      grpc::ServerContext* context,
      const proto::RangeSearchRequest* request,
      proto::RangeSearchResponse* response) override;

  grpc::Status ListVectors(
      grpc::ServerContext* context,
      const proto::ListVectorsRequest* request,
      proto::ListVectorsResponse* response) override;

  // Hybrid search (BM25 + vector)
  grpc::Status HybridSearch(
      grpc::ServerContext* context,
      const proto::HybridSearchRequest* request,
      proto::HybridSearchResponse* response) override;

  // Server-side bulk import
  grpc::Status BulkImport(
      grpc::ServerContext* context,
      const proto::BulkImportRequest* request,
      proto::BulkImportResponse* response) override;

  grpc::Status GetImportStatus(
      grpc::ServerContext* context,
      const proto::GetImportStatusRequest* request,
      proto::GetImportStatusResponse* response) override;

  grpc::Status CancelImport(
      grpc::ServerContext* context,
      const proto::CancelImportRequest* request,
      proto::CancelImportResponse* response) override;

  // Backup and restore
  grpc::Status BackupCollection(
      grpc::ServerContext* context,
      const proto::BackupCollectionRequest* request,
      proto::BackupCollectionResponse* response) override;

  grpc::Status RestoreCollection(
      grpc::ServerContext* context,
      const proto::RestoreCollectionRequest* request,
      proto::RestoreCollectionResponse* response) override;

  grpc::Status GetBackupStatus(
      grpc::ServerContext* context,
      const proto::GetBackupStatusRequest* request,
      proto::GetBackupStatusResponse* response) override;

  grpc::Status GetRestoreStatus(
      grpc::ServerContext* context,
      const proto::GetRestoreStatusRequest* request,
      proto::GetRestoreStatusResponse* response) override;

  grpc::Status ListBackups(
      grpc::ServerContext* context,
      const proto::ListBackupsRequest* request,
      proto::ListBackupsResponse* response) override;

  grpc::Status CancelBackup(
      grpc::ServerContext* context,
      const proto::CancelBackupRequest* request,
      proto::CancelBackupResponse* response) override;

  // Health and stats
  grpc::Status HealthCheck(
      grpc::ServerContext* context,
      const proto::HealthCheckRequest* request,
      proto::HealthCheckResponse* response) override;

  grpc::Status GetStats(
      grpc::ServerContext* context,
      const proto::GetStatsRequest* request,
      proto::GetStatsResponse* response) override;

 private:
  // Check RBAC permission for the current request. Returns OK if allowed.
  grpc::Status CheckPermission(auth::Permission perm,
                                const std::string& collection_name) const;

  // Get segment locally, or pull from coordinator if in distributed mode
  storage::Segment* GetOrReplicateSegment(core::SegmentId segment_id);

  // Fan out search to remote data nodes holding shards for the collection
  grpc::Status SearchDistributed(
      const proto::SearchRequest* request,
      proto::SearchResponse* response,
      const core::Vector& query);

  // Fan out range search to remote data nodes
  grpc::Status RangeSearchDistributed(
      const proto::RangeSearchRequest* request,
      proto::RangeSearchResponse* response);

  // Per-write-call primary-term gate. Reads the gvdb-shard-term header
  // from the gRPC client metadata and consults primary_term_tracker_
  // for "is this node primary for the shard at that term?". The
  // sample_vector_id is mapped to a shard_id via the same hash the
  // proxy uses (vid % num_shards) so the gate fires per-shard, not
  // per-collection.
  //
  // Behavior:
  //   - tracker is null (single-node / tests / query-nodes): OK.
  //   - header absent (pre-1.x client): OK with a one-shot warn log
  //     so a rolling upgrade doesn't break writes from old proxies.
  //   - header malformed: INVALID_ARGUMENT.
  //   - tracker says StaleTerm or NotPrimary: ABORTED with detail.
  //   - tracker says UnknownShard (proxy raced past heartbeat sync,
  //     or this node really doesn't own the shard): FAILED_PRECONDITION.
  //   - tracker says Accept: OK.
  grpc::Status EvaluateWriteGate(grpc::ServerContext* context,
                                  const std::string& collection_name,
                                  uint64_t sample_vector_id);

  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<ICollectionResolver> resolver_;
  std::shared_ptr<auth::RbacStore> rbac_store_;
  std::shared_ptr<storage::BulkImporter> bulk_importer_;
  // Optional storage-layer engines. Null on a binary that doesn't host
  // backup orchestration; the handlers return UNIMPLEMENTED in that case.
  std::shared_ptr<storage::BackupManager> backup_manager_;
  std::shared_ptr<storage::RestoreManager> restore_manager_;

  // Optional write-path primary-term gate. Null on single-node / query-
  // nodes / tests; non-null on a real distributed data-node, populated
  // via SetPrimaryTermTracker before serving begins.
  cluster::PrimaryTermTracker* primary_term_tracker_ = nullptr;

  // Statistics
  std::atomic<uint64_t> total_queries_{0};
  std::atomic<uint64_t> total_query_time_ms_{0};
};

} // namespace network
} // namespace gvdb