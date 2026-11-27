// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include "auth/rbac.h"
#include "compute/query_executor.h"
#include "internal.grpc.pb.h"
#include "service/vectordb/collection_resolver.h"
#include "storage/import/bulk_importer.h"
#include "storage/segment/segment_store.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace cluster {
class PrimaryTermTracker;
class ShardWriteGate;
class Coordinator;
}  // namespace cluster
namespace storage {
class BackupManager;
class RestoreManager;
}  // namespace storage

namespace service {

// The optional collaborators of a VectorDBService. A null member turns off
// the feature that needs it. The caller owns the objects behind the raw
// pointers, and they must outlive the service.
struct VectorDBServiceOptions {
  // Every member has a default member initializer. Clang then accepts a
  // designated initializer that leaves the member out.

  // Checks the permission of each request. Null turns off RBAC.
  std::shared_ptr<auth::RbacStore> rbac_store = nullptr;
  // Runs BulkImport. Null makes the import RPCs return UNIMPLEMENTED.
  std::shared_ptr<storage::BulkImporter> bulk_importer = nullptr;
  // Run the single-shard backup and restore. Null makes these RPCs return
  // UNIMPLEMENTED, unless a coordinator is set.
  std::shared_ptr<storage::BackupManager> backup_manager = nullptr;
  std::shared_ptr<storage::RestoreManager> restore_manager = nullptr;
  // Set on a coordinator. Backup and restore then fan out to the shards
  // through Coordinator::StartBackupDistributed and
  // StartRestoreDistributed instead of the local managers.
  std::shared_ptr<cluster::Coordinator> coordinator = nullptr;
  // The local view of the shard primaries. The write path rejects a write
  // when this node is not the primary of the shard at the term of the
  // request. Null skips the check (single node, query node, tests).
  cluster::PrimaryTermTracker* primary_term_tracker = nullptr;
  // The backup write fence. The write path returns ABORTED while a backup
  // freezes the shard. Null skips the check.
  cluster::ShardWriteGate* shard_write_gate = nullptr;
};

// Implementation of the VectorDBService gRPC service.
// Collection management is delegated to an ICollectionResolver,
// which encapsulates the mode-specific behavior (single-node, distributed, coordinator).
class VectorDBService final : public proto::VectorDBService::Service {
 public:
  VectorDBService(std::shared_ptr<storage::ISegmentStore> segment_store,
                  std::shared_ptr<compute::QueryExecutor> query_executor,
                  std::unique_ptr<ICollectionResolver> resolver,
                  VectorDBServiceOptions options = {});

  ~VectorDBService();

  // Collection management
  grpc::Status CreateCollection(grpc::ServerContext* context,
                                const proto::CreateCollectionRequest* request,
                                proto::CreateCollectionResponse* response) override;

  grpc::Status DropCollection(grpc::ServerContext* context,
                              const proto::DropCollectionRequest* request,
                              proto::DropCollectionResponse* response) override;

  grpc::Status ListCollections(grpc::ServerContext* context,
                               const proto::ListCollectionsRequest* request,
                               proto::ListCollectionsResponse* response) override;

  // Vector operations
  grpc::Status Insert(grpc::ServerContext* context, const proto::InsertRequest* request,
                      proto::InsertResponse* response) override;

  grpc::Status StreamInsert(grpc::ServerContext* context,
                            grpc::ServerReader<proto::InsertRequest>* reader,
                            proto::InsertResponse* response) override;

  grpc::Status Search(grpc::ServerContext* context, const proto::SearchRequest* request,
                      proto::SearchResponse* response) override;

  grpc::Status Get(grpc::ServerContext* context, const proto::GetRequest* request,
                   proto::GetResponse* response) override;

  grpc::Status Delete(grpc::ServerContext* context, const proto::DeleteRequest* request,
                      proto::DeleteResponse* response) override;

  grpc::Status UpdateMetadata(grpc::ServerContext* context,
                              const proto::UpdateMetadataRequest* request,
                              proto::UpdateMetadataResponse* response) override;

  grpc::Status Upsert(grpc::ServerContext* context, const proto::UpsertRequest* request,
                      proto::UpsertResponse* response) override;

  grpc::Status RangeSearch(grpc::ServerContext* context, const proto::RangeSearchRequest* request,
                           proto::RangeSearchResponse* response) override;

  grpc::Status ListVectors(grpc::ServerContext* context, const proto::ListVectorsRequest* request,
                           proto::ListVectorsResponse* response) override;

  // Hybrid search (BM25 + vector)
  grpc::Status HybridSearch(grpc::ServerContext* context, const proto::HybridSearchRequest* request,
                            proto::HybridSearchResponse* response) override;

  // Server-side bulk import
  grpc::Status BulkImport(grpc::ServerContext* context, const proto::BulkImportRequest* request,
                          proto::BulkImportResponse* response) override;

  grpc::Status GetImportStatus(grpc::ServerContext* context,
                               const proto::GetImportStatusRequest* request,
                               proto::GetImportStatusResponse* response) override;

  grpc::Status CancelImport(grpc::ServerContext* context, const proto::CancelImportRequest* request,
                            proto::CancelImportResponse* response) override;

  // Backup and restore
  grpc::Status BackupCollection(grpc::ServerContext* context,
                                const proto::BackupCollectionRequest* request,
                                proto::BackupCollectionResponse* response) override;

  grpc::Status RestoreCollection(grpc::ServerContext* context,
                                 const proto::RestoreCollectionRequest* request,
                                 proto::RestoreCollectionResponse* response) override;

  grpc::Status GetBackupStatus(grpc::ServerContext* context,
                               const proto::GetBackupStatusRequest* request,
                               proto::GetBackupStatusResponse* response) override;

  grpc::Status GetRestoreStatus(grpc::ServerContext* context,
                                const proto::GetRestoreStatusRequest* request,
                                proto::GetRestoreStatusResponse* response) override;

  grpc::Status ListBackups(grpc::ServerContext* context, const proto::ListBackupsRequest* request,
                           proto::ListBackupsResponse* response) override;

  grpc::Status CancelBackup(grpc::ServerContext* context, const proto::CancelBackupRequest* request,
                            proto::CancelBackupResponse* response) override;

  // Health and stats
  grpc::Status HealthCheck(grpc::ServerContext* context, const proto::HealthCheckRequest* request,
                           proto::HealthCheckResponse* response) override;

  grpc::Status GetStats(grpc::ServerContext* context, const proto::GetStatsRequest* request,
                        proto::GetStatsResponse* response) override;

 private:
  // Check RBAC permission for the current request. Returns OK if allowed.
  grpc::Status CheckPermission(auth::Permission perm, const std::string& collection_name) const;

  // CheckPermission for a request on one collection. When the check passes,
  // it also records the collection, and the item count when given, for the
  // audit log.
  grpc::Status AuthorizeCollection(auth::Permission perm, const std::string& collection_name,
                                   std::optional<size_t> item_count = std::nullopt) const;

  // The metadata of `id` from the first segment in `segment_ids` that has
  // it, or nullopt when no segment has it.
  std::optional<core::Metadata> FindMetadata(core::VectorId id,
                                             const std::vector<core::SegmentId>& segment_ids) const;

  // Get segment locally, or pull from coordinator if in distributed mode
  storage::Segment* GetOrReplicateSegment(core::SegmentId segment_id);

  // Gather every segment that may hold vectors for a collection: the
  // resolver's shard-primary segments (pulled via replica fallback when not
  // resident locally) followed by every rotated segment the store tracks for
  // the collection. The list is de-duplicated with the shard primaries first.
  // Returns NotFound when the collection resolves to no segments. Used by the
  // by-id handlers so vectors that moved to a rotated segment stay reachable.
  absl::StatusOr<std::vector<storage::Segment*>> CollectCollectionSegments(
      const std::string& collection_name);

  // Fan out search to remote data nodes holding shards for the collection
  grpc::Status SearchDistributed(const proto::SearchRequest* request,
                                 proto::SearchResponse* response, const core::Vector& query);

  // Fan out range search to remote data nodes
  grpc::Status RangeSearchDistributed(const proto::RangeSearchRequest* request,
                                      proto::RangeSearchResponse* response);

  // Per-write-call primary-term gate. Reads the gvdb-shard-term header
  // from the gRPC client metadata and consults primary_term_tracker_
  // for "is this node primary for the shard at that term?". The
  // sample_vector_id is mapped to a shard_id via the same hash the
  // proxy uses (core::ShardIndexForVector) so the gate fires per-shard, not
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
  grpc::Status EvaluateWriteGate(grpc::ServerContext* context, const std::string& collection_name,
                                 uint64_t sample_vector_id);

  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<ICollectionResolver> resolver_;
  // The optional collaborators. VectorDBServiceOptions describes each one.
  std::shared_ptr<auth::RbacStore> rbac_store_;
  std::shared_ptr<storage::BulkImporter> bulk_importer_;
  std::shared_ptr<storage::BackupManager> backup_manager_;
  std::shared_ptr<storage::RestoreManager> restore_manager_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
  cluster::PrimaryTermTracker* primary_term_tracker_;
  cluster::ShardWriteGate* shard_write_gate_;

  // Statistics
  std::atomic<uint64_t> total_queries_{0};
  std::atomic<uint64_t> total_query_time_ms_{0};
};

}  // namespace service
}  // namespace gvdb