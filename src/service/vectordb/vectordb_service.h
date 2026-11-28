// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <optional>
#include <string>
#include "auth/rbac.h"
#include "compute/query_executor.h"
#include "internal.grpc.pb.h"
#include "service/vectordb/backup_ops.h"
#include "service/vectordb/collection_admin.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/import_ops.h"
#include "service/vectordb/read_path.h"
#include "service/vectordb/request_guard.h"
#include "service/vectordb/segment_locator.h"
#include "service/vectordb/write_path.h"
#include "storage/import/bulk_importer.h"
#include "storage/segment/segment_store.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
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
  // UNIMPLEMENTED.
  std::shared_ptr<storage::BackupManager> backup_manager = nullptr;
  std::shared_ptr<storage::RestoreManager> restore_manager = nullptr;
  // The local view of the shard primaries. The write path rejects a write
  // when this node is not the primary of the shard at the term of the
  // request. Null skips the check (single node, query node, tests).
  cluster::PrimaryTermTracker* primary_term_tracker = nullptr;
  // The backup write fence. The write path returns ABORTED while a backup
  // freezes the shard. Null skips the check.
  cluster::ShardWriteGate* shard_write_gate = nullptr;
};

// The client API of the programs that hold vectors: the single node, the
// data nodes and the query nodes. Each handler checks the permission and the
// request, then calls one of the parts below. The resolver decides where the
// collections live: on this node (single node) or in the coordinator's
// registry (data and query nodes). The coordinator program serves
// CoordinatorService instead.
class VectorDBService final : public proto::VectorDBService::Service {
 public:
  VectorDBService(std::shared_ptr<storage::ISegmentStore> segment_store,
                  std::shared_ptr<compute::QueryExecutor> query_executor,
                  std::unique_ptr<ICollectionResolver> resolver,
                  VectorDBServiceOptions options = {});

  ~VectorDBService() override;

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
  // The members are initialized in this order: the parts below resolver_
  // keep references to it, the paths to locator_, and backup_ and import_ to
  // guard_.
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<ICollectionResolver> resolver_;
  RequestGuard guard_;
  SegmentLocator locator_;
  WritePath write_path_;
  ReadPath read_path_;
  CollectionAdmin admin_;
  BackupOps backup_;
  ImportOps import_;
};

}  // namespace service
}  // namespace gvdb