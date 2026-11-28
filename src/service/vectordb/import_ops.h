// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>

#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/request_guard.h"
#include "storage/import/bulk_importer.h"
#include "vectordb.pb.h"

namespace gvdb::service {

// Server-side bulk import: start, status and cancel. A null importer makes
// these RPCs return UNIMPLEMENTED. The handler checks the permission of
// BulkImport first. GetImportStatus and CancelImport check it here, after
// the job lookup gives the collection.
//
// The resolver and the guard must outlive the import ops.
class ImportOps {
 public:
  ImportOps(ICollectionResolver& resolver, const RequestGuard& guard,
            std::shared_ptr<storage::BulkImporter> bulk_importer);

  [[nodiscard]] grpc::Status BulkImport(const proto::BulkImportRequest& request,
                                        proto::BulkImportResponse* response);
  [[nodiscard]] grpc::Status GetImportStatus(const proto::GetImportStatusRequest& request,
                                             proto::GetImportStatusResponse* response);
  [[nodiscard]] grpc::Status CancelImport(const proto::CancelImportRequest& request,
                                          proto::CancelImportResponse* response);

  [[nodiscard]] bool enabled() const { return bulk_importer_ != nullptr; }

 private:
  ICollectionResolver& resolver_;
  const RequestGuard& guard_;
  std::shared_ptr<storage::BulkImporter> bulk_importer_;
};

}  // namespace gvdb::service
