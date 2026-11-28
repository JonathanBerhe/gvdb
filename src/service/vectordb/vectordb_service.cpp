// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// VectorDBService construction. The handlers are in the *_handlers.cpp files
// of this folder.

#include "service/vectordb/vectordb_service.h"

#include <utility>

#include "common/logger.h"
#include "storage/backup/backup_manager.h"
#include "storage/backup/restore_manager.h"

namespace gvdb::service {

VectorDBService::VectorDBService(std::shared_ptr<storage::ISegmentStore> segment_store,
                                 std::shared_ptr<compute::QueryExecutor> query_executor,
                                 std::unique_ptr<ICollectionResolver> resolver,
                                 VectorDBServiceOptions options)
    : segment_store_(std::move(segment_store)),
      query_executor_(std::move(query_executor)),
      resolver_(std::move(resolver)),
      guard_(std::move(options.rbac_store)),
      locator_(segment_store_, *resolver_),
      write_path_(segment_store_, query_executor_, *resolver_, locator_,
                  options.primary_term_tracker, options.shard_write_gate),
      bulk_importer_(std::move(options.bulk_importer)),
      backup_manager_(std::move(options.backup_manager)),
      restore_manager_(std::move(options.restore_manager)),
      coordinator_(std::move(options.coordinator)) {
  common::Logger::Instance().Info(
      "VectorDBService initialized (RBAC {}, BulkImport {}, Backup {}, Restore {})",
      guard_.enabled() ? "enabled" : "disabled", bulk_importer_ ? "enabled" : "disabled",
      backup_manager_ ? "enabled" : "disabled", restore_manager_ ? "enabled" : "disabled");
}

VectorDBService::~VectorDBService() = default;

}  // namespace gvdb::service
