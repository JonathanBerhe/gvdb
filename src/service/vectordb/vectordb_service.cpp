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
      read_path_(segment_store_, query_executor_, *resolver_, locator_),
      admin_(*resolver_, query_executor_),
      backup_(*resolver_, segment_store_, guard_, std::move(options.backup_manager),
              std::move(options.restore_manager), std::move(options.coordinator)),
      import_(*resolver_, guard_, std::move(options.bulk_importer)) {
  common::Logger::Instance().Info(
      "VectorDBService initialized (RBAC {}, BulkImport {}, Backup {}, Restore {})",
      guard_.enabled() ? "enabled" : "disabled", import_.enabled() ? "enabled" : "disabled",
      backup_.backup_enabled() ? "enabled" : "disabled",
      backup_.restore_enabled() ? "enabled" : "disabled");
}

VectorDBService::~VectorDBService() = default;

}  // namespace gvdb::service
