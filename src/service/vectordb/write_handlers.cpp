// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Insert, stream insert, upsert, delete and metadata update. Each handler
// checks the permission and the request, then calls WritePath.

#include <grpcpp/grpcpp.h>

#include "absl/strings/str_cat.h"
#include "common/logger.h"
#include "common/metrics.h"
#include "core/status.h"
#include "rpc/primary_term_header.h"
#include "service/vectordb/vectordb_service.h"

namespace gvdb::service {

grpc::Status VectorDBService::Insert(grpc::ServerContext* context,
                                     const proto::InsertRequest* request,
                                     proto::InsertResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::INSERT, request->collection_name(),
                                   request->vectors().size()));

  common::Logger::Instance().Info("Insert: {} vectors into {}", request->vectors().size(),
                                  request->collection_name());

  common::MetricsRegistry::Instance().RecordBatchSize(request->vectors().size());

  common::MetricsTimer timer(common::MetricsRegistry::Instance(),
                             common::MetricsTimer::OperationType::INSERT,
                             request->collection_name());

  constexpr size_t kMaxBatchSize = 50000;

  if (static_cast<size_t>(request->vectors().size()) > kMaxBatchSize) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        absl::StrCat("Batch size exceeds maximum (", kMaxBatchSize,
                                     " vectors). Current batch: ", request->vectors().size()));
  }

  if (request->vectors().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Cannot insert empty vector batch");
  }

  // The first vector's id is the shard sample: the proxy splits per shard
  // before it sends, so all vectors in an RPC share a shard. A malformed
  // client that sends mixed shards still gets the gate of the dominant one.
  RETURN_IF_ERROR(write_path_.CheckGate(rpc::ReadPrimaryTermHeader(context),
                                        request->collection_name(), request->vectors(0).id()));

  size_t inserted = 0;
  RETURN_IF_ERROR(write_path_.Insert(*request, &inserted));

  response->set_inserted_count(inserted);
  response->set_message("Vectors inserted successfully");
  write_path_.InvalidateCache(request->collection_name());
  return grpc::Status::OK;
}

grpc::Status VectorDBService::StreamInsert(grpc::ServerContext* /*context*/,
                                           grpc::ServerReader<proto::InsertRequest>* reader,
                                           proto::InsertResponse* response) {
  common::MetricsTimer timer(common::MetricsRegistry::Instance(),
                             common::MetricsTimer::OperationType::INSERT, "stream");

  uint64_t total_inserted = 0;
  proto::InsertRequest chunk;
  std::string collection_name;
  bool perm_checked = false;

  while (reader->Read(&chunk)) {
    if (chunk.vectors().empty()) continue;
    if (collection_name.empty()) collection_name = chunk.collection_name();
    if (!perm_checked) {
      RETURN_IF_ERROR(guard_.Authorize(auth::Permission::STREAM_INSERT, collection_name));
      perm_checked = true;
    }
    RETURN_IF_ERROR(write_path_.StreamInsertChunk(chunk, collection_name, &total_inserted));
  }

  common::MetricsRegistry::Instance().RecordInsert(collection_name, true, total_inserted);
  common::MetricsRegistry::Instance().RecordBatchSize(total_inserted);

  response->set_inserted_count(total_inserted);
  response->set_message("Vectors inserted successfully");
  if (!collection_name.empty()) write_path_.InvalidateCache(collection_name);
  return grpc::Status::OK;
}

grpc::Status VectorDBService::Upsert(grpc::ServerContext* context,
                                     const proto::UpsertRequest* request,
                                     proto::UpsertResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::UPSERT, request->collection_name(),
                                   request->vectors().size()));
  if (request->collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Collection name is required");
  }
  if (request->vectors().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "No vectors provided");
  }

  RETURN_IF_ERROR(write_path_.CheckGate(rpc::ReadPrimaryTermHeader(context),
                                        request->collection_name(), request->vectors(0).id()));

  storage::Segment::UpsertResult result{0, 0};
  RETURN_IF_ERROR(write_path_.Upsert(*request, &result));

  response->set_upserted_count(result.inserted_count + result.updated_count);
  response->set_inserted_count(result.inserted_count);
  response->set_updated_count(result.updated_count);
  response->set_message("Upsert completed");

  common::Logger::Instance().Debug("Upsert: {} inserted, {} updated in collection '{}'",
                                   result.inserted_count, result.updated_count,
                                   request->collection_name());
  return grpc::Status::OK;
}

grpc::Status VectorDBService::Delete(grpc::ServerContext* context,
                                     const proto::DeleteRequest* request,
                                     proto::DeleteResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::DELETE, request->collection_name(),
                                   request->ids().size()));

  common::Logger::Instance().Info("Delete: {} IDs from {}", request->ids().size(),
                                  request->collection_name());

  common::MetricsTimer timer(common::MetricsRegistry::Instance(),
                             common::MetricsTimer::OperationType::DELETE,
                             request->collection_name());

  // Records a failed delete in the metrics and returns `status`.
  auto fail = [&](grpc::Status status) {
    common::MetricsRegistry::Instance().RecordDelete(request->collection_name(), false);
    return status;
  };

  if (request->ids().empty()) {
    return fail(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "IDs list cannot be empty"));
  }

  constexpr size_t kMaxDeleteBatchSize = 10000;
  if (static_cast<size_t>(request->ids().size()) > kMaxDeleteBatchSize) {
    return fail(
        grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                     absl::StrCat("Cannot delete more than ", kMaxDeleteBatchSize,
                                  " vectors in one request. Requested: ", request->ids().size())));
  }

  if (auto gate = write_path_.CheckGate(rpc::ReadPrimaryTermHeader(context),
                                        request->collection_name(), request->ids(0));
      !gate.ok()) {
    return fail(gate);
  }

  WritePath::DeleteOutcome outcome;
  if (auto status = write_path_.Delete(*request, &outcome); !status.ok()) {
    return fail(status);
  }

  response->set_deleted_count(outcome.deleted_count);
  for (const auto& id : outcome.not_found_ids) {
    response->add_not_found_ids(core::ToUInt64(id));
  }
  response->set_message(absl::StrCat("Deleted ", outcome.deleted_count,
                                     " vector(s) from collection '", request->collection_name(),
                                     "'"));

  common::MetricsRegistry::Instance().RecordDelete(request->collection_name(), true);
  write_path_.InvalidateCache(request->collection_name());
  return grpc::Status::OK;
}

grpc::Status VectorDBService::UpdateMetadata(grpc::ServerContext* context,
                                             const proto::UpdateMetadataRequest* request,
                                             proto::UpdateMetadataResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::UPDATE_METADATA, request->collection_name()));

  common::Logger::Instance().Info("UpdateMetadata: ID {} in {}", request->id(),
                                  request->collection_name());

  common::MetricsTimer timer(common::MetricsRegistry::Instance(),
                             common::MetricsTimer::OperationType::UPDATE_METADATA,
                             request->collection_name());

  // Records a failed update in the metrics and returns `status`.
  auto fail = [&](grpc::Status status) {
    common::MetricsRegistry::Instance().RecordUpdateMetadata(request->collection_name(), false);
    return status;
  };

  if (request->id() == 0) {
    return fail(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Vector ID cannot be 0"));
  }

  if (request->metadata().fields().empty()) {
    return fail(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Metadata cannot be empty"));
  }

  if (auto gate = write_path_.CheckGate(rpc::ReadPrimaryTermHeader(context),
                                        request->collection_name(), request->id());
      !gate.ok()) {
    return fail(gate);
  }

  if (auto status = write_path_.UpdateMetadata(*request, response); !status.ok()) {
    return fail(status);
  }

  common::MetricsRegistry::Instance().RecordUpdateMetadata(request->collection_name(), true);

  response->set_updated(true);
  response->set_message(absl::StrCat("Updated metadata for vector ID ", request->id(),
                                     " in collection '", request->collection_name(), "'"));
  return grpc::Status::OK;
}

}  // namespace gvdb::service
