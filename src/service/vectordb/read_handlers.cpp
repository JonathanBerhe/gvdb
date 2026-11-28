// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Search, range search, hybrid search, get and list. Each handler checks the
// permission and the request, then calls ReadPath.

#include <grpcpp/grpcpp.h>

#include "absl/strings/str_cat.h"
#include "common/logger.h"
#include "common/metrics.h"
#include "core/status.h"
#include "service/vectordb/vectordb_service.h"

namespace gvdb::service {

grpc::Status VectorDBService::RangeSearch(grpc::ServerContext* /*context*/,
                                          const proto::RangeSearchRequest* request,
                                          proto::RangeSearchResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::RANGE_SEARCH, request->collection_name()));

  if (request->collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Collection name is required");
  }
  if (!request->has_query_vector()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Query vector is required");
  }
  return read_path_.RangeSearch(*request, response);
}

grpc::Status VectorDBService::Search(grpc::ServerContext* /*context*/,
                                     const proto::SearchRequest* request,
                                     proto::SearchResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::SEARCH, request->collection_name()));

  common::MetricsTimer metrics_timer(common::MetricsRegistry::Instance(),
                                     common::MetricsTimer::OperationType::SEARCH,
                                     request->collection_name());

  return read_path_.Search(*request, response);
}

grpc::Status VectorDBService::HybridSearch(grpc::ServerContext* /*context*/,
                                           const proto::HybridSearchRequest* request,
                                           proto::HybridSearchResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::HYBRID_SEARCH, request->collection_name()));

  common::Logger::Instance().Info("HybridSearch: collection={}, text_query='{}'",
                                  request->collection_name(), request->text_query());

  bool has_vector = request->has_query_vector() && request->query_vector().dimension() > 0;
  bool has_text = !request->text_query().empty();
  bool has_sparse = request->has_sparse_query() && request->sparse_query().indices_size() > 0;

  if (!has_vector && !has_text && !has_sparse) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        "At least one of query_vector, text_query, or sparse_query must be provided");
  }

  return read_path_.HybridSearch(*request, response);
}

grpc::Status VectorDBService::Get(grpc::ServerContext* /*context*/,
                                  const proto::GetRequest* request, proto::GetResponse* response) {
  RETURN_IF_ERROR(
      guard_.Authorize(auth::Permission::GET, request->collection_name(), request->ids().size()));

  common::Logger::Instance().Info("Get: {} IDs from {}", request->ids().size(),
                                  request->collection_name());

  common::MetricsTimer timer(common::MetricsRegistry::Instance(),
                             common::MetricsTimer::OperationType::GET, request->collection_name());

  // Records a failed get in the metrics and returns `status`.
  auto fail = [&](grpc::Status status) {
    common::MetricsRegistry::Instance().RecordGet(request->collection_name(), false);
    return status;
  };

  if (request->ids().empty()) {
    return fail(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "IDs list cannot be empty"));
  }

  constexpr size_t kMaxGetBatchSize = 10000;
  if (static_cast<size_t>(request->ids().size()) > kMaxGetBatchSize) {
    return fail(
        grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                     absl::StrCat("Cannot get more than ", kMaxGetBatchSize,
                                  " vectors in one request. Requested: ", request->ids().size())));
  }

  return read_path_.Get(*request, response);
}

grpc::Status VectorDBService::ListVectors(grpc::ServerContext* /*context*/,
                                          const proto::ListVectorsRequest* request,
                                          proto::ListVectorsResponse* response) {
  RETURN_IF_ERROR(guard_.Check(auth::Permission::LIST_VECTORS, request->collection_name()));

  return read_path_.ListVectors(*request, response);
}

}  // namespace gvdb::service
