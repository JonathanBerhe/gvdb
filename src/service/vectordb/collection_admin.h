// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <memory>

#include "compute/query_executor.h"
#include "service/vectordb/collection_resolver.h"
#include "vectordb.pb.h"

namespace gvdb::service {

// Collection create, drop and list, stats and health. VectorDBService and
// CoordinatorService both serve these RPCs: they check the permission, then
// call these functions. The resolver must outlive the admin.
class CollectionAdmin {
 public:
  CollectionAdmin(ICollectionResolver& resolver,
                  std::shared_ptr<compute::QueryExecutor> query_executor);

  [[nodiscard]] grpc::Status CreateCollection(const proto::CreateCollectionRequest& request,
                                              proto::CreateCollectionResponse* response);
  [[nodiscard]] grpc::Status DropCollection(const proto::DropCollectionRequest& request,
                                            proto::DropCollectionResponse* response);
  [[nodiscard]] grpc::Status ListCollections(proto::ListCollectionsResponse* response);

  // `total_queries` and `total_query_time_ms` come from the read path of the
  // service, or are 0 on a service without one.
  [[nodiscard]] grpc::Status GetStats(uint64_t total_queries, uint64_t total_query_time_ms,
                                      proto::GetStatsResponse* response);

  static void HealthCheck(proto::HealthCheckResponse* response);

 private:
  ICollectionResolver& resolver_;
  // Its query cache, when it has one, is invalidated on a drop.
  std::shared_ptr<compute::QueryExecutor> query_executor_;
};

}  // namespace gvdb::service
