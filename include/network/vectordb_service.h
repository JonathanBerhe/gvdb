// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "vectordb.grpc.pb.h"
#include "internal.grpc.pb.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "network/collection_resolver.h"
#include <memory>
#include <atomic>
#include <string>

namespace gvdb {
namespace network {

// Implementation of the VectorDBService gRPC service.
// Collection management is delegated to an ICollectionResolver,
// which encapsulates the mode-specific behavior (single-node, distributed, coordinator).
class VectorDBService final : public proto::VectorDBService::Service {
 public:
  VectorDBService(
      std::shared_ptr<storage::SegmentManager> segment_manager,
      std::shared_ptr<compute::QueryExecutor> query_executor,
      std::unique_ptr<ICollectionResolver> resolver);

  ~VectorDBService();

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

  grpc::Status ListVectors(
      grpc::ServerContext* context,
      const proto::ListVectorsRequest* request,
      proto::ListVectorsResponse* response) override;

  // Hybrid search (BM25 + vector)
  grpc::Status HybridSearch(
      grpc::ServerContext* context,
      const proto::HybridSearchRequest* request,
      proto::HybridSearchResponse* response) override;

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
  // Get segment locally, or pull from coordinator if in distributed mode
  storage::Segment* GetOrReplicateSegment(core::SegmentId segment_id);

  // Fan out search to remote data nodes holding shards for the collection
  grpc::Status SearchDistributed(
      const proto::SearchRequest* request,
      proto::SearchResponse* response,
      const core::Vector& query);

  std::shared_ptr<storage::SegmentManager> segment_manager_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<ICollectionResolver> resolver_;

  // Statistics
  std::atomic<uint64_t> total_queries_{0};
  std::atomic<uint64_t> total_query_time_ms_{0};
};

} // namespace network
} // namespace gvdb