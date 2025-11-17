#pragma once

#include "vectordb.grpc.pb.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include <memory>
#include <atomic>

namespace gvdb {
namespace network {

// Forward declaration
class CollectionRegistry;

// Implementation of the VectorDBService gRPC service
class VectorDBService final : public proto::VectorDBService::Service {
 public:
  VectorDBService(
      std::shared_ptr<storage::SegmentManager> segment_manager,
      std::shared_ptr<compute::QueryExecutor> query_executor);

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
  std::shared_ptr<storage::SegmentManager> segment_manager_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;

  // Collection registry (in-memory for Phase 1)
  std::unique_ptr<CollectionRegistry> collection_registry_;
  uint32_t next_collection_id_;

  // Statistics
  std::atomic<uint64_t> total_queries_{0};
  std::atomic<uint64_t> total_query_time_ms_{0};
};

} // namespace network
} // namespace gvdb
