// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <mutex>

#include "vectordb.grpc.pb.h"
#include "internal.grpc.pb.h"
#include "cluster/load_balancer.h"
#include <grpcpp/grpcpp.h>

namespace gvdb {
namespace network {

// Node information for load balancing
struct QueryNode {
  core::NodeId id;
  std::string address;
  std::unique_ptr<proto::VectorDBService::Stub> client;
};

// Proxy service that routes client requests to backend nodes
class ProxyService final : public proto::VectorDBService::Service {
 public:
  ProxyService(
      const std::vector<std::string>& coordinator_addrs,
      const std::vector<std::string>& query_node_addrs,
      const std::vector<std::string>& data_node_addrs);

  ~ProxyService() override = default;

  // Metadata operations → Coordinator
  grpc::Status HealthCheck(grpc::ServerContext* context,
                          const proto::HealthCheckRequest* request,
                          proto::HealthCheckResponse* response) override;

  grpc::Status CreateCollection(grpc::ServerContext* context,
                               const proto::CreateCollectionRequest* request,
                               proto::CreateCollectionResponse* response) override;

  grpc::Status DropCollection(grpc::ServerContext* context,
                             const proto::DropCollectionRequest* request,
                             proto::DropCollectionResponse* response) override;

  grpc::Status ListCollections(grpc::ServerContext* context,
                              const proto::ListCollectionsRequest* request,
                              proto::ListCollectionsResponse* response) override;

  // Data operations → Data Nodes (shard-aware)
  grpc::Status Insert(grpc::ServerContext* context,
                     const proto::InsertRequest* request,
                     proto::InsertResponse* response) override;

  grpc::Status StreamInsert(grpc::ServerContext* context,
                           grpc::ServerReader<proto::InsertRequest>* reader,
                           proto::InsertResponse* response) override;

  grpc::Status Get(grpc::ServerContext* context,
                  const proto::GetRequest* request,
                  proto::GetResponse* response) override;

  grpc::Status Delete(grpc::ServerContext* context,
                     const proto::DeleteRequest* request,
                     proto::DeleteResponse* response) override;

  grpc::Status UpdateMetadata(grpc::ServerContext* context,
                             const proto::UpdateMetadataRequest* request,
                             proto::UpdateMetadataResponse* response) override;

  // Hybrid search → Data Nodes (shard-aware)
  grpc::Status HybridSearch(grpc::ServerContext* context,
                            const proto::HybridSearchRequest* request,
                            proto::HybridSearchResponse* response) override;

  // Query operations → Query Nodes (load balanced)
  grpc::Status Search(grpc::ServerContext* context,
                     const proto::SearchRequest* request,
                     proto::SearchResponse* response) override;

  // Stats aggregation
  grpc::Status GetStats(grpc::ServerContext* context,
                       const proto::GetStatsRequest* request,
                       proto::GetStatsResponse* response) override;

 private:
  // Backend node addresses
  std::vector<std::string> coordinator_addrs_;
  std::vector<std::string> query_node_addrs_;
  std::vector<std::string> data_node_addrs_;

  // gRPC clients (lazy initialized)
  std::mutex clients_mutex_;
  std::unique_ptr<proto::VectorDBService::Stub> coordinator_client_;
  std::unique_ptr<proto::internal::InternalService::Stub> coordinator_internal_client_;
  std::vector<QueryNode> query_nodes_;  // Query nodes with load balancing
  std::vector<std::unique_ptr<proto::VectorDBService::Stub>> data_clients_;

  // Dynamic data node clients (by address, for shard-aware routing)
  std::map<std::string, std::unique_ptr<proto::VectorDBService::Stub>> data_client_by_addr_;

  // Load balancer for query nodes
  std::unique_ptr<cluster::LoadBalancer> load_balancer_;

  // Round-robin counter for data node routing (fallback)
  std::atomic<uint32_t> data_node_counter_{0};

  // Helper methods
  proto::VectorDBService::Stub* GetCoordinatorClient();
  proto::internal::InternalService::Stub* GetCoordinatorInternalClient();
  proto::VectorDBService::Stub* GetQueryNodeClient();
  proto::VectorDBService::Stub* GetDataNodeClient(int shard_id);
  proto::VectorDBService::Stub* GetDataNodeClientForCollection(const std::string& collection_name);
  proto::VectorDBService::Stub* GetOrCreateDataClient(const std::string& address);
};

}  // namespace network
}  // namespace gvdb