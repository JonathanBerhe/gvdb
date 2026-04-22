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
#include <grpcpp/grpcpp.h>

namespace gvdb {
namespace network {

// Proxy service that routes client requests to backend nodes.
//
// Query-node discovery is DNS-based: callers pass a single dns:/// URI
// pointing at the query-node headless service. gRPC resolves all A records
// and round-robins across them via the round_robin LB policy, re-resolving
// on failures. This replaces the pre-1.7 static comma-separated FQDN list
// so `kubectl scale query-node` works without operator intervention.
//
// Data-node discovery is dynamic per-request via the coordinator's
// RouteQuery RPC; the proxy dials specific pod FQDNs on demand. No
// --data-nodes flag is needed today (there is no read-only fallback — if
// the coordinator is unreachable, the proxy returns UNAVAILABLE rather
// than fanning out to a random data node).
class ProxyService final : public proto::VectorDBService::Service {
 public:
  ProxyService(
      const std::vector<std::string>& coordinator_addrs,
      const std::string& query_node_uri);

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

  grpc::Status Upsert(grpc::ServerContext* context,
                     const proto::UpsertRequest* request,
                     proto::UpsertResponse* response) override;

  grpc::Status ListVectors(grpc::ServerContext* context,
                           const proto::ListVectorsRequest* request,
                           proto::ListVectorsResponse* response) override;

  // Hybrid search → Data Nodes (shard-aware)
  grpc::Status HybridSearch(grpc::ServerContext* context,
                            const proto::HybridSearchRequest* request,
                            proto::HybridSearchResponse* response) override;

  // Query operations → Query Nodes (load balanced)
  grpc::Status Search(grpc::ServerContext* context,
                     const proto::SearchRequest* request,
                     proto::SearchResponse* response) override;

  grpc::Status RangeSearch(grpc::ServerContext* context,
                          const proto::RangeSearchRequest* request,
                          proto::RangeSearchResponse* response) override;

  // Stats aggregation
  grpc::Status GetStats(grpc::ServerContext* context,
                       const proto::GetStatsRequest* request,
                       proto::GetStatsResponse* response) override;

 private:
  // Backend targets
  std::vector<std::string> coordinator_addrs_;
  // Single dns:///<headless-svc>:<port> URI (or a bare host:port for
  // single-node / non-K8s deployments). Empty means query-node ops should
  // fail with UNAVAILABLE.
  std::string query_node_uri_;

  // gRPC clients (lazy initialized). The three stubs below are written
  // exactly once under clients_mutex_, then hot-path reads are lock-free via
  // acquire loads on the atomic "ready" flags — stubs are thread-safe for
  // concurrent invocations once constructed (gRPC guarantee).
  std::mutex clients_mutex_;
  std::atomic<bool> coordinator_client_ready_{false};
  std::atomic<bool> coordinator_internal_client_ready_{false};
  std::atomic<bool> query_node_client_ready_{false};
  std::unique_ptr<proto::VectorDBService::Stub> coordinator_client_;
  std::unique_ptr<proto::internal::InternalService::Stub> coordinator_internal_client_;
  // Single round_robin channel for the query-node headless service — gRPC
  // handles fan-out across all resolved A records.
  std::unique_ptr<proto::VectorDBService::Stub> query_node_client_;

  // Dynamic data node clients (by address, for shard-aware routing).
  // Populated on-demand from coordinator RouteQuery responses.
  std::map<std::string, std::unique_ptr<proto::VectorDBService::Stub>> data_client_by_addr_;

  // Helper methods
  proto::VectorDBService::Stub* GetCoordinatorClient();
  proto::internal::InternalService::Stub* GetCoordinatorInternalClient();
  proto::VectorDBService::Stub* GetQueryNodeClient();
  // Resolve a collection to one of its data nodes via the coordinator's
  // RouteQuery RPC. Set `read_only=true` for read operations so the
  // coordinator may return a routable replica when the primary is draining
  // (roadmap 0b.1). Writes must leave read_only=false — they go to the
  // primary (possibly draining) for correct ordering.
  proto::VectorDBService::Stub* GetDataNodeClientForCollection(
      const std::string& collection_name, bool read_only = false);
  proto::VectorDBService::Stub* GetOrCreateDataClient(const std::string& address);
};

}  // namespace network
}  // namespace gvdb