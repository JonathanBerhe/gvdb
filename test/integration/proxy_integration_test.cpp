// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Integration test: full flow through proxy -> coordinator + data node

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <initializer_list>
#include <memory>
#include <string>

#include "cluster/coordinator/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/coordinator/node_registry.h"
#include "cluster/coordinator/internal_client.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "service/internal/internal_service.h"
#include "service/vectordb/vectordb_service.h"
#include "service/proxy/proxy_service.h"
#include "service/vectordb/collection_resolver.h"
#include "storage/segment/segment_manager.h"
#include "support/temp_dir.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace test {

namespace {

// Starts a server on a free local port with the given services and the
// 256 MB message limits of the production servers. Sets *address to
// "localhost:<port>".
std::unique_ptr<grpc::Server> StartServer(
    std::initializer_list<grpc::Service*> services, std::string* address) {
  grpc::ServerBuilder builder;
  int port = 0;
  builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
  for (auto* service : services) {
    builder.RegisterService(service);
  }
  builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
  builder.SetMaxSendMessageSize(256 * 1024 * 1024);
  auto server = builder.BuildAndStart();
  REQUIRE_NE(server, nullptr);
  REQUIRE_GT(port, 0);
  *address = "localhost:" + std::to_string(port);
  return server;
}

}  // namespace

class ProxyIntegrationTest {
 public:
  ProxyIntegrationTest() {
    index_factory_ = std::make_unique<index::IndexFactory>();

    // --- Step 1: Start coordinator first (data node needs its address) ---
    shard_manager_ = std::make_shared<cluster::ShardManager>(8, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30));

    coord_segment_store_ = std::make_shared<storage::SegmentManager>(
        (dir_ / "coordinator").string(), index_factory_.get());
    coord_query_executor_ = std::make_shared<compute::QueryExecutor>(
        *coord_segment_store_);

    // The client factory lets CreateCollection send CreateSegment to the
    // data node that step 3 registers.
    auto client_factory = std::make_shared<cluster::GrpcInternalServiceClientFactory>();
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_,
        cluster::CoordinatorOptions{.client_factory = client_factory});

    coord_internal_service_ = std::make_unique<service::InternalService>(
        shard_manager_, coord_segment_store_, coord_query_executor_,
        service::InternalServiceOptions{.node_registry = node_registry_, .coordinator = coordinator_});

    auto coord_resolver = service::MakeCoordinatorResolver(coordinator_);
    coord_vectordb_service_ = std::make_unique<service::VectorDBService>(
        coord_segment_store_, coord_query_executor_, std::move(coord_resolver));

    coord_server_ = StartServer(
        {coord_internal_service_.get(), coord_vectordb_service_.get()}, &coord_address_);

    // --- Step 2: Start data node (InternalService for CreateSegment + VectorDBService for Insert/Search) ---
    dn_segment_store_ = std::make_shared<storage::SegmentManager>(
        (dir_ / "data_node").string(), index_factory_.get());
    dn_query_executor_ = std::make_shared<compute::QueryExecutor>(
        *dn_segment_store_);

    auto dn_shard_manager = std::make_shared<cluster::ShardManager>(8, cluster::ShardingStrategy::HASH);
    dn_internal_service_ = std::make_unique<service::InternalService>(
        dn_shard_manager, dn_segment_store_, dn_query_executor_);

    // Data node's VectorDBService uses CachedCoordinatorResolver -> can resolve collection names
    auto dn_resolver = service::MakeCachedCoordinatorResolver(coord_address_);
    dn_vectordb_service_ = std::make_unique<service::VectorDBService>(
        dn_segment_store_, dn_query_executor_, std::move(dn_resolver));

    dn_server_ = StartServer(
        {dn_internal_service_.get(), dn_vectordb_service_.get()}, &dn_address_);

    // --- Step 3: Register data node with the coordinator ---
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(100);
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node.set_grpc_address(dn_address_);
    node_registry_->UpdateNode(proto_node);

    // --- Step 4: Start proxy ---
    // Post-1.7: proxy takes a single query-node URI (no more static
    // --data-nodes fallback list). In production this is a dns:///
    // headless-service URI; here we substitute the data-node address as
    // the query-node target because the data-node's VectorDBService
    // happens to implement Search locally, which keeps this test
    // exercising the same code paths without spinning up a separate
    // query-node binary. Do NOT take this as "data-nodes serve Search in
    // production": they do not, because a real deployment always has query-node
    // pods.
    proxy_service_ = std::make_unique<service::ProxyService>(
        std::vector<std::string>{coord_address_},
        dn_address_);
    proxy_server_ = StartServer({proxy_service_.get()}, &proxy_address_);

    // Client connects to proxy
    auto channel = grpc::CreateChannel(proxy_address_, grpc::InsecureChannelCredentials());
    client_ = proto::VectorDBService::NewStub(channel);
  }

  ~ProxyIntegrationTest() {
    // Close every client channel before the server that it dials. A
    // graceful server shutdown waits until each open client connection
    // answers the server's GOAWAY, and on macOS an idle in-process channel
    // answers only at gRPC's next client backup poll (every 5 s).
    client_.reset();
    proxy_server_->Shutdown();
    proxy_server_.reset();
    proxy_service_.reset();  // Its channels dial the coordinator and the data node.

    // The coordinator and the data node dial each other: the coordinator's
    // clients dial the data node, and the data node's resolver dials the
    // coordinator. A deadline that has passed makes the coordinator close
    // its connections at once instead of waiting for the data node's
    // channel. No call is running at this point, so none is cancelled.
    coord_server_->Shutdown(std::chrono::system_clock::now());
    coord_server_.reset();
    coord_vectordb_service_.reset();
    coord_internal_service_.reset();
    coordinator_.reset();  // Its clients dial the data node.
    dn_server_->Shutdown();
    dn_server_.reset();
  }

  ScopedTempDir dir_{"gvdb_proxy_integration"};
  std::unique_ptr<index::IndexFactory> index_factory_;

  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;

  std::shared_ptr<storage::ISegmentStore> coord_segment_store_;
  std::shared_ptr<compute::QueryExecutor> coord_query_executor_;
  std::unique_ptr<service::InternalService> coord_internal_service_;
  std::unique_ptr<service::VectorDBService> coord_vectordb_service_;
  std::unique_ptr<grpc::Server> coord_server_;
  std::string coord_address_;

  std::shared_ptr<storage::ISegmentStore> dn_segment_store_;
  std::shared_ptr<compute::QueryExecutor> dn_query_executor_;
  std::unique_ptr<service::InternalService> dn_internal_service_;
  std::unique_ptr<service::VectorDBService> dn_vectordb_service_;
  std::unique_ptr<grpc::Server> dn_server_;
  std::string dn_address_;

  std::unique_ptr<service::ProxyService> proxy_service_;
  std::unique_ptr<grpc::Server> proxy_server_;
  std::string proxy_address_;

  std::unique_ptr<proto::VectorDBService::Stub> client_;
};

TEST_CASE_FIXTURE(ProxyIntegrationTest, "FullWorkflowThroughProxy") {
  // 1. Create collection via proxy -> coordinator -> CreateSegment on data node
  {
    proto::CreateCollectionRequest req;
    req.set_collection_name("proxy_workflow");
    req.set_dimension(4);
    req.set_metric(proto::CreateCollectionRequest::L2);
    req.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse resp;
    grpc::ClientContext ctx;
    auto status = client_->CreateCollection(&ctx, req, &resp);
    INFO("CreateCollection failed: " << status.error_message());
    REQUIRE(status.ok());
    CHECK_GT(resp.collection_id(), 0);
  }

  // 2. Insert vectors via proxy -> data node
  {
    proto::InsertRequest req;
    req.set_collection_name("proxy_workflow");

    for (int i = 1; i <= 5; ++i) {
      auto* vec = req.add_vectors();
      vec->set_id(i);
      auto* v = vec->mutable_vector();
      v->set_dimension(4);
      v->add_values(static_cast<float>(i));
      v->add_values(0.0f);
      v->add_values(0.0f);
      v->add_values(0.0f);
    }

    proto::InsertResponse resp;
    grpc::ClientContext ctx;
    auto status = client_->Insert(&ctx, req, &resp);
    INFO("Insert failed: " << status.error_message());
    REQUIRE(status.ok());
    CHECK_EQ(resp.inserted_count(), 5);
  }

  // 3. Search via proxy -> data node
  {
    proto::SearchRequest req;
    req.set_collection_name("proxy_workflow");
    req.set_top_k(3);
    auto* query = req.mutable_query_vector();
    query->set_dimension(4);
    query->add_values(1.0f);
    query->add_values(0.0f);
    query->add_values(0.0f);
    query->add_values(0.0f);

    proto::SearchResponse resp;
    grpc::ClientContext ctx;
    auto status = client_->Search(&ctx, req, &resp);
    INFO("Search failed: " << status.error_message());
    REQUIRE(status.ok());
    CHECK_GT(resp.results_size(), 0);
    CHECK_EQ(resp.results(0).id(), 1);
  }

  // 4. List collections via proxy -> coordinator
  {
    proto::ListCollectionsRequest req;
    proto::ListCollectionsResponse resp;
    grpc::ClientContext ctx;
    auto status = client_->ListCollections(&ctx, req, &resp);
    INFO(status.error_message());
    REQUIRE(status.ok());
    CHECK_GE(resp.collections_size(), 1);
  }

  // 5. Drop collection via proxy -> coordinator
  {
    proto::DropCollectionRequest req;
    req.set_collection_name("proxy_workflow");
    proto::DropCollectionResponse resp;
    grpc::ClientContext ctx;
    auto status = client_->DropCollection(&ctx, req, &resp);
    INFO(status.error_message());
    REQUIRE(status.ok());
  }
}

TEST_CASE_FIXTURE(ProxyIntegrationTest, "HealthCheckThroughProxy") {
  proto::HealthCheckRequest req;
  proto::HealthCheckResponse resp;
  grpc::ClientContext ctx;
  auto status = client_->HealthCheck(&ctx, req, &resp);
  REQUIRE(status.ok());
  CHECK_EQ(resp.status(), proto::HealthCheckResponse::SERVING);
}

}  // namespace test
}  // namespace gvdb
