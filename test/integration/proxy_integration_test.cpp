// test/integration/proxy_integration_test.cpp
// Integration test: full flow through proxy → coordinator + data node

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <filesystem>
#include <memory>

#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/node_registry.h"
#include "cluster/internal_client.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/internal_service.h"
#include "network/vectordb_service.h"
#include "network/proxy_service.h"
#include "network/collection_resolver.h"
#include "storage/segment_manager.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace test {

class ProxyIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all("/tmp/gvdb-proxy-integration-test");
    std::filesystem::create_directories("/tmp/gvdb-proxy-integration-test/coordinator");
    std::filesystem::create_directories("/tmp/gvdb-proxy-integration-test/data_node");

    index_factory_ = std::make_unique<index::IndexFactory>();

    // --- Step 1: Start coordinator first (data node needs its address) ---
    shard_manager_ = std::make_shared<cluster::ShardManager>(8, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30));

    coord_segment_manager_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-proxy-integration-test/coordinator", index_factory_.get());
    coord_query_executor_ = std::make_shared<compute::QueryExecutor>(
        coord_segment_manager_.get());

    // Coordinator without client_factory initially (will recreate after data node is up)
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_, nullptr);

    coord_internal_service_ = std::make_unique<network::InternalService>(
        shard_manager_, coord_segment_manager_, coord_query_executor_,
        node_registry_, nullptr, coordinator_);

    auto coord_resolver = network::MakeCoordinatorResolver(coordinator_);
    coord_vectordb_service_ = std::make_unique<network::VectorDBService>(
        coord_segment_manager_, coord_query_executor_, std::move(coord_resolver));

    {
      grpc::ServerBuilder builder;
      int port = 0;
      builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
      builder.RegisterService(coord_internal_service_.get());
      builder.RegisterService(coord_vectordb_service_.get());
      builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
      builder.SetMaxSendMessageSize(256 * 1024 * 1024);
      coord_server_ = builder.BuildAndStart();
      ASSERT_NE(coord_server_, nullptr);
      coord_address_ = "localhost:" + std::to_string(port);
    }

    // --- Step 2: Start data node (InternalService for CreateSegment + VectorDBService for Insert/Search) ---
    dn_segment_manager_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-proxy-integration-test/data_node", index_factory_.get());
    dn_query_executor_ = std::make_shared<compute::QueryExecutor>(
        dn_segment_manager_.get());

    auto dn_shard_manager = std::make_shared<cluster::ShardManager>(8, cluster::ShardingStrategy::HASH);
    dn_internal_service_ = std::make_unique<network::InternalService>(
        dn_shard_manager, dn_segment_manager_, dn_query_executor_);

    // Data node's VectorDBService uses CachedCoordinatorResolver → can resolve collection names
    auto dn_resolver = network::MakeCachedCoordinatorResolver(coord_address_);
    dn_vectordb_service_ = std::make_unique<network::VectorDBService>(
        dn_segment_manager_, dn_query_executor_, std::move(dn_resolver));

    {
      grpc::ServerBuilder builder;
      int port = 0;
      builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
      builder.RegisterService(dn_internal_service_.get());
      builder.RegisterService(dn_vectordb_service_.get());
      builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
      builder.SetMaxSendMessageSize(256 * 1024 * 1024);
      dn_server_ = builder.BuildAndStart();
      ASSERT_NE(dn_server_, nullptr);
      dn_address_ = "localhost:" + std::to_string(port);
    }

    // --- Step 3: Register data node and recreate coordinator with client factory ---
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(100);
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node.set_grpc_address(dn_address_);
    node_registry_->UpdateNode(proto_node);

    // Recreate coordinator with client factory so CreateCollection can send CreateSegment to data node
    auto client_factory = std::make_shared<cluster::GrpcInternalServiceClientFactory>();
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_, client_factory);

    // Update internal service's coordinator reference
    coord_internal_service_ = std::make_unique<network::InternalService>(
        shard_manager_, coord_segment_manager_, coord_query_executor_,
        node_registry_, nullptr, coordinator_);

    auto coord_resolver2 = network::MakeCoordinatorResolver(coordinator_);
    coord_vectordb_service_ = std::make_unique<network::VectorDBService>(
        coord_segment_manager_, coord_query_executor_, std::move(coord_resolver2));

    // Restart coordinator server with new services
    coord_server_->Shutdown();
    coord_server_->Wait();

    {
      grpc::ServerBuilder builder;
      builder.AddListeningPort(coord_address_, grpc::InsecureServerCredentials());
      builder.RegisterService(coord_internal_service_.get());
      builder.RegisterService(coord_vectordb_service_.get());
      builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
      builder.SetMaxSendMessageSize(256 * 1024 * 1024);
      coord_server_ = builder.BuildAndStart();
      ASSERT_NE(coord_server_, nullptr);
    }

    // --- Step 4: Start proxy ---
    proxy_service_ = std::make_unique<network::ProxyService>(
        std::vector<std::string>{coord_address_},
        std::vector<std::string>{},  // no query nodes — falls back to data nodes
        std::vector<std::string>{dn_address_});

    {
      grpc::ServerBuilder builder;
      int port = 0;
      builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
      builder.RegisterService(proxy_service_.get());
      builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
      builder.SetMaxSendMessageSize(256 * 1024 * 1024);
      proxy_server_ = builder.BuildAndStart();
      ASSERT_NE(proxy_server_, nullptr);
      proxy_address_ = "localhost:" + std::to_string(port);
    }

    // Client connects to proxy
    auto channel = grpc::CreateChannel(proxy_address_, grpc::InsecureChannelCredentials());
    client_ = proto::VectorDBService::NewStub(channel);
  }

  void TearDown() override {
    client_.reset();
    if (proxy_server_) { proxy_server_->Shutdown(); proxy_server_->Wait(); }
    if (coord_server_) { coord_server_->Shutdown(); coord_server_->Wait(); }
    if (dn_server_) { dn_server_->Shutdown(); dn_server_->Wait(); }

    proxy_service_.reset();
    coord_vectordb_service_.reset();
    coord_internal_service_.reset();
    dn_vectordb_service_.reset();
    dn_internal_service_.reset();
    coordinator_.reset();

    std::filesystem::remove_all("/tmp/gvdb-proxy-integration-test");
  }

  std::unique_ptr<index::IndexFactory> index_factory_;

  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;

  std::shared_ptr<storage::SegmentManager> coord_segment_manager_;
  std::shared_ptr<compute::QueryExecutor> coord_query_executor_;
  std::unique_ptr<network::InternalService> coord_internal_service_;
  std::unique_ptr<network::VectorDBService> coord_vectordb_service_;
  std::unique_ptr<grpc::Server> coord_server_;
  std::string coord_address_;

  std::shared_ptr<storage::SegmentManager> dn_segment_manager_;
  std::shared_ptr<compute::QueryExecutor> dn_query_executor_;
  std::unique_ptr<network::InternalService> dn_internal_service_;
  std::unique_ptr<network::VectorDBService> dn_vectordb_service_;
  std::unique_ptr<grpc::Server> dn_server_;
  std::string dn_address_;

  std::unique_ptr<network::ProxyService> proxy_service_;
  std::unique_ptr<grpc::Server> proxy_server_;
  std::string proxy_address_;

  std::unique_ptr<proto::VectorDBService::Stub> client_;
};

TEST_F(ProxyIntegrationTest, FullWorkflowThroughProxy) {
  // 1. Create collection via proxy → coordinator → CreateSegment on data node
  {
    proto::CreateCollectionRequest req;
    req.set_collection_name("proxy_workflow");
    req.set_dimension(4);
    req.set_metric(proto::CreateCollectionRequest::L2);
    req.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse resp;
    grpc::ClientContext ctx;
    auto status = client_->CreateCollection(&ctx, req, &resp);
    ASSERT_TRUE(status.ok()) << "CreateCollection failed: " << status.error_message();
    EXPECT_GT(resp.collection_id(), 0);
  }

  // 2. Insert vectors via proxy → data node
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
    ASSERT_TRUE(status.ok()) << "Insert failed: " << status.error_message();
    EXPECT_EQ(resp.inserted_count(), 5);
  }

  // 3. Search via proxy → data node
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
    ASSERT_TRUE(status.ok()) << "Search failed: " << status.error_message();
    EXPECT_GT(resp.results_size(), 0);
    EXPECT_EQ(resp.results(0).id(), 1);
  }

  // 4. List collections via proxy → coordinator
  {
    proto::ListCollectionsRequest req;
    proto::ListCollectionsResponse resp;
    grpc::ClientContext ctx;
    auto status = client_->ListCollections(&ctx, req, &resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_GE(resp.collections_size(), 1);
  }

  // 5. Drop collection via proxy → coordinator
  {
    proto::DropCollectionRequest req;
    req.set_collection_name("proxy_workflow");
    proto::DropCollectionResponse resp;
    grpc::ClientContext ctx;
    auto status = client_->DropCollection(&ctx, req, &resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
  }
}

TEST_F(ProxyIntegrationTest, HealthCheckThroughProxy) {
  proto::HealthCheckRequest req;
  proto::HealthCheckResponse resp;
  grpc::ClientContext ctx;
  auto status = client_->HealthCheck(&ctx, req, &resp);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(resp.status(), proto::HealthCheckResponse::SERVING);
}

}  // namespace test
}  // namespace gvdb
