#include "network/internal_service.h"
#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/node_registry.h"
#include "internal.grpc.pb.h"

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <memory>
#include <thread>

namespace gvdb {
namespace network {
namespace integration {

using namespace gvdb::cluster;

// ============================================================================
// Metadata Synchronization Integration Tests
// ============================================================================

class MetadataSyncIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Pick a unique port for this test
    server_address_ = "localhost:50099";

    // Create coordinator with shard manager
    shard_manager_ = std::make_shared<ShardManager>(8, ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30));
    coordinator_ = std::make_shared<Coordinator>(shard_manager_, node_registry_);

    // Register a fake data node via NodeRegistry (production flow)
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(1);
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node.set_grpc_address("localhost:50051");
    node_registry_->UpdateNode(proto_node);

    // Create InternalService
    internal_service_ = std::make_unique<InternalService>(
        shard_manager_,
        nullptr,  // segment_manager not needed for metadata tests
        nullptr,  // query_executor not needed
        nullptr,  // node_registry not needed
        nullptr,  // timestamp_oracle not needed
        coordinator_);

    // Start gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(internal_service_.get());

    // Increase message sizes for testing
    builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
    builder.SetMaxSendMessageSize(256 * 1024 * 1024);

    server_ = builder.BuildAndStart();
    ASSERT_TRUE(server_ != nullptr) << "Failed to start gRPC server";

    // Create gRPC client
    auto channel = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    stub_ = proto::internal::InternalService::NewStub(channel);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
  }

  std::string server_address_;
  std::shared_ptr<ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<Coordinator> coordinator_;
  std::unique_ptr<InternalService> internal_service_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<proto::internal::InternalService::Stub> stub_;
};

// Test 1: Basic metadata retrieval via gRPC
TEST_F(MetadataSyncIntegrationTest, GetCollectionMetadataByNameViaGrpc) {
  // Create a collection in the coordinator
  auto collection_id = coordinator_->CreateCollection(
      "test_collection", 128, core::MetricType::L2, core::IndexType::HNSW, 1);
  ASSERT_TRUE(collection_id.ok());

  // Query metadata via gRPC client
  grpc::ClientContext context;
  proto::internal::GetCollectionMetadataRequest request;
  request.set_collection_name("test_collection");
  proto::internal::GetCollectionMetadataResponse response;

  grpc::Status status = stub_->GetCollectionMetadata(&context, request, &response);

  // Verify gRPC call succeeded
  ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();

  // Verify response
  EXPECT_TRUE(response.found());
  EXPECT_EQ(response.metadata().collection_name(), "test_collection");
  EXPECT_EQ(response.metadata().dimension(), 128);
  EXPECT_EQ(response.metadata().metric_type(), "L2");
  EXPECT_EQ(response.metadata().index_type(), "HNSW");
  EXPECT_EQ(response.metadata().collection_id(), core::ToUInt32(collection_id.value()));
}

// Test 2: Metadata retrieval by ID via gRPC
TEST_F(MetadataSyncIntegrationTest, GetCollectionMetadataByIdViaGrpc) {
  // Create a collection in the coordinator
  auto collection_id = coordinator_->CreateCollection(
      "id_test_collection", 256, core::MetricType::COSINE, core::IndexType::IVF_FLAT, 1);
  ASSERT_TRUE(collection_id.ok());

  // Query metadata via gRPC client
  grpc::ClientContext context;
  proto::internal::GetCollectionMetadataRequest request;
  request.set_collection_id(core::ToUInt32(collection_id.value()));
  proto::internal::GetCollectionMetadataResponse response;

  grpc::Status status = stub_->GetCollectionMetadata(&context, request, &response);

  // Verify gRPC call succeeded
  ASSERT_TRUE(status.ok());

  // Verify response
  EXPECT_TRUE(response.found());
  EXPECT_EQ(response.metadata().collection_name(), "id_test_collection");
  EXPECT_EQ(response.metadata().dimension(), 256);
  EXPECT_EQ(response.metadata().metric_type(), "COSINE");
  EXPECT_EQ(response.metadata().index_type(), "IVF_FLAT");
}

// Test 3: Non-existent collection via gRPC
TEST_F(MetadataSyncIntegrationTest, GetNonExistentCollectionViaGrpc) {
  grpc::ClientContext context;
  proto::internal::GetCollectionMetadataRequest request;
  request.set_collection_name("nonexistent_collection");
  proto::internal::GetCollectionMetadataResponse response;

  grpc::Status status = stub_->GetCollectionMetadata(&context, request, &response);

  // gRPC call should succeed (it's not an error, just not found)
  ASSERT_TRUE(status.ok());

  // But collection should not be found
  EXPECT_FALSE(response.found());
}

// Test 4: Multiple concurrent gRPC requests
TEST_F(MetadataSyncIntegrationTest, ConcurrentGrpcRequests) {
  // Create multiple collections
  const int kNumCollections = 10;
  std::vector<core::CollectionId> collection_ids;

  for (int i = 0; i < kNumCollections; ++i) {
    auto result = coordinator_->CreateCollection(
        "collection_" + std::to_string(i),
        128 + i,
        core::MetricType::L2,
        core::IndexType::FLAT,
        1);
    ASSERT_TRUE(result.ok());
    collection_ids.push_back(result.value());
  }

  // Concurrent gRPC requests
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < kNumCollections; ++i) {
    threads.emplace_back([this, i, &success_count]() {
      grpc::ClientContext context;
      proto::internal::GetCollectionMetadataRequest request;
      request.set_collection_name("collection_" + std::to_string(i));
      proto::internal::GetCollectionMetadataResponse response;

      grpc::Status status = stub_->GetCollectionMetadata(&context, request, &response);

      if (status.ok() && response.found()) {
        success_count++;
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All requests should succeed
  EXPECT_EQ(success_count.load(), kNumCollections);
}

// Test 5: Invalid request (no collection_id or name) via gRPC
TEST_F(MetadataSyncIntegrationTest, InvalidRequestViaGrpc) {
  grpc::ClientContext context;
  proto::internal::GetCollectionMetadataRequest request;
  // Don't set collection_id or collection_name
  proto::internal::GetCollectionMetadataResponse response;

  grpc::Status status = stub_->GetCollectionMetadata(&context, request, &response);

  // Should return gRPC error
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

// Test 6: All metric and index types via gRPC
TEST_F(MetadataSyncIntegrationTest, AllTypesViaGrpc) {
  struct TestCase {
    std::string name;
    core::MetricType metric;
    core::IndexType index;
    std::string expected_metric_str;
    std::string expected_index_str;
  };

  std::vector<TestCase> test_cases = {
      {"l2_flat", core::MetricType::L2, core::IndexType::FLAT, "L2", "FLAT"},
      {"ip_hnsw", core::MetricType::INNER_PRODUCT, core::IndexType::HNSW, "INNER_PRODUCT", "HNSW"},
      {"cosine_ivf", core::MetricType::COSINE, core::IndexType::IVF_FLAT, "COSINE", "IVF_FLAT"},
  };

  for (const auto& tc : test_cases) {
    // Create collection
    auto result = coordinator_->CreateCollection(tc.name, 128, tc.metric, tc.index, 1);
    ASSERT_TRUE(result.ok());

    // Query via gRPC
    grpc::ClientContext context;
    proto::internal::GetCollectionMetadataRequest request;
    request.set_collection_name(tc.name);
    proto::internal::GetCollectionMetadataResponse response;

    grpc::Status status = stub_->GetCollectionMetadata(&context, request, &response);

    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(response.found());
    EXPECT_EQ(response.metadata().metric_type(), tc.expected_metric_str);
    EXPECT_EQ(response.metadata().index_type(), tc.expected_index_str);
  }
}

// Test 7: Simulate data node fetching metadata on first access
TEST_F(MetadataSyncIntegrationTest, SimulateDataNodePullOnMiss) {
  // Coordinator creates a collection
  auto collection_id = coordinator_->CreateCollection(
      "data_node_test", 768, core::MetricType::COSINE, core::IndexType::HNSW, 1);
  ASSERT_TRUE(collection_id.ok());

  // Simulate data node: doesn't have metadata, needs to fetch it
  // This is what data/query nodes will do on cache miss

  grpc::ClientContext context;
  proto::internal::GetCollectionMetadataRequest request;
  request.set_collection_name("data_node_test");
  proto::internal::GetCollectionMetadataResponse response;

  grpc::Status status = stub_->GetCollectionMetadata(&context, request, &response);

  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(response.found());

  // Data node would now cache this metadata
  EXPECT_EQ(response.metadata().collection_name(), "data_node_test");
  EXPECT_EQ(response.metadata().dimension(), 768);
  EXPECT_EQ(response.metadata().metric_type(), "COSINE");
  EXPECT_EQ(response.metadata().index_type(), "HNSW");

  // Subsequent requests would use the cached metadata (not tested here,
  // but that's the pattern we'll implement in VectorDBService)
}

}  // namespace integration
}  // namespace network
}  // namespace gvdb
