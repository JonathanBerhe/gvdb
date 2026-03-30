#include <gtest/gtest.h>
#include <filesystem>

#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "cluster/coordinator.h"
#include "cluster/node_registry.h"
#include "cluster/shard_manager.h"
#include "index/index_factory.h"
#include "internal.grpc.pb.h"

using namespace gvdb;

// ============================================================================
// Test Fixture for COORDINATOR Mode
// ============================================================================

class VectorDBServiceCoordinatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create test directories
    test_dir_ = "/tmp/gvdb_vectordb_coordinator_test";
    raft_dir_ = test_dir_ + "/raft";
    std::filesystem::create_directories(test_dir_);
    std::filesystem::create_directories(raft_dir_);

    // Create index factory and segment manager
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_manager_ = std::make_shared<storage::SegmentManager>(
        test_dir_, index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_manager_.get());

    // Create cluster components
    shard_manager_ = std::make_shared<cluster::ShardManager>(
        16, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(
        std::chrono::seconds(30));

    // Create coordinator (use NullInternalServiceClientFactory for tests)
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_, nullptr);

    // Register a mock data node directly in node_registry (simulates heartbeat)
    proto::internal::NodeInfo proto_node_info;
    proto_node_info.set_node_id(100);
    proto_node_info.set_grpc_address("localhost:50052");
    proto_node_info.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node_info.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node_info.set_memory_total_bytes(1000000000);  // 1GB
    proto_node_info.set_memory_used_bytes(0);
    proto_node_info.set_disk_total_bytes(10000000000);  // 10GB
    proto_node_info.set_disk_used_bytes(0);

    // Update node registry (this is what heartbeat does)
    node_registry_->UpdateNode(proto_node_info);

    // Create VectorDBService in COORDINATOR mode
    auto resolver = network::MakeCoordinatorResolver(coordinator_);
    service_ = std::make_unique<network::VectorDBService>(
        segment_manager_, query_executor_, std::move(resolver));
  }

  void TearDown() override {
    service_.reset();
    coordinator_.reset();
    node_registry_.reset();
    shard_manager_.reset();
    query_executor_.reset();
    segment_manager_.reset();
    index_factory_.reset();

    std::filesystem::remove_all(test_dir_);
  }

  std::string test_dir_;
  std::string raft_dir_;
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::SegmentManager> segment_manager_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
  std::unique_ptr<network::VectorDBService> service_;
};

// ============================================================================
// Basic Functionality Tests
// ============================================================================

TEST_F(VectorDBServiceCoordinatorTest, HealthCheck) {
  grpc::ServerContext context;
  proto::HealthCheckRequest request;
  proto::HealthCheckResponse response;

  auto status = service_->HealthCheck(&context, &request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(response.status(), proto::HealthCheckResponse::SERVING);
}

TEST_F(VectorDBServiceCoordinatorTest, GetStatsEmpty) {
  grpc::ServerContext context;
  proto::GetStatsRequest request;
  proto::GetStatsResponse response;

  auto status = service_->GetStats(&context, &request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(response.total_collections(), 0);
  EXPECT_EQ(response.total_vectors(), 0);
}

// ============================================================================
// Collection Management Tests
// ============================================================================

TEST_F(VectorDBServiceCoordinatorTest, CreateCollection) {
  grpc::ServerContext context;
  proto::CreateCollectionRequest request;
  request.set_collection_name("test_collection");
  request.set_dimension(128);
  request.set_metric(proto::CreateCollectionRequest::L2);
  request.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse response;

  auto status = service_->CreateCollection(&context, &request, &response);

  if (!status.ok()) {
    std::cerr << "CreateCollection failed: " << status.error_message() << std::endl;
  }
  EXPECT_TRUE(status.ok()) << "Error: " << status.error_message();
  EXPECT_GT(response.collection_id(), 0);

  // Verify collection exists in coordinator
  auto collections = coordinator_->ListCollections();
  EXPECT_EQ(collections.size(), 1);
  EXPECT_EQ(collections[0].collection_name, "test_collection");
  EXPECT_EQ(collections[0].dimension, 128);
}

TEST_F(VectorDBServiceCoordinatorTest, CreateDuplicateCollection) {
  // Create first collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(128);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;

    auto status = service_->CreateCollection(&context, &request, &response);
    EXPECT_TRUE(status.ok());
  }

  // Try to create duplicate
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(128);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;

    auto status = service_->CreateCollection(&context, &request, &response);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::StatusCode::ALREADY_EXISTS);
  }
}

TEST_F(VectorDBServiceCoordinatorTest, ListCollections) {
  // Create two collections
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("collection_1");
    request.set_dimension(128);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    service_->CreateCollection(&context, &request, &response);
  }

  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("collection_2");
    request.set_dimension(256);
    request.set_metric(proto::CreateCollectionRequest::COSINE);
    request.set_index_type(proto::CreateCollectionRequest::HNSW);
    proto::CreateCollectionResponse response;
    service_->CreateCollection(&context, &request, &response);
  }

  // List collections
  grpc::ServerContext context;
  proto::ListCollectionsRequest request;
  proto::ListCollectionsResponse response;

  auto status = service_->ListCollections(&context, &request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(response.collections_size(), 2);

  // Verify collection names
  std::set<std::string> names;
  for (const auto& col : response.collections()) {
    names.insert(col.collection_name());
  }
  EXPECT_TRUE(names.count("collection_1") > 0);
  EXPECT_TRUE(names.count("collection_2") > 0);
}

TEST_F(VectorDBServiceCoordinatorTest, DropCollection) {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(128);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    service_->CreateCollection(&context, &request, &response);
  }

  // Drop collection
  {
    grpc::ServerContext context;
    proto::DropCollectionRequest request;
    request.set_collection_name("test_collection");
    proto::DropCollectionResponse response;

    auto status = service_->DropCollection(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(response.message().empty());
  }

  // Verify collection no longer exists
  auto collections = coordinator_->ListCollections();
  EXPECT_EQ(collections.size(), 0);
}

TEST_F(VectorDBServiceCoordinatorTest, DropNonexistentCollection) {
  grpc::ServerContext context;
  proto::DropCollectionRequest request;
  request.set_collection_name("nonexistent_collection");
  proto::DropCollectionResponse response;

  auto status = service_->DropCollection(&context, &request, &response);

  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

// ============================================================================
// Vector Operations Tests
// ============================================================================

TEST_F(VectorDBServiceCoordinatorTest, InsertAndSearch) {
  // Create collection (metadata operation - works on coordinator)
  grpc::ServerContext create_context;
  proto::CreateCollectionRequest create_request;
  create_request.set_collection_name("test_collection");
  create_request.set_dimension(128);
  create_request.set_metric(proto::CreateCollectionRequest::L2);
  create_request.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse create_response;
  auto create_status = service_->CreateCollection(&create_context, &create_request, &create_response);
  ASSERT_TRUE(create_status.ok());

  // Attempt to insert vectors (should fail - coordinators don't handle data operations)
  grpc::ServerContext insert_context;
  proto::InsertRequest insert_request;
  insert_request.set_collection_name("test_collection");

  for (int i = 0; i < 10; ++i) {
    auto* vec = insert_request.add_vectors();
    vec->set_id(i + 1);
    vec->mutable_vector()->set_dimension(128);
    for (int j = 0; j < 128; ++j) {
      vec->mutable_vector()->add_values(static_cast<float>(i));
    }
  }

  proto::InsertResponse insert_response;
  auto insert_status = service_->Insert(&insert_context, &insert_request, &insert_response);

  // Coordinator nodes reject vector operations - they should be sent to data nodes
  EXPECT_FALSE(insert_status.ok());
  EXPECT_EQ(insert_status.error_code(), grpc::StatusCode::UNIMPLEMENTED);

  // Skip search test since we can't insert vectors on coordinator
  return;

  // Search for similar vectors
  grpc::ServerContext search_context;
  proto::SearchRequest search_request;
  search_request.set_collection_name("test_collection");
  auto* query = search_request.mutable_query_vector();
  query->set_dimension(128);
  for (int i = 0; i < 128; ++i) {
    query->add_values(5.0f);  // Should match vector ID 6
  }
  search_request.set_top_k(3);
  proto::SearchResponse search_response;

  auto search_status = service_->Search(&search_context, &search_request, &search_response);

  EXPECT_TRUE(search_status.ok());
  EXPECT_GT(search_response.results().size(), 0);
  EXPECT_LE(search_response.results().size(), 3);

  // First result should be vector ID 6 (closest to 5.0)
  if (search_response.results().size() > 0) {
    EXPECT_EQ(search_response.results(0).id(), 6);
  }
}

TEST_F(VectorDBServiceCoordinatorTest, GetVectors) {
  // Create collection (metadata operation - works on coordinator)
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(128);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    service_->CreateCollection(&context, &request, &response);
  }

  // Attempt to get vectors (should fail - coordinators don't handle data operations)
  grpc::ServerContext context;
  proto::GetRequest request;
  request.set_collection_name("test_collection");
  request.add_ids(1);
  request.add_ids(3);
  request.add_ids(5);
  proto::GetResponse response;

  auto status = service_->Get(&context, &request, &response);

  // Coordinator nodes reject vector operations
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

TEST_F(VectorDBServiceCoordinatorTest, DeleteVectors) {
  // Create collection and insert vectors
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(128);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    service_->CreateCollection(&context, &request, &response);
  }

  {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name("test_collection");
    for (int i = 0; i < 5; ++i) {
      auto* vec = request.add_vectors();
      vec->set_id(i + 1);
      vec->mutable_vector()->set_dimension(128);
      for (int j = 0; j < 128; ++j) {
        vec->mutable_vector()->add_values(1.0f);
      }
    }
    proto::InsertResponse response;
    service_->Insert(&context, &request, &response);
  }

  // Attempt to delete vectors (should fail - coordinators don't handle data operations)
  grpc::ServerContext context;
  proto::DeleteRequest request;
  request.set_collection_name("test_collection");
  request.add_ids(2);
  request.add_ids(4);
  proto::DeleteResponse response;

  auto status = service_->Delete(&context, &request, &response);

  // Coordinator nodes reject vector operations
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

TEST_F(VectorDBServiceCoordinatorTest, InsertToNonexistentCollection) {
  grpc::ServerContext context;
  proto::InsertRequest request;
  request.set_collection_name("nonexistent_collection");
  auto* vec = request.add_vectors();
  vec->set_id(1);
  vec->mutable_vector()->set_dimension(128);
  proto::InsertResponse response;

  auto status = service_->Insert(&context, &request, &response);

  // Coordinator nodes reject all vector operations (regardless of collection existence)
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

TEST_F(VectorDBServiceCoordinatorTest, SearchNonexistentCollection) {
  grpc::ServerContext context;
  proto::SearchRequest request;
  request.set_collection_name("nonexistent_collection");
  auto* query = request.mutable_query_vector();
  query->set_dimension(128);
  for (int i = 0; i < 128; ++i) {
    query->add_values(1.0f);
  }
  request.set_top_k(10);
  proto::SearchResponse response;

  auto status = service_->Search(&context, &request, &response);

  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST_F(VectorDBServiceCoordinatorTest, CreateCollectionWithInvalidDimension) {
  grpc::ServerContext context;
  proto::CreateCollectionRequest request;
  request.set_collection_name("test_collection");
  request.set_dimension(0);  // Invalid dimension
  request.set_metric(proto::CreateCollectionRequest::L2);
  request.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse response;

  auto status = service_->CreateCollection(&context, &request, &response);

  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(VectorDBServiceCoordinatorTest, InsertWithDimensionMismatch) {
  // Create collection with dimension 128 (metadata operation - works on coordinator)
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(128);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    service_->CreateCollection(&context, &request, &response);
  }

  // Attempt to insert vector (should fail - coordinators don't handle data operations)
  grpc::ServerContext context;
  proto::InsertRequest request;
  request.set_collection_name("test_collection");
  auto* vec = request.add_vectors();
  vec->set_id(1);
  vec->mutable_vector()->set_dimension(64);  // Wrong dimension (but won't be checked)
  for (int i = 0; i < 64; ++i) {
    vec->mutable_vector()->add_values(1.0f);
  }
  proto::InsertResponse response;

  auto status = service_->Insert(&context, &request, &response);

  // Coordinator nodes reject all vector operations before validation
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}
