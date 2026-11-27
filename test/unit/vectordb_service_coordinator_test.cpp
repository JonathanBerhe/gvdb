// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include <filesystem>
#include <string>

#include "service/vectordb/vectordb_service.h"
#include "service/vectordb/collection_resolver.h"
#include "storage/segment/segment_manager.h"
#include "compute/query_executor.h"
#include "cluster/coordinator/coordinator.h"
#include "cluster/coordinator/node_registry.h"
#include "cluster/shard_manager.h"
#include "index/index_factory.h"
#include "internal.grpc.pb.h"
#include "support/temp_dir.h"

using namespace gvdb;

// ============================================================================
// Test Fixture for COORDINATOR Mode
// ============================================================================

struct VectorDBServiceCoordinatorTest {
  VectorDBServiceCoordinatorTest() {
    // Create test directories
    raft_dir_ = (test_dir_ / "raft").string();
    std::filesystem::create_directories(raft_dir_);

    // Create index factory and segment manager
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>(
        test_dir_.str(), index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        *segment_store_);

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
    // The coordinator has no client factory, so it never dials this address.
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
    auto resolver = service::MakeCoordinatorResolver(coordinator_);
    service_ = std::make_unique<service::VectorDBService>(
        segment_store_, query_executor_, std::move(resolver));
  }

  ~VectorDBServiceCoordinatorTest() {
    service_.reset();
    coordinator_.reset();
    node_registry_.reset();
    shard_manager_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();
    // test_dir_ is the first member, so it removes the folder last.
  }

  test::ScopedTempDir test_dir_{"gvdb_vectordb_coordinator_test"};
  std::string raft_dir_;
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
  std::unique_ptr<service::VectorDBService> service_;
};

// ============================================================================
// Stats
// ============================================================================

// The proxy forwards every client GetStats to the coordinator, so GetStats
// must be served (not refused like the data operations) and must count the
// collections the coordinator holds.
TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest,
                  "GetStatsCountsCoordinatorCollections") {
  for (const char* name : {"stats_a", "stats_b"}) {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name(name);
    request.set_dimension(16);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
  }

  grpc::ServerContext context;
  proto::GetStatsRequest request;
  proto::GetStatsResponse response;

  auto status = service_->GetStats(&context, &request, &response);

  INFO(status.error_message());
  CHECK(status.ok());
  CHECK_EQ(response.total_collections(), 2);
  CHECK_EQ(response.total_vectors(), 0);
}

// ============================================================================
// Collection Management Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "CreateCollection") {
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
  INFO("Error: ", status.error_message()); CHECK(status.ok());
  CHECK_GT(response.collection_id(), 0);

  // Verify collection exists in coordinator
  auto collections = coordinator_->ListCollections();
  CHECK_EQ(collections.size(), 1);
  CHECK_EQ(collections[0].collection_name, "test_collection");
  CHECK_EQ(collections[0].dimension, 128);
}

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "CreateDuplicateCollection") {
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
    CHECK(status.ok());
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

    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::ALREADY_EXISTS);
  }
}

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "ListCollections") {
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

  CHECK(status.ok());
  CHECK_EQ(response.collections_size(), 2);

  // Verify collection names
  std::set<std::string> names;
  for (const auto& col : response.collections()) {
    names.insert(col.collection_name());
  }
  CHECK(names.count("collection_1") > 0);
  CHECK(names.count("collection_2") > 0);
}

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "DropCollection") {
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

    CHECK(status.ok());
    CHECK_FALSE(response.message().empty());
  }

  // Verify collection no longer exists
  auto collections = coordinator_->ListCollections();
  CHECK_EQ(collections.size(), 0);
}

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "DropNonexistentCollection") {
  grpc::ServerContext context;
  proto::DropCollectionRequest request;
  request.set_collection_name("nonexistent_collection");
  proto::DropCollectionResponse response;

  auto status = service_->DropCollection(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

// ============================================================================
// Vector Operations Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "GetVectors") {
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
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "DeleteVectors") {
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
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "InsertToNonexistentCollection") {
  grpc::ServerContext context;
  proto::InsertRequest request;
  request.set_collection_name("nonexistent_collection");
  auto* vec = request.add_vectors();
  vec->set_id(1);
  vec->mutable_vector()->set_dimension(128);
  proto::InsertResponse response;

  auto status = service_->Insert(&context, &request, &response);

  // Coordinator nodes reject all vector operations (regardless of collection existence)
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "SearchNonexistentCollection") {
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

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "CreateCollectionWithInvalidDimension") {
  grpc::ServerContext context;
  proto::CreateCollectionRequest request;
  request.set_collection_name("test_collection");
  request.set_dimension(0);  // Invalid dimension
  request.set_metric(proto::CreateCollectionRequest::L2);
  request.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse response;

  auto status = service_->CreateCollection(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(VectorDBServiceCoordinatorTest, "InsertWithDimensionMismatch") {
  // Create collection with dimension 128 (metadata operation - works on
  // coordinator). The create must succeed, so the refusal below is for an
  // insert into a collection that exists.
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(128);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
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
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}
