// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "service/internal/internal_service.h"
#include "cluster/coordinator/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/coordinator/node_registry.h"
#include "internal.grpc.pb.h"
#include "support/grpc_server.h"
#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace gvdb;
using namespace gvdb::service;
using namespace gvdb::cluster;

class InternalServiceMetadataTest {
 public:
  InternalServiceMetadataTest() {
    // Create dependencies
    shard_manager_ = std::make_shared<ShardManager>(8, ShardingStrategy::HASH);
    node_registry_ = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
    coordinator_ = std::make_shared<Coordinator>(shard_manager_, node_registry_);

    // Register a fake data node via NodeRegistry (production flow)
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(1);
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    // The coordinator has no client factory, so it never dials this address.
    proto_node.set_grpc_address("localhost:50051");
    node_registry_->UpdateNode(proto_node);

    // InternalService needs these, but they can be null for metadata tests
    segment_store_ = nullptr;
    query_executor_ = nullptr;

    // Create InternalService with coordinator (node_registry_ is already created above)
    internal_service_ = std::make_unique<InternalService>(
        shard_manager_, segment_store_, query_executor_,
        node_registry_, coordinator_);
  }

  std::shared_ptr<ShardManager> shard_manager_;
  std::shared_ptr<Coordinator> coordinator_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::shared_ptr<NodeRegistry> node_registry_;
  std::unique_ptr<InternalService> internal_service_;
};

// The fixture's InternalService, also served by a real in-process gRPC
// server on an ephemeral port, so requests and responses cross the wire.
class InternalServiceMetadataServerTest : public InternalServiceMetadataTest {
 public:
  InternalServiceMetadataServerTest() {
    REQUIRE(server_.ok());
    stub_ = proto::internal::InternalService::NewStub(server_.NewChannel());
  }

  // Drops the stub, then shuts the server down before the base class
  // destroys the service.
  ~InternalServiceMetadataServerTest() {
    stub_.reset();
    server_.Shutdown();
  }

  // Sends one GetCollectionMetadata call through the stub.
  grpc::Status GetMetadata(
      const proto::internal::GetCollectionMetadataRequest& request,
      proto::internal::GetCollectionMetadataResponse* response) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(10));
    return stub_->GetCollectionMetadata(&context, request, response);
  }

  // The base class is built first, so the service exists here.
  test::TestGrpcServer server_{internal_service_.get()};
  std::unique_ptr<proto::internal::InternalService::Stub> stub_;
};

// Test getting collection metadata by name
TEST_CASE_FIXTURE(InternalServiceMetadataTest, "GetCollectionMetadataByName") {
  // Create a collection in the coordinator
  auto collection_id = coordinator_->CreateCollection(
      "test_collection", 128, core::MetricType::L2, core::IndexType::HNSW, 1);
  REQUIRE(collection_id.ok());

  // Query metadata via InternalService
  grpc::ServerContext context;
  proto::internal::GetCollectionMetadataRequest request;
  request.set_collection_name("test_collection");
  proto::internal::GetCollectionMetadataResponse response;

  auto status = internal_service_->GetCollectionMetadata(&context, &request, &response);

  // Verify success
  CHECK(status.ok());
  CHECK(response.found());
  CHECK_EQ(response.metadata().collection_name(), "test_collection");
  CHECK_EQ(response.metadata().dimension(), 128);
  CHECK_EQ(response.metadata().metric_type(), "L2");
  CHECK_EQ(response.metadata().index_type(), "HNSW");
  CHECK_EQ(response.metadata().collection_id(), core::ToUInt32(collection_id.value()));
}

// Test getting collection metadata by ID
TEST_CASE_FIXTURE(InternalServiceMetadataTest, "GetCollectionMetadataById") {
  // Create a collection in the coordinator
  auto collection_id = coordinator_->CreateCollection(
      "test_collection2", 256, core::MetricType::INNER_PRODUCT, core::IndexType::IVF_FLAT, 1);
  REQUIRE(collection_id.ok());

  // Query metadata by ID via InternalService
  grpc::ServerContext context;
  proto::internal::GetCollectionMetadataRequest request;
  request.set_collection_id(core::ToUInt32(collection_id.value()));
  proto::internal::GetCollectionMetadataResponse response;

  auto status = internal_service_->GetCollectionMetadata(&context, &request, &response);

  // Verify success
  CHECK(status.ok());
  CHECK(response.found());
  CHECK_EQ(response.metadata().collection_name(), "test_collection2");
  CHECK_EQ(response.metadata().dimension(), 256);
  CHECK_EQ(response.metadata().metric_type(), "INNER_PRODUCT");
  CHECK_EQ(response.metadata().index_type(), "IVF_FLAT");
}

// Test getting non-existent collection by name
TEST_CASE_FIXTURE(InternalServiceMetadataTest, "GetNonExistentCollectionByName") {
  grpc::ServerContext context;
  proto::internal::GetCollectionMetadataRequest request;
  request.set_collection_name("nonexistent");
  proto::internal::GetCollectionMetadataResponse response;

  auto status = internal_service_->GetCollectionMetadata(&context, &request, &response);

  // Should succeed but not found
  CHECK(status.ok());
  CHECK_FALSE(response.found());
}

// Test getting non-existent collection by ID
TEST_CASE_FIXTURE(InternalServiceMetadataTest, "GetNonExistentCollectionById") {
  grpc::ServerContext context;
  proto::internal::GetCollectionMetadataRequest request;
  request.set_collection_id(999999);
  proto::internal::GetCollectionMetadataResponse response;

  auto status = internal_service_->GetCollectionMetadata(&context, &request, &response);

  // Should succeed but not found
  CHECK(status.ok());
  CHECK_FALSE(response.found());
}

// Test with no coordinator
TEST_CASE_FIXTURE(InternalServiceMetadataTest, "GetMetadataWithoutCoordinator") {
  // Create InternalService without coordinator
  auto service_without_coordinator = std::make_unique<InternalService>(
      shard_manager_, segment_store_, query_executor_,
      node_registry_, nullptr);

  grpc::ServerContext context;
  proto::internal::GetCollectionMetadataRequest request;
  request.set_collection_name("test");
  proto::internal::GetCollectionMetadataResponse response;

  auto status = service_without_coordinator->GetCollectionMetadata(&context, &request, &response);

  // Should succeed but not found
  CHECK(status.ok());
  CHECK_FALSE(response.found());
}

// Test with different metric types
TEST_CASE_FIXTURE(InternalServiceMetadataTest, "DifferentMetricTypes") {
  struct TestCase {
    std::string name;
    core::MetricType metric_type;
    std::string expected_metric_str;
  };

  std::vector<TestCase> test_cases = {
      {"l2_collection", core::MetricType::L2, "L2"},
      {"ip_collection", core::MetricType::INNER_PRODUCT, "INNER_PRODUCT"},
      {"cosine_collection", core::MetricType::COSINE, "COSINE"},
  };

  for (const auto& tc : test_cases) {
    auto collection_id = coordinator_->CreateCollection(
        tc.name, 128, tc.metric_type, core::IndexType::FLAT, 1);
    REQUIRE(collection_id.ok());

    grpc::ServerContext context;
    proto::internal::GetCollectionMetadataRequest request;
    request.set_collection_name(tc.name);
    proto::internal::GetCollectionMetadataResponse response;

    auto status = internal_service_->GetCollectionMetadata(&context, &request, &response);

    CHECK(status.ok());
    CHECK(response.found());
    CHECK_EQ(response.metadata().metric_type(), tc.expected_metric_str);
  }
}

// Test with different index types
TEST_CASE_FIXTURE(InternalServiceMetadataTest, "DifferentIndexTypes") {
  struct TestCase {
    std::string name;
    core::IndexType index_type;
    std::string expected_index_str;
  };

  std::vector<TestCase> test_cases = {
      {"flat_collection", core::IndexType::FLAT, "FLAT"},
      {"hnsw_collection", core::IndexType::HNSW, "HNSW"},
      {"ivf_flat_collection", core::IndexType::IVF_FLAT, "IVF_FLAT"},
      {"ivf_pq_collection", core::IndexType::IVF_PQ, "IVF_PQ"},
      {"ivf_sq_collection", core::IndexType::IVF_SQ, "IVF_SQ"},
  };

  for (const auto& tc : test_cases) {
    auto collection_id = coordinator_->CreateCollection(
        tc.name, 128, core::MetricType::L2, tc.index_type, 1);
    REQUIRE(collection_id.ok());

    grpc::ServerContext context;
    proto::internal::GetCollectionMetadataRequest request;
    request.set_collection_name(tc.name);
    proto::internal::GetCollectionMetadataResponse response;

    auto status = internal_service_->GetCollectionMetadata(&context, &request, &response);

    CHECK(status.ok());
    CHECK(response.found());
    CHECK_EQ(response.metadata().index_type(), tc.expected_index_str);
  }
}

// Test invalid request (no collection_id or collection_name)
TEST_CASE_FIXTURE(InternalServiceMetadataTest, "InvalidRequest") {
  grpc::ServerContext context;
  proto::internal::GetCollectionMetadataRequest request;
  // Don't set collection_id or collection_name
  proto::internal::GetCollectionMetadataResponse response;

  auto status = internal_service_->GetCollectionMetadata(&context, &request, &response);

  // Should return error
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

// The by-id lookup, the not-found answer and the empty-request error, sent
// through a real channel to the real handler.
TEST_CASE_FIXTURE(InternalServiceMetadataServerTest,
                  "GetCollectionMetadataOverGrpc") {
  proto::internal::GetCollectionMetadataRequest request;
  proto::internal::GetCollectionMetadataResponse response;

  SUBCASE("by id") {
    auto collection_id = coordinator_->CreateCollection(
        "id_test_collection", 256, core::MetricType::COSINE,
        core::IndexType::IVF_FLAT, 1);
    REQUIRE(collection_id.ok());
    request.set_collection_id(core::ToUInt32(collection_id.value()));

    auto status = GetMetadata(request, &response);

    INFO("gRPC error: " << status.error_message());
    REQUIRE(status.ok());
    CHECK(response.found());
    CHECK_EQ(response.metadata().collection_id(),
             core::ToUInt32(collection_id.value()));
    CHECK_EQ(response.metadata().collection_name(), "id_test_collection");
    CHECK_EQ(response.metadata().dimension(), 256);
    CHECK_EQ(response.metadata().metric_type(), "COSINE");
    CHECK_EQ(response.metadata().index_type(), "IVF_FLAT");
  }

  SUBCASE("unknown name is not an RPC error") {
    request.set_collection_name("nonexistent_collection");

    auto status = GetMetadata(request, &response);

    INFO("gRPC error: " << status.error_message());
    REQUIRE(status.ok());
    CHECK_FALSE(response.found());
  }

  SUBCASE("request without id or name") {
    auto status = GetMetadata(request, &response);

    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }
}

// Parallel calls through one channel to a real server: each caller gets its
// own collection back, field for field.
TEST_CASE_FIXTURE(InternalServiceMetadataServerTest,
                  "ConcurrentGetCollectionMetadataOverGrpc") {
  constexpr int kNumCollections = 10;
  std::vector<uint32_t> ids(kNumCollections);
  for (int i = 0; i < kNumCollections; ++i) {
    auto collection_id = coordinator_->CreateCollection(
        "collection_" + std::to_string(i), 128 + i, core::MetricType::L2,
        core::IndexType::FLAT, 1);
    REQUIRE(collection_id.ok());
    ids[i] = core::ToUInt32(collection_id.value());
  }

  std::vector<grpc::Status> statuses(kNumCollections);
  std::vector<proto::internal::GetCollectionMetadataResponse> responses(
      kNumCollections);
  std::vector<std::thread> threads;
  threads.reserve(kNumCollections);
  for (int i = 0; i < kNumCollections; ++i) {
    // Each thread writes only its own slot. Assertions run on this thread.
    threads.emplace_back([this, i, &statuses, &responses] {
      proto::internal::GetCollectionMetadataRequest request;
      request.set_collection_name("collection_" + std::to_string(i));
      statuses[i] = GetMetadata(request, &responses[i]);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  for (int i = 0; i < kNumCollections; ++i) {
    INFO("request " << i << ": " << statuses[i].error_message());
    REQUIRE(statuses[i].ok());
    CHECK(responses[i].found());
    const auto& metadata = responses[i].metadata();
    CHECK_EQ(metadata.collection_id(), ids[i]);
    CHECK_EQ(metadata.collection_name(), "collection_" + std::to_string(i));
    CHECK_EQ(metadata.dimension(), static_cast<uint32_t>(128 + i));
    CHECK_EQ(metadata.metric_type(), "L2");
    CHECK_EQ(metadata.index_type(), "FLAT");
    CHECK_EQ(metadata.shard_count(), 1u);
  }
}
