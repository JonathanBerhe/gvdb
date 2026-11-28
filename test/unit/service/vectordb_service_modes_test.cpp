// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// VectorDBService in its two in-process modes. Single-node mode keeps
// collections and data in its own segment store. Coordinator mode keeps
// collections in the coordinator's registry and refuses data operations,
// which data nodes serve. The collection operations behave the same in both
// modes, so their cases run once per mode. Distributed mode (a data or query
// node that asks a remote coordinator) is in vectordb_service_distributed_test.cpp.

#include <doctest/doctest.h>

#include <chrono>
#include <memory>
#include <set>
#include <string>

#include "cluster/coordinator/coordinator.h"
#include "cluster/coordinator/node_registry.h"
#include "cluster/shard_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/vectordb_service.h"
#include "storage/segment/segment_manager.h"
#include "support/temp_dir.h"

namespace {

using namespace gvdb;

// Owns the parts that both modes share and calls the service RPCs. A derived
// fixture builds service_ with its resolver.
struct ServiceModeFixture {
  ServiceModeFixture() {
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ =
        std::make_shared<storage::SegmentManager>(test_dir_.str(), index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(*segment_store_);
  }

  ~ServiceModeFixture() {
    service_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();
    // test_dir_ is the first member, so it removes the folder last.
  }

  grpc::Status CreateCollection(
      const std::string& name, uint32_t dimension,
      proto::CreateCollectionRequest::MetricType metric = proto::CreateCollectionRequest::L2,
      proto::CreateCollectionRequest::IndexType index = proto::CreateCollectionRequest::FLAT,
      proto::CreateCollectionResponse* response = nullptr) {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name(name);
    request.set_dimension(dimension);
    request.set_metric(metric);
    request.set_index_type(index);
    proto::CreateCollectionResponse local_response;
    return service_->CreateCollection(&context, &request,
                                      response != nullptr ? response : &local_response);
  }

  grpc::Status DropCollection(const std::string& name,
                              proto::DropCollectionResponse* response = nullptr) {
    grpc::ServerContext context;
    proto::DropCollectionRequest request;
    request.set_collection_name(name);
    proto::DropCollectionResponse local_response;
    return service_->DropCollection(&context, &request,
                                    response != nullptr ? response : &local_response);
  }

  // Lists the collections; the call must succeed.
  proto::ListCollectionsResponse ListCollections() {
    grpc::ServerContext context;
    proto::ListCollectionsRequest request;
    proto::ListCollectionsResponse response;
    auto status = service_->ListCollections(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return response;
  }

  // Reads the stats; the call must succeed.
  proto::GetStatsResponse GetStats() {
    grpc::ServerContext context;
    proto::GetStatsRequest request;
    proto::GetStatsResponse response;
    auto status = service_->GetStats(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return response;
  }

  test::ScopedTempDir test_dir_{"gvdb_vectordb_modes_test"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<service::VectorDBService> service_;
};

// Single-node mode: the local resolver and the node's own segment store.
struct SingleNodeFixture : ServiceModeFixture {
  SingleNodeFixture() {
    service_ = std::make_unique<service::VectorDBService>(
        segment_store_, query_executor_, service::MakeLocalResolver(segment_store_));
  }
};

// Coordinator mode: the coordinator resolver. The coordinator has no client
// factory, so it makes no RPC to the data node that it knows.
struct CoordinatorFixture : ServiceModeFixture {
  CoordinatorFixture() {
    shard_manager_ = std::make_shared<cluster::ShardManager>(16, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30));
    coordinator_ = std::make_shared<cluster::Coordinator>(shard_manager_, node_registry_);

    // CreateCollection needs a ready data node to assign shards to. This is
    // what its heartbeat would register.
    proto::internal::NodeInfo node;
    node.set_node_id(100);
    node.set_grpc_address("localhost:50052");
    node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    node.set_memory_total_bytes(1000000000);
    node.set_memory_used_bytes(0);
    node.set_disk_total_bytes(10000000000);
    node.set_disk_used_bytes(0);
    node_registry_->UpdateNode(node);

    service_ = std::make_unique<service::VectorDBService>(
        segment_store_, query_executor_, service::MakeCoordinatorResolver(coordinator_));
  }

  ~CoordinatorFixture() {
    // The service uses the coordinator, so it goes first.
    service_.reset();
    coordinator_.reset();
    node_registry_.reset();
    shard_manager_.reset();
  }

  // Creates test_collection, so that a refused data RPC targets a
  // collection that exists.
  void RequireCollection() { REQUIRE(CreateCollection("test_collection", 128).ok()); }

  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
};

}  // namespace

TYPE_TO_STRING_AS("single-node", SingleNodeFixture);
TYPE_TO_STRING_AS("coordinator", CoordinatorFixture);

namespace {

// ============================================================================
// Collection operations: both modes
// ============================================================================

#define SERVICE_MODES SingleNodeFixture, CoordinatorFixture

TEST_CASE_TEMPLATE("VectorDBService modes: CreateCollection", F, SERVICE_MODES) {
  F f;
  proto::CreateCollectionResponse response;
  auto status = f.CreateCollection("test_collection", 128, proto::CreateCollectionRequest::L2,
                                   proto::CreateCollectionRequest::FLAT, &response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  CHECK_GT(response.collection_id(), 0);
  CHECK_FALSE(response.message().empty());

  auto list = f.ListCollections();
  REQUIRE_EQ(list.collections_size(), 1);
  CHECK_EQ(list.collections(0).collection_name(), "test_collection");
  CHECK_EQ(list.collections(0).dimension(), 128);
}

TEST_CASE_TEMPLATE("VectorDBService modes: CreateCollection rejects a duplicate name", F,
                   SERVICE_MODES) {
  F f;
  REQUIRE(f.CreateCollection("test_collection", 128).ok());

  auto status = f.CreateCollection("test_collection", 64, proto::CreateCollectionRequest::COSINE,
                                   proto::CreateCollectionRequest::HNSW);
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::ALREADY_EXISTS);
}

TEST_CASE_TEMPLATE("VectorDBService modes: CreateCollection rejects invalid arguments", F,
                   SERVICE_MODES) {
  F f;
  SUBCASE("empty name") {
    auto status = f.CreateCollection("", 128);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("dimension 0") {
    auto status = f.CreateCollection("zero_dim", 0);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("dimension above the maximum of 8192") {
    auto status = f.CreateCollection("too_wide", 8193);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  // A rejected create leaves nothing behind.
  CHECK_EQ(f.ListCollections().collections_size(), 0);
}

TEST_CASE_TEMPLATE("VectorDBService modes: ListCollections returns every collection", F,
                   SERVICE_MODES) {
  F f;
  REQUIRE(f.CreateCollection("collection_1", 64).ok());
  REQUIRE(f.CreateCollection("collection_2", 256, proto::CreateCollectionRequest::COSINE,
                             proto::CreateCollectionRequest::HNSW)
              .ok());

  auto list = f.ListCollections();
  REQUIRE_EQ(list.collections_size(), 2);

  // The order is unspecified, so the entries are found by name.
  std::set<std::string> names;
  for (const auto& collection : list.collections()) {
    names.insert(collection.collection_name());
    if (collection.collection_name() == "collection_1") {
      CHECK_EQ(collection.dimension(), 64);
      CHECK_EQ(collection.metric_type(), "L2");
    } else {
      CHECK_EQ(collection.dimension(), 256);
      CHECK_EQ(collection.metric_type(), "COSINE");
    }
  }
  CHECK_EQ(names, std::set<std::string>{"collection_1", "collection_2"});
}

TEST_CASE_TEMPLATE("VectorDBService modes: DropCollection removes the collection", F,
                   SERVICE_MODES) {
  F f;
  REQUIRE(f.CreateCollection("test_collection", 64).ok());

  proto::DropCollectionResponse response;
  auto status = f.DropCollection("test_collection", &response);
  INFO(status.error_message());
  CHECK(status.ok());
  CHECK_FALSE(response.message().empty());

  CHECK_EQ(f.ListCollections().collections_size(), 0);
}

TEST_CASE_TEMPLATE("VectorDBService modes: DropCollection of an unknown name is NOT_FOUND", F,
                   SERVICE_MODES) {
  F f;
  auto status = f.DropCollection("nonexistent_collection");
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST_CASE_TEMPLATE("VectorDBService modes: Search of an unknown collection is NOT_FOUND", F,
                   SERVICE_MODES) {
  F f;
  grpc::ServerContext context;
  proto::SearchRequest request;
  request.set_collection_name("nonexistent_collection");
  request.set_top_k(10);
  auto* query = request.mutable_query_vector();
  query->set_dimension(4);
  for (int i = 0; i < 4; ++i) query->add_values(1.0F);
  proto::SearchResponse response;

  auto status = f.service_->Search(&context, &request, &response);
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

// The proxy forwards every client GetStats to the coordinator, so GetStats
// must be served in coordinator mode too (not refused like the data
// operations), and it must count the collections that the coordinator holds.
TEST_CASE_TEMPLATE("VectorDBService modes: GetStats counts the collections", F, SERVICE_MODES) {
  F f;
  auto empty = f.GetStats();
  CHECK_EQ(empty.total_collections(), 0);
  CHECK_EQ(empty.total_vectors(), 0);
  CHECK_EQ(empty.total_queries(), 0);

  REQUIRE(f.CreateCollection("stats_a", 16).ok());
  REQUIRE(f.CreateCollection("stats_b", 16).ok());

  auto stats = f.GetStats();
  CHECK_EQ(stats.total_collections(), 2);
  CHECK_EQ(stats.total_vectors(), 0);
}

#undef SERVICE_MODES

// ============================================================================
// Coordinator mode only: data operations are refused
// ============================================================================

TEST_CASE_FIXTURE(CoordinatorFixture, "Coordinator mode refuses Get") {
  RequireCollection();
  grpc::ServerContext context;
  proto::GetRequest request;
  request.set_collection_name("test_collection");
  request.add_ids(1);
  request.add_ids(3);
  proto::GetResponse response;

  auto status = service_->Get(&context, &request, &response);
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

TEST_CASE_FIXTURE(CoordinatorFixture, "Coordinator mode refuses Delete") {
  RequireCollection();
  grpc::ServerContext context;
  proto::DeleteRequest request;
  request.set_collection_name("test_collection");
  request.add_ids(2);
  request.add_ids(4);
  proto::DeleteResponse response;

  auto status = service_->Delete(&context, &request, &response);
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

// The refusal comes before any validation: an insert with the wrong
// dimension, and an insert into a collection that does not exist, get the
// same code as a valid insert.
TEST_CASE_FIXTURE(CoordinatorFixture, "Coordinator mode refuses Insert") {
  RequireCollection();

  struct Row {
    const char* name;
    const char* collection;
    int dimension;
  };
  const Row rows[] = {
      {"valid insert", "test_collection", 128},
      {"wrong dimension", "test_collection", 64},
      {"unknown collection", "nonexistent_collection", 128},
  };
  for (const Row& row : rows) {
    CAPTURE(row.name);
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name(row.collection);
    auto* vector = request.add_vectors();
    vector->set_id(1);
    vector->mutable_vector()->set_dimension(row.dimension);
    for (int i = 0; i < row.dimension; ++i) vector->mutable_vector()->add_values(1.0F);
    proto::InsertResponse response;

    auto status = service_->Insert(&context, &request, &response);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
  }
}

}  // namespace
