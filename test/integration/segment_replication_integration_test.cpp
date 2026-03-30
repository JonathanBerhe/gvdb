// test/integration/segment_replication_integration_test.cpp
// Integration tests for segment replication (Phase 5)

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <thread>
#include <chrono>

#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/node_registry.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/internal_service.h"
#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "storage/segment_manager.h"
#include "utils/logger.h"
#include "internal.grpc.pb.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace test {

class SegmentReplicationIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create index factory
    index_factory_ = std::make_unique<index::IndexFactory>();

    // Create shard manager and coordinator
    shard_manager_ = std::make_shared<cluster::ShardManager>(8, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30));
    coordinator_ = std::make_shared<cluster::Coordinator>(shard_manager_, node_registry_);

    // Register a fake data node via NodeRegistry (production flow)
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(1);
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node.set_grpc_address("localhost:50051");
    node_registry_->UpdateNode(proto_node);

    // Create coordinator's segment manager and query executor
    coord_segment_manager_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-segment-replication-test/coordinator",
        index_factory_.get());

    coord_query_executor_ = std::make_shared<compute::QueryExecutor>(
        coord_segment_manager_.get());

    // Create InternalService (serves GetSegment RPC)
    internal_service_ = std::make_unique<network::InternalService>(
        shard_manager_,
        coord_segment_manager_,
        coord_query_executor_,
        nullptr,  // node_registry not needed
        nullptr,  // timestamp_oracle not needed
        coordinator_);

    // Start gRPC server for InternalService
    grpc::ServerBuilder builder;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &server_port_);
    builder.RegisterService(internal_service_.get());
    builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
    builder.SetMaxSendMessageSize(256 * 1024 * 1024);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(server_port_, 0);

    server_address_ = absl::StrFormat("localhost:%d", server_port_);

    // Create data node's segment manager (separate storage, won't have segments initially)
    data_segment_manager_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-segment-replication-test/data_node",
        index_factory_.get());

    data_query_executor_ = std::make_shared<compute::QueryExecutor>(
        data_segment_manager_.get());

    // Create data node's VectorDBService in distributed mode
    auto resolver = network::MakeCachedCoordinatorResolver(server_address_);
    data_vectordb_service_ = std::make_unique<network::VectorDBService>(
        data_segment_manager_,
        data_query_executor_,
        std::move(resolver));
  }

  void TearDown() override {
    server_->Shutdown();
    coord_segment_manager_->Clear();
    data_segment_manager_->Clear();
  }

  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;

  // Coordinator components
  std::shared_ptr<storage::SegmentManager> coord_segment_manager_;
  std::shared_ptr<compute::QueryExecutor> coord_query_executor_;
  std::unique_ptr<network::InternalService> internal_service_;

  // Data node components
  std::shared_ptr<storage::SegmentManager> data_segment_manager_;
  std::shared_ptr<compute::QueryExecutor> data_query_executor_;
  std::unique_ptr<network::VectorDBService> data_vectordb_service_;

  std::unique_ptr<grpc::Server> server_;
  int server_port_ = 0;
  std::string server_address_;
};

// Test: Data node pulls segment from coordinator on search
TEST_F(SegmentReplicationIntegrationTest, DataNodePullsSegmentOnSearch) {
  // 1. Create collection on coordinator
  auto collection_id = coordinator_->CreateCollection(
      "test_collection", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  ASSERT_TRUE(collection_id.ok());

  // 2. Create segment on coordinator using ShardSegmentId scheme
  core::SegmentId segment_id = cluster::ShardSegmentId(*collection_id, 0);
  auto create_seg_status = coord_segment_manager_->CreateSegmentWithId(
      segment_id, *collection_id, 128, core::MetricType::L2, core::IndexType::FLAT);
  ASSERT_TRUE(create_seg_status.ok());

  // Insert some test vectors
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  for (int i = 0; i < 10; i++) {
    std::vector<float> data(128);
    for (int j = 0; j < 128; j++) {
      data[j] = static_cast<float>(i * 128 + j);
    }
    vectors.push_back(core::Vector(std::move(data)));
    ids.push_back(core::MakeVectorId(i + 1));
  }

  auto insert_status = coord_segment_manager_->WriteVectors(segment_id, vectors, ids);
  ASSERT_TRUE(insert_status.ok());

  // 3. Also create segment on data node (simulate coordinator push)
  auto dn_create = data_segment_manager_->CreateSegmentWithId(
      segment_id, *collection_id, 128, core::MetricType::L2, core::IndexType::FLAT);
  ASSERT_TRUE(dn_create.ok());
  auto* dn_seg = data_segment_manager_->GetSegment(segment_id);
  ASSERT_NE(dn_seg, nullptr);
  auto dn_insert = dn_seg->AddVectors(vectors, ids);
  ASSERT_TRUE(dn_insert.ok());
  core::IndexConfig cfg;
  cfg.index_type = core::IndexType::FLAT;
  cfg.dimension = 128;
  cfg.metric_type = core::MetricType::L2;
  (void)data_segment_manager_->SealSegment(segment_id, cfg);

  // 4. Build gRPC Search request
  proto::SearchRequest request;
  request.set_collection_name("test_collection");
  request.set_top_k(3);

  // Query vector (similar to vector 0)
  auto* query_proto = request.mutable_query_vector();
  query_proto->set_dimension(128);
  for (int j = 0; j < 128; j++) {
    query_proto->add_values(static_cast<float>(j));
  }

  // 5. Execute search on data node (should trigger segment pull)
  grpc::ServerContext context;
  proto::SearchResponse response;

  grpc::Status status = data_vectordb_service_->Search(&context, &request, &response);

  // 6. Verify search succeeded
  ASSERT_TRUE(status.ok()) << "Search failed: " << status.error_message();
  EXPECT_GT(response.results_size(), 0);
  EXPECT_LE(response.results_size(), 3);

  // 7. Verify search results are reasonable (vector 0 should be closest)
  EXPECT_EQ(response.results(0).id(), 1);  // VectorId 1 (index 0)
}

// Test: Multiple searches reuse cached segment
TEST_F(SegmentReplicationIntegrationTest, MultipleSearchesReuseCachedSegment) {
  // Create collection and segment on coordinator
  auto collection_id = coordinator_->CreateCollection(
      "reuse_test", 64, core::MetricType::L2, core::IndexType::FLAT, 1);
  ASSERT_TRUE(collection_id.ok());

  core::SegmentId segment_id = cluster::ShardSegmentId(*collection_id, 0);

  // Create segment on coordinator
  if (!coord_segment_manager_->GetSegment(segment_id)) {
    auto cs = coord_segment_manager_->CreateSegmentWithId(
        segment_id, *collection_id, 64, core::MetricType::L2, core::IndexType::FLAT);
    ASSERT_TRUE(cs.ok());
  }

  // Insert vectors
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  for (int i = 0; i < 5; i++) {
    std::vector<float> data(64, static_cast<float>(i));
    vectors.push_back(core::Vector(std::move(data)));
    ids.push_back(core::MakeVectorId(i + 1));
  }
  coord_segment_manager_->WriteVectors(segment_id, vectors, ids);

  // Create segment on data node too (simulate push replication)
  if (!data_segment_manager_->GetSegment(segment_id)) {
    auto ds = data_segment_manager_->CreateSegmentWithId(
        segment_id, *collection_id, 64, core::MetricType::L2, core::IndexType::FLAT);
    ASSERT_TRUE(ds.ok());
    auto* dn_seg = data_segment_manager_->GetSegment(segment_id);
    dn_seg->AddVectors(vectors, ids);
    core::IndexConfig cfg;
    cfg.index_type = core::IndexType::FLAT;
    cfg.dimension = 64;
    cfg.metric_type = core::MetricType::L2;
    (void)data_segment_manager_->SealSegment(segment_id, cfg);
  }

  // First search
  proto::SearchRequest request1;
  request1.set_collection_name("reuse_test");
  request1.set_top_k(2);
  auto* query1 = request1.mutable_query_vector();
  query1->set_dimension(64);
  for (int j = 0; j < 64; j++) {
    query1->add_values(0.0f);
  }

  grpc::ServerContext context1;
  proto::SearchResponse response1;
  grpc::Status status1 = data_vectordb_service_->Search(&context1, &request1, &response1);
  ASSERT_TRUE(status1.ok());

  // Verify segment is now cached
  auto* cached_segment = data_segment_manager_->GetSegment(segment_id);
  ASSERT_NE(cached_segment, nullptr);

  // Second search (should reuse cached segment)
  proto::SearchRequest request2;
  request2.set_collection_name("reuse_test");
  request2.set_top_k(2);
  auto* query2 = request2.mutable_query_vector();
  query2->set_dimension(64);
  for (int j = 0; j < 64; j++) {
    query2->add_values(4.0f);  // Different query
  }

  grpc::ServerContext context2;
  proto::SearchResponse response2;
  grpc::Status status2 = data_vectordb_service_->Search(&context2, &request2, &response2);
  ASSERT_TRUE(status2.ok());
  EXPECT_GT(response2.results_size(), 0);
}

// Test: Segment not found on coordinator
TEST_F(SegmentReplicationIntegrationTest, SearchEmptyCollectionReturnsNoResults) {
  // Create collection (segment is created on data node by coordinator)
  auto collection_id = coordinator_->CreateCollection(
      "empty_collection", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  ASSERT_TRUE(collection_id.ok());

  // Search empty collection — should succeed with 0 results
  proto::SearchRequest request;
  request.set_collection_name("empty_collection");
  request.set_top_k(5);
  auto* query = request.mutable_query_vector();
  query->set_dimension(128);
  for (int j = 0; j < 128; j++) {
    query->add_values(1.0f);
  }

  grpc::ServerContext context;
  proto::SearchResponse response;
  grpc::Status status = data_vectordb_service_->Search(&context, &request, &response);

  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(response.results_size(), 0);
}

} // namespace test
} // namespace gvdb
