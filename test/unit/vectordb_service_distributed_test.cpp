#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <filesystem>
#include <thread>

#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "cluster/coordinator.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "internal.grpc.pb.h"

using namespace gvdb;

// ============================================================================
// Mock Coordinator InternalService
// ============================================================================

class MockCoordinatorInternalService : public proto::internal::InternalService::Service {
 public:
  std::atomic<int> get_collection_metadata_calls{0};
  bool should_fail{false};

  grpc::Status GetCollectionMetadata(
      grpc::ServerContext* context,
      const proto::internal::GetCollectionMetadataRequest* request,
      proto::internal::GetCollectionMetadataResponse* response) override {
    get_collection_metadata_calls++;

    if (should_fail) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Collection not found");
    }

    // Return mock collection metadata
    if (request->has_collection_name()) {
      if (request->collection_name() == "test_collection") {
        auto* metadata = response->mutable_metadata();
        metadata->set_collection_id(42);
        metadata->set_collection_name("test_collection");
        metadata->set_dimension(128);
        metadata->set_metric_type("L2");
        metadata->set_index_type("FLAT");
        response->set_found(true);
        return grpc::Status::OK;
      }
    } else if (request->has_collection_id()) {
      if (request->collection_id() == 42) {
        auto* metadata = response->mutable_metadata();
        metadata->set_collection_id(42);
        metadata->set_collection_name("test_collection");
        metadata->set_dimension(128);
        metadata->set_metric_type("L2");
        metadata->set_index_type("FLAT");
        response->set_found(true);
        return grpc::Status::OK;
      }
    }

    return grpc::Status(grpc::StatusCode::NOT_FOUND, "Collection not found");
  }
};

// ============================================================================
// Test Fixture for DISTRIBUTED Mode
// ============================================================================

struct VectorDBServiceDistributedTest {
  VectorDBServiceDistributedTest() {
    // Create test directory
    test_dir_ = "/tmp/gvdb_vectordb_distributed_test";
    std::filesystem::create_directories(test_dir_);

    // Create index factory and segment manager
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_manager_ = std::make_shared<storage::SegmentManager>(
        test_dir_, index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_manager_.get());

    // Start mock coordinator InternalService on dynamic port
    mock_coordinator_ = std::make_unique<MockCoordinatorInternalService>();
    coordinator_address_ = StartCoordinator();

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Create VectorDBService in DISTRIBUTED mode
    auto resolver = network::MakeCachedCoordinatorResolver(coordinator_address_);
    service_ = std::make_unique<network::VectorDBService>(
        segment_manager_, query_executor_, std::move(resolver));
  }

  ~VectorDBServiceDistributedTest() {
    // CRITICAL: Destroy service BEFORE shutting down server
    // VectorDBService may have pending RPC calls to coordinator
    service_.reset();

    // Now safe to shutdown the mock coordinator server
    if (coordinator_server_) {
      coordinator_server_->Shutdown();
      coordinator_server_->Wait();  // Wait for all RPCs to complete
    }

    // Clean up remaining components
    coordinator_server_.reset();
    mock_coordinator_.reset();
    query_executor_.reset();
    segment_manager_.reset();
    index_factory_.reset();

    std::filesystem::remove_all(test_dir_);
  }

  std::string StartCoordinator() {
    grpc::ServerBuilder builder;

    // Use port 0 to let OS assign available port (avoids port reuse conflicts)
    int selected_port = 0;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &selected_port);
    builder.RegisterService(mock_coordinator_.get());
    coordinator_server_ = builder.BuildAndStart();

    // Return the actual address with assigned port
    return "localhost:" + std::to_string(selected_port);
  }

  std::string test_dir_;
  std::string coordinator_address_;
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::SegmentManager> segment_manager_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<MockCoordinatorInternalService> mock_coordinator_;
  std::unique_ptr<grpc::Server> coordinator_server_;
  std::unique_ptr<network::VectorDBService> service_;
};

// ============================================================================
// Basic Functionality Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "HealthCheck") {
  grpc::ServerContext context;
  proto::HealthCheckRequest request;
  proto::HealthCheckResponse response;

  auto status = service_->HealthCheck(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.status(), proto::HealthCheckResponse::SERVING);
}

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "GetStatsEmpty") {
  grpc::ServerContext context;
  proto::GetStatsRequest request;
  proto::GetStatsResponse response;

  auto status = service_->GetStats(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.total_collections(), 0);
  CHECK_EQ(response.total_vectors(), 0);
}

// ============================================================================
// Metadata Pull-on-Miss Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "SearchPullsMetadataFromCoordinator") {
  // Insert vectors first (requires collection to exist in segment manager)
  // Segment ID uses ShardSegmentId scheme: collection_id * 1000 + shard_index
  auto segment_id = cluster::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_manager_->CreateSegmentWithId(
      segment_id, core::MakeCollectionId(42), 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(create_status.ok());

  // Add some test vectors
  std::vector<core::Vector> vectors;
  for (int i = 0; i < 10; ++i) {
    std::vector<float> data(128, 1.0f);
    vectors.emplace_back(data);
  }
  std::vector<core::VectorId> ids;
  for (int i = 0; i < 10; ++i) {
    ids.push_back(core::MakeVectorId(i + 1));
  }

  auto* segment = segment_manager_->GetSegment(segment_id);
  REQUIRE_NE(segment, nullptr);
  auto add_status = segment->AddVectors(vectors, ids);
  REQUIRE(add_status.ok());

  // Build index and seal segment
  core::IndexConfig index_config;
  index_config.index_type = core::IndexType::FLAT;
  index_config.dimension = 128;
  index_config.metric_type = core::MetricType::L2;
  auto index_result = index_factory_->CreateIndex(index_config);
  REQUIRE(index_result.ok());
  auto seal_status = segment->Seal(index_result.value().release());
  REQUIRE(seal_status.ok());

  // Now search - this should trigger metadata pull from coordinator
  grpc::ServerContext context;
  proto::SearchRequest request;
  request.set_collection_name("test_collection");
  auto* query = request.mutable_query_vector();
  query->set_dimension(128);
  for (int i = 0; i < 128; ++i) {
    query->add_values(1.0f);
  }
  request.set_top_k(5);
  proto::SearchResponse response;

  auto status = service_->Search(&context, &request, &response);

  CHECK(status.ok());
  CHECK_GT(response.results().size(), 0);

  // Verify coordinator was called for metadata
  CHECK_GE(mock_coordinator_->get_collection_metadata_calls.load(), 1);
}

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "MetadataCacheReusedOnSecondSearch") {
  // Set up collection and vectors
  auto segment_id = cluster::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_manager_->CreateSegmentWithId(
      segment_id, core::MakeCollectionId(42), 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(create_status.ok());

  std::vector<core::Vector> vectors;
  for (int i = 0; i < 10; ++i) {
    std::vector<float> data(128, 1.0f);
    vectors.emplace_back(data);
  }
  std::vector<core::VectorId> ids;
  for (int i = 0; i < 10; ++i) {
    ids.push_back(core::MakeVectorId(i + 1));
  }

  auto* segment = segment_manager_->GetSegment(segment_id);
  REQUIRE_NE(segment, nullptr);
  auto add_status = segment->AddVectors(vectors, ids);
  REQUIRE(add_status.ok());

  // Build index and seal segment
  core::IndexConfig index_config;
  index_config.index_type = core::IndexType::FLAT;
  index_config.dimension = 128;
  index_config.metric_type = core::MetricType::L2;
  auto index_result = index_factory_->CreateIndex(index_config);
  REQUIRE(index_result.ok());
  auto seal_status = segment->Seal(index_result.value().release());
  REQUIRE(seal_status.ok());

  // First search - pulls metadata
  {
    grpc::ServerContext context;
    proto::SearchRequest request;
    request.set_collection_name("test_collection");
    auto* query = request.mutable_query_vector();
    query->set_dimension(128);
    for (int i = 0; i < 128; ++i) {
      query->add_values(1.0f);
    }
    request.set_top_k(5);
    proto::SearchResponse response;

    auto status = service_->Search(&context, &request, &response);
    CHECK(status.ok());
  }

  int calls_after_first_search = mock_coordinator_->get_collection_metadata_calls.load();

  // Second search - should use cached metadata
  {
    grpc::ServerContext context;
    proto::SearchRequest request;
    request.set_collection_name("test_collection");
    auto* query = request.mutable_query_vector();
    query->set_dimension(128);
    for (int i = 0; i < 128; ++i) {
      query->add_values(1.0f);
    }
    request.set_top_k(5);
    proto::SearchResponse response;

    auto status = service_->Search(&context, &request, &response);
    CHECK(status.ok());
  }

  int calls_after_second_search = mock_coordinator_->get_collection_metadata_calls.load();

  // Metadata should be cached - coordinator calls should not increase
  CHECK_EQ(calls_after_first_search, calls_after_second_search);
}

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "SearchNonexistentCollectionFailsGracefully") {
  mock_coordinator_->should_fail = true;

  grpc::ServerContext context;
  proto::SearchRequest request;
  request.set_collection_name("nonexistent_collection");
  auto* query = request.mutable_query_vector();
  query->set_dimension(128);
  for (int i = 0; i < 128; ++i) {
    query->add_values(1.0f);
  }
  request.set_top_k(5);
  proto::SearchResponse response;

  auto status = service_->Search(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
  CHECK_GE(mock_coordinator_->get_collection_metadata_calls.load(), 1);
}

// ============================================================================
// Vector Operations Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "InsertVectors") {
  // Create segment for collection
  auto segment_id = cluster::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_manager_->CreateSegmentWithId(
      segment_id, core::MakeCollectionId(42), 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(create_status.ok());

  grpc::ServerContext context;
  proto::InsertRequest request;
  request.set_collection_name("test_collection");

  // Add 5 vectors
  for (int i = 0; i < 5; ++i) {
    auto* vec = request.add_vectors();
    vec->set_id(i + 1);
    vec->mutable_vector()->set_dimension(128);
    for (int j = 0; j < 128; ++j) {
      vec->mutable_vector()->add_values(1.0f);
    }
  }

  proto::InsertResponse response;

  auto status = service_->Insert(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.inserted_count(), 5);

  // Verify metadata was pulled from coordinator
  CHECK_GE(mock_coordinator_->get_collection_metadata_calls.load(), 1);
}

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "GetVectors") {
  // Create segment and add vectors
  auto segment_id = cluster::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_manager_->CreateSegmentWithId(
      segment_id, core::MakeCollectionId(42), 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(create_status.ok());

  std::vector<core::Vector> vectors;
  for (int i = 0; i < 5; ++i) {
    std::vector<float> data(128, static_cast<float>(i));
    vectors.emplace_back(data);
  }
  std::vector<core::VectorId> ids;
  for (int i = 0; i < 5; ++i) {
    ids.push_back(core::MakeVectorId(i + 1));
  }

  auto* segment = segment_manager_->GetSegment(segment_id);
  REQUIRE_NE(segment, nullptr);
  segment->AddVectors(vectors, ids);

  grpc::ServerContext context;
  proto::GetRequest request;
  request.set_collection_name("test_collection");
  request.add_ids(1);
  request.add_ids(3);
  request.add_ids(5);
  proto::GetResponse response;

  auto status = service_->Get(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.vectors_size(), 3);

  // Verify metadata was pulled
  CHECK_GE(mock_coordinator_->get_collection_metadata_calls.load(), 1);
}

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "DeleteVectors") {
  // Create segment and add vectors
  auto segment_id = cluster::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_manager_->CreateSegmentWithId(
      segment_id, core::MakeCollectionId(42), 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(create_status.ok());

  std::vector<core::Vector> vectors;
  for (int i = 0; i < 5; ++i) {
    std::vector<float> data(128, 1.0f);
    vectors.emplace_back(data);
  }
  std::vector<core::VectorId> ids;
  for (int i = 0; i < 5; ++i) {
    ids.push_back(core::MakeVectorId(i + 1));
  }

  auto* segment = segment_manager_->GetSegment(segment_id);
  REQUIRE_NE(segment, nullptr);
  segment->AddVectors(vectors, ids);

  grpc::ServerContext context;
  proto::DeleteRequest request;
  request.set_collection_name("test_collection");
  request.add_ids(1);
  request.add_ids(3);
  proto::DeleteResponse response;

  auto status = service_->Delete(&context, &request, &response);

  CHECK(status.ok());
  CHECK_GE(response.deleted_count(), 0);

  // Verify metadata was pulled
  CHECK_GE(mock_coordinator_->get_collection_metadata_calls.load(), 1);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "CoordinatorUnavailableDuringSearch") {
  // Shutdown coordinator
  coordinator_server_->Shutdown();
  coordinator_server_.reset();

  // Wait for shutdown
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  grpc::ServerContext context;
  proto::SearchRequest request;
  request.set_collection_name("test_collection");
  auto* query = request.mutable_query_vector();
  query->set_dimension(128);
  for (int i = 0; i < 128; ++i) {
    query->add_values(1.0f);
  }
  request.set_top_k(5);
  proto::SearchResponse response;

  auto status = service_->Search(&context, &request, &response);

  // Should fail because coordinator is unavailable
  CHECK_FALSE(status.ok());
}

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "InsertWithEmptyVectorList") {
  grpc::ServerContext context;
  proto::InsertRequest request;
  request.set_collection_name("test_collection");
  // No vectors added
  proto::InsertResponse response;

  auto status = service_->Insert(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "SearchWithInvalidDimension") {
  // Create segment with dimension 128
  auto segment_id = cluster::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_manager_->CreateSegmentWithId(
      segment_id, core::MakeCollectionId(42), 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(create_status.ok());

  grpc::ServerContext context;
  proto::SearchRequest request;
  request.set_collection_name("test_collection");
  auto* query = request.mutable_query_vector();
  query->set_dimension(64);  // Wrong dimension
  for (int i = 0; i < 64; ++i) {
    query->add_values(1.0f);
  }
  request.set_top_k(5);
  proto::SearchResponse response;

  auto status = service_->Search(&context, &request, &response);

  CHECK_FALSE(status.ok());
}
