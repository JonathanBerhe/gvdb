#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <string>

#include "service/vectordb/vectordb_service.h"
#include "service/vectordb/collection_resolver.h"
#include "cluster/coordinator.h"
#include "storage/segment/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "internal.grpc.pb.h"
#include "support/grpc_server.h"
#include "support/temp_dir.h"

using namespace gvdb;

// ============================================================================
// Mock Coordinator InternalService
// ============================================================================

// Answers like the real InternalService::GetCollectionMetadata: a known
// collection gets found=true with its metadata, and an unknown one gets OK
// with found=false (not a gRPC error). should_fail makes the RPC itself fail,
// as when the coordinator cannot serve the call.
class MockCoordinatorInternalService : public proto::internal::InternalService::Service {
 public:
  std::atomic<int> get_collection_metadata_calls{0};
  std::atomic<bool> should_fail{false};

  grpc::Status GetCollectionMetadata(
      grpc::ServerContext* /*context*/,
      const proto::internal::GetCollectionMetadataRequest* request,
      proto::internal::GetCollectionMetadataResponse* response) override {
    get_collection_metadata_calls++;

    if (should_fail) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                          "Coordinator unavailable");
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

    response->set_found(false);
    return grpc::Status::OK;
  }
};

// ============================================================================
// Test Fixture for DISTRIBUTED Mode
// ============================================================================

struct VectorDBServiceDistributedTest {
  VectorDBServiceDistributedTest() {
    // Create index factory and segment manager
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>(
        test_dir_.str(), index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_store_.get());

    // The mock coordinator InternalService listens on a port that the
    // operating system picks. TestGrpcServer listens once it is built, and
    // members are built before this body runs, so no wait is needed.
    REQUIRE(coordinator_server_.ok());

    // Create VectorDBService in DISTRIBUTED mode
    auto resolver =
        service::MakeCachedCoordinatorResolver(coordinator_server_.address());
    service_ = std::make_unique<service::VectorDBService>(
        segment_store_, query_executor_, std::move(resolver));
  }

  ~VectorDBServiceDistributedTest() {
    // CRITICAL: Destroy service BEFORE shutting down server
    // VectorDBService may have pending RPC calls to coordinator
    service_.reset();

    // Now safe to shut down the mock coordinator server, before the mock
    // it serves.
    coordinator_server_.Shutdown();

    // Clean up remaining components
    mock_coordinator_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();
    // test_dir_ is the first member, so it removes the folder last.
  }

  test::ScopedTempDir test_dir_{"gvdb_vectordb_distributed_test"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<MockCoordinatorInternalService> mock_coordinator_ =
      std::make_unique<MockCoordinatorInternalService>();
  test::TestGrpcServer coordinator_server_{mock_coordinator_.get()};
  std::unique_ptr<service::VectorDBService> service_;
};

// ============================================================================
// Metadata Pull-on-Miss Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "SearchPullsMetadataFromCoordinator") {
  // Insert vectors first (requires collection to exist in segment manager)
  // Segment ID uses ShardSegmentId scheme: collection_id * 1000 + shard_index
  auto segment_id = core::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_store_->CreateSegmentWithId(
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

  auto* segment = segment_store_->GetSegment(segment_id);
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
  auto segment_id = core::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_store_->CreateSegmentWithId(
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

  auto* segment = segment_store_->GetSegment(segment_id);
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
  // The coordinator answers the lookup with OK and found=false, as the real
  // InternalService does. The data node must turn that into NOT_FOUND.
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

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "CoordinatorLookupErrorIsNotCached") {
  auto segment_id = core::ShardSegmentId(core::MakeCollectionId(42), 0);
  REQUIRE(segment_store_->CreateSegmentWithId(
      segment_id, core::MakeCollectionId(42), 128, core::MetricType::L2,
      core::IndexType::FLAT).ok());
  auto* segment = segment_store_->GetSegment(segment_id);
  REQUIRE_NE(segment, nullptr);
  REQUIRE(segment->AddVectors({core::Vector(std::vector<float>(128, 1.0f))},
                              {core::MakeVectorId(1)}).ok());

  auto search = [&](proto::SearchResponse* response) {
    grpc::ServerContext context;
    proto::SearchRequest request;
    request.set_collection_name("test_collection");
    auto* query = request.mutable_query_vector();
    query->set_dimension(128);
    for (int i = 0; i < 128; ++i) {
      query->add_values(1.0f);
    }
    request.set_top_k(5);
    return service_->Search(&context, &request, response);
  };

  // The lookup RPC fails. The search fails too, and the failure is not
  // reported as a missing collection.
  mock_coordinator_->should_fail = true;
  {
    proto::SearchResponse response;
    auto status = search(&response);
    CHECK_FALSE(status.ok());
    CHECK_NE(status.error_code(), grpc::StatusCode::NOT_FOUND);
    CHECK_EQ(response.results_size(), 0);
  }
  CHECK_EQ(mock_coordinator_->get_collection_metadata_calls.load(), 1);

  // Once the coordinator answers again, the next search asks it again and
  // succeeds: the failed lookup was not cached.
  mock_coordinator_->should_fail = false;
  {
    proto::SearchResponse response;
    auto status = search(&response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.results_size(), 1);
    CHECK_EQ(response.results(0).id(), 1);
  }
  CHECK_EQ(mock_coordinator_->get_collection_metadata_calls.load(), 2);
}

// ============================================================================
// Vector Operations Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceDistributedTest, "InsertVectors") {
  // Create segment for collection
  auto segment_id = core::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_store_->CreateSegmentWithId(
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
  auto segment_id = core::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_store_->CreateSegmentWithId(
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

  auto* segment = segment_store_->GetSegment(segment_id);
  REQUIRE_NE(segment, nullptr);
  REQUIRE(segment->AddVectors(vectors, ids).ok());

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
  auto segment_id = core::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_store_->CreateSegmentWithId(
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

  auto* segment = segment_store_->GetSegment(segment_id);
  REQUIRE_NE(segment, nullptr);
  REQUIRE(segment->AddVectors(vectors, ids).ok());

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
  // Shutdown coordinator. Shutdown blocks until the server stops serving, so
  // no wait is needed afterwards. The address has a port that the operating
  // system gave this server, and no test listens on a fixed port, so nothing
  // listens there now (only a new port-0 server that gets the same port
  // from the operating system again could).
  coordinator_server_.Shutdown();

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
  auto segment_id = core::ShardSegmentId(core::MakeCollectionId(42), 0);
  auto create_status = segment_store_->CreateSegmentWithId(
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
