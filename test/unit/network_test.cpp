#include <doctest/doctest.h>

#include "network/proto_conversions.h"
#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "core/vector.h"

using namespace gvdb;

// ============================================================================
// Proto Conversion Tests
// ============================================================================

class ProtoConversionTest {
 public:
  ProtoConversionTest() {}
};

TEST_CASE_FIXTURE(ProtoConversionTest, "VectorConversion") {
  // Create proto vector
  proto::Vector proto_vec;
  proto_vec.set_dimension(3);
  proto_vec.add_values(1.0f);
  proto_vec.add_values(2.0f);
  proto_vec.add_values(3.0f);

  // Convert to core Vector
  auto result = network::fromProto(proto_vec);
  INFO(result.status().ToString());
  REQUIRE(result.ok());

  const auto& vec = *result;
  CHECK_EQ(vec.dimension(), 3);
  CHECK(vec.data()[0] == doctest::Approx(1.0f));
  CHECK(vec.data()[1] == doctest::Approx(2.0f));
  CHECK(vec.data()[2] == doctest::Approx(3.0f));
}

TEST_CASE_FIXTURE(ProtoConversionTest, "VectorDimensionMismatch") {
  proto::Vector proto_vec;
  proto_vec.set_dimension(5);  // Wrong dimension
  proto_vec.add_values(1.0f);
  proto_vec.add_values(2.0f);
  proto_vec.add_values(3.0f);  // Only 3 values

  auto result = network::fromProto(proto_vec);
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_CASE_FIXTURE(ProtoConversionTest, "VectorWithIdConversion") {
  proto::VectorWithId proto_vec;
  proto_vec.set_id(42);
  proto_vec.mutable_vector()->set_dimension(2);
  proto_vec.mutable_vector()->add_values(1.5f);
  proto_vec.mutable_vector()->add_values(2.5f);

  auto result = network::fromProto(proto_vec);
  REQUIRE(result.ok());

  CHECK_EQ(result->first, core::MakeVectorId(42));
  CHECK_EQ(result->second.dimension(), 2);
  CHECK(result->second.data()[0] == doctest::Approx(1.5f));
  CHECK(result->second.data()[1] == doctest::Approx(2.5f));
}

TEST_CASE_FIXTURE(ProtoConversionTest, "MetricTypeConversion") {
  auto l2 = network::fromProto(proto::CreateCollectionRequest::L2);
  REQUIRE(l2.ok());
  CHECK_EQ(*l2, core::MetricType::L2);

  auto ip = network::fromProto(proto::CreateCollectionRequest::INNER_PRODUCT);
  REQUIRE(ip.ok());
  CHECK_EQ(*ip, core::MetricType::INNER_PRODUCT);

  auto cosine = network::fromProto(proto::CreateCollectionRequest::COSINE);
  REQUIRE(cosine.ok());
  CHECK_EQ(*cosine, core::MetricType::COSINE);
}

TEST_CASE_FIXTURE(ProtoConversionTest, "IndexTypeConversion") {
  auto flat = network::fromProto(proto::CreateCollectionRequest::FLAT);
  REQUIRE(flat.ok());
  CHECK_EQ(*flat, core::IndexType::FLAT);

  auto hnsw = network::fromProto(proto::CreateCollectionRequest::HNSW);
  REQUIRE(hnsw.ok());
  CHECK_EQ(*hnsw, core::IndexType::HNSW);

  auto ivf = network::fromProto(proto::CreateCollectionRequest::IVF_FLAT);
  REQUIRE(ivf.ok());
  CHECK_EQ(*ivf, core::IndexType::IVF_FLAT);
}

TEST_CASE_FIXTURE(ProtoConversionTest, "CoreToProtoVector") {
  std::vector<float> data = {1.0f, 2.0f, 3.0f, 4.0f};
  core::Vector vec(data);

  proto::Vector proto_vec;
  network::toProto(vec, &proto_vec);

  CHECK_EQ(proto_vec.dimension(), 4);
  REQUIRE_EQ(proto_vec.values().size(), 4);
  CHECK(proto_vec.values(0) == doctest::Approx(1.0f));
  CHECK(proto_vec.values(1) == doctest::Approx(2.0f));
  CHECK(proto_vec.values(2) == doctest::Approx(3.0f));
  CHECK(proto_vec.values(3) == doctest::Approx(4.0f));
}

TEST_CASE_FIXTURE(ProtoConversionTest, "SearchResultConversion") {
  core::SearchResultEntry entry{core::MakeVectorId(123), 0.5f};

  proto::SearchResultEntry proto_entry;
  network::toProto(entry, &proto_entry);

  CHECK_EQ(proto_entry.id(), 123);
  CHECK(proto_entry.distance() == doctest::Approx(0.5f));
}

TEST_CASE_FIXTURE(ProtoConversionTest, "StatusConversion") {
  // OK status
  auto ok_status = network::toGrpcStatus(absl::OkStatus());
  CHECK(ok_status.ok());

  // Error statuses
  auto invalid = network::toGrpcStatus(
      absl::InvalidArgumentError("bad input"));
  CHECK_EQ(invalid.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  CHECK_EQ(invalid.error_message(), "bad input");

  auto not_found = network::toGrpcStatus(
      absl::NotFoundError("not found"));
  CHECK_EQ(not_found.error_code(), grpc::StatusCode::NOT_FOUND);

  auto already_exists = network::toGrpcStatus(
      absl::AlreadyExistsError("exists"));
  CHECK_EQ(already_exists.error_code(), grpc::StatusCode::ALREADY_EXISTS);
}

// ============================================================================
// VectorDBService Tests
// ============================================================================

class VectorDBServiceTest {
 public:
  VectorDBServiceTest() {
    // Create temporary directory for test data
    test_dir_ = "/tmp/gvdb_network_test";
    std::filesystem::create_directories(test_dir_);

    // Create index factory
    index_factory_ = std::make_unique<index::IndexFactory>();

    // Create segment manager
    segment_manager_ = std::make_shared<storage::SegmentManager>(
        test_dir_, index_factory_.get());

    // Create query executor
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_manager_.get());

    // Create service with local resolver
    auto resolver = network::MakeLocalResolver(segment_manager_);
    service_ = std::make_unique<network::VectorDBService>(
        segment_manager_, query_executor_, std::move(resolver));
  }

  ~VectorDBServiceTest() {
    service_.reset();
    query_executor_.reset();
    segment_manager_.reset();
    index_factory_.reset();

    // Clean up test directory
    std::filesystem::remove_all(test_dir_);
  }

  std::string test_dir_;
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::SegmentManager> segment_manager_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<network::VectorDBService> service_;
};

TEST_CASE_FIXTURE(VectorDBServiceTest, "HealthCheck") {
  grpc::ServerContext context;
  proto::HealthCheckRequest request;
  proto::HealthCheckResponse response;

  auto status = service_->HealthCheck(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.status(), proto::HealthCheckResponse::SERVING);
  CHECK_FALSE(response.message().empty());
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "GetStatsEmpty") {
  grpc::ServerContext context;
  proto::GetStatsRequest request;
  proto::GetStatsResponse response;

  auto status = service_->GetStats(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.total_vectors(), 0);
  CHECK_EQ(response.total_collections(), 0);
  CHECK_EQ(response.total_queries(), 0);
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "CreateCollection") {
  grpc::ServerContext context;
  proto::CreateCollectionRequest request;
  request.set_collection_name("test_collection");
  request.set_dimension(128);
  request.set_metric(proto::CreateCollectionRequest::L2);
  request.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse response;

  auto status = service_->CreateCollection(&context, &request, &response);

  INFO(status.error_message());
  CHECK(status.ok());
  CHECK_GT(response.collection_id(), 0);
  CHECK_FALSE(response.message().empty());
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "CreateDuplicateCollection") {
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
    REQUIRE(status.ok());
  }

  // Try to create duplicate
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");  // Same name
    request.set_dimension(64);
    request.set_metric(proto::CreateCollectionRequest::COSINE);
    request.set_index_type(proto::CreateCollectionRequest::HNSW);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::ALREADY_EXISTS);
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "ListCollections") {
  // Create collections
  for (int i = 0; i < 3; ++i) {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("collection_" + std::to_string(i));
    request.set_dimension(64);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // List collections
  {
    grpc::ServerContext context;
    proto::ListCollectionsRequest request;
    proto::ListCollectionsResponse response;

    auto status = service_->ListCollections(&context, &request, &response);
    CHECK(status.ok());
    CHECK_EQ(response.collections().size(), 3);

    for (const auto& coll : response.collections()) {
      CHECK_EQ(coll.dimension(), 64);
      CHECK_EQ(coll.metric_type(), "L2");
    }
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "InsertAndSearch") {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(4);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Insert vectors
  {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name("test_collection");

    // Add 3 vectors
    for (int i = 0; i < 3; ++i) {
      auto* vec = request.add_vectors();
      vec->set_id(i);
      vec->mutable_vector()->set_dimension(4);
      for (int j = 0; j < 4; ++j) {
        vec->mutable_vector()->add_values(static_cast<float>(i * 4 + j));
      }
    }

    proto::InsertResponse response;
    auto status = service_->Insert(&context, &request, &response);
    INFO(status.error_message());
    CHECK(status.ok());
    CHECK_EQ(response.inserted_count(), 3);
  }

  // Search
  {
    grpc::ServerContext context;
    proto::SearchRequest request;
    request.set_collection_name("test_collection");
    request.set_top_k(2);

    // Query vector = [0, 1, 2, 3] (same as vector 0)
    auto* query = request.mutable_query_vector();
    query->set_dimension(4);
    query->add_values(0.0f);
    query->add_values(1.0f);
    query->add_values(2.0f);
    query->add_values(3.0f);

    proto::SearchResponse response;
    auto status = service_->Search(&context, &request, &response);
    INFO(status.error_message());
    CHECK(status.ok());
    REQUIRE_GE(response.results().size(), 1);

    // First result should be vector 0 with distance ~0
    CHECK_EQ(response.results(0).id(), 0);
    CHECK(response.results(0).distance() == doctest::Approx(0.0f).epsilon(0.01f));
    CHECK_GE(response.query_time_ms(), 0.0f);  // Can be 0 for very fast queries
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "SearchNonexistentCollection") {
  grpc::ServerContext context;
  proto::SearchRequest request;
  request.set_collection_name("nonexistent");
  request.set_top_k(10);

  auto* query = request.mutable_query_vector();
  query->set_dimension(4);
  for (int i = 0; i < 4; ++i) {
    query->add_values(1.0f);
  }

  proto::SearchResponse response;
  auto status = service_->Search(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "DropCollection") {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(64);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Drop collection
  {
    grpc::ServerContext context;
    proto::DropCollectionRequest request;
    request.set_collection_name("test_collection");

    proto::DropCollectionResponse response;
    auto status = service_->DropCollection(&context, &request, &response);
    INFO(status.error_message());
    CHECK(status.ok());
  }

  // Verify collection is gone
  {
    grpc::ServerContext context;
    proto::ListCollectionsRequest request;
    proto::ListCollectionsResponse response;

    auto status = service_->ListCollections(&context, &request, &response);
    CHECK(status.ok());
    CHECK_EQ(response.collections().size(), 0);
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "GetVectors") {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(4);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Insert vectors
  {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name("test_collection");

    for (int i = 0; i < 5; ++i) {
      auto* vec = request.add_vectors();
      vec->set_id(i + 1);
      vec->mutable_vector()->set_dimension(4);
      for (int j = 0; j < 4; ++j) {
        vec->mutable_vector()->add_values(static_cast<float>(i * 4 + j));
      }
    }

    proto::InsertResponse response;
    auto status = service_->Insert(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Get vectors
  {
    grpc::ServerContext context;
    proto::GetRequest request;
    request.set_collection_name("test_collection");
    request.add_ids(1);
    request.add_ids(3);
    request.add_ids(5);
    request.set_return_metadata(false);

    proto::GetResponse response;
    auto status = service_->Get(&context, &request, &response);
    INFO(status.error_message());
    CHECK(status.ok());
    CHECK_EQ(response.vectors().size(), 3);
    CHECK_EQ(response.not_found_ids().size(), 0);

    // Verify vector IDs
    CHECK_EQ(response.vectors(0).id(), 1);
    CHECK_EQ(response.vectors(1).id(), 3);
    CHECK_EQ(response.vectors(2).id(), 5);
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "GetVectorsPartialMatch") {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(4);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Insert vectors
  {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name("test_collection");

    for (int i = 0; i < 3; ++i) {
      auto* vec = request.add_vectors();
      vec->set_id(i + 1);
      vec->mutable_vector()->set_dimension(4);
      for (int j = 0; j < 4; ++j) {
        vec->mutable_vector()->add_values(1.0f);
      }
    }

    proto::InsertResponse response;
    auto status = service_->Insert(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Get mix of existing and non-existing IDs
  {
    grpc::ServerContext context;
    proto::GetRequest request;
    request.set_collection_name("test_collection");
    request.add_ids(1);     // exists
    request.add_ids(999);   // doesn't exist
    request.add_ids(2);     // exists
    request.add_ids(888);   // doesn't exist

    proto::GetResponse response;
    auto status = service_->Get(&context, &request, &response);
    CHECK(status.ok());
    CHECK_EQ(response.vectors().size(), 2);
    CHECK_EQ(response.not_found_ids().size(), 2);

    // Verify found IDs
    CHECK_EQ(response.vectors(0).id(), 1);
    CHECK_EQ(response.vectors(1).id(), 2);

    // Verify not found IDs
    CHECK_EQ(response.not_found_ids(0), 999);
    CHECK_EQ(response.not_found_ids(1), 888);
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "GetVectorsWithMetadata") {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(4);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Insert vectors with metadata
  {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name("test_collection");

    for (int i = 0; i < 3; ++i) {
      auto* vec = request.add_vectors();
      vec->set_id(i + 1);
      vec->mutable_vector()->set_dimension(4);
      for (int j = 0; j < 4; ++j) {
        vec->mutable_vector()->add_values(1.0f);
      }

      // Add metadata
      auto* metadata = vec->mutable_metadata();
      auto* name_field = &(*metadata->mutable_fields())["name"];
      name_field->set_string_value("vector_" + std::to_string(i));
    }

    proto::InsertResponse response;
    auto status = service_->Insert(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Get vectors with metadata
  {
    grpc::ServerContext context;
    proto::GetRequest request;
    request.set_collection_name("test_collection");
    request.add_ids(1);
    request.add_ids(3);
    request.set_return_metadata(true);

    proto::GetResponse response;
    auto status = service_->Get(&context, &request, &response);
    CHECK(status.ok());
    CHECK_EQ(response.vectors().size(), 2);

    // Verify metadata is returned
    CHECK(response.vectors(0).has_metadata());
    CHECK(response.vectors(1).has_metadata());

    auto& meta0 = response.vectors(0).metadata().fields();
    auto& meta1 = response.vectors(1).metadata().fields();

    CHECK_EQ(meta0.at("name").string_value(), "vector_0");
    CHECK_EQ(meta1.at("name").string_value(), "vector_2");
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "GetVectorsEmptyRequest") {
  grpc::ServerContext context;
  proto::GetRequest request;
  request.set_collection_name("test_collection");
  // No IDs added

  proto::GetResponse response;
  auto status = service_->Get(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "GetVectorsNonexistentCollection") {
  grpc::ServerContext context;
  proto::GetRequest request;
  request.set_collection_name("nonexistent");
  request.add_ids(1);

  proto::GetResponse response;
  auto status = service_->Get(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

// ============================================================================
// Delete Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceTest, "DeleteVectors") {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(4);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Insert vectors
  {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name("test_collection");

    for (int i = 0; i < 5; ++i) {
      auto* vec = request.add_vectors();
      vec->set_id(i + 1);
      vec->mutable_vector()->set_dimension(4);
      for (int j = 0; j < 4; ++j) {
        vec->mutable_vector()->add_values(1.0f);
      }
    }

    proto::InsertResponse response;
    auto status = service_->Insert(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Delete 3 vectors
  {
    grpc::ServerContext context;
    proto::DeleteRequest request;
    request.set_collection_name("test_collection");
    request.add_ids(1);
    request.add_ids(3);
    request.add_ids(5);

    proto::DeleteResponse response;
    auto status = service_->Delete(&context, &request, &response);
    CHECK(status.ok());
    CHECK_EQ(response.deleted_count(), 3);
    CHECK_EQ(response.not_found_ids().size(), 0);
  }

  // Verify deletion by trying to get deleted vectors
  {
    grpc::ServerContext context;
    proto::GetRequest request;
    request.set_collection_name("test_collection");
    request.add_ids(1);
    request.add_ids(2);
    request.add_ids(3);

    proto::GetResponse response;
    auto status = service_->Get(&context, &request, &response);
    CHECK(status.ok());
    CHECK_EQ(response.vectors().size(), 1);  // Only ID 2 should be found
    CHECK_EQ(response.vectors(0).id(), 2);
    CHECK_EQ(response.not_found_ids().size(), 2);  // IDs 1 and 3 not found
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "DeleteVectorsPartialMatch") {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(4);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Insert 3 vectors
  {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name("test_collection");

    for (int i = 0; i < 3; ++i) {
      auto* vec = request.add_vectors();
      vec->set_id(i + 1);
      vec->mutable_vector()->set_dimension(4);
      for (int j = 0; j < 4; ++j) {
        vec->mutable_vector()->add_values(1.0f);
      }
    }

    proto::InsertResponse response;
    auto status = service_->Insert(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Delete mix of existing and non-existing IDs
  {
    grpc::ServerContext context;
    proto::DeleteRequest request;
    request.set_collection_name("test_collection");
    request.add_ids(1);      // exists
    request.add_ids(999);    // doesn't exist
    request.add_ids(3);      // exists
    request.add_ids(888);    // doesn't exist

    proto::DeleteResponse response;
    auto status = service_->Delete(&context, &request, &response);
    CHECK(status.ok());
    CHECK_EQ(response.deleted_count(), 2);
    CHECK_EQ(response.not_found_ids().size(), 2);

    // Verify not found IDs
    CHECK_EQ(response.not_found_ids(0), 999);
    CHECK_EQ(response.not_found_ids(1), 888);
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "DeleteVectorsEmptyRequest") {
  grpc::ServerContext context;
  proto::DeleteRequest request;
  request.set_collection_name("test_collection");
  // Empty IDs list

  proto::DeleteResponse response;
  auto status = service_->Delete(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "DeleteVectorsNonexistentCollection") {
  grpc::ServerContext context;
  proto::DeleteRequest request;
  request.set_collection_name("nonexistent");
  request.add_ids(1);

  proto::DeleteResponse response;
  auto status = service_->Delete(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "DeleteVectorsBatchSizeLimit") {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(4);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Try to delete more than max batch size (10000)
  {
    grpc::ServerContext context;
    proto::DeleteRequest request;
    request.set_collection_name("test_collection");

    for (int i = 0; i < 10001; ++i) {
      request.add_ids(i);
    }

    proto::DeleteResponse response;
    auto status = service_->Delete(&context, &request, &response);

    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }
}

// ============================================================================
// UpdateMetadata Tests
// ============================================================================

TEST_CASE_FIXTURE(VectorDBServiceTest, "UpdateMetadataReplace") {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(4);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Insert vector with metadata
  {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name("test_collection");

    auto* vec = request.add_vectors();
    vec->set_id(1);
    vec->mutable_vector()->set_dimension(4);
    for (int j = 0; j < 4; ++j) {
      vec->mutable_vector()->add_values(1.0f);
    }

    auto* metadata = vec->mutable_metadata();
    (*metadata->mutable_fields())["price"].set_double_value(100.0);
    (*metadata->mutable_fields())["brand"].set_string_value("Nike");
    (*metadata->mutable_fields())["in_stock"].set_bool_value(true);

    proto::InsertResponse response;
    auto status = service_->Insert(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Update metadata (replace mode)
  {
    grpc::ServerContext context;
    proto::UpdateMetadataRequest request;
    request.set_collection_name("test_collection");
    request.set_id(1);
    request.set_merge(false);  // Replace mode

    auto* metadata = request.mutable_metadata();
    (*metadata->mutable_fields())["price"].set_double_value(80.0);
    (*metadata->mutable_fields())["rating"].set_double_value(4.5);

    proto::UpdateMetadataResponse response;
    auto status = service_->UpdateMetadata(&context, &request, &response);
    CHECK(status.ok());
    CHECK(response.updated());
  }

  // Verify metadata was replaced
  {
    grpc::ServerContext context;
    proto::GetRequest request;
    request.set_collection_name("test_collection");
    request.add_ids(1);
    request.set_return_metadata(true);

    proto::GetResponse response;
    auto status = service_->Get(&context, &request, &response);
    CHECK(status.ok());
    CHECK_EQ(response.vectors().size(), 1);

    auto& fields = response.vectors(0).metadata().fields();
    CHECK_EQ(fields.size(), 2);  // Only 2 fields
    CHECK_EQ(fields.at("price").double_value(), 80.0);
    CHECK_EQ(fields.at("rating").double_value(), 4.5);
    CHECK_EQ(fields.find("brand"), fields.end());  // Removed
    CHECK_EQ(fields.find("in_stock"), fields.end());  // Removed
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "UpdateMetadataMerge") {
  // Create collection and insert vector with metadata
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(4);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name("test_collection");

    auto* vec = request.add_vectors();
    vec->set_id(1);
    vec->mutable_vector()->set_dimension(4);
    for (int j = 0; j < 4; ++j) {
      vec->mutable_vector()->add_values(1.0f);
    }

    auto* metadata = vec->mutable_metadata();
    (*metadata->mutable_fields())["price"].set_double_value(100.0);
    (*metadata->mutable_fields())["brand"].set_string_value("Nike");

    proto::InsertResponse response;
    auto status = service_->Insert(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Update metadata (merge mode)
  {
    grpc::ServerContext context;
    proto::UpdateMetadataRequest request;
    request.set_collection_name("test_collection");
    request.set_id(1);
    request.set_merge(true);  // Merge mode

    auto* metadata = request.mutable_metadata();
    (*metadata->mutable_fields())["price"].set_double_value(80.0);  // Update
    (*metadata->mutable_fields())["rating"].set_double_value(4.5);  // Add

    proto::UpdateMetadataResponse response;
    auto status = service_->UpdateMetadata(&context, &request, &response);
    CHECK(status.ok());
    CHECK(response.updated());
  }

  // Verify metadata was merged
  {
    grpc::ServerContext context;
    proto::GetRequest request;
    request.set_collection_name("test_collection");
    request.add_ids(1);
    request.set_return_metadata(true);

    proto::GetResponse response;
    auto status = service_->Get(&context, &request, &response);
    CHECK(status.ok());

    auto& fields = response.vectors(0).metadata().fields();
    CHECK_EQ(fields.size(), 3);  // All 3 fields
    CHECK_EQ(fields.at("price").double_value(), 80.0);  // Updated
    CHECK_EQ(fields.at("brand").string_value(), "Nike");  // Preserved
    CHECK_EQ(fields.at("rating").double_value(), 4.5);  // Added
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "UpdateMetadataNotFound") {
  // Create collection
  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("test_collection");
    request.set_dimension(4);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);

    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    REQUIRE(status.ok());
  }

  // Try to update non-existent vector
  {
    grpc::ServerContext context;
    proto::UpdateMetadataRequest request;
    request.set_collection_name("test_collection");
    request.set_id(999);
    request.set_merge(false);

    auto* metadata = request.mutable_metadata();
    (*metadata->mutable_fields())["price"].set_double_value(100.0);

    proto::UpdateMetadataResponse response;
    auto status = service_->UpdateMetadata(&context, &request, &response);

    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
  }
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "UpdateMetadataInvalidRequest") {
  grpc::ServerContext context;
  proto::UpdateMetadataRequest request;
  request.set_collection_name("test_collection");
  request.set_id(0);  // Invalid ID

  auto* metadata = request.mutable_metadata();
  (*metadata->mutable_fields())["price"].set_double_value(100.0);

  proto::UpdateMetadataResponse response;
  auto status = service_->UpdateMetadata(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "UpdateMetadataNonexistentCollection") {
  grpc::ServerContext context;
  proto::UpdateMetadataRequest request;
  request.set_collection_name("nonexistent");
  request.set_id(1);

  auto* metadata = request.mutable_metadata();
  (*metadata->mutable_fields())["price"].set_double_value(100.0);

  proto::UpdateMetadataResponse response;
  auto status = service_->UpdateMetadata(&context, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}
