#include <gtest/gtest.h>
#include <thread>
#include <chrono>

#include "network/proto_conversions.h"
#include "network/grpc_server.h"
#include "network/vectordb_service.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "core/vector.h"

using namespace gvdb;

// ============================================================================
// Proto Conversion Tests
// ============================================================================

class ProtoConversionTest : public ::testing::Test {
 protected:
  void SetUp() override {}
};

TEST_F(ProtoConversionTest, VectorConversion) {
  // Create proto vector
  proto::Vector proto_vec;
  proto_vec.set_dimension(3);
  proto_vec.add_values(1.0f);
  proto_vec.add_values(2.0f);
  proto_vec.add_values(3.0f);

  // Convert to core Vector
  auto result = network::fromProto(proto_vec);
  ASSERT_TRUE(result.ok()) << result.status();

  const auto& vec = *result;
  EXPECT_EQ(vec.dimension(), 3);
  EXPECT_FLOAT_EQ(vec.data()[0], 1.0f);
  EXPECT_FLOAT_EQ(vec.data()[1], 2.0f);
  EXPECT_FLOAT_EQ(vec.data()[2], 3.0f);
}

TEST_F(ProtoConversionTest, VectorDimensionMismatch) {
  proto::Vector proto_vec;
  proto_vec.set_dimension(5);  // Wrong dimension
  proto_vec.add_values(1.0f);
  proto_vec.add_values(2.0f);
  proto_vec.add_values(3.0f);  // Only 3 values

  auto result = network::fromProto(proto_vec);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(ProtoConversionTest, VectorWithIdConversion) {
  proto::VectorWithId proto_vec;
  proto_vec.set_id(42);
  proto_vec.mutable_vector()->set_dimension(2);
  proto_vec.mutable_vector()->add_values(1.5f);
  proto_vec.mutable_vector()->add_values(2.5f);

  auto result = network::fromProto(proto_vec);
  ASSERT_TRUE(result.ok());

  EXPECT_EQ(result->first, core::MakeVectorId(42));
  EXPECT_EQ(result->second.dimension(), 2);
  EXPECT_FLOAT_EQ(result->second.data()[0], 1.5f);
  EXPECT_FLOAT_EQ(result->second.data()[1], 2.5f);
}

TEST_F(ProtoConversionTest, MetricTypeConversion) {
  auto l2 = network::fromProto(proto::CreateCollectionRequest::L2);
  ASSERT_TRUE(l2.ok());
  EXPECT_EQ(*l2, core::MetricType::L2);

  auto ip = network::fromProto(proto::CreateCollectionRequest::INNER_PRODUCT);
  ASSERT_TRUE(ip.ok());
  EXPECT_EQ(*ip, core::MetricType::INNER_PRODUCT);

  auto cosine = network::fromProto(proto::CreateCollectionRequest::COSINE);
  ASSERT_TRUE(cosine.ok());
  EXPECT_EQ(*cosine, core::MetricType::COSINE);
}

TEST_F(ProtoConversionTest, IndexTypeConversion) {
  auto flat = network::fromProto(proto::CreateCollectionRequest::FLAT);
  ASSERT_TRUE(flat.ok());
  EXPECT_EQ(*flat, core::IndexType::FLAT);

  auto hnsw = network::fromProto(proto::CreateCollectionRequest::HNSW);
  ASSERT_TRUE(hnsw.ok());
  EXPECT_EQ(*hnsw, core::IndexType::HNSW);

  auto ivf = network::fromProto(proto::CreateCollectionRequest::IVF_FLAT);
  ASSERT_TRUE(ivf.ok());
  EXPECT_EQ(*ivf, core::IndexType::IVF_FLAT);
}

TEST_F(ProtoConversionTest, CoreToProtoVector) {
  std::vector<float> data = {1.0f, 2.0f, 3.0f, 4.0f};
  core::Vector vec(data);

  proto::Vector proto_vec;
  network::toProto(vec, &proto_vec);

  EXPECT_EQ(proto_vec.dimension(), 4);
  ASSERT_EQ(proto_vec.values().size(), 4);
  EXPECT_FLOAT_EQ(proto_vec.values(0), 1.0f);
  EXPECT_FLOAT_EQ(proto_vec.values(1), 2.0f);
  EXPECT_FLOAT_EQ(proto_vec.values(2), 3.0f);
  EXPECT_FLOAT_EQ(proto_vec.values(3), 4.0f);
}

TEST_F(ProtoConversionTest, SearchResultConversion) {
  core::SearchResultEntry entry{core::MakeVectorId(123), 0.5f};

  proto::SearchResultEntry proto_entry;
  network::toProto(entry, &proto_entry);

  EXPECT_EQ(proto_entry.id(), 123);
  EXPECT_FLOAT_EQ(proto_entry.distance(), 0.5f);
}

TEST_F(ProtoConversionTest, StatusConversion) {
  // OK status
  auto ok_status = network::toGrpcStatus(absl::OkStatus());
  EXPECT_TRUE(ok_status.ok());

  // Error statuses
  auto invalid = network::toGrpcStatus(
      absl::InvalidArgumentError("bad input"));
  EXPECT_EQ(invalid.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(invalid.error_message(), "bad input");

  auto not_found = network::toGrpcStatus(
      absl::NotFoundError("not found"));
  EXPECT_EQ(not_found.error_code(), grpc::StatusCode::NOT_FOUND);

  auto already_exists = network::toGrpcStatus(
      absl::AlreadyExistsError("exists"));
  EXPECT_EQ(already_exists.error_code(), grpc::StatusCode::ALREADY_EXISTS);
}

// ============================================================================
// gRPC Server Tests
// ============================================================================

class GrpcServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.address = "localhost";
    config_.port = 50052;  // Use different port to avoid conflicts
  }

  network::GrpcServerConfig config_;
};

TEST_F(GrpcServerTest, ServerConstruction) {
  network::GrpcServer server(config_);
  EXPECT_EQ(server.address(), "localhost:50052");
}

TEST_F(GrpcServerTest, ServerLifecycle) {
  network::GrpcServer server(config_);

  // Start server in background thread
  std::thread server_thread([&server]() {
    auto status = server.start();
    // Will block until shutdown
  });

  // Give server time to start
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Shutdown
  server.shutdown();

  // Wait for server thread to finish
  server_thread.join();

  // Test passes if we reach here without hanging
  SUCCEED();
}

// ============================================================================
// VectorDBService Tests
// ============================================================================

class VectorDBServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
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

    // Create service
    service_ = std::make_unique<network::VectorDBService>(
        segment_manager_, query_executor_);
  }

  void TearDown() override {
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

TEST_F(VectorDBServiceTest, HealthCheck) {
  grpc::ServerContext context;
  proto::HealthCheckRequest request;
  proto::HealthCheckResponse response;

  auto status = service_->HealthCheck(&context, &request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(response.status(), proto::HealthCheckResponse::SERVING);
  EXPECT_FALSE(response.message().empty());
}

TEST_F(VectorDBServiceTest, GetStatsEmpty) {
  grpc::ServerContext context;
  proto::GetStatsRequest request;
  proto::GetStatsResponse response;

  auto status = service_->GetStats(&context, &request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(response.total_vectors(), 0);
  EXPECT_EQ(response.total_collections(), 0);
  EXPECT_EQ(response.total_queries(), 0);
}

TEST_F(VectorDBServiceTest, CreateCollection) {
  grpc::ServerContext context;
  proto::CreateCollectionRequest request;
  request.set_collection_name("test_collection");
  request.set_dimension(128);
  request.set_metric(proto::CreateCollectionRequest::L2);
  request.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse response;

  auto status = service_->CreateCollection(&context, &request, &response);

  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_GT(response.collection_id(), 0);
  EXPECT_FALSE(response.message().empty());
}

TEST_F(VectorDBServiceTest, CreateDuplicateCollection) {
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
    ASSERT_TRUE(status.ok());
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
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::StatusCode::ALREADY_EXISTS);
  }
}

TEST_F(VectorDBServiceTest, ListCollections) {
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
    ASSERT_TRUE(status.ok());
  }

  // List collections
  {
    grpc::ServerContext context;
    proto::ListCollectionsRequest request;
    proto::ListCollectionsResponse response;

    auto status = service_->ListCollections(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.collections().size(), 3);

    for (const auto& coll : response.collections()) {
      EXPECT_EQ(coll.dimension(), 64);
      EXPECT_EQ(coll.metric_type(), "L2");
    }
  }
}

TEST_F(VectorDBServiceTest, InsertAndSearch) {
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
    ASSERT_TRUE(status.ok());
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
    EXPECT_TRUE(status.ok()) << status.error_message();
    EXPECT_EQ(response.inserted_count(), 3);
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
    EXPECT_TRUE(status.ok()) << status.error_message();
    ASSERT_GE(response.results().size(), 1);

    // First result should be vector 0 with distance ~0
    EXPECT_EQ(response.results(0).id(), 0);
    EXPECT_NEAR(response.results(0).distance(), 0.0f, 0.01f);
    EXPECT_GE(response.query_time_ms(), 0.0f);  // Can be 0 for very fast queries
  }
}

TEST_F(VectorDBServiceTest, SearchNonexistentCollection) {
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

  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST_F(VectorDBServiceTest, DropCollection) {
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
    ASSERT_TRUE(status.ok());
  }

  // Drop collection
  {
    grpc::ServerContext context;
    proto::DropCollectionRequest request;
    request.set_collection_name("test_collection");

    proto::DropCollectionResponse response;
    auto status = service_->DropCollection(&context, &request, &response);
    EXPECT_TRUE(status.ok()) << status.error_message();
  }

  // Verify collection is gone
  {
    grpc::ServerContext context;
    proto::ListCollectionsRequest request;
    proto::ListCollectionsResponse response;

    auto status = service_->ListCollections(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.collections().size(), 0);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
