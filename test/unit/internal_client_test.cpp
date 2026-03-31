#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <thread>

#include "cluster/internal_client.h"
#include "internal.grpc.pb.h"

using namespace gvdb;

// ============================================================================
// Mock InternalService for Testing
// ============================================================================

class MockInternalService : public proto::internal::InternalService::Service {
 public:
  std::atomic<int> create_segment_calls{0};
  std::atomic<int> delete_segment_calls{0};
  bool should_fail{false};

  grpc::Status CreateSegment(
      grpc::ServerContext* context,
      const proto::internal::CreateSegmentRequest* request,
      proto::internal::CreateSegmentResponse* response) override {
    create_segment_calls++;

    if (should_fail) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Mock failure");
    }

    response->set_segment_id(12345);
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status DeleteSegment(
      grpc::ServerContext* context,
      const proto::internal::DeleteSegmentRequest* request,
      proto::internal::DeleteSegmentResponse* response) override {
    delete_segment_calls++;

    if (should_fail) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Segment not found");
    }

    response->set_success(true);
    return grpc::Status::OK;
  }
};

// ============================================================================
// Test Fixture
// ============================================================================

class InternalClientTest {
 public:
  InternalClientTest() {
    // Create mock service
    mock_service_ = std::make_unique<MockInternalService>();

    // Start server
    server_address_ = "localhost:59051";
    StartServer();

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  ~InternalClientTest() {
    if (server_) {
      server_->Shutdown();
    }
    server_.reset();
    mock_service_.reset();
  }

  void StartServer() {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(mock_service_.get());
    server_ = builder.BuildAndStart();
  }

  std::string server_address_;
  std::unique_ptr<MockInternalService> mock_service_;
  std::unique_ptr<grpc::Server> server_;
};

// ============================================================================
// GrpcInternalServiceClientFactory Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalClientTest, "FactoryCreatesClientSuccessfully") {
  cluster::GrpcInternalServiceClientFactory factory;

  auto client = factory.CreateClient(core::MakeNodeId(1), server_address_);

  REQUIRE_NE(client, nullptr);
}

TEST_CASE_FIXTURE(InternalClientTest, "FactoryCreatesClientWithInvalidAddress") {
  cluster::GrpcInternalServiceClientFactory factory;

  // Even with invalid address, client creation succeeds (lazy connection)
  auto client = factory.CreateClient(core::MakeNodeId(1), "invalid:99999");

  CHECK_NE(client, nullptr);
}

// ============================================================================
// GrpcInternalServiceClient RPC Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalClientTest, "CreateSegmentSucceeds") {
  // Create client
  cluster::GrpcInternalServiceClientFactory factory;
  auto client = factory.CreateClient(core::MakeNodeId(1), server_address_);
  REQUIRE_NE(client, nullptr);

  // Call CreateSegment RPC
  grpc::ClientContext context;
  proto::internal::CreateSegmentRequest request;
  request.set_collection_id(42);
  request.set_dimension(128);
  request.set_metric_type("L2");
  request.set_index_type("FLAT");
  proto::internal::CreateSegmentResponse response;

  auto status = client->CreateSegment(&context, request, &response);

  CHECK(status.ok());
  CHECK(response.success());
  CHECK_EQ(response.segment_id(), 12345);
  CHECK_EQ(mock_service_->create_segment_calls.load(), 1);
}

TEST_CASE_FIXTURE(InternalClientTest, "CreateSegmentFailsWithError") {
  // Set mock to fail
  mock_service_->should_fail = true;

  // Create client
  cluster::GrpcInternalServiceClientFactory factory;
  auto client = factory.CreateClient(core::MakeNodeId(1), server_address_);
  REQUIRE_NE(client, nullptr);

  // Call CreateSegment RPC (should fail)
  grpc::ClientContext context;
  proto::internal::CreateSegmentRequest request;
  request.set_collection_id(42);
  request.set_dimension(128);
  proto::internal::CreateSegmentResponse response;

  auto status = client->CreateSegment(&context, request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INTERNAL);
  CHECK_EQ(mock_service_->create_segment_calls.load(), 1);
}

TEST_CASE_FIXTURE(InternalClientTest, "DeleteSegmentSucceeds") {
  // Create client
  cluster::GrpcInternalServiceClientFactory factory;
  auto client = factory.CreateClient(core::MakeNodeId(1), server_address_);
  REQUIRE_NE(client, nullptr);

  // Call DeleteSegment RPC
  grpc::ClientContext context;
  proto::internal::DeleteSegmentRequest request;
  request.set_segment_id(12345);
  proto::internal::DeleteSegmentResponse response;

  auto status = client->DeleteSegment(&context, request, &response);

  CHECK(status.ok());
  CHECK(response.success());
  CHECK_EQ(mock_service_->delete_segment_calls.load(), 1);
}

TEST_CASE_FIXTURE(InternalClientTest, "DeleteSegmentFailsWithNotFound") {
  // Set mock to fail
  mock_service_->should_fail = true;

  // Create client
  cluster::GrpcInternalServiceClientFactory factory;
  auto client = factory.CreateClient(core::MakeNodeId(1), server_address_);
  REQUIRE_NE(client, nullptr);

  // Call DeleteSegment RPC (should fail)
  grpc::ClientContext context;
  proto::internal::DeleteSegmentRequest request;
  request.set_segment_id(99999);
  proto::internal::DeleteSegmentResponse response;

  auto status = client->DeleteSegment(&context, request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
  CHECK_EQ(mock_service_->delete_segment_calls.load(), 1);
}

TEST_CASE_FIXTURE(InternalClientTest, "MultipleRPCCallsSucceed") {
  // Create client
  cluster::GrpcInternalServiceClientFactory factory;
  auto client = factory.CreateClient(core::MakeNodeId(1), server_address_);
  REQUIRE_NE(client, nullptr);

  // Call CreateSegment multiple times
  for (int i = 0; i < 5; ++i) {
    grpc::ClientContext context;
    proto::internal::CreateSegmentRequest request;
    request.set_collection_id(i + 1);
    request.set_dimension(128);
    request.set_metric_type("L2");
    request.set_index_type("FLAT");
    proto::internal::CreateSegmentResponse response;

    auto status = client->CreateSegment(&context, request, &response);
    CHECK(status.ok());
  }

  CHECK_EQ(mock_service_->create_segment_calls.load(), 5);
}

// ============================================================================
// NullInternalServiceClientFactory Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalClientTest, "NullFactoryReturnsNullptr") {
  cluster::NullInternalServiceClientFactory factory;

  auto client = factory.CreateClient(core::MakeNodeId(1), server_address_);

  CHECK_EQ(client, nullptr);
}

TEST_CASE_FIXTURE(InternalClientTest, "NullFactoryReturnsNullptrForAnyAddress") {
  cluster::NullInternalServiceClientFactory factory;

  auto client1 = factory.CreateClient(core::MakeNodeId(1), "localhost:50051");
  auto client2 = factory.CreateClient(core::MakeNodeId(2), "192.168.1.1:8080");
  auto client3 = factory.CreateClient(core::MakeNodeId(3), "invalid:99999");

  CHECK_EQ(client1, nullptr);
  CHECK_EQ(client2, nullptr);
  CHECK_EQ(client3, nullptr);
}

// ============================================================================
// Connection Handling Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalClientTest, "ClientHandlesServerUnavailable") {
  // Shutdown server
  server_->Shutdown();
  server_.reset();

  // Wait for shutdown
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Create client (should succeed - lazy connection)
  cluster::GrpcInternalServiceClientFactory factory;
  auto client = factory.CreateClient(core::MakeNodeId(1), server_address_);
  REQUIRE_NE(client, nullptr);

  // Call RPC (should fail with UNAVAILABLE)
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
  proto::internal::CreateSegmentRequest request;
  request.set_collection_id(42);
  request.set_dimension(128);
  proto::internal::CreateSegmentResponse response;

  auto status = client->CreateSegment(&context, request, &response);

  CHECK_FALSE(status.ok());
  // Status code should indicate connection failure
  CHECK((status.error_code() == grpc::StatusCode::UNAVAILABLE ||
              status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED));
}

TEST_CASE_FIXTURE(InternalClientTest, "ClientReconnectsAfterServerRestart") {
  // Create client
  cluster::GrpcInternalServiceClientFactory factory;
  auto client = factory.CreateClient(core::MakeNodeId(1), server_address_);
  REQUIRE_NE(client, nullptr);

  // First RPC succeeds
  {
    grpc::ClientContext context;
    proto::internal::CreateSegmentRequest request;
    request.set_collection_id(1);
    request.set_dimension(128);
    request.set_metric_type("L2");
    request.set_index_type("FLAT");
    proto::internal::CreateSegmentResponse response;

    auto status = client->CreateSegment(&context, request, &response);
    CHECK(status.ok());
  }

  // Shutdown server
  server_->Shutdown();
  server_.reset();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Restart server
  StartServer();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Second RPC should succeed after reconnection
  {
    grpc::ClientContext context;
    proto::internal::CreateSegmentRequest request;
    request.set_collection_id(2);
    request.set_dimension(128);
    request.set_metric_type("L2");
    request.set_index_type("FLAT");
    proto::internal::CreateSegmentResponse response;

    auto status = client->CreateSegment(&context, request, &response);
    CHECK(status.ok());
  }
}

// ============================================================================
// Concurrent Client Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalClientTest, "ConcurrentRPCCalls") {
  // Create client
  cluster::GrpcInternalServiceClientFactory factory;
  auto client = factory.CreateClient(core::MakeNodeId(1), server_address_);
  REQUIRE_NE(client, nullptr);

  // Launch multiple threads making concurrent RPC calls
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&, i]() {
      grpc::ClientContext context;
      proto::internal::CreateSegmentRequest request;
      request.set_collection_id(i + 1);
      request.set_dimension(128);
      request.set_metric_type("L2");
      request.set_index_type("FLAT");
      proto::internal::CreateSegmentResponse response;

      auto status = client->CreateSegment(&context, request, &response);
      if (status.ok()) {
        success_count++;
      }
    });
  }

  // Wait for all threads
  for (auto& t : threads) {
    t.join();
  }

  CHECK_EQ(success_count.load(), 10);
  CHECK_EQ(mock_service_->create_segment_calls.load(), 10);
}

TEST_CASE_FIXTURE(InternalClientTest, "MultipleClientsToSameServer") {
  // Create multiple clients
  cluster::GrpcInternalServiceClientFactory factory;
  auto client1 = factory.CreateClient(core::MakeNodeId(1), server_address_);
  auto client2 = factory.CreateClient(core::MakeNodeId(2), server_address_);
  auto client3 = factory.CreateClient(core::MakeNodeId(3), server_address_);

  REQUIRE_NE(client1, nullptr);
  REQUIRE_NE(client2, nullptr);
  REQUIRE_NE(client3, nullptr);

  // All clients should be able to make RPC calls
  {
    grpc::ClientContext context;
    proto::internal::CreateSegmentRequest request;
    request.set_collection_id(1);
    request.set_dimension(128);
    request.set_metric_type("L2");
    request.set_index_type("FLAT");
    proto::internal::CreateSegmentResponse response;
    auto status = client1->CreateSegment(&context, request, &response);
    CHECK(status.ok());
  }

  {
    grpc::ClientContext context;
    proto::internal::CreateSegmentRequest request;
    request.set_collection_id(2);
    request.set_dimension(256);
    request.set_metric_type("COSINE");
    request.set_index_type("HNSW");
    proto::internal::CreateSegmentResponse response;
    auto status = client2->CreateSegment(&context, request, &response);
    CHECK(status.ok());
  }

  {
    grpc::ClientContext context;
    proto::internal::DeleteSegmentRequest request;
    request.set_segment_id(12345);
    proto::internal::DeleteSegmentResponse response;
    auto status = client3->DeleteSegment(&context, request, &response);
    CHECK(status.ok());
  }

  CHECK_EQ(mock_service_->create_segment_calls.load(), 2);
  CHECK_EQ(mock_service_->delete_segment_calls.load(), 1);
}
