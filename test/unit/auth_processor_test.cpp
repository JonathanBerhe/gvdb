#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <filesystem>
#include <memory>

#include "network/auth_processor.h"
#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "vectordb.grpc.pb.h"

using namespace gvdb;

// ============================================================================
// Integration test: Auth via real gRPC server with interceptor
// ============================================================================

class AuthIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all("/tmp/gvdb-auth-test");
    std::filesystem::create_directories("/tmp/gvdb-auth-test");

    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_manager_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-auth-test", index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_manager_.get());

    auto resolver = network::MakeLocalResolver(segment_manager_);
    service_ = std::make_unique<network::VectorDBService>(
        segment_manager_, query_executor_, std::move(resolver));

    // Create auth interceptor factory
    utils::AuthConfig auth_config;
    auth_config.enabled = true;
    auth_config.api_keys = {"valid-key"};

    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> creators;
    creators.push_back(
        std::make_unique<network::ApiKeyAuthInterceptorFactory>(auth_config));

    // Start server with interceptor
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(service_.get());
    builder.experimental().SetInterceptorCreators(std::move(creators));
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    address_ = "localhost:" + std::to_string(port);
  }

  void TearDown() override {
    if (server_) { server_->Shutdown(); server_->Wait(); }
    std::filesystem::remove_all("/tmp/gvdb-auth-test");
  }

  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::SegmentManager> segment_manager_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<network::VectorDBService> service_;
  std::unique_ptr<grpc::Server> server_;
  std::string address_;
};

TEST_F(AuthIntegrationTest, RequestWithValidKeySucceeds) {
  auto channel = grpc::CreateChannel(address_, grpc::InsecureChannelCredentials());
  auto stub = proto::VectorDBService::NewStub(channel);

  proto::HealthCheckRequest req;
  proto::HealthCheckResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer valid-key");

  auto status = stub->HealthCheck(&ctx, req, &resp);
  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(resp.status(), proto::HealthCheckResponse::SERVING);
}

TEST_F(AuthIntegrationTest, RequestWithoutKeyFails) {
  auto channel = grpc::CreateChannel(address_, grpc::InsecureChannelCredentials());
  auto stub = proto::VectorDBService::NewStub(channel);

  proto::HealthCheckRequest req;
  proto::HealthCheckResponse resp;
  grpc::ClientContext ctx;

  auto status = stub->HealthCheck(&ctx, req, &resp);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.error_code() == grpc::StatusCode::UNAUTHENTICATED ||
              status.error_code() == grpc::StatusCode::CANCELLED);
}

TEST_F(AuthIntegrationTest, RequestWithWrongKeyFails) {
  auto channel = grpc::CreateChannel(address_, grpc::InsecureChannelCredentials());
  auto stub = proto::VectorDBService::NewStub(channel);

  proto::HealthCheckRequest req;
  proto::HealthCheckResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer wrong-key");

  auto status = stub->HealthCheck(&ctx, req, &resp);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.error_code() == grpc::StatusCode::UNAUTHENTICATED ||
              status.error_code() == grpc::StatusCode::CANCELLED);
}

TEST_F(AuthIntegrationTest, RequestWithBadFormatFails) {
  auto channel = grpc::CreateChannel(address_, grpc::InsecureChannelCredentials());
  auto stub = proto::VectorDBService::NewStub(channel);

  proto::HealthCheckRequest req;
  proto::HealthCheckResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Basic abc123");

  auto status = stub->HealthCheck(&ctx, req, &resp);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.error_code() == grpc::StatusCode::UNAUTHENTICATED ||
              status.error_code() == grpc::StatusCode::CANCELLED);
}

TEST_F(AuthIntegrationTest, DataOperationWithValidKey) {
  auto channel = grpc::CreateChannel(address_, grpc::InsecureChannelCredentials());
  auto stub = proto::VectorDBService::NewStub(channel);

  // Create collection with auth
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("auth_test");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer valid-key");

  auto status = stub->CreateCollection(&ctx, create_req, &create_resp);
  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_GT(create_resp.collection_id(), 0);
}
