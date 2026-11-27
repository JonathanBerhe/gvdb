// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "auth/audit_logger.h"
#include "auth/rbac.h"
#include "compute/query_executor.h"
#include "config/config.h"
#include "index/index_factory.h"
#include "rpc/interceptors/audit_interceptor.h"
#include "rpc/interceptors/auth_processor.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/vectordb_service.h"
#include "storage/segment/segment_manager.h"
#include "support/temp_dir.h"
#include "vectordb.grpc.pb.h"

namespace {

using namespace gvdb;

// Read all lines from a file
std::vector<std::string> ReadLines(const std::string& path) {
  std::vector<std::string> lines;
  std::ifstream f(path);
  std::string line;
  while (std::getline(f, line)) {
    if (!line.empty()) lines.push_back(line);
  }
  return lines;
}

// Check if a JSON line contains a key-value pair like "key":"value"
bool JsonContains(const std::string& json, const std::string& key, const std::string& value) {
  std::string needle = "\"" + key + "\":\"" + value + "\"";
  return json.contains(needle);
}

// Check if a JSON line contains a key-value pair like "key":123
bool JsonContainsInt(const std::string& json, const std::string& key, int value) {
  std::string needle = "\"" + key + "\":" + std::to_string(value);
  return json.contains(needle);
}

// ============================================================================
// Test fixture: real gRPC server with auth + audit interceptors
// ============================================================================

class AuditIntegrationTest {
 public:
  AuditIntegrationTest() {
    audit_log_path_ = (test_dir_ / "audit.jsonl").string();

    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>((test_dir_ / "data").string(),
                                                               index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(*segment_store_);

    // Auth config
    auth::AuthConfig auth_config;
    auth_config.enabled = true;
    auth_config.roles = {
        {"admin-key", "admin", {}},
        {"ro-key", "readonly", {"*"}},
    };
    auto rbac_result = auth::RbacStore::Create(auth_config);
    REQUIRE(rbac_result.ok());
    rbac_store_ = std::move(*rbac_result);

    // Audit logger
    auth::AuditLogConfig audit_config;
    audit_config.enabled = true;
    audit_config.file_path = audit_log_path_;
    audit_config.max_file_size_mb = 10;
    audit_config.max_files = 2;
    auth::AuditLogger::Initialize(audit_config);

    auto resolver = service::MakeLocalResolver(segment_store_);
    service_ = std::make_unique<service::VectorDBService>(
        segment_store_, query_executor_, std::move(resolver),
        service::VectorDBServiceOptions{.rbac_store = rbac_store_});

    // Auth interceptor first, audit second
    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> creators;
    creators.push_back(std::make_unique<rpc::ApiKeyAuthInterceptorFactory>(rbac_store_));
    creators.push_back(std::make_unique<rpc::AuditInterceptorFactory>());

    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(service_.get());
    builder.experimental().SetInterceptorCreators(std::move(creators));
    server_ = builder.BuildAndStart();
    REQUIRE(server_ != nullptr);
    address_ = "localhost:" + std::to_string(port);
  }

  ~AuditIntegrationTest() {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
    auth::AuditLogger::Shutdown();
  }

  std::unique_ptr<proto::VectorDBService::Stub> MakeStub() {
    auto channel = grpc::CreateChannel(address_, grpc::InsecureChannelCredentials());
    return proto::VectorDBService::NewStub(channel);
  }

  // No wait is needed for a call that the handler completes: the audit
  // interceptor writes and flushes the line at PRE_SEND_STATUS, before the
  // server sends the status, so the line is in the file when the client call
  // returns.
  std::vector<std::string> GetAuditLines() { return ReadLines(audit_log_path_); }

  // Declared first, so the folder is removed after everything that uses it.
  test::ScopedTempDir test_dir_{"gvdb_audit_test"};
  std::string audit_log_path_;
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::shared_ptr<auth::RbacStore> rbac_store_;
  std::unique_ptr<service::VectorDBService> service_;
  std::unique_ptr<grpc::Server> server_;
  std::string address_;
};

// ============================================================================
// Tests
// ============================================================================

TEST_CASE_FIXTURE(AuditIntegrationTest, "CreateCollection produces valid audit JSON") {
  auto stub = MakeStub();
  proto::CreateCollectionRequest req;
  req.set_collection_name("audit_test_coll");
  req.set_dimension(4);
  req.set_metric(proto::CreateCollectionRequest::L2);
  req.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer admin-key");
  auto status = stub->CreateCollection(&ctx, req, &resp);
  CHECK(status.ok());

  auto lines = GetAuditLines();
  REQUIRE(lines.size() >= 1);
  auto& entry = lines.back();
  INFO("Audit entry: " << entry);
  CHECK(JsonContains(entry, "operation", "CreateCollection"));
  CHECK(JsonContains(entry, "collection", "audit_test_coll"));
  CHECK(JsonContains(entry, "api_key_id", "admin-key"));
  CHECK(JsonContains(entry, "status", "OK"));
  CHECK(entry.find("\"timestamp\"") != std::string::npos);
  CHECK(entry.find("\"latency_ms\"") != std::string::npos);
  CHECK(JsonContainsInt(entry, "grpc_code", 0));
}

TEST_CASE_FIXTURE(AuditIntegrationTest, "Insert logs collection and item_count") {
  auto stub = MakeStub();

  // Create collection first
  {
    proto::CreateCollectionRequest req;
    req.set_collection_name("insert_audit");
    req.set_dimension(4);
    req.set_metric(proto::CreateCollectionRequest::L2);
    req.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse resp;
    grpc::ClientContext ctx;
    ctx.AddMetadata("authorization", "Bearer admin-key");
    stub->CreateCollection(&ctx, req, &resp);
  }

  // Insert vectors
  proto::InsertRequest req;
  req.set_collection_name("insert_audit");
  for (int i = 0; i < 5; ++i) {
    auto* vec = req.add_vectors();
    vec->set_id(i + 1);
    auto* v = vec->mutable_vector();
    v->set_dimension(4);
    v->add_values(1.0f);
    v->add_values(0.0f);
    v->add_values(0.0f);
    v->add_values(1.0f);
  }
  proto::InsertResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer admin-key");
  auto status = stub->Insert(&ctx, req, &resp);
  CHECK(status.ok());

  auto lines = GetAuditLines();
  REQUIRE(lines.size() >= 2);  // CreateCollection + Insert
  auto& entry = lines.back();
  CHECK(JsonContains(entry, "operation", "Insert"));
  CHECK(JsonContains(entry, "collection", "insert_audit"));
  CHECK(JsonContainsInt(entry, "item_count", 5));
}

TEST_CASE_FIXTURE(AuditIntegrationTest, "HealthCheck is NOT audited") {
  auto stub = MakeStub();
  proto::HealthCheckRequest req;
  proto::HealthCheckResponse resp;
  grpc::ClientContext ctx;
  auto status = stub->HealthCheck(&ctx, req, &resp);
  CHECK(status.ok());

  auto lines = GetAuditLines();
  for (const auto& line : lines) {
    CHECK_FALSE(JsonContains(line, "operation", "HealthCheck"));
  }
}

TEST_CASE_FIXTURE(AuditIntegrationTest, "GetStats is NOT audited") {
  auto stub = MakeStub();
  proto::GetStatsRequest req;
  proto::GetStatsResponse resp;
  grpc::ClientContext ctx;
  auto status = stub->GetStats(&ctx, req, &resp);
  CHECK(status.ok());

  auto lines = GetAuditLines();
  for (const auto& line : lines) {
    CHECK_FALSE(JsonContains(line, "operation", "GetStats"));
  }
}

TEST_CASE_FIXTURE(AuditIntegrationTest, "Unauthenticated request is rejected") {
  auto stub = MakeStub();
  proto::CreateCollectionRequest req;
  req.set_collection_name("no_auth_coll");
  req.set_dimension(4);
  proto::CreateCollectionResponse resp;
  grpc::ClientContext ctx;
  // No authorization header
  auto status = stub->CreateCollection(&ctx, req, &resp);
  CHECK_FALSE(status.ok());

  // Auth interceptor uses TryCancel() which cancels the RPC server-side.
  // PRE_SEND_STATUS may or may not fire for cancelled RPCs (implementation-defined).
  // If an entry was logged, verify it shows a non-OK status with empty collection.
  //
  // A cancelled call can reach PRE_SEND_STATUS after the client has its
  // status. Shutdown returns only when every call has finished, interceptor
  // hooks included, so a line that this call writes is in the file before it
  // is read. The deadline is now: no call needs a grace period, and without
  // it the shutdown waits for the idle client connection to ack a GOAWAY.
  server_->Shutdown(std::chrono::system_clock::now());
  auto lines = GetAuditLines();
  bool found = false;
  for (const auto& line : lines) {
    if (JsonContains(line, "operation", "CreateCollection") &&
        JsonContains(line, "collection", "")) {
      found = true;
      CHECK_FALSE(JsonContains(line, "status", "OK"));
    }
  }
  // Either no entry (cancelled before hook fires) or a non-OK entry, both valid
  (void)found;
}

TEST_CASE_FIXTURE(AuditIntegrationTest, "Multiple RPCs produce multiple audit lines") {
  auto stub = MakeStub();

  // HealthCheck (should NOT be logged)
  {
    proto::HealthCheckRequest req;
    proto::HealthCheckResponse resp;
    grpc::ClientContext ctx;
    stub->HealthCheck(&ctx, req, &resp);
  }

  // CreateCollection
  {
    proto::CreateCollectionRequest req;
    req.set_collection_name("multi_test");
    req.set_dimension(4);
    req.set_metric(proto::CreateCollectionRequest::L2);
    req.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse resp;
    grpc::ClientContext ctx;
    ctx.AddMetadata("authorization", "Bearer admin-key");
    stub->CreateCollection(&ctx, req, &resp);
  }

  // DropCollection
  {
    proto::DropCollectionRequest req;
    req.set_collection_name("multi_test");
    proto::DropCollectionResponse resp;
    grpc::ClientContext ctx;
    ctx.AddMetadata("authorization", "Bearer admin-key");
    stub->DropCollection(&ctx, req, &resp);
  }

  auto lines = GetAuditLines();
  CHECK(lines.size() == 2);  // CreateCollection + DropCollection (no HealthCheck)
  CHECK(JsonContains(lines[0], "operation", "CreateCollection"));
  CHECK(JsonContains(lines[1], "operation", "DropCollection"));
}

TEST_CASE_FIXTURE(AuditIntegrationTest, "Permission denied is audited") {
  auto stub = MakeStub();

  // Create collection first as admin
  {
    proto::CreateCollectionRequest req;
    req.set_collection_name("perm_test");
    req.set_dimension(4);
    req.set_metric(proto::CreateCollectionRequest::L2);
    req.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse resp;
    grpc::ClientContext ctx;
    ctx.AddMetadata("authorization", "Bearer admin-key");
    stub->CreateCollection(&ctx, req, &resp);
  }

  // Try to insert with readonly key
  {
    proto::InsertRequest req;
    req.set_collection_name("perm_test");
    auto* vec = req.add_vectors();
    vec->set_id(1);
    auto* v = vec->mutable_vector();
    v->set_dimension(4);
    v->add_values(1.0f);
    v->add_values(0.0f);
    v->add_values(0.0f);
    v->add_values(0.0f);
    proto::InsertResponse resp;
    grpc::ClientContext ctx;
    ctx.AddMetadata("authorization", "Bearer ro-key");
    auto status = stub->Insert(&ctx, req, &resp);
    CHECK_FALSE(status.ok());
    CHECK(status.error_code() == grpc::StatusCode::PERMISSION_DENIED);
  }

  auto lines = GetAuditLines();
  REQUIRE(lines.size() >= 2);
  auto& entry = lines.back();
  CHECK(JsonContains(entry, "operation", "Insert"));
  CHECK(JsonContains(entry, "status", "PERMISSION_DENIED"));
  CHECK(JsonContains(entry, "api_key_id", "ro-key"));
}

}  // namespace
