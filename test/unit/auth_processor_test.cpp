#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <filesystem>
#include <memory>

#include "network/auth_processor.h"
#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "auth/rbac.h"
#include "utils/config.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "vectordb.grpc.pb.h"

using namespace gvdb;

// ============================================================================
// Test fixture: real gRPC server with RBAC interceptor
// Roles:
//   admin-key        → admin (all ops, all collections)
//   legacy-admin-key → admin (backward compat from api_keys)
//   rw-key           → readwrite on ["test_collection", "second_collection"]
//   ro-key           → readonly on ["*"] (all collections)
//   ca-key           → collection_admin on ["test_collection"]
// ============================================================================

class AuthIntegrationTest {
 public:
  AuthIntegrationTest() {
    std::filesystem::remove_all("/tmp/gvdb-auth-test");
    std::filesystem::create_directories("/tmp/gvdb-auth-test");

    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_manager_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-auth-test", index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_manager_.get());

    utils::AuthConfig auth_config;
    auth_config.enabled = true;
    auth_config.api_keys = {"legacy-admin-key"};
    auth_config.roles = {
        {"admin-key", "admin", {}},
        {"rw-key", "readwrite", {"test_collection", "second_collection"}},
        {"ro-key", "readonly", {"*"}},
        {"ca-key", "collection_admin", {"test_collection"}},
    };

    auto rbac_result = auth::RbacStore::Create(auth_config);
    REQUIRE(rbac_result.ok());
    rbac_store_ = std::move(*rbac_result);

    auto resolver = network::MakeLocalResolver(segment_manager_);
    service_ = std::make_unique<network::VectorDBService>(
        segment_manager_, query_executor_, std::move(resolver), rbac_store_);

    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> creators;
    creators.push_back(
        std::make_unique<network::ApiKeyAuthInterceptorFactory>(rbac_store_));

    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(service_.get());
    builder.experimental().SetInterceptorCreators(std::move(creators));
    server_ = builder.BuildAndStart();
    REQUIRE(server_ != nullptr);
    address_ = "localhost:" + std::to_string(port);
  }

  ~AuthIntegrationTest() {
    if (server_) { server_->Shutdown(); server_->Wait(); }
    std::filesystem::remove_all("/tmp/gvdb-auth-test");
  }

  std::unique_ptr<proto::VectorDBService::Stub> MakeStub() {
    auto channel = grpc::CreateChannel(address_, grpc::InsecureChannelCredentials());
    return proto::VectorDBService::NewStub(channel);
  }

  // Create a collection as admin. Returns collection ID.
  uint32_t CreateCollectionAsAdmin(const std::string& name, uint32_t dim = 4) {
    auto stub = MakeStub();
    proto::CreateCollectionRequest req;
    req.set_collection_name(name);
    req.set_dimension(dim);
    req.set_metric(proto::CreateCollectionRequest::L2);
    req.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse resp;
    grpc::ClientContext ctx;
    ctx.AddMetadata("authorization", "Bearer admin-key");
    auto status = stub->CreateCollection(&ctx, req, &resp);
    REQUIRE(status.ok());
    return resp.collection_id();
  }

  // Insert vectors as admin into a collection.
  void InsertAsAdmin(const std::string& collection, int count = 10) {
    auto stub = MakeStub();
    proto::InsertRequest req;
    req.set_collection_name(collection);
    for (int i = 0; i < count; ++i) {
      auto* vec = req.add_vectors();
      vec->set_id(i + 1);
      auto* v = vec->mutable_vector();
      v->set_dimension(4);
      v->add_values(static_cast<float>(i)); v->add_values(0.0f);
      v->add_values(0.0f); v->add_values(1.0f);
    }
    proto::InsertResponse resp;
    grpc::ClientContext ctx;
    ctx.AddMetadata("authorization", "Bearer admin-key");
    auto status = stub->Insert(&ctx, req, &resp);
    REQUIRE(status.ok());
  }

  // Build a 4D query vector proto
  static proto::Vector MakeQueryVector() {
    proto::Vector v;
    v.set_dimension(4);
    v.add_values(1.0f); v.add_values(0.0f); v.add_values(0.0f); v.add_values(0.0f);
    return v;
  }

  // Build a single VectorWithId proto
  static proto::VectorWithId MakeVectorWithId(uint64_t id) {
    proto::VectorWithId vwi;
    vwi.set_id(id);
    auto* v = vwi.mutable_vector();
    v->set_dimension(4);
    v->add_values(1.0f); v->add_values(0.0f); v->add_values(0.0f); v->add_values(0.0f);
    return vwi;
  }

  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::SegmentManager> segment_manager_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::shared_ptr<auth::RbacStore> rbac_store_;
  std::unique_ptr<network::VectorDBService> service_;
  std::unique_ptr<grpc::Server> server_;
  std::string address_;
};

// ============================================================================
// Authentication: public endpoints skip auth
// ============================================================================

TEST_CASE_FIXTURE(AuthIntegrationTest, "HealthCheckSkipsAuth") {
  auto stub = MakeStub();
  proto::HealthCheckRequest req;
  proto::HealthCheckResponse resp;
  grpc::ClientContext ctx;

  auto status = stub->HealthCheck(&ctx, req, &resp);
  INFO(status.error_message());
  CHECK(status.ok());
  CHECK_EQ(resp.status(), proto::HealthCheckResponse::SERVING);
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "GetStatsSkipsAuth") {
  auto stub = MakeStub();
  proto::GetStatsRequest req;
  proto::GetStatsResponse resp;
  grpc::ClientContext ctx;

  auto status = stub->GetStats(&ctx, req, &resp);
  INFO(status.error_message());
  CHECK(status.ok());
}

// ============================================================================
// Authentication: rejection for missing/invalid keys
// ============================================================================

TEST_CASE_FIXTURE(AuthIntegrationTest, "NoKeyRejected") {
  auto stub = MakeStub();
  proto::ListCollectionsRequest req;
  proto::ListCollectionsResponse resp;
  grpc::ClientContext ctx;

  auto status = stub->ListCollections(&ctx, req, &resp);
  CHECK_FALSE(status.ok());
  CHECK((status.error_code() == grpc::StatusCode::UNAUTHENTICATED ||
         status.error_code() == grpc::StatusCode::CANCELLED));
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "WrongKeyRejected") {
  auto stub = MakeStub();
  proto::ListCollectionsRequest req;
  proto::ListCollectionsResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer nonexistent-key");

  auto status = stub->ListCollections(&ctx, req, &resp);
  CHECK_FALSE(status.ok());
  CHECK((status.error_code() == grpc::StatusCode::UNAUTHENTICATED ||
         status.error_code() == grpc::StatusCode::CANCELLED));
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "BadFormatRejected") {
  auto stub = MakeStub();
  proto::ListCollectionsRequest req;
  proto::ListCollectionsResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Basic abc123");

  auto status = stub->ListCollections(&ctx, req, &resp);
  CHECK_FALSE(status.ok());
  CHECK((status.error_code() == grpc::StatusCode::UNAUTHENTICATED ||
         status.error_code() == grpc::StatusCode::CANCELLED));
}

// ============================================================================
// Admin role: full access
// ============================================================================

TEST_CASE_FIXTURE(AuthIntegrationTest, "AdminCanCreateAndDropCollection") {
  auto stub = MakeStub();
  // Create
  {
    proto::CreateCollectionRequest req;
    req.set_collection_name("admin_coll");
    req.set_dimension(4);
    req.set_metric(proto::CreateCollectionRequest::L2);
    req.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse resp;
    grpc::ClientContext ctx;
    ctx.AddMetadata("authorization", "Bearer admin-key");
    auto status = stub->CreateCollection(&ctx, req, &resp);
    CHECK(status.ok());
  }
  // Drop
  {
    proto::DropCollectionRequest req;
    req.set_collection_name("admin_coll");
    proto::DropCollectionResponse resp;
    grpc::ClientContext ctx;
    ctx.AddMetadata("authorization", "Bearer admin-key");
    auto status = stub->DropCollection(&ctx, req, &resp);
    CHECK(status.ok());
  }
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "LegacyApiKeyIsAdmin") {
  auto stub = MakeStub();
  proto::CreateCollectionRequest req;
  req.set_collection_name("legacy_test");
  req.set_dimension(4);
  req.set_metric(proto::CreateCollectionRequest::L2);
  req.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer legacy-admin-key");

  auto status = stub->CreateCollection(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "AdminCanAccessAnyCollection") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::SearchRequest req;
  req.set_collection_name("test_collection");
  req.set_top_k(5);
  *req.mutable_query_vector() = MakeQueryVector();
  proto::SearchResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer admin-key");

  auto status = stub->Search(&ctx, req, &resp);
  CHECK(status.ok());
}

// ============================================================================
// Readwrite role: insert/search/delete/upsert/update but not create/drop
// ============================================================================

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadwriteCannotCreateCollection") {
  auto stub = MakeStub();
  proto::CreateCollectionRequest req;
  req.set_collection_name("rw_blocked");
  req.set_dimension(4);
  req.set_metric(proto::CreateCollectionRequest::L2);
  req.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer rw-key");

  auto status = stub->CreateCollection(&ctx, req, &resp);
  CHECK_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadwriteCannotDropCollection") {
  CreateCollectionAsAdmin("test_collection");

  auto stub = MakeStub();
  proto::DropCollectionRequest req;
  req.set_collection_name("test_collection");
  proto::DropCollectionResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer rw-key");

  auto status = stub->DropCollection(&ctx, req, &resp);
  CHECK_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadwriteCanInsertOnAllowedCollection") {
  CreateCollectionAsAdmin("test_collection");

  auto stub = MakeStub();
  proto::InsertRequest req;
  req.set_collection_name("test_collection");
  *req.add_vectors() = MakeVectorWithId(100);
  proto::InsertResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer rw-key");

  auto status = stub->Insert(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadwriteCanUpsertOnAllowedCollection") {
  CreateCollectionAsAdmin("test_collection");

  auto stub = MakeStub();
  proto::UpsertRequest req;
  req.set_collection_name("test_collection");
  *req.add_vectors() = MakeVectorWithId(200);
  proto::UpsertResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer rw-key");

  auto status = stub->Upsert(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadwriteCanDeleteOnAllowedCollection") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::DeleteRequest req;
  req.set_collection_name("test_collection");
  req.add_ids(1);
  proto::DeleteResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer rw-key");

  auto status = stub->Delete(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadwriteCanSearchOnAllowedCollection") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::SearchRequest req;
  req.set_collection_name("test_collection");
  req.set_top_k(3);
  *req.mutable_query_vector() = MakeQueryVector();
  proto::SearchResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer rw-key");

  auto status = stub->Search(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadwriteCanGetOnAllowedCollection") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::GetRequest req;
  req.set_collection_name("test_collection");
  req.add_ids(1);
  proto::GetResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer rw-key");

  auto status = stub->Get(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadwriteDeniedOnWrongCollection") {
  CreateCollectionAsAdmin("forbidden_coll");

  auto stub = MakeStub();
  proto::InsertRequest req;
  req.set_collection_name("forbidden_coll");
  *req.add_vectors() = MakeVectorWithId(1);
  proto::InsertResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer rw-key");

  auto status = stub->Insert(&ctx, req, &resp);
  CHECK_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadwriteCanAccessSecondAllowedCollection") {
  CreateCollectionAsAdmin("second_collection");

  auto stub = MakeStub();
  proto::InsertRequest req;
  req.set_collection_name("second_collection");
  *req.add_vectors() = MakeVectorWithId(1);
  proto::InsertResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer rw-key");

  auto status = stub->Insert(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadwriteCanListCollections") {
  auto stub = MakeStub();
  proto::ListCollectionsRequest req;
  proto::ListCollectionsResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer rw-key");

  auto status = stub->ListCollections(&ctx, req, &resp);
  CHECK(status.ok());
}

// ============================================================================
// Readonly role: search/get/range_search/hybrid_search/list only
// ============================================================================

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadonlyCannotInsert") {
  CreateCollectionAsAdmin("test_collection");

  auto stub = MakeStub();
  proto::InsertRequest req;
  req.set_collection_name("test_collection");
  *req.add_vectors() = MakeVectorWithId(1);
  proto::InsertResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ro-key");

  auto status = stub->Insert(&ctx, req, &resp);
  CHECK_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadonlyCannotUpsert") {
  CreateCollectionAsAdmin("test_collection");

  auto stub = MakeStub();
  proto::UpsertRequest req;
  req.set_collection_name("test_collection");
  *req.add_vectors() = MakeVectorWithId(1);
  proto::UpsertResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ro-key");

  auto status = stub->Upsert(&ctx, req, &resp);
  CHECK_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadonlyCannotDelete") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::DeleteRequest req;
  req.set_collection_name("test_collection");
  req.add_ids(1);
  proto::DeleteResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ro-key");

  auto status = stub->Delete(&ctx, req, &resp);
  CHECK_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadonlyCannotUpdateMetadata") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::UpdateMetadataRequest req;
  req.set_collection_name("test_collection");
  req.set_id(1);
  proto::UpdateMetadataResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ro-key");

  auto status = stub->UpdateMetadata(&ctx, req, &resp);
  CHECK_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadonlyCanSearch") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::SearchRequest req;
  req.set_collection_name("test_collection");
  req.set_top_k(3);
  *req.mutable_query_vector() = MakeQueryVector();
  proto::SearchResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ro-key");

  auto status = stub->Search(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadonlyCanGet") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::GetRequest req;
  req.set_collection_name("test_collection");
  req.add_ids(1);
  proto::GetResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ro-key");

  auto status = stub->Get(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadonlyCanRangeSearch") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::RangeSearchRequest req;
  req.set_collection_name("test_collection");
  req.set_radius(100.0f);
  *req.mutable_query_vector() = MakeQueryVector();
  proto::RangeSearchResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ro-key");

  auto status = stub->RangeSearch(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadonlyCanHybridSearch") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::HybridSearchRequest req;
  req.set_collection_name("test_collection");
  req.set_top_k(3);
  *req.mutable_query_vector() = MakeQueryVector();
  proto::HybridSearchResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ro-key");

  auto status = stub->HybridSearch(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadonlyCanListVectors") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::ListVectorsRequest req;
  req.set_collection_name("test_collection");
  req.set_limit(10);
  proto::ListVectorsResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ro-key");

  auto status = stub->ListVectors(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "ReadonlyCanListCollections") {
  auto stub = MakeStub();
  proto::ListCollectionsRequest req;
  proto::ListCollectionsResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ro-key");

  auto status = stub->ListCollections(&ctx, req, &resp);
  CHECK(status.ok());
}

// ============================================================================
// Collection admin role: all ops on allowed collection, no create/drop
// ============================================================================

TEST_CASE_FIXTURE(AuthIntegrationTest, "CollectionAdminCannotCreateCollection") {
  auto stub = MakeStub();
  proto::CreateCollectionRequest req;
  req.set_collection_name("ca_blocked");
  req.set_dimension(4);
  req.set_metric(proto::CreateCollectionRequest::L2);
  req.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ca-key");

  auto status = stub->CreateCollection(&ctx, req, &resp);
  CHECK_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "CollectionAdminCannotDropCollection") {
  CreateCollectionAsAdmin("test_collection");

  auto stub = MakeStub();
  proto::DropCollectionRequest req;
  req.set_collection_name("test_collection");
  proto::DropCollectionResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ca-key");

  auto status = stub->DropCollection(&ctx, req, &resp);
  CHECK_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "CollectionAdminCanInsertOnOwnCollection") {
  CreateCollectionAsAdmin("test_collection");

  auto stub = MakeStub();
  proto::InsertRequest req;
  req.set_collection_name("test_collection");
  *req.add_vectors() = MakeVectorWithId(1);
  proto::InsertResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ca-key");

  auto status = stub->Insert(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "CollectionAdminCanDeleteOnOwnCollection") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::DeleteRequest req;
  req.set_collection_name("test_collection");
  req.add_ids(1);
  proto::DeleteResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ca-key");

  auto status = stub->Delete(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "CollectionAdminCanSearchOnOwnCollection") {
  CreateCollectionAsAdmin("test_collection");
  InsertAsAdmin("test_collection", 5);

  auto stub = MakeStub();
  proto::SearchRequest req;
  req.set_collection_name("test_collection");
  req.set_top_k(3);
  *req.mutable_query_vector() = MakeQueryVector();
  proto::SearchResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ca-key");

  auto status = stub->Search(&ctx, req, &resp);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(AuthIntegrationTest, "CollectionAdminDeniedOnOtherCollection") {
  CreateCollectionAsAdmin("other_coll");

  auto stub = MakeStub();
  proto::InsertRequest req;
  req.set_collection_name("other_coll");
  *req.add_vectors() = MakeVectorWithId(1);
  proto::InsertResponse resp;
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer ca-key");

  auto status = stub->Insert(&ctx, req, &resp);
  CHECK_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}
