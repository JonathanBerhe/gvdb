// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <cmath>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "core/type_names.h"
#include "rpc/proto_conversions.h"
#include "service/vectordb/vectordb_service.h"
#include "service/vectordb/collection_resolver.h"
#include "storage/segment/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "core/vector.h"
#include "support/temp_dir.h"

using namespace gvdb;

namespace {

proto::VectorWithId MakeVector(uint64_t id, const std::vector<float>& values) {
  proto::VectorWithId v;
  v.set_id(id);
  v.mutable_vector()->set_dimension(static_cast<uint32_t>(values.size()));
  for (float f : values) v.mutable_vector()->add_values(f);
  return v;
}

std::set<uint64_t> ResultIds(const proto::SearchResponse& response) {
  std::set<uint64_t> ids;
  for (const auto& r : response.results()) ids.insert(r.id());
  return ids;
}

// Returns a copy of the named metadata field, which must be present. A copy,
// because callers pass the key as a temporary string.
proto::MetadataValue FieldOf(const proto::Metadata& metadata, const std::string& key) {
  auto it = metadata.fields().find(key);
  REQUIRE_MESSAGE(it != metadata.fields().end(),
                  "missing metadata field: " << key);
  return it->second;
}

}  // namespace

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
  auto result = rpc::FromProto(proto_vec);
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

  auto result = rpc::FromProto(proto_vec);
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_CASE_FIXTURE(ProtoConversionTest, "VectorWithIdConversion") {
  proto::VectorWithId proto_vec;
  proto_vec.set_id(42);
  proto_vec.mutable_vector()->set_dimension(2);
  proto_vec.mutable_vector()->add_values(1.5f);
  proto_vec.mutable_vector()->add_values(2.5f);

  auto result = rpc::FromProto(proto_vec);
  REQUIRE(result.ok());

  CHECK_EQ(result->first, core::MakeVectorId(42));
  CHECK_EQ(result->second.dimension(), 2);
  CHECK(result->second.data()[0] == doctest::Approx(1.5f));
  CHECK(result->second.data()[1] == doctest::Approx(2.5f));
}

TEST_CASE_FIXTURE(ProtoConversionTest, "MetricTypeConversion") {
  auto l2 = rpc::FromProto(proto::CreateCollectionRequest::L2);
  REQUIRE(l2.ok());
  CHECK_EQ(*l2, core::MetricType::L2);

  auto ip = rpc::FromProto(proto::CreateCollectionRequest::INNER_PRODUCT);
  REQUIRE(ip.ok());
  CHECK_EQ(*ip, core::MetricType::INNER_PRODUCT);

  auto cosine = rpc::FromProto(proto::CreateCollectionRequest::COSINE);
  REQUIRE(cosine.ok());
  CHECK_EQ(*cosine, core::MetricType::COSINE);
}

TEST_CASE_FIXTURE(ProtoConversionTest, "IndexTypeConversion") {
  struct Case {
    proto::CreateCollectionRequest::IndexType proto_type;
    core::IndexType core_type;
  };
  const std::vector<Case> cases = {
      {proto::CreateCollectionRequest::FLAT, core::IndexType::FLAT},
      {proto::CreateCollectionRequest::HNSW, core::IndexType::HNSW},
      {proto::CreateCollectionRequest::IVF_FLAT, core::IndexType::IVF_FLAT},
      {proto::CreateCollectionRequest::IVF_PQ, core::IndexType::IVF_PQ},
      {proto::CreateCollectionRequest::IVF_SQ, core::IndexType::IVF_SQ},
      {proto::CreateCollectionRequest::IVF_TURBOQUANT,
       core::IndexType::IVF_TURBOQUANT},
      {proto::CreateCollectionRequest::TURBOQUANT, core::IndexType::TURBOQUANT},
      {proto::CreateCollectionRequest::AUTO, core::IndexType::AUTO},
  };
  // Every proto value is listed, so a new one must be added here too.
  CHECK_EQ(cases.size(), static_cast<size_t>(
      proto::CreateCollectionRequest::IndexType_descriptor()->value_count()));

  for (const auto& c : cases) {
    const std::string name =
        proto::CreateCollectionRequest::IndexType_Name(c.proto_type);
    CAPTURE(name);
    auto result = rpc::FromProto(c.proto_type);
    REQUIRE(result.ok());
    CHECK_EQ(*result, c.core_type);
  }
}

// The coordinator sends the index type to data nodes as a string (ToString)
// and the data node parses it back (IndexTypeFromString).
TEST_CASE_FIXTURE(ProtoConversionTest, "IndexTypeStringRoundTrip") {
  struct Case {
    core::IndexType type;
    std::string name;
  };
  const std::vector<Case> cases = {
      {core::IndexType::FLAT, "FLAT"},
      {core::IndexType::HNSW, "HNSW"},
      {core::IndexType::IVF_FLAT, "IVF_FLAT"},
      {core::IndexType::IVF_PQ, "IVF_PQ"},
      {core::IndexType::IVF_SQ, "IVF_SQ"},
      {core::IndexType::TURBOQUANT, "TURBOQUANT"},
      {core::IndexType::IVF_TURBOQUANT, "IVF_TURBOQUANT"},
      {core::IndexType::AUTO, "AUTO"},
  };

  for (const auto& c : cases) {
    CAPTURE(c.name);
    CHECK_EQ(core::ToString(c.type), c.name);
    auto parsed = core::IndexTypeFromString(core::ToString(c.type));
    REQUIRE(parsed.ok());
    CHECK_EQ(*parsed, c.type);
  }
}

TEST_CASE_FIXTURE(ProtoConversionTest, "CoreToProtoVector") {
  std::vector<float> data = {1.0f, 2.0f, 3.0f, 4.0f};
  core::Vector vec(data);

  proto::Vector proto_vec;
  rpc::ToProto(vec, &proto_vec);

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
  rpc::ToProto(entry, &proto_entry);

  CHECK_EQ(proto_entry.id(), 123);
  CHECK(proto_entry.distance() == doctest::Approx(0.5f));
}

TEST_CASE_FIXTURE(ProtoConversionTest, "StatusConversion") {
  // OK status
  auto ok_status = rpc::ToGrpcStatus(absl::OkStatus());
  CHECK(ok_status.ok());

  // Error statuses
  auto invalid = rpc::ToGrpcStatus(
      absl::InvalidArgumentError("bad input"));
  CHECK_EQ(invalid.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  CHECK_EQ(invalid.error_message(), "bad input");

  auto not_found = rpc::ToGrpcStatus(
      absl::NotFoundError("not found"));
  CHECK_EQ(not_found.error_code(), grpc::StatusCode::NOT_FOUND);

  auto already_exists = rpc::ToGrpcStatus(
      absl::AlreadyExistsError("exists"));
  CHECK_EQ(already_exists.error_code(), grpc::StatusCode::ALREADY_EXISTS);
}

// ============================================================================
// VectorDBService Tests
// ============================================================================

class VectorDBServiceTest {
 public:
  VectorDBServiceTest() {
    // Create index factory
    index_factory_ = std::make_unique<index::IndexFactory>();

    // Create segment manager
    segment_store_ = std::make_shared<storage::SegmentManager>(
        test_dir_.str(), index_factory_.get());

    // Create query executor
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_store_.get());

    // Create service with local resolver
    auto resolver = service::MakeLocalResolver(segment_store_);
    service_ = std::make_unique<service::VectorDBService>(
        segment_store_, query_executor_, std::move(resolver));
  }

  ~VectorDBServiceTest() {
    service_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();
    // test_dir_ is the first member, so it removes the folder last.
  }

  grpc::Status CreateCollection(
      const std::string& name, uint32_t dimension,
      proto::CreateCollectionRequest::MetricType metric =
          proto::CreateCollectionRequest::L2) {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name(name);
    request.set_dimension(dimension);
    request.set_metric(metric);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    return service_->CreateCollection(&context, &request, &response);
  }

  grpc::Status Insert(const std::string& name,
                      const std::vector<proto::VectorWithId>& vectors) {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name(name);
    for (const auto& v : vectors) *request.add_vectors() = v;
    proto::InsertResponse response;
    return service_->Insert(&context, &request, &response);
  }

  grpc::Status Search(const std::string& name, const std::vector<float>& query,
                      uint32_t top_k, proto::SearchResponse* response,
                      const std::string& filter = "",
                      bool return_metadata = false) {
    grpc::ServerContext context;
    proto::SearchRequest request;
    request.set_collection_name(name);
    request.mutable_query_vector()->set_dimension(
        static_cast<uint32_t>(query.size()));
    for (float f : query) request.mutable_query_vector()->add_values(f);
    request.set_top_k(top_k);
    request.set_filter(filter);
    request.set_return_metadata(return_metadata);
    return service_->Search(&context, &request, response);
  }

  // Fetches ids with their metadata; the call must succeed.
  proto::GetResponse GetWithMetadata(const std::string& name,
                                     const std::vector<uint64_t>& ids) {
    grpc::ServerContext context;
    proto::GetRequest request;
    request.set_collection_name(name);
    for (uint64_t id : ids) request.add_ids(id);
    request.set_return_metadata(true);
    proto::GetResponse response;
    auto status = service_->Get(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return response;
  }

  proto::ListCollectionsResponse ListCollections() {
    grpc::ServerContext context;
    proto::ListCollectionsRequest request;
    proto::ListCollectionsResponse response;
    auto status = service_->ListCollections(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return response;
  }

  uint64_t TotalCount(const std::string& name) {
    grpc::ServerContext context;
    proto::ListVectorsRequest request;
    request.set_collection_name(name);
    request.set_limit(1);
    proto::ListVectorsResponse response;
    auto status = service_->ListVectors(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return response.total_count();
  }

  test::ScopedTempDir test_dir_{"gvdb_network_test"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<service::VectorDBService> service_;
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

TEST_CASE_FIXTURE(VectorDBServiceTest, "CreateCollectionRejectsInvalidArguments") {
  SUBCASE("empty name") {
    auto status = CreateCollection("", 128);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("dimension above the maximum of 8192") {
    auto status = CreateCollection("too_wide", 8193);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  // A rejected create leaves nothing behind.
  CHECK_EQ(ListCollections().collections_size(), 0);
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

TEST_CASE_FIXTURE(VectorDBServiceTest, "SearchEmptyCollectionReturnsNoResults") {
  REQUIRE(CreateCollection("empty_collection", 4).ok());

  proto::SearchResponse response;
  auto status = Search("empty_collection", {1, 2, 3, 4}, 5, &response);
  INFO(status.error_message());
  CHECK(status.ok());
  CHECK_EQ(response.results_size(), 0);
}

TEST_CASE_FIXTURE(VectorDBServiceTest, "InsertRejectsInvalidRequests") {
  REQUIRE(CreateCollection("insert_invalid", 128).ok());

  SUBCASE("unknown collection") {
    auto status = Insert("no_such_collection",
                         {MakeVector(1, std::vector<float>(128, 1.0f))});
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
  }

  SUBCASE("dimension 64 into a 128-dim collection") {
    auto status = Insert("insert_invalid",
                         {MakeVector(1, std::vector<float>(64, 1.0f))});
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("dimension 256 into a 128-dim collection") {
    auto status = Insert("insert_invalid",
                         {MakeVector(1, std::vector<float>(256, 1.0f))});
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("vector with no values") {
    auto status = Insert("insert_invalid", {MakeVector(1, {})});
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  // A rejected insert writes nothing.
  CHECK_EQ(TotalCount("insert_invalid"), 0);
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

TEST_CASE_FIXTURE(VectorDBServiceTest, "DropAndRecreateCollection") {
  for (const char* name : {"keep_a", "dropped", "keep_b"}) {
    REQUIRE(CreateCollection(name, 4).ok());
    REQUIRE(Insert(name, {MakeVector(1, {1, 0, 0, 0}),
                          MakeVector(2, {0, 1, 0, 0})}).ok());
  }

  {
    grpc::ServerContext context;
    proto::DropCollectionRequest request;
    request.set_collection_name("dropped");
    proto::DropCollectionResponse response;
    auto status = service_->DropCollection(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
  }

  // Only the dropped collection is gone.
  {
    const auto list = ListCollections();
    std::set<std::string> names;
    for (const auto& c : list.collections()) {
      names.insert(c.collection_name());
    }
    CHECK_EQ(names, std::set<std::string>{"keep_a", "keep_b"});

    proto::SearchResponse response;
    auto status = Search("dropped", {1, 0, 0, 0}, 1, &response);
    CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
  }

  // A survivor still takes writes and answers searches.
  {
    REQUIRE(Insert("keep_b", {MakeVector(3, {0, 0, 1, 0})}).ok());
    proto::SearchResponse response;
    auto status = Search("keep_b", {0, 0, 1, 0}, 5, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.results_size(), 3);
    CHECK_EQ(response.results(0).id(), 3);
    CHECK(response.results(0).distance() == doctest::Approx(0.0f));
  }

  // Recreate the dropped name with another dimension and metric.
  {
    auto status = CreateCollection("dropped", 8,
                                   proto::CreateCollectionRequest::COSINE);
    INFO(status.error_message());
    REQUIRE(status.ok());
  }
  {
    auto list = ListCollections();
    REQUIRE_EQ(list.collections_size(), 3);
    for (const auto& c : list.collections()) {
      CAPTURE(c.collection_name());
      if (c.collection_name() == "dropped") {
        CHECK_EQ(c.dimension(), 8);
        CHECK_EQ(c.metric_type(), "COSINE");
        CHECK_EQ(c.vector_count(), 0);
      } else {
        CHECK_EQ(c.dimension(), 4);
        CHECK_EQ(c.metric_type(), "L2");
      }
    }
  }

  // The new collection takes the new dimension only.
  {
    auto status = Insert("dropped", {MakeVector(1, {1, 0, 0, 0})});
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }
  {
    auto status = Insert("dropped", {MakeVector(1, {1, 0, 0, 0, 0, 0, 0, 0}),
                                     MakeVector(2, {0, 1, 0, 0, 0, 0, 0, 0}),
                                     MakeVector(3, {1, 1, 0, 0, 0, 0, 0, 0})});
    INFO(status.error_message());
    REQUIRE(status.ok());
  }

  // The search uses the new metric: cosine distance is 1 - cos, so a query
  // along id 2 is at distance 0 from it, 1 - 1/sqrt(2) from id 3 and 1 from
  // id 1.
  {
    proto::SearchResponse response;
    auto status = Search("dropped", {0, 2, 0, 0, 0, 0, 0, 0}, 5, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.results_size(), 3);
    CHECK_EQ(response.results(0).id(), 2);
    CHECK_EQ(response.results(1).id(), 3);
    CHECK_EQ(response.results(2).id(), 1);
    CHECK(response.results(0).distance() == doctest::Approx(0.0f));
    CHECK(response.results(1).distance() ==
          doctest::Approx(1.0f - 1.0f / std::sqrt(2.0f)));
    CHECK(response.results(2).distance() == doctest::Approx(1.0f));
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

// Each metadata value type goes in through Insert and comes back from Get with
// the same type and value.
TEST_CASE_FIXTURE(VectorDBServiceTest, "MetadataValueTypesRoundTrip") {
  struct Row {
    uint64_t id;
    std::vector<float> values;
    int64_t count;
    double score;
    bool active;
    std::string label;
  };
  // 2^53 + 1 has no exact double, so it shows the int64 is not carried as a
  // double anywhere on the way.
  constexpr int64_t kBeyondDouble = (int64_t{1} << 53) + 1;
  const std::vector<Row> rows = {
      {1, {1, 0, 0, 0}, -20, -1.5, true, "alpha"},
      {2, {0, 1, 0, 0}, 0, 0.25, false, "beta"},
      {3, {0, 0, 1, 0}, 7, 2.5, true, "gamma"},
      {4, {0, 0, 0, 1}, 8, 1e10, false, "delta"},
      {5, {1, 1, 0, 0}, kBeyondDouble, 3.75, true, "epsilon"},
  };

  REQUIRE(CreateCollection("metadata_types", 4).ok());
  {
    std::vector<proto::VectorWithId> vectors;
    for (const auto& row : rows) {
      auto v = MakeVector(row.id, row.values);
      auto& fields = *v.mutable_metadata()->mutable_fields();
      fields["count"].set_int_value(row.count);
      fields["score"].set_double_value(row.score);
      fields["active"].set_bool_value(row.active);
      fields["label"].set_string_value(row.label);
      vectors.push_back(std::move(v));
    }
    auto status = Insert("metadata_types", vectors);
    INFO(status.error_message());
    REQUIRE(status.ok());
  }

  auto response = GetWithMetadata("metadata_types", {1, 2, 3, 4, 5});
  REQUIRE_EQ(response.vectors_size(), static_cast<int>(rows.size()));
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto& row = rows[i];
    const auto& vec = response.vectors(static_cast<int>(i));
    CAPTURE(row.id);
    REQUIRE_EQ(vec.id(), row.id);
    CHECK_EQ(vec.metadata().fields().size(), 4);

    const auto& count = FieldOf(vec.metadata(), "count");
    CHECK_EQ(count.value_case(), proto::MetadataValue::kIntValue);
    CHECK_EQ(count.int_value(), row.count);

    const auto& score = FieldOf(vec.metadata(), "score");
    CHECK_EQ(score.value_case(), proto::MetadataValue::kDoubleValue);
    CHECK_EQ(score.double_value(), row.score);

    const auto& active = FieldOf(vec.metadata(), "active");
    CHECK_EQ(active.value_case(), proto::MetadataValue::kBoolValue);
    CHECK_EQ(active.bool_value(), row.active);

    const auto& label = FieldOf(vec.metadata(), "label");
    CHECK_EQ(label.value_case(), proto::MetadataValue::kStringValue);
    CHECK_EQ(label.string_value(), row.label);
  }

  // A filter on the int64 field selects the rows by their int value.
  {
    proto::SearchResponse search;
    auto status = Search("metadata_types", {1, 0, 0, 0}, 10, &search,
                         "count >= 7", /*return_metadata=*/true);
    INFO(status.error_message());
    REQUIRE(status.ok());
    CHECK_EQ(ResultIds(search), std::set<uint64_t>{3, 4, 5});
    for (const auto& r : search.results()) {
      CAPTURE(r.id());
      const auto& count = FieldOf(r.metadata(), "count");
      CHECK_EQ(count.value_case(), proto::MetadataValue::kIntValue);
      CHECK_GE(count.int_value(), 7);
    }
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

TEST_CASE_FIXTURE(VectorDBServiceTest, "UpdateMetadataMergeOntoVectorWithoutMetadata") {
  REQUIRE(CreateCollection("merge_target", 4).ok());
  REQUIRE(Insert("merge_target", {MakeVector(1, {1, 0, 0, 0}),
                                  MakeVector(2, {0, 1, 0, 0})}).ok());

  {
    grpc::ServerContext context;
    proto::UpdateMetadataRequest request;
    request.set_collection_name("merge_target");
    request.set_id(1);
    request.set_merge(true);
    auto& fields = *request.mutable_metadata()->mutable_fields();
    fields["price"].set_double_value(80.5);
    fields["in_stock"].set_bool_value(true);
    fields["brand"].set_string_value("Nike");

    proto::UpdateMetadataResponse response;
    auto status = service_->UpdateMetadata(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    CHECK(response.updated());
  }

  auto response = GetWithMetadata("merge_target", {1, 2});
  REQUIRE_EQ(response.vectors_size(), 2);

  // The merged vector carries exactly the merged fields.
  REQUIRE_EQ(response.vectors(0).id(), 1);
  const auto& merged = response.vectors(0).metadata();
  CHECK_EQ(merged.fields().size(), 3);
  const auto& price = FieldOf(merged, "price");
  CHECK_EQ(price.value_case(), proto::MetadataValue::kDoubleValue);
  CHECK_EQ(price.double_value(), 80.5);
  const auto& in_stock = FieldOf(merged, "in_stock");
  CHECK_EQ(in_stock.value_case(), proto::MetadataValue::kBoolValue);
  CHECK(in_stock.bool_value());
  const auto& brand = FieldOf(merged, "brand");
  CHECK_EQ(brand.value_case(), proto::MetadataValue::kStringValue);
  CHECK_EQ(brand.string_value(), "Nike");

  // The other vector is untouched.
  REQUIRE_EQ(response.vectors(1).id(), 2);
  CHECK(response.vectors(1).metadata().fields().empty());
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

// ============================================================================
// Oversized-batch Insert Tests (batch larger than the segment size cap)
// ============================================================================

// Builds the same stack as VectorDBServiceTest but with a tiny segment size
// cap so a normal-looking insert batch exceeds one segment's capacity.
class SmallSegmentServiceTest {
 public:
  static constexpr size_t kMaxSegmentBytes = 1200;  // fits 4 vectors of dim 64

  SmallSegmentServiceTest() {
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>(
        test_dir_.str(), index_factory_.get(), kMaxSegmentBytes);
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_store_.get());
    auto resolver = service::MakeLocalResolver(segment_store_);
    service_ = std::make_unique<service::VectorDBService>(
        segment_store_, query_executor_, std::move(resolver));
  }

  ~SmallSegmentServiceTest() {
    service_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();
    // test_dir_ is the first member, so it removes the folder last.
  }

 protected:
  // Create a FLAT collection and insert `count` vectors of dim 64 in one
  // batch. With kMaxSegmentBytes = 1200 the batch spans several segments, so
  // vector i (id i+1, all components equal to i) lands in a rotated segment
  // for larger i. Asserts the insert succeeds.
  void CreateAndInsert(const std::string& collection, int count) {
    {
      grpc::ServerContext context;
      proto::CreateCollectionRequest request;
      request.set_collection_name(collection);
      request.set_dimension(64);
      request.set_metric(proto::CreateCollectionRequest::L2);
      request.set_index_type(proto::CreateCollectionRequest::FLAT);
      proto::CreateCollectionResponse response;
      REQUIRE(service_->CreateCollection(&context, &request, &response).ok());
    }
    {
      grpc::ServerContext context;
      proto::InsertRequest request;
      request.set_collection_name(collection);
      for (int i = 0; i < count; ++i) {
        auto* vec = request.add_vectors();
        vec->set_id(i + 1);
        vec->mutable_vector()->set_dimension(64);
        for (int j = 0; j < 64; ++j) {
          vec->mutable_vector()->add_values(static_cast<float>(i));
        }
      }
      proto::InsertResponse response;
      auto status = service_->Insert(&context, &request, &response);
      INFO(status.error_message());
      REQUIRE(status.ok());
      CHECK_EQ(response.inserted_count(), count);
    }
    // The batch cannot fit one segment, so the split must have used several.
    CHECK_GT(segment_store_->GetSegmentCount(), 1);
  }

  test::ScopedTempDir test_dir_{"gvdb_network_small_segment_test"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::SegmentManager> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<service::VectorDBService> service_;
};

TEST_CASE_FIXTURE(SmallSegmentServiceTest,
                  "InsertBatchLargerThanSegmentSucceedsViaSplit") {
  constexpr int kDim = 64;   // 256 bytes per vector
  constexpr int kCount = 20; // 5120 bytes, > kMaxSegmentBytes

  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("small_seg");
    request.set_dimension(kDim);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    REQUIRE(service_->CreateCollection(&context, &request, &response).ok());
  }

  {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name("small_seg");
    for (int i = 0; i < kCount; ++i) {
      auto* vec = request.add_vectors();
      vec->set_id(i + 1);
      vec->mutable_vector()->set_dimension(kDim);
      for (int j = 0; j < kDim; ++j) {
        vec->mutable_vector()->add_values(static_cast<float>(i));
      }
    }
    proto::InsertResponse response;
    auto status = service_->Insert(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    CHECK_EQ(response.inserted_count(), kCount);
  }

  // The batch cannot fit one segment, so the split must have used several.
  CHECK_GT(segment_store_->GetSegmentCount(), 1);

  // Vectors from a rotated (non-initial) segment must be searchable. Vector
  // i has every component equal to i, so the exact match for an all-15 query
  // is id 16, which the split placed well past the first segment.
  {
    grpc::ServerContext context;
    proto::SearchRequest request;
    request.set_collection_name("small_seg");
    request.set_top_k(1);
    auto* query = request.mutable_query_vector();
    query->set_dimension(kDim);
    for (int j = 0; j < kDim; ++j) query->add_values(15.0f);
    proto::SearchResponse response;
    auto status = service_->Search(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_GE(response.results().size(), 1);
    CHECK_EQ(response.results(0).id(), 16);
    CHECK(response.results(0).distance() ==
          doctest::Approx(0.0f).epsilon(0.01f));
  }
}

TEST_CASE_FIXTURE(SmallSegmentServiceTest,
                  "InsertSingleVectorLargerThanSegmentFailsFast") {
  // One vector of dim 512 is 2048 bytes, more than any segment can hold.
  constexpr int kDim = 512;

  {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name("too_big");
    request.set_dimension(kDim);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    REQUIRE(service_->CreateCollection(&context, &request, &response).ok());
  }

  grpc::ServerContext context;
  proto::InsertRequest request;
  request.set_collection_name("too_big");
  auto* vec = request.add_vectors();
  vec->set_id(1);
  vec->mutable_vector()->set_dimension(kDim);
  for (int j = 0; j < kDim; ++j) {
    vec->mutable_vector()->add_values(1.0f);
  }
  proto::InsertResponse response;
  auto status = service_->Insert(&context, &request, &response);
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  CHECK(std::string(status.error_message()).find("exceeds") !=
        std::string::npos);
}

TEST_CASE_FIXTURE(SmallSegmentServiceTest,
                  "GetAcrossRotatedSegmentsFindsAll") {
  constexpr int kCount = 20;
  CreateAndInsert("rotated", kCount);

  // Ask for every id in a single request. Vectors past the first segment must
  // come back too, not land in not_found_ids.
  grpc::ServerContext context;
  proto::GetRequest request;
  request.set_collection_name("rotated");
  for (int id = 1; id <= kCount; ++id) request.add_ids(id);

  proto::GetResponse response;
  auto status = service_->Get(&context, &request, &response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  CHECK_EQ(response.vectors().size(), kCount);
  CHECK_EQ(response.not_found_ids().size(), 0);

  // Results follow the requested id order; vector for id k has every
  // component equal to k - 1.
  for (int i = 0; i < kCount; ++i) {
    CHECK_EQ(response.vectors(i).id(), i + 1);
    REQUIRE(response.vectors(i).vector().values_size() > 0);
    CHECK(response.vectors(i).vector().values(0) ==
          doctest::Approx(static_cast<float>(i)).epsilon(0.01f));
  }
}

TEST_CASE_FIXTURE(SmallSegmentServiceTest,
                  "GetAcrossRotatedSegmentsPartitionsMissing") {
  CreateAndInsert("rotated", 20);

  // Mix ids that live in rotated segments with ids that were never inserted.
  grpc::ServerContext context;
  proto::GetRequest request;
  request.set_collection_name("rotated");
  request.add_ids(3);    // exists, first segment
  request.add_ids(99);   // missing
  request.add_ids(17);   // exists, a rotated segment
  request.add_ids(20);   // exists, the newest segment
  request.add_ids(21);   // missing

  proto::GetResponse response;
  auto status = service_->Get(&context, &request, &response);
  REQUIRE(status.ok());

  REQUIRE_EQ(response.vectors().size(), 3);
  CHECK_EQ(response.vectors(0).id(), 3);
  CHECK_EQ(response.vectors(1).id(), 17);
  CHECK_EQ(response.vectors(2).id(), 20);

  REQUIRE_EQ(response.not_found_ids().size(), 2);
  CHECK_EQ(response.not_found_ids(0), 99);
  CHECK_EQ(response.not_found_ids(1), 21);
}

TEST_CASE_FIXTURE(SmallSegmentServiceTest,
                  "ListVectorsAcrossRotatedSegments") {
  constexpr int kCount = 20;
  CreateAndInsert("rotated", kCount);

  grpc::ServerContext context;
  proto::ListVectorsRequest request;
  request.set_collection_name("rotated");
  request.set_limit(kCount);
  request.set_offset(0);

  proto::ListVectorsResponse response;
  auto status = service_->ListVectors(&context, &request, &response);
  REQUIRE(status.ok());
  CHECK_EQ(response.total_count(), kCount);
  CHECK_FALSE(response.has_more());
  CHECK_EQ(response.vectors().size(), kCount);
}

TEST_CASE_FIXTURE(SmallSegmentServiceTest,
                  "DeleteAcrossRotatedSegments") {
  CreateAndInsert("rotated", 20);

  // Id 18 lives in a rotated segment; the split places it well past the
  // first. Deleting it must find that segment and remove exactly one vector.
  {
    grpc::ServerContext context;
    proto::DeleteRequest request;
    request.set_collection_name("rotated");
    request.add_ids(18);
    proto::DeleteResponse response;
    auto status = service_->Delete(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    CHECK_EQ(response.deleted_count(), 1);
    CHECK_EQ(response.not_found_ids().size(), 0);
  }

  // The delete must be visible: id 18 gone, a neighbour in another segment
  // still present.
  {
    grpc::ServerContext context;
    proto::GetRequest request;
    request.set_collection_name("rotated");
    request.add_ids(18);
    request.add_ids(17);
    proto::GetResponse response;
    auto status = service_->Get(&context, &request, &response);
    REQUIRE(status.ok());
    REQUIRE_EQ(response.vectors().size(), 1);
    CHECK_EQ(response.vectors(0).id(), 17);
    REQUIRE_EQ(response.not_found_ids().size(), 1);
    CHECK_EQ(response.not_found_ids(0), 18);
  }
}
