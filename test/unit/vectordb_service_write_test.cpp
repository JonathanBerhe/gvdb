// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "service/vectordb_service.h"
#include "service/collection_resolver.h"
#include "storage/segment/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "support/grpc_server.h"
#include "support/temp_dir.h"
#include "support/wait.h"
#include "vectordb.grpc.pb.h"

using namespace gvdb;

// ============================================================================
// Helpers
// ============================================================================

namespace {

constexpr uint32_t kDim = 4;

// Builds a VectorWithId. An empty name means no metadata; a zero ttl means
// the vector never expires.
proto::VectorWithId MakeVector(uint64_t id, const std::vector<float>& values,
                               const std::string& name = "",
                               uint32_t ttl_seconds = 0) {
  proto::VectorWithId v;
  v.set_id(id);
  v.mutable_vector()->set_dimension(static_cast<uint32_t>(values.size()));
  for (float f : values) v.mutable_vector()->add_values(f);
  if (!name.empty()) {
    (*v.mutable_metadata()->mutable_fields())["name"].set_string_value(name);
  }
  if (ttl_seconds > 0) v.set_ttl_seconds(ttl_seconds);
  return v;
}

proto::Vector MakeQuery(const std::vector<float>& values) {
  proto::Vector q;
  q.set_dimension(static_cast<uint32_t>(values.size()));
  for (float f : values) q.add_values(f);
  return q;
}

// Returns the "name" metadata field of a fetched vector, or nullopt when the
// vector carries no such field.
std::optional<std::string> NameOf(const proto::VectorWithId& v) {
  if (!v.has_metadata()) return std::nullopt;
  auto it = v.metadata().fields().find("name");
  if (it == v.metadata().fields().end()) return std::nullopt;
  return it->second.string_value();
}

void CheckValues(const proto::Vector& actual, const std::vector<float>& expected) {
  REQUIRE_EQ(static_cast<size_t>(actual.values_size()), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    CHECK(actual.values(static_cast<int>(i)) == doctest::Approx(expected[i]));
  }
}

// Result ids of a Search or RangeSearch response.
template <typename SearchResponse>
std::set<uint64_t> IdsOf(const SearchResponse& response) {
  std::set<uint64_t> ids;
  for (const auto& r : response.results()) ids.insert(r.id());
  return ids;
}

std::set<uint64_t> IdsOf(const proto::GetResponse& response) {
  std::set<uint64_t> ids;
  for (const auto& v : response.vectors()) ids.insert(v.id());
  return ids;
}

std::set<uint64_t> NotFoundOf(const proto::GetResponse& response) {
  return {response.not_found_ids().begin(), response.not_found_ids().end()};
}

int64_t EpochMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

}  // namespace

// ============================================================================
// Test fixture: VectorDBService in local (single-node) mode
// ============================================================================

class ServiceWriteTest {
 public:
  ServiceWriteTest() {
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>(
        test_dir_.str(), index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_store_.get());

    auto resolver = service::MakeLocalResolver(segment_store_);
    service_ = std::make_unique<service::VectorDBService>(
        segment_store_, query_executor_, std::move(resolver));
  }

  ~ServiceWriteTest() {
    service_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();
    // test_dir_ is the first member, so it removes the folder last.
  }

  void CreateCollection(const std::string& name, uint32_t dim = kDim) {
    grpc::ServerContext ctx;
    proto::CreateCollectionRequest req;
    req.set_collection_name(name);
    req.set_dimension(dim);
    req.set_metric(proto::CreateCollectionRequest::L2);
    req.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse resp;
    auto status = service_->CreateCollection(&ctx, &req, &resp);
    INFO(status.error_message());
    REQUIRE(status.ok());
  }

  grpc::Status Insert(const std::string& name,
                      const std::vector<proto::VectorWithId>& vectors,
                      proto::InsertResponse* resp) {
    grpc::ServerContext ctx;
    proto::InsertRequest req;
    req.set_collection_name(name);
    for (const auto& v : vectors) *req.add_vectors() = v;
    return service_->Insert(&ctx, &req, resp);
  }

  void InsertOk(const std::string& name,
                const std::vector<proto::VectorWithId>& vectors) {
    proto::InsertResponse resp;
    auto status = Insert(name, vectors, &resp);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(resp.inserted_count(), vectors.size());
  }

  grpc::Status Upsert(const std::string& name,
                      const std::vector<proto::VectorWithId>& vectors,
                      proto::UpsertResponse* resp) {
    grpc::ServerContext ctx;
    proto::UpsertRequest req;
    req.set_collection_name(name);
    for (const auto& v : vectors) *req.add_vectors() = v;
    return service_->Upsert(&ctx, &req, resp);
  }

  proto::GetResponse Get(const std::string& name,
                         const std::vector<uint64_t>& ids) {
    grpc::ServerContext ctx;
    proto::GetRequest req;
    req.set_collection_name(name);
    for (uint64_t id : ids) req.add_ids(id);
    req.set_return_metadata(true);
    proto::GetResponse resp;
    auto status = service_->Get(&ctx, &req, &resp);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return resp;
  }

  // Fetches one id that must exist.
  proto::VectorWithId GetOne(const std::string& name, uint64_t id) {
    auto resp = Get(name, {id});
    REQUIRE_EQ(resp.vectors_size(), 1);
    REQUIRE_EQ(resp.vectors(0).id(), id);
    return resp.vectors(0);
  }

  proto::SearchResponse Search(const std::string& name,
                               const std::vector<float>& query,
                               uint32_t top_k,
                               const std::string& filter = "") {
    grpc::ServerContext ctx;
    proto::SearchRequest req;
    req.set_collection_name(name);
    *req.mutable_query_vector() = MakeQuery(query);
    req.set_top_k(top_k);
    req.set_filter(filter);
    proto::SearchResponse resp;
    auto status = service_->Search(&ctx, &req, &resp);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return resp;
  }

  // RangeSearch with metadata in the results.
  proto::RangeSearchResponse RangeSearch(const std::string& name,
                                         const std::vector<float>& query,
                                         float radius,
                                         const std::string& filter = "") {
    grpc::ServerContext ctx;
    proto::RangeSearchRequest req;
    req.set_collection_name(name);
    *req.mutable_query_vector() = MakeQuery(query);
    req.set_radius(radius);
    req.set_filter(filter);
    req.set_return_metadata(true);
    proto::RangeSearchResponse resp;
    auto status = service_->RangeSearch(&ctx, &req, &resp);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return resp;
  }

  uint64_t TotalCount(const std::string& name) {
    grpc::ServerContext ctx;
    proto::ListVectorsRequest req;
    req.set_collection_name(name);
    req.set_limit(1);
    proto::ListVectorsResponse resp;
    auto status = service_->ListVectors(&ctx, &req, &resp);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return resp.total_count();
  }

  test::ScopedTempDir test_dir_{"gvdb_service_write_test"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<service::VectorDBService> service_;
};

// Same service, also served by a real in-process gRPC server on an
// ephemeral port, for the client-streaming RPC.
class ServiceWriteServerTest : public ServiceWriteTest {
 public:
  ServiceWriteServerTest() {
    REQUIRE(server_.ok());
    stub_ = proto::VectorDBService::NewStub(server_.NewChannel());
  }

  // Drops the stub, then shuts the server down before the base class
  // destroys the service.
  ~ServiceWriteServerTest() {
    stub_.reset();
    server_.Shutdown();
  }

  // Sends every message on one StreamInsert call and returns the final status.
  grpc::Status StreamInsert(const std::vector<proto::InsertRequest>& messages,
                            proto::InsertResponse* resp) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    auto writer = stub_->StreamInsert(&ctx, resp);
    for (const auto& msg : messages) {
      // A false return means the server already ended the call; Finish
      // reports why.
      if (!writer->Write(msg)) break;
    }
    writer->WritesDone();
    return writer->Finish();
  }

  // The base class is built first, so the service exists here.
  test::TestGrpcServer server_{service_.get()};
  std::unique_ptr<proto::VectorDBService::Stub> stub_;
};

// ============================================================================
// Upsert
// ============================================================================

TEST_CASE_FIXTURE(ServiceWriteTest, "UpsertInsertsNewIds") {
  CreateCollection("upsert_new");

  proto::UpsertResponse resp;
  auto status = Upsert("upsert_new",
                       {MakeVector(1, {1, 0, 0, 0}, "one"),
                        MakeVector(2, {0, 1, 0, 0}, "two"),
                        MakeVector(3, {0, 0, 1, 0})},
                       &resp);
  INFO(status.error_message());
  REQUIRE(status.ok());
  CHECK_EQ(resp.upserted_count(), 3);
  CHECK_EQ(resp.inserted_count(), 3);
  CHECK_EQ(resp.updated_count(), 0);
  CHECK_FALSE(resp.message().empty());

  CHECK_EQ(TotalCount("upsert_new"), 3);

  auto one = GetOne("upsert_new", 1);
  CheckValues(one.vector(), {1, 0, 0, 0});
  CHECK_EQ(NameOf(one), std::optional<std::string>("one"));

  auto two = GetOne("upsert_new", 2);
  CheckValues(two.vector(), {0, 1, 0, 0});
  CHECK_EQ(NameOf(two), std::optional<std::string>("two"));

  auto three = GetOne("upsert_new", 3);
  CheckValues(three.vector(), {0, 0, 1, 0});
  CHECK_FALSE(NameOf(three).has_value());

  // Upserted vectors are searchable: exact match first, at distance 0.
  auto search = Search("upsert_new", {0, 1, 0, 0}, 10);
  REQUIRE_EQ(search.results_size(), 3);
  CHECK_EQ(search.results(0).id(), 2);
  CHECK(search.results(0).distance() == doctest::Approx(0.0f));
}

TEST_CASE_FIXTURE(ServiceWriteTest, "UpsertReplacesVectorAndMetadataOfExistingIds") {
  CreateCollection("upsert_replace");
  InsertOk("upsert_replace", {MakeVector(1, {0.1f, 0, 0, 0}, "original_1"),
                              MakeVector(2, {0.5f, 0, 0, 0}, "original_2"),
                              MakeVector(3, {1.0f, 0, 0, 0}, "original_3")});

  // Id 2 gets a new vector and new metadata; id 3 gets a new vector and no
  // metadata, which replaces (clears) its old metadata.
  proto::UpsertResponse resp;
  auto status = Upsert("upsert_replace",
                       {MakeVector(2, {0.9f, 0, 0, 0}, "updated_2"),
                        MakeVector(3, {0, 0, 0, 3.0f})},
                       &resp);
  INFO(status.error_message());
  REQUIRE(status.ok());
  CHECK_EQ(resp.upserted_count(), 2);
  CHECK_EQ(resp.inserted_count(), 0);
  CHECK_EQ(resp.updated_count(), 2);

  // Replacement, not a second copy.
  CHECK_EQ(TotalCount("upsert_replace"), 3);

  auto two = GetOne("upsert_replace", 2);
  CheckValues(two.vector(), {0.9f, 0, 0, 0});
  CHECK_EQ(NameOf(two), std::optional<std::string>("updated_2"));

  auto three = GetOne("upsert_replace", 3);
  CheckValues(three.vector(), {0, 0, 0, 3.0f});
  CHECK_FALSE(NameOf(three).has_value());

  // Id 1 is untouched.
  auto one = GetOne("upsert_replace", 1);
  CheckValues(one.vector(), {0.1f, 0, 0, 0});
  CHECK_EQ(NameOf(one), std::optional<std::string>("original_1"));

  // Search sees the new vector of id 2 and each id exactly once.
  auto search = Search("upsert_replace", {0.9f, 0, 0, 0}, 10);
  REQUIRE_EQ(search.results_size(), 3);
  CHECK_EQ(search.results(0).id(), 2);
  CHECK(search.results(0).distance() == doctest::Approx(0.0f));
  CHECK_EQ(IdsOf(search), std::set<uint64_t>{1, 2, 3});
}

TEST_CASE_FIXTURE(ServiceWriteTest, "UpsertMixedBatchInsertsAndUpdates") {
  CreateCollection("upsert_mixed");
  InsertOk("upsert_mixed", {MakeVector(1, {0.1f, 0, 0, 0}, "original_1"),
                            MakeVector(2, {0.5f, 0, 0, 0}, "original_2"),
                            MakeVector(3, {1.0f, 0, 0, 0}, "original_3")});

  // Update id 2, insert id 4.
  proto::UpsertResponse resp;
  auto status = Upsert("upsert_mixed",
                       {MakeVector(2, {0.9f, 0, 0, 0}, "updated_2"),
                        MakeVector(4, {2.0f, 0, 0, 0}, "new_4")},
                       &resp);
  INFO(status.error_message());
  REQUIRE(status.ok());
  CHECK_EQ(resp.upserted_count(), 2);
  CHECK_EQ(resp.inserted_count(), 1);
  CHECK_EQ(resp.updated_count(), 1);

  CHECK_EQ(TotalCount("upsert_mixed"), 4);

  auto two = GetOne("upsert_mixed", 2);
  CheckValues(two.vector(), {0.9f, 0, 0, 0});
  CHECK_EQ(NameOf(two), std::optional<std::string>("updated_2"));

  auto four = GetOne("upsert_mixed", 4);
  CheckValues(four.vector(), {2.0f, 0, 0, 0});
  CHECK_EQ(NameOf(four), std::optional<std::string>("new_4"));

  // RangeSearch sees one copy of the replaced id, at its new vector, and
  // filters on the metadata that the upsert wrote.
  const std::vector<float> origin = {0, 0, 0, 0};
  {
    auto range = RangeSearch("upsert_mixed", origin, 100.0f);
    CHECK_EQ(range.results_size(), 4);
    CHECK_EQ(IdsOf(range), std::set<uint64_t>{1, 2, 3, 4});

    auto updated = RangeSearch("upsert_mixed", origin, 100.0f, "name = 'updated_2'");
    REQUIRE_EQ(updated.results_size(), 1);
    CHECK_EQ(updated.results(0).id(), 2);
    CHECK(updated.results(0).distance() == doctest::Approx(0.9f));
    REQUIRE(updated.results(0).has_metadata());
    CHECK_EQ(updated.results(0).metadata().fields().at("name").string_value(), "updated_2");

    CHECK_EQ(RangeSearch("upsert_mixed", origin, 100.0f, "name = 'original_2'").results_size(),
             0);
  }

  // A filtered Search, which reads the scalar indexes that the upsert
  // updated, agrees.
  {
    auto updated = Search("upsert_mixed", {0.9f, 0, 0, 0}, 10, "name = 'updated_2'");
    REQUIRE_EQ(updated.results_size(), 1);
    CHECK_EQ(updated.results(0).id(), 2);
    CHECK(updated.results(0).distance() == doctest::Approx(0.0f));

    CHECK_EQ(Search("upsert_mixed", {0.9f, 0, 0, 0}, 10, "name = 'original_2'").results_size(),
             0);
  }

  // A second mixed batch: one existing id and one far-away new id.
  proto::UpsertResponse resp2;
  status = Upsert("upsert_mixed",
                  {MakeVector(1, {0, 1, 0, 0}),
                   MakeVector(99999, {0, 0, 1, 0})},
                  &resp2);
  INFO(status.error_message());
  REQUIRE(status.ok());
  CHECK_EQ(resp2.upserted_count(), 2);
  CHECK_EQ(resp2.inserted_count(), 1);
  CHECK_EQ(resp2.updated_count(), 1);
  CHECK_EQ(TotalCount("upsert_mixed"), 5);

  auto all = Get("upsert_mixed", {1, 2, 3, 4, 99999});
  CHECK_EQ(IdsOf(all), std::set<uint64_t>{1, 2, 3, 4, 99999});
  CHECK_EQ(all.not_found_ids_size(), 0);

  // Id 1 was replaced without metadata, so no filter matches its old name.
  CHECK_EQ(Search("upsert_mixed", {0, 1, 0, 0}, 10, "name = 'original_1'").results_size(), 0);
  CHECK_EQ(RangeSearch("upsert_mixed", origin, 100.0f, "name = 'original_1'").results_size(),
           0);
}

TEST_CASE_FIXTURE(ServiceWriteTest, "UpsertRejectsInvalidRequests") {
  CreateCollection("upsert_invalid");
  InsertOk("upsert_invalid", {MakeVector(1, {1, 0, 0, 0}, "original_1")});

  SUBCASE("dimension mismatch rejects the whole batch") {
    // The first vector is valid; the second has 3 values in a 4-dim
    // collection. Nothing may be written.
    proto::UpsertResponse resp;
    auto status = Upsert("upsert_invalid",
                         {MakeVector(1, {0, 0, 0, 9}, "changed_1"),
                          MakeVector(2, {1, 2, 3})},
                         &resp);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);

    CHECK_EQ(TotalCount("upsert_invalid"), 1);
    auto one = GetOne("upsert_invalid", 1);
    CheckValues(one.vector(), {1, 0, 0, 0});
    CHECK_EQ(NameOf(one), std::optional<std::string>("original_1"));
  }

  SUBCASE("declared dimension disagrees with the value count") {
    auto bad = MakeVector(2, {1, 2, 3, 4});
    bad.mutable_vector()->set_dimension(5);
    proto::UpsertResponse resp;
    auto status = Upsert("upsert_invalid", {bad}, &resp);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
    CHECK_EQ(TotalCount("upsert_invalid"), 1);
  }

  SUBCASE("empty vector list") {
    proto::UpsertResponse resp;
    auto status = Upsert("upsert_invalid", {}, &resp);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("empty collection name") {
    proto::UpsertResponse resp;
    auto status = Upsert("", {MakeVector(2, {1, 2, 3, 4})}, &resp);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("unknown collection") {
    proto::UpsertResponse resp;
    auto status = Upsert("no_such_collection", {MakeVector(2, {1, 2, 3, 4})},
                         &resp);
    CHECK_FALSE(status.ok());
    CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
  }
}

// ============================================================================
// TTL (Insert and Upsert with ttl_seconds)
// ============================================================================
//
// ttl_seconds is whole seconds and the server stores expiry as
// "insert second + ttl", with a vector expired once the wall-clock second
// reaches that value. So ttl_seconds = 1 expires at the next second
// boundary, 0 to 1 s after the write. The API has no finer unit and no
// injectable clock, so this one test waits for real time to pass, about
// 1.5 s at most: up to 0.5 s to start early in a second (so the "still
// visible" checks have at least 0.5 s of margin) and up to 1 s for the
// boundary. Both waits poll with WaitFor, whose timeout only stops a broken
// run.

TEST_CASE_FIXTURE(ServiceWriteTest, "TtlVectorsExpireFromGetAndSearch") {
  const std::string coll = "ttl_coll";
  CreateCollection(coll);
  // Every vector of this collection has a TTL, so it ends up empty.
  const std::string ttl_only = "ttl_only_coll";
  CreateCollection(ttl_only);

  // Permanent vectors (no TTL).
  InsertOk(coll, {MakeVector(100, {100, 0, 0, 1}),
                  MakeVector(101, {101, 0, 0, 1}),
                  MakeVector(102, {102, 0, 0, 1}),
                  MakeVector(103, {103, 0, 0, 1}),
                  MakeVector(104, {104, 0, 0, 1})});
  // Wait until the wall clock is in the first half of a second.
  REQUIRE(test::WaitFor([] { return EpochMillis() % 1000 < 500; }));
  const auto ttl_start = std::chrono::steady_clock::now();

  // Insert with TTL: ids 1-5 in a dense-only batch, ids 6 and 8 with
  // metadata (the metadata write path also carries expiry).
  InsertOk(coll, {MakeVector(1, {1, 0, 0, 1}, "", 1),
                  MakeVector(2, {2, 0, 0, 1}, "", 1),
                  MakeVector(3, {3, 0, 0, 1}, "", 1),
                  MakeVector(4, {4, 0, 0, 1}, "", 1),
                  MakeVector(5, {5, 0, 0, 1}, "", 1)});
  InsertOk(coll, {MakeVector(6, {6, 0, 0, 1}, "ttl_6", 1),
                  MakeVector(8, {8, 0, 0, 1}, "ttl_8", 1)});
  InsertOk(ttl_only, {MakeVector(1, {1, 0, 0, 1}, "", 1),
                      MakeVector(2, {2, 0, 0, 1}, "", 1),
                      MakeVector(3, {3, 0, 0, 1}, "", 1)});
  InsertOk(ttl_only, {MakeVector(4, {4, 0, 0, 1}, "ttl_only_4", 1)});
  const std::vector<uint64_t> ttl_only_ids = {1, 2, 3, 4};
  const std::set<uint64_t> ttl_only_set(ttl_only_ids.begin(), ttl_only_ids.end());

  // Upsert with TTL: id 7 is new, id 100 was permanent and now gets a TTL.
  // Id 6 is replaced without a TTL, so it no longer expires.
  {
    proto::UpsertResponse resp;
    auto status = Upsert(coll,
                         {MakeVector(7, {7, 0, 0, 1}, "", 1),
                          MakeVector(100, {100, 0, 0, 2}, "", 1),
                          MakeVector(6, {6, 0, 0, 2}, "permanent_6")},
                         &resp);
    INFO(status.error_message());
    REQUIRE(status.ok());
    CHECK_EQ(resp.inserted_count(), 1);
    CHECK_EQ(resp.updated_count(), 2);
  }

  const std::vector<uint64_t> all_ids = {1, 2, 3, 4, 5, 6, 7, 8,
                                         100, 101, 102, 103, 104};
  const std::set<uint64_t> expiring = {1, 2, 3, 4, 5, 7, 8, 100};
  const std::set<uint64_t> permanent = {6, 101, 102, 103, 104};

  // Before expiry: everything is visible to Get and Search.
  {
    auto got = Get(coll, all_ids);
    CHECK_EQ(IdsOf(got), std::set<uint64_t>(all_ids.begin(), all_ids.end()));
    CHECK_EQ(got.not_found_ids_size(), 0);

    auto search = Search(coll, {0, 0, 0, 1}, 20);
    CHECK_EQ(IdsOf(search), std::set<uint64_t>(all_ids.begin(), all_ids.end()));

    CHECK_EQ(IdsOf(Get(ttl_only, ttl_only_ids)), ttl_only_set);
    CHECK_EQ(IdsOf(Search(ttl_only, {0, 0, 0, 1}, 20)), ttl_only_set);
  }
  // INFO evaluates lazily, so take the time now.
  const auto visible_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - ttl_start)
                              .count();
  INFO("visibility checks finished " << visible_ms << " ms after the TTL writes started");

  // Poll until every TTL vector of both collections is gone from Get.
  const std::vector<uint64_t> expiring_ids(expiring.begin(), expiring.end());
  REQUIRE(test::WaitFor([&] {
    return Get(coll, expiring_ids).vectors_size() == 0 &&
           Get(ttl_only, ttl_only_ids).vectors_size() == 0;
  }));

  // After expiry: Get reports TTL ids as not found and keeps the rest.
  auto got = Get(coll, all_ids);
  CHECK_EQ(IdsOf(got), permanent);
  CHECK_EQ(NotFoundOf(got), expiring);
  auto six = GetOne(coll, 6);
  CheckValues(six.vector(), {6, 0, 0, 2});
  CHECK_EQ(NameOf(six), std::optional<std::string>("permanent_6"));

  // Search (different query than before) returns only the permanent ids.
  auto search = Search(coll, {50, 0, 0, 1}, 20);
  CHECK_EQ(IdsOf(search), permanent);

  // In the collection where every vector expired, Search is OK and empty,
  // and Get reports every id as not found.
  CHECK_EQ(Search(ttl_only, {50, 0, 0, 1}, 20).results_size(), 0);
  auto ttl_only_got = Get(ttl_only, ttl_only_ids);
  CHECK_EQ(ttl_only_got.vectors_size(), 0);
  CHECK_EQ(NotFoundOf(ttl_only_got), ttl_only_set);
}

// ============================================================================
// StreamInsert over a real gRPC server
// ============================================================================

namespace {

constexpr uint32_t kStreamDim = 8;

// Deterministic, distinct vector for an id.
std::vector<float> StreamVector(uint64_t id) {
  std::vector<float> v(kStreamDim);
  for (uint32_t d = 0; d < kStreamDim; ++d) {
    v[d] = static_cast<float>((id * 31 + d * 17) % 97) / 10.0f +
           static_cast<float>(d == id % kStreamDim) * 5.0f;
  }
  return v;
}

proto::InsertRequest StreamMessage(const std::string& coll, uint64_t first_id,
                                   uint64_t count, bool with_metadata) {
  proto::InsertRequest req;
  req.set_collection_name(coll);
  for (uint64_t id = first_id; id < first_id + count; ++id) {
    *req.add_vectors() = MakeVector(
        id, StreamVector(id), with_metadata ? "item_" + std::to_string(id) : "");
  }
  return req;
}

}  // namespace

TEST_CASE_FIXTURE(ServiceWriteServerTest, "StreamInsertWritesEveryMessage") {
  const std::string coll = "stream_coll";
  CreateCollection(coll, kStreamDim);

  // 200 vectors in 4 messages of 50 (as the SDK batches them); the second
  // and fourth messages carry metadata.
  std::vector<proto::InsertRequest> messages = {
      StreamMessage(coll, 1, 50, false),
      StreamMessage(coll, 51, 50, true),
      StreamMessage(coll, 101, 50, false),
      StreamMessage(coll, 151, 50, true),
  };

  proto::InsertResponse resp;
  auto status = StreamInsert(messages, &resp);
  INFO(status.error_message());
  REQUIRE(status.ok());
  CHECK_EQ(resp.inserted_count(), 200);
  CHECK_FALSE(resp.message().empty());

  CHECK_EQ(TotalCount(coll), 200);

  // Spot-check vectors from every message, with metadata where it was sent.
  auto got = Get(coll, {1, 50, 51, 100, 101, 150, 151, 200});
  REQUIRE_EQ(got.vectors_size(), 8);
  CHECK_EQ(got.not_found_ids_size(), 0);
  for (const auto& v : got.vectors()) {
    INFO("id " << v.id());
    CheckValues(v.vector(), StreamVector(v.id()));
    const bool has_meta = (v.id() >= 51 && v.id() <= 100) || v.id() >= 151;
    if (has_meta) {
      CHECK_EQ(NameOf(v), std::optional<std::string>("item_" + std::to_string(v.id())));
    } else {
      CHECK_FALSE(NameOf(v).has_value());
    }
  }

  // Streamed vectors are searchable: an exact query finds its id first at
  // distance 0, results are sorted by distance, top_k is honored.
  for (uint64_t id : {7u, 77u, 137u, 199u}) {
    INFO("query id " << id);
    auto search = Search(coll, StreamVector(id), 10);
    REQUIRE_EQ(search.results_size(), 10);
    CHECK_EQ(search.results(0).id(), id);
    CHECK(search.results(0).distance() == doctest::Approx(0.0f));
    for (int i = 1; i < search.results_size(); ++i) {
      CHECK_GE(search.results(i).distance(), search.results(i - 1).distance());
    }
  }
}

TEST_CASE_FIXTURE(ServiceWriteServerTest, "StreamInsertRejectsDimensionMismatch") {
  const std::string coll = "stream_bad_dim";
  CreateCollection(coll, kStreamDim);

  proto::InsertRequest bad;
  bad.set_collection_name(coll);
  *bad.add_vectors() = MakeVector(1000, {1, 2, 3});

  proto::InsertResponse resp;
  auto status = StreamInsert({StreamMessage(coll, 1, 10, false), bad}, &resp);
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);

  // The bad vector was never stored.
  auto got = Get(coll, {1000});
  CHECK_EQ(got.vectors_size(), 0);
}

TEST_CASE_FIXTURE(ServiceWriteServerTest, "StreamInsertUnknownCollectionFails") {
  proto::InsertResponse resp;
  auto status = StreamInsert({StreamMessage("no_such_collection", 1, 5, false)},
                             &resp);
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}
