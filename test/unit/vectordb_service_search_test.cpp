// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "compute/query_cache.h"
#include "compute/query_executor.h"
#include "core/types.h"
#include "index/index_factory.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/vectordb_service.h"
#include "storage/segment/segment_manager.h"
#include "support/temp_dir.h"
#include "support/vectors.h"

using namespace gvdb;

// ============================================================================
// Helpers
// ============================================================================

namespace {

// One document of the small hybrid/sparse corpus.
struct Doc {
  uint64_t id;
  std::vector<float> vector;
  std::string text;
  std::vector<uint32_t> sparse_indices;
  std::vector<float> sparse_values;
};

// The corpus of the hybrid and sparse e2e programs: 4D unit-ish vectors, a
// short text per document, and a sparse term-weight vector.
std::vector<Doc> HybridCorpus() {
  return {
      {1, {1, 0, 0, 0}, "running shoes for marathon athletes", {0, 5, 10}, {1.0f, 0.8f, 0.3f}},
      {2, {0, 1, 0, 0}, "basketball sneakers indoor courts", {1, 6}, {1.0f, 0.9f}},
      {3, {0, 0, 1, 0}, "hiking boots waterproof mountain trails", {2, 7, 10}, {1.0f, 0.7f, 0.5f}},
      {4, {0, 0, 0, 1}, "running gear jogging fitness tracker", {0, 8}, {0.9f, 1.0f}},
      {5, {0.9f, 0.1f, 0, 0}, "casual walking shoes comfortable daily", {3, 5}, {1.0f, 0.6f}},
  };
}

void SetProtoVector(const std::vector<float>& values, proto::Vector* out) {
  out->set_dimension(static_cast<uint32_t>(values.size()));
  for (float v : values) out->add_values(v);
}

void SetSparse(const std::vector<uint32_t>& indices, const std::vector<float>& values,
               proto::SparseVector* out) {
  for (auto i : indices) out->add_indices(i);
  for (auto v : values) out->add_values(v);
}

void SetStringField(proto::Metadata* metadata, const std::string& key, const std::string& value) {
  (*metadata->mutable_fields())[key].set_string_value(value);
}

double L2(const std::vector<float>& a, const std::vector<float>& b) {
  double sum = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    sum += diff * diff;
  }
  return std::sqrt(sum);
}

double Dot(const std::vector<float>& a, const std::vector<float>& b) {
  double sum = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    sum += static_cast<double>(a[i]) * static_cast<double>(b[i]);
  }
  return sum;
}

// 1 - cos(a, b), in [0, 2]. Both vectors must be non-zero.
double CosineDistance(const std::vector<float>& a, const std::vector<float>& b) {
  return 1.0 - Dot(a, b) / (std::sqrt(Dot(a, a)) * std::sqrt(Dot(b, b)));
}

// Brute-force distance under a collection metric, where a lower value is
// nearer: the L2 distance, the cosine distance, or the negative dot product.
double ReferenceDistance(proto::CreateCollectionRequest::MetricType metric,
                         const std::vector<float>& a, const std::vector<float>& b) {
  switch (metric) {
    case proto::CreateCollectionRequest::COSINE:
      return CosineDistance(a, b);
    case proto::CreateCollectionRequest::INNER_PRODUCT:
      return -Dot(a, b);
    default:
      return L2(a, b);
  }
}

std::vector<uint64_t> Ids(
    const google::protobuf::RepeatedPtrField<proto::SearchResultEntry>& results) {
  std::vector<uint64_t> ids;
  for (const auto& r : results) ids.push_back(r.id());
  return ids;
}

// Hybrid scores are "higher is better".
template <typename Results>
void CheckScoresNonIncreasing(const Results& results) {
  for (int i = 1; i < results.size(); ++i) {
    CHECK(results[i].distance() <= results[i - 1].distance());
  }
}

template <typename Results>
void CheckDistancesNonDecreasing(const Results& results) {
  for (int i = 1; i < results.size(); ++i) {
    CHECK(results[i].distance() >= results[i - 1].distance());
  }
}

// The values of a core::Vector as a plain float vector.
std::vector<float> ToFloats(const core::Vector& v) { return {v.data(), v.data() + v.dimension()}; }

}  // namespace

// ============================================================================
// Fixture: VectorDBService in local (single-node) mode
// ============================================================================

class SearchServiceTest {
 public:
  explicit SearchServiceTest(std::shared_ptr<compute::QueryCache> cache = nullptr) {
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ =
        std::make_shared<storage::SegmentManager>(test_dir_.str(), index_factory_.get());
    query_executor_ =
        std::make_shared<compute::QueryExecutor>(*segment_store_, nullptr, std::move(cache));

    auto resolver = service::MakeLocalResolver(segment_store_);
    service_ = std::make_unique<service::VectorDBService>(segment_store_, query_executor_,
                                                          std::move(resolver));
  }

  ~SearchServiceTest() {
    service_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();
    // test_dir_ is the first member, so it removes the folder last.
  }

  // Creates the collection and returns its id.
  uint32_t CreateCollection(
      const std::string& name, uint32_t dimension,
      proto::CreateCollectionRequest::MetricType metric = proto::CreateCollectionRequest::L2,
      proto::CreateCollectionRequest::IndexType index_type = proto::CreateCollectionRequest::FLAT) {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name(name);
    request.set_dimension(dimension);
    request.set_metric(metric);
    request.set_index_type(index_type);
    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return response.collection_id();
  }

  grpc::Status ListCollections(proto::ListCollectionsResponse* response) {
    grpc::ServerContext context;
    proto::ListCollectionsRequest request;
    return service_->ListCollections(&context, &request, response);
  }

  grpc::Status Insert(const proto::InsertRequest& request, proto::InsertResponse* response) {
    grpc::ServerContext context;
    return service_->Insert(&context, &request, response);
  }

  // Inserts the hybrid corpus with text metadata, and sparse vectors when
  // with_sparse is set.
  void InsertCorpus(const std::string& name, bool with_sparse) {
    proto::InsertRequest request;
    request.set_collection_name(name);
    for (const auto& doc : HybridCorpus()) {
      auto* v = request.add_vectors();
      v->set_id(doc.id);
      SetProtoVector(doc.vector, v->mutable_vector());
      SetStringField(v->mutable_metadata(), "text", doc.text);
      if (with_sparse) {
        SetSparse(doc.sparse_indices, doc.sparse_values, v->mutable_sparse_vector());
      }
    }
    proto::InsertResponse response;
    auto status = Insert(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.inserted_count(), 5);
  }

  grpc::Status RangeSearch(const proto::RangeSearchRequest& request,
                           proto::RangeSearchResponse* response) {
    grpc::ServerContext context;
    return service_->RangeSearch(&context, &request, response);
  }

  grpc::Status HybridSearch(const proto::HybridSearchRequest& request,
                            proto::HybridSearchResponse* response) {
    grpc::ServerContext context;
    return service_->HybridSearch(&context, &request, response);
  }

  grpc::Status Search(const proto::SearchRequest& request, proto::SearchResponse* response) {
    grpc::ServerContext context;
    return service_->Search(&context, &request, response);
  }

  test::ScopedTempDir test_dir_{"gvdb_service_search_test"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<service::VectorDBService> service_;
};

// ============================================================================
// RangeSearch (FLAT, L2)
// ============================================================================

// A collection of deterministic random points for the range tests. All
// points go into one segment of one shard.
class RangeSearchTest : public SearchServiceTest {
 public:
  static constexpr uint32_t kDim = 8;
  static constexpr int kCount = 300;
  static constexpr const char* kCollection = "range_collection";

  explicit RangeSearchTest(
      proto::CreateCollectionRequest::MetricType metric = proto::CreateCollectionRequest::L2)
      : metric_(metric) {
    CreateCollection(kCollection, kDim, metric);

    const auto vectors = test::RandomVectors(kCount, kDim, 1234);
    proto::InsertRequest request;
    request.set_collection_name(kCollection);
    for (int i = 1; i <= kCount; ++i) {
      const std::vector<float> values = ToFloats(vectors[static_cast<size_t>(i - 1)]);
      points_[static_cast<uint64_t>(i)] = values;

      auto* v = request.add_vectors();
      v->set_id(static_cast<uint64_t>(i));
      SetProtoVector(values, v->mutable_vector());
      SetStringField(v->mutable_metadata(), "name", "v" + std::to_string(i));
      SetStringField(v->mutable_metadata(), "parity", i % 2 == 0 ? "even" : "odd");
    }
    proto::InsertResponse response;
    auto status = Insert(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.inserted_count(), static_cast<uint64_t>(kCount));
  }

  // Exact distance between two vectors under the collection's metric.
  double Distance(const std::vector<float>& a, const std::vector<float>& b) const {
    return ReferenceDistance(metric_, a, b);
  }

  // All points sorted by exact distance to the query.
  std::vector<std::pair<double, uint64_t>> SortedByDistance(const std::vector<float>& query) const {
    std::vector<std::pair<double, uint64_t>> out;
    for (const auto& [id, values] : points_) out.emplace_back(Distance(query, values), id);
    std::sort(out.begin(), out.end());
    return out;
  }

  // A radius halfway between the n-th and (n+1)-th nearest distances, so no
  // point sits on the boundary.
  float RadiusCovering(const std::vector<float>& query, size_t n) const {
    auto sorted = SortedByDistance(query);
    return static_cast<float>((sorted[n - 1].first + sorted[n].first) / 2.0);
  }

  proto::RangeSearchRequest MakeRequest(const std::vector<float>& query, float radius) const {
    proto::RangeSearchRequest request;
    request.set_collection_name(kCollection);
    SetProtoVector(query, request.mutable_query_vector());
    request.set_radius(radius);
    return request;
  }

  proto::CreateCollectionRequest::MetricType metric_;
  std::unordered_map<uint64_t, std::vector<float>> points_;
};

// The same points in a COSINE collection. Random points in [-1, 1]^8 have
// no zero vector and no two points on the same ray, so every cosine
// distance from a stored point to another point is above 0.
class CosineRangeSearchTest : public RangeSearchTest {
 public:
  CosineRangeSearchTest() : RangeSearchTest(proto::CreateCollectionRequest::COSINE) {}
};

TEST_CASE_FIXTURE(RangeSearchTest, "RangeSearchReturnsExactlyThePointsInsideTheRadius") {
  // Query a point that is itself in the collection, so the nearest point
  // is at distance 0.
  const auto query = points_.at(7);
  const float radius = RadiusCovering(query, 20);
  auto sorted = SortedByDistance(query);

  auto request = MakeRequest(query, radius);
  request.set_max_results(1000);
  request.set_return_metadata(true);
  proto::RangeSearchResponse response;
  auto status = RangeSearch(request, &response);
  INFO(status.error_message());
  REQUIRE(status.ok());

  REQUIRE_EQ(response.results_size(), 20);

  // The nearest point is the query itself and comes first.
  CHECK_EQ(response.results(0).id(), 7);
  CHECK(response.results(0).distance() == doctest::Approx(0.0f));

  // Same set as a brute-force scan, in ascending distance, every distance
  // exact and inside the radius.
  std::set<uint64_t> expected;
  for (size_t i = 0; i < 20; ++i) expected.insert(sorted[i].second);
  std::set<uint64_t> got;
  for (const auto& r : response.results()) {
    got.insert(r.id());
    CHECK(r.distance() <= radius);
    CHECK(r.distance() == doctest::Approx(L2(query, points_.at(r.id()))).epsilon(1e-4));
    REQUIRE(r.has_metadata());
    CHECK_EQ(r.metadata().fields().at("name").string_value(), "v" + std::to_string(r.id()));
  }
  CHECK_EQ(got, expected);
  CheckDistancesNonDecreasing(response.results());
}

TEST_CASE_FIXTURE(RangeSearchTest, "RangeSearchFromAnOffCollectionQuery") {
  // A query that is not a stored point: the result set still matches a
  // brute-force scan and includes the nearest point.
  const std::vector<float> query = {0.1f, -0.2f, 0.3f, 0.0f, -0.1f, 0.2f, 0.05f, -0.3f};
  const float radius = RadiusCovering(query, 35);
  auto sorted = SortedByDistance(query);

  auto request = MakeRequest(query, radius);
  proto::RangeSearchResponse response;
  auto status = RangeSearch(request, &response);
  INFO(status.error_message());
  REQUIRE(status.ok());

  REQUIRE_EQ(response.results_size(), 35);
  CHECK_EQ(response.results(0).id(), sorted[0].second);
  CHECK(response.results(0).distance() == doctest::Approx(sorted[0].first).epsilon(1e-4));

  std::set<uint64_t> expected;
  for (size_t i = 0; i < 35; ++i) expected.insert(sorted[i].second);
  std::set<uint64_t> got;
  for (const auto& r : response.results()) {
    got.insert(r.id());
    CHECK(r.distance() <= radius);
  }
  CHECK_EQ(got, expected);
  CheckDistancesNonDecreasing(response.results());
}

TEST_CASE_FIXTURE(RangeSearchTest, "RangeSearchRadiusContainingEverything") {
  const std::vector<float> query(kDim, 0.0f);
  auto request = MakeRequest(query, 1.0e6f);
  // max_results = 0 falls back to the server default, which is above kCount.
  proto::RangeSearchResponse response;
  auto status = RangeSearch(request, &response);
  INFO(status.error_message());
  REQUIRE(status.ok());

  CHECK_EQ(response.results_size(), kCount);
  std::set<uint64_t> got;
  for (const auto& r : response.results()) got.insert(r.id());
  CHECK_EQ(got.size(), static_cast<size_t>(kCount));
  CHECK_EQ(*got.begin(), 1);
  CHECK_EQ(*got.rbegin(), static_cast<uint64_t>(kCount));
  CheckDistancesNonDecreasing(response.results());
}

TEST_CASE_FIXTURE(RangeSearchTest, "RangeSearchRadiusContainingNothing") {
  // Every stored point is inside [-1, 1]^8, so a query far away with a small
  // radius matches nothing.
  const std::vector<float> query(kDim, 100.0f);
  auto request = MakeRequest(query, 1.0f);
  proto::RangeSearchResponse response;
  auto status = RangeSearch(request, &response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  CHECK_EQ(response.results_size(), 0);

  // A radius just below the nearest distance from an off-collection query
  // also matches nothing.
  const std::vector<float> near_query(kDim, 0.5f);
  auto sorted = SortedByDistance(near_query);
  auto tight = MakeRequest(near_query, static_cast<float>(sorted[0].first * 0.5));
  proto::RangeSearchResponse tight_response;
  status = RangeSearch(tight, &tight_response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  CHECK_EQ(tight_response.results_size(), 0);
}

TEST_CASE_FIXTURE(RangeSearchTest, "RangeSearchMaxResultsCapsTheResultCount") {
  const std::vector<float> query(kDim, 0.0f);
  const float radius = 1.0e6f;

  for (uint32_t cap : {1u, 2u, 20u}) {
    CAPTURE(cap);
    auto request = MakeRequest(query, radius);
    request.set_max_results(cap);
    proto::RangeSearchResponse response;
    auto status = RangeSearch(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    CHECK_EQ(response.results_size(), static_cast<int>(cap));
    auto ids = Ids(response.results());
    CHECK_EQ(std::set<uint64_t>(ids.begin(), ids.end()).size(), cap);
    for (const auto& r : response.results()) CHECK(r.distance() <= radius);
    CheckDistancesNonDecreasing(response.results());
  }
}

TEST_CASE_FIXTURE(RangeSearchTest, "RangeSearchWithFilter") {
  const std::vector<float> query(kDim, 0.0f);

  SUBCASE("a large radius and an exact-match filter returns one point") {
    auto request = MakeRequest(query, 100.0f);
    request.set_filter("name = 'v42'");
    request.set_return_metadata(true);
    request.set_max_results(10);
    proto::RangeSearchResponse response;
    auto status = RangeSearch(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.results_size(), 1);
    CHECK_EQ(response.results(0).id(), 42);
    CHECK_EQ(response.results(0).metadata().fields().at("name").string_value(), "v42");
  }

  SUBCASE("radius and filter both restrict the result") {
    const float radius = RadiusCovering(query, 40);
    auto sorted = SortedByDistance(query);
    std::set<uint64_t> expected;
    for (size_t i = 0; i < 40; ++i) {
      if (sorted[i].second % 2 == 0) expected.insert(sorted[i].second);
    }
    REQUIRE_FALSE(expected.empty());

    auto request = MakeRequest(query, radius);
    request.set_filter("parity = 'even'");
    proto::RangeSearchResponse response;
    auto status = RangeSearch(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    std::set<uint64_t> got;
    for (const auto& r : response.results()) {
      got.insert(r.id());
      CHECK(r.distance() <= radius);
    }
    CHECK_EQ(got, expected);
    CheckDistancesNonDecreasing(response.results());
  }
}

TEST_CASE_FIXTURE(RangeSearchTest, "RangeSearchArgumentErrors") {
  const std::vector<float> query(kDim, 0.0f);

  SUBCASE("empty collection name") {
    auto request = MakeRequest(query, 1.0f);
    request.clear_collection_name();
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("missing query vector") {
    auto request = MakeRequest(query, 1.0f);
    request.clear_query_vector();
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("zero radius") {
    auto request = MakeRequest(query, 0.0f);
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("negative radius") {
    auto request = MakeRequest(query, -1.0f);
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("query vector whose dimension field disagrees with its values") {
    auto request = MakeRequest(query, 1.0f);
    request.mutable_query_vector()->set_dimension(kDim + 1);
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("unknown collection") {
    auto request = MakeRequest(query, 1.0f);
    request.set_collection_name("no_such_collection");
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(), grpc::StatusCode::NOT_FOUND);
  }
}

// ============================================================================
// RangeSearch (FLAT, COSINE)
// ============================================================================

TEST_CASE_FIXTURE(CosineRangeSearchTest,
                  "CosineRangeSearchReturnsExactlyThePointsInsideTheRadius") {
  // The result set, order and distances (1 - cos) match a brute-force scan.
  auto check = [this](const std::vector<float>& query, size_t n) {
    const float radius = RadiusCovering(query, n);
    auto sorted = SortedByDistance(query);

    auto request = MakeRequest(query, radius);
    request.set_return_metadata(true);
    proto::RangeSearchResponse response;
    auto status = RangeSearch(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());

    REQUIRE_EQ(response.results_size(), static_cast<int>(n));
    CHECK_EQ(response.results(0).id(), sorted[0].second);

    std::set<uint64_t> expected;
    for (size_t i = 0; i < n; ++i) expected.insert(sorted[i].second);
    std::set<uint64_t> got;
    for (const auto& r : response.results()) {
      got.insert(r.id());
      CHECK(r.distance() <= radius);
      CHECK(r.distance() ==
            doctest::Approx(CosineDistance(query, points_.at(r.id()))).epsilon(1e-4));
      REQUIRE(r.has_metadata());
      CHECK_EQ(r.metadata().fields().at("name").string_value(), "v" + std::to_string(r.id()));
    }
    CHECK_EQ(got, expected);
    CheckDistancesNonDecreasing(response.results());
    return response;
  };

  SUBCASE("a stored point as the query is first at distance 0") {
    auto response = check(points_.at(7), 20);
    CHECK_EQ(response.results(0).id(), 7);
    CHECK(response.results(0).distance() == doctest::Approx(0.0f));
  }

  SUBCASE("an off-collection query") {
    check({0.1f, -0.2f, 0.3f, 0.0f, -0.1f, 0.2f, 0.05f, -0.3f}, 35);
  }
}

TEST_CASE_FIXTURE(CosineRangeSearchTest, "CosineRangeSearchWithFilterAndMaxResults") {
  // The cosine distance is at most 2, so radius 2 covers every point.
  const std::vector<float> query = {0.3f, 0.1f, -0.4f, 0.2f, 0.0f, -0.1f, 0.5f, 0.2f};
  const float radius = 2.0f;

  SUBCASE("radius 2 returns every point") {
    proto::RangeSearchResponse response;
    auto status = RangeSearch(MakeRequest(query, radius), &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    CHECK_EQ(response.results_size(), kCount);
    auto ids = Ids(response.results());
    CHECK_EQ(std::set<uint64_t>(ids.begin(), ids.end()).size(), static_cast<size_t>(kCount));
    CheckDistancesNonDecreasing(response.results());
  }

  SUBCASE("an exact-match filter returns one point") {
    auto request = MakeRequest(query, radius);
    request.set_filter("name = 'v42'");
    request.set_return_metadata(true);
    request.set_max_results(10);
    proto::RangeSearchResponse response;
    auto status = RangeSearch(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.results_size(), 1);
    CHECK_EQ(response.results(0).id(), 42);
    CHECK(response.results(0).distance() ==
          doctest::Approx(CosineDistance(query, points_.at(42))).epsilon(1e-4));
    CHECK_EQ(response.results(0).metadata().fields().at("name").string_value(), "v42");
  }

  SUBCASE("max_results caps the result count") {
    for (uint32_t cap : {1u, 5u}) {
      CAPTURE(cap);
      auto request = MakeRequest(query, radius);
      request.set_max_results(cap);
      proto::RangeSearchResponse response;
      auto status = RangeSearch(request, &response);
      INFO(status.error_message());
      REQUIRE(status.ok());
      CHECK_EQ(response.results_size(), static_cast<int>(cap));
      auto ids = Ids(response.results());
      CHECK_EQ(std::set<uint64_t>(ids.begin(), ids.end()).size(), cap);
      for (const auto& r : response.results()) CHECK(r.distance() <= radius);
      CheckDistancesNonDecreasing(response.results());
    }
  }
}

// ============================================================================
// HybridSearch: dense vector + BM25 text
// ============================================================================

class HybridSearchTest : public SearchServiceTest {
 public:
  static constexpr const char* kCollection = "hybrid_collection";

  HybridSearchTest() {
    CreateCollection(kCollection, 4);
    InsertCorpus(kCollection, /*with_sparse=*/false);
  }

  proto::HybridSearchRequest MakeRequest() const {
    proto::HybridSearchRequest request;
    request.set_collection_name(kCollection);
    request.set_text_field("text");
    return request;
  }
};

TEST_CASE_FIXTURE(HybridSearchTest, "HybridSearchRanksTheCombinedMatchFirst") {
  // Vector near doc 1 plus text "running" (docs 1 and 4). Doc 1 wins both
  // signals; doc 4 (text only) and doc 5 (near vector only) beat docs 2 and
  // 3, which match neither.
  auto request = MakeRequest();
  SetProtoVector({1, 0, 0, 0}, request.mutable_query_vector());
  request.set_text_query("running");
  request.set_top_k(5);
  request.set_vector_weight(0.5f);
  request.set_text_weight(0.5f);
  request.set_return_metadata(true);

  proto::HybridSearchResponse response;
  auto status = HybridSearch(request, &response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  REQUIRE_EQ(response.results_size(), 5);

  CHECK_EQ(response.results(0).id(), 1);
  std::set<uint64_t> top3 = {response.results(1).id(), response.results(2).id()};
  CHECK_EQ(top3, std::set<uint64_t>{4, 5});
  CheckScoresNonIncreasing(response.results());
  CHECK(response.results(0).distance() > response.results(1).distance());
  CHECK(response.results(2).distance() > response.results(3).distance());

  // Metadata comes back with each result and belongs to that result.
  std::unordered_map<uint64_t, std::string> text_by_id;
  for (const auto& doc : HybridCorpus()) text_by_id[doc.id] = doc.text;
  for (const auto& r : response.results()) {
    REQUIRE(r.has_metadata());
    CHECK_EQ(r.metadata().fields().at("text").string_value(), text_by_id.at(r.id()));
  }
}

TEST_CASE_FIXTURE(HybridSearchTest, "HybridSearchTextOnly") {
  SUBCASE("all query terms in one document") {
    auto request = MakeRequest();
    request.set_text_query("hiking mountain waterproof");
    request.set_top_k(3);
    request.set_text_weight(1.0f);
    proto::HybridSearchResponse response;
    auto status = HybridSearch(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_GE(response.results_size(), 1);
    CHECK_LE(response.results_size(), 3);
    CHECK_EQ(response.results(0).id(), 3);
    for (int i = 1; i < response.results_size(); ++i) {
      CHECK(response.results(i).distance() < response.results(0).distance());
    }
  }

  SUBCASE("a term in two documents ranks exactly those two first") {
    auto request = MakeRequest();
    request.set_text_query("running");
    request.set_top_k(2);
    request.set_text_weight(1.0f);
    proto::HybridSearchResponse response;
    auto status = HybridSearch(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.results_size(), 2);
    auto ids = Ids(response.results());
    CHECK_EQ(std::set<uint64_t>(ids.begin(), ids.end()), std::set<uint64_t>{1, 4});
    CHECK(response.results(1).distance() > 0.0f);
  }

  SUBCASE("text_field selects the metadata field to index") {
    // There is no "title" field, so no document matches the text, and the
    // text-only query gives no document a positive score.
    auto request = MakeRequest();
    request.set_text_field("title");
    request.set_text_query("hiking");
    request.set_top_k(5);
    request.set_text_weight(1.0f);
    proto::HybridSearchResponse response;
    auto status = HybridSearch(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    for (const auto& r : response.results()) CHECK(r.distance() <= 0.0f);
  }
}

TEST_CASE_FIXTURE(HybridSearchTest, "HybridSearchVectorOnly") {
  // Vector-only hybrid ranks by distance: doc 1 (exact) then doc 5.
  auto request = MakeRequest();
  SetProtoVector({1, 0, 0, 0}, request.mutable_query_vector());
  request.set_top_k(3);
  request.set_vector_weight(1.0f);
  proto::HybridSearchResponse response;
  auto status = HybridSearch(request, &response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  REQUIRE_EQ(response.results_size(), 3);
  CHECK_EQ(response.results(0).id(), 1);
  CHECK_EQ(response.results(1).id(), 5);
  CheckScoresNonIncreasing(response.results());
  CHECK(response.results(0).distance() > response.results(1).distance());
  CHECK(response.results(1).distance() > response.results(2).distance());

  // Plain Search on the same data agrees, with exact L2 distances.
  proto::SearchRequest search;
  search.set_collection_name(kCollection);
  SetProtoVector({1, 0, 0, 0}, search.mutable_query_vector());
  search.set_top_k(3);
  proto::SearchResponse search_response;
  status = Search(search, &search_response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  REQUIRE_EQ(search_response.results_size(), 3);
  CHECK_EQ(search_response.results(0).id(), 1);
  CHECK_EQ(search_response.results(1).id(), 5);
  CHECK(search_response.results(0).distance() == doctest::Approx(0.0f));
  CHECK(search_response.results(1).distance() == doctest::Approx(std::sqrt(0.02f)).epsilon(1e-4));
  CHECK(search_response.results(2).distance() == doctest::Approx(std::sqrt(2.0f)).epsilon(1e-4));
}

TEST_CASE_FIXTURE(HybridSearchTest, "HybridSearchWeightsDecideBetweenSignals") {
  // The vector points at doc 4 and the text matches only doc 3, so the
  // weights decide which one ranks first.
  auto make = [this](float vector_weight, float text_weight) {
    auto request = MakeRequest();
    SetProtoVector({0, 0, 0, 1}, request.mutable_query_vector());
    request.set_text_query("hiking");
    request.set_top_k(5);
    request.set_vector_weight(vector_weight);
    request.set_text_weight(text_weight);
    return request;
  };

  SUBCASE("text weight dominates") {
    proto::HybridSearchResponse response;
    auto status = HybridSearch(make(0.01f, 1.0f), &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_GE(response.results_size(), 2);
    CHECK_EQ(response.results(0).id(), 3);
    CheckScoresNonIncreasing(response.results());
  }

  SUBCASE("vector weight dominates") {
    proto::HybridSearchResponse response;
    auto status = HybridSearch(make(1.0f, 0.01f), &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_GE(response.results_size(), 2);
    CHECK_EQ(response.results(0).id(), 4);
    CheckScoresNonIncreasing(response.results());
  }
}

TEST_CASE_FIXTURE(HybridSearchTest, "HybridSearchTopKLimitsResults") {
  for (uint32_t k : {1u, 2u, 4u}) {
    CAPTURE(k);
    auto request = MakeRequest();
    SetProtoVector({0.5f, 0.5f, 0, 0}, request.mutable_query_vector());
    request.set_text_query("shoes");
    request.set_top_k(k);
    proto::HybridSearchResponse response;
    auto status = HybridSearch(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    CHECK_EQ(response.results_size(), static_cast<int>(k));
    auto ids = Ids(response.results());
    CHECK_EQ(std::set<uint64_t>(ids.begin(), ids.end()).size(), k);
  }
}

TEST_CASE_FIXTURE(HybridSearchTest, "HybridSearchArgumentErrors") {
  SUBCASE("no vector, text or sparse query") {
    auto request = MakeRequest();
    request.set_top_k(5);
    proto::HybridSearchResponse response;
    CHECK_EQ(HybridSearch(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("unknown collection") {
    auto request = MakeRequest();
    request.set_collection_name("no_such_collection");
    request.set_text_query("running");
    request.set_top_k(5);
    proto::HybridSearchResponse response;
    CHECK_EQ(HybridSearch(request, &response).error_code(), grpc::StatusCode::NOT_FOUND);
  }

  SUBCASE("empty collection name") {
    auto request = MakeRequest();
    request.clear_collection_name();
    request.set_text_query("running");
    request.set_top_k(5);
    proto::HybridSearchResponse response;
    CHECK_FALSE(HybridSearch(request, &response).ok());
  }

  SUBCASE("query vector whose dimension field disagrees with its values") {
    auto request = MakeRequest();
    SetProtoVector({1, 0, 0}, request.mutable_query_vector());
    request.mutable_query_vector()->set_dimension(4);
    request.set_top_k(5);
    proto::HybridSearchResponse response;
    CHECK_EQ(HybridSearch(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("sparse query with mismatched indices and values") {
    auto request = MakeRequest();
    request.mutable_sparse_query()->add_indices(0);
    request.mutable_sparse_query()->add_indices(1);
    request.mutable_sparse_query()->add_values(1.0f);
    request.set_sparse_weight(1.0f);
    request.set_top_k(5);
    proto::HybridSearchResponse response;
    CHECK_EQ(HybridSearch(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("sparse query with unsorted indices") {
    auto request = MakeRequest();
    SetSparse({5, 0}, {1.0f, 1.0f}, request.mutable_sparse_query());
    request.set_sparse_weight(1.0f);
    request.set_top_k(5);
    proto::HybridSearchResponse response;
    CHECK_EQ(HybridSearch(request, &response).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }
}

// ============================================================================
// Sparse vectors: insert through the service, sparse and dense+sparse search
// ============================================================================

class SparseSearchTest : public SearchServiceTest {
 public:
  static constexpr const char* kCollection = "sparse_collection";

  SparseSearchTest() {
    CreateCollection(kCollection, 4);
    InsertCorpus(kCollection, /*with_sparse=*/true);
  }

  proto::HybridSearchRequest SparseOnly(const std::vector<uint32_t>& indices,
                                        const std::vector<float>& values, uint32_t top_k) const {
    proto::HybridSearchRequest request;
    request.set_collection_name(kCollection);
    SetSparse(indices, values, request.mutable_sparse_query());
    request.set_sparse_weight(1.0f);
    request.set_top_k(top_k);
    return request;
  }
};

TEST_CASE_FIXTURE(SparseSearchTest, "SparseSearchRanksByDotProduct") {
  SUBCASE("single dimension") {
    // Dim 0: doc 1 has 1.0, doc 4 has 0.9, no other doc has it.
    proto::HybridSearchResponse response;
    auto status = HybridSearch(SparseOnly({0}, {1.0f}, 5), &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_GE(response.results_size(), 2);
    CHECK_EQ(response.results(0).id(), 1);
    CHECK_EQ(response.results(1).id(), 4);
    CHECK(response.results(0).distance() > response.results(1).distance());
    for (int i = 2; i < response.results_size(); ++i) {
      CHECK(response.results(i).distance() < response.results(1).distance());
    }
  }

  SUBCASE("several dimensions") {
    // Dims 5 and 10: doc 1 = 0.8 + 0.3 = 1.1, doc 5 = 0.6, doc 3 = 0.5.
    proto::HybridSearchResponse response;
    auto status = HybridSearch(SparseOnly({5, 10}, {1.0f, 1.0f}, 5), &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_GE(response.results_size(), 3);
    CHECK_EQ(response.results(0).id(), 1);
    CHECK_EQ(response.results(1).id(), 5);
    CHECK_EQ(response.results(2).id(), 3);
    CHECK(response.results(0).distance() > response.results(1).distance());
    CHECK(response.results(1).distance() > response.results(2).distance());
    for (int i = 3; i < response.results_size(); ++i) {
      CHECK(response.results(i).distance() < response.results(2).distance());
    }
  }
}

TEST_CASE_FIXTURE(SearchServiceTest, "SparseSearchOrderOnALargerCorpus") {
  // 60 documents with sparse vectors where dim 0 carries a distinct weight,
  // plus 10 documents with no sparse vector at all, in one insert batch.
  constexpr const char* kName = "sparse_large";
  CreateCollection(kName, 4);

  proto::InsertRequest request;
  request.set_collection_name(kName);
  for (uint64_t id = 1; id <= 70; ++id) {
    auto* v = request.add_vectors();
    v->set_id(id);
    float f = static_cast<float>(id);
    SetProtoVector({f, -f, 0.5f * f, 1.0f}, v->mutable_vector());
    SetStringField(v->mutable_metadata(), "name", "d" + std::to_string(id));
    if (id <= 60) {
      SetSparse({0, static_cast<uint32_t>(100 + id)}, {0.01f * static_cast<float>(id), 1.0f},
                v->mutable_sparse_vector());
    }
  }
  proto::InsertResponse insert_response;
  auto status = Insert(request, &insert_response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  REQUIRE_EQ(insert_response.inserted_count(), 70);

  proto::HybridSearchRequest search;
  search.set_collection_name(kName);
  SetSparse({0}, {1.0f}, search.mutable_sparse_query());
  search.set_sparse_weight(1.0f);
  search.set_top_k(10);
  proto::HybridSearchResponse response;
  status = HybridSearch(search, &response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  REQUIRE_EQ(response.results_size(), 10);

  // Dot product with dim 0 is 0.01 * id, so the order is 60, 59, ..., 51.
  for (int i = 0; i < 10; ++i) {
    CHECK_EQ(response.results(i).id(), static_cast<uint64_t>(60 - i));
  }
  CheckScoresNonIncreasing(response.results());

  // A dimension held by one document finds exactly that document first.
  proto::HybridSearchRequest single;
  single.set_collection_name(kName);
  SetSparse({142}, {1.0f}, single.mutable_sparse_query());
  single.set_sparse_weight(1.0f);
  single.set_top_k(3);
  proto::HybridSearchResponse single_response;
  status = HybridSearch(single, &single_response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  REQUIRE_GE(single_response.results_size(), 1);
  CHECK_EQ(single_response.results(0).id(), 42);
  for (int i = 1; i < single_response.results_size(); ++i) {
    CHECK(single_response.results(i).distance() < single_response.results(0).distance());
  }
}

TEST_CASE_FIXTURE(SparseSearchTest, "DenseTextSparseHybridRanksTheBestOverallMatchFirst") {
  // Doc 1 is nearest the vector, matches both text terms, and has the
  // largest sparse dot product (1.0 + 0.8 * 0.8).
  proto::HybridSearchRequest request;
  request.set_collection_name(kCollection);
  SetProtoVector({1, 0, 0, 0}, request.mutable_query_vector());
  request.set_text_query("running shoes");
  request.set_text_field("text");
  SetSparse({0, 5}, {1.0f, 0.8f}, request.mutable_sparse_query());
  request.set_top_k(5);
  request.set_vector_weight(0.4f);
  request.set_text_weight(0.3f);
  request.set_sparse_weight(0.3f);
  request.set_return_metadata(true);

  proto::HybridSearchResponse response;
  auto status = HybridSearch(request, &response);
  INFO(status.error_message());
  REQUIRE(status.ok());
  REQUIRE_EQ(response.results_size(), 5);
  CHECK_EQ(response.results(0).id(), 1);
  CHECK(response.results(0).distance() > response.results(1).distance());
  CheckScoresNonIncreasing(response.results());
  REQUIRE(response.results(0).has_metadata());
  CHECK_EQ(response.results(0).metadata().fields().at("text").string_value(),
           "running shoes for marathon athletes");
}

TEST_CASE_FIXTURE(SparseSearchTest, "DenseSparseHybridWeightsDecideBetweenSignals") {
  // The vector points at doc 4; sparse dim 2 is held only by doc 3.
  auto make = [](float vector_weight, float sparse_weight) {
    proto::HybridSearchRequest request;
    request.set_collection_name(kCollection);
    SetProtoVector({0, 0, 0, 1}, request.mutable_query_vector());
    SetSparse({2}, {1.0f}, request.mutable_sparse_query());
    request.set_top_k(5);
    request.set_vector_weight(vector_weight);
    request.set_sparse_weight(sparse_weight);
    return request;
  };

  SUBCASE("sparse weight dominates") {
    proto::HybridSearchResponse response;
    auto status = HybridSearch(make(0.01f, 1.0f), &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_GE(response.results_size(), 2);
    CHECK_EQ(response.results(0).id(), 3);
    CheckScoresNonIncreasing(response.results());
  }

  SUBCASE("vector weight dominates") {
    proto::HybridSearchResponse response;
    auto status = HybridSearch(make(1.0f, 0.01f), &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_GE(response.results_size(), 2);
    CHECK_EQ(response.results(0).id(), 4);
    CheckScoresNonIncreasing(response.results());
  }
}

TEST_CASE_FIXTURE(SearchServiceTest, "InsertRejectsMalformedSparseVectors") {
  CreateCollection("sparse_errors", 4);

  auto make = [](const std::vector<uint32_t>& indices, const std::vector<float>& values) {
    proto::InsertRequest request;
    request.set_collection_name("sparse_errors");
    auto* v = request.add_vectors();
    v->set_id(1);
    SetProtoVector({1, 0, 0, 0}, v->mutable_vector());
    SetStringField(v->mutable_metadata(), "text", "doc");
    auto* sparse = v->mutable_sparse_vector();
    for (auto i : indices) sparse->add_indices(i);
    for (auto x : values) sparse->add_values(x);
    return request;
  };

  SUBCASE("indices and values differ in length") {
    proto::InsertResponse response;
    CHECK_EQ(Insert(make({0, 1}, {1.0f}), &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("indices not sorted") {
    proto::InsertResponse response;
    CHECK_EQ(Insert(make({3, 1}, {1.0f, 1.0f}), &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("duplicate indices") {
    proto::InsertResponse response;
    CHECK_EQ(Insert(make({2, 2}, {1.0f, 1.0f}), &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }
}

// ============================================================================
// Search with metadata filters
// ============================================================================

namespace {

// One row of the filter corpus.
struct Product {
  std::vector<float> vector;
  int64_t price = 0;
  std::string brand;
  std::string category;
  double rating = 0.0;
  bool in_stock = false;
};

}  // namespace

// 100 products with an int64 price, string brand and category, double
// rating and bool in_stock. Category and in_stock vary independently of
// brand, so a filter that combines them selects rows that no single field
// selects.
class SearchFilterTest : public SearchServiceTest {
 public:
  static constexpr uint32_t kDim = 8;
  static constexpr int kCount = 100;
  static constexpr const char* kCollection = "filter_collection";

  SearchFilterTest() {
    CreateCollection(kCollection, kDim);

    const std::vector<std::string> brands = {"Nike", "Adidas", "Puma", "Reebok"};
    const std::vector<std::string> categories = {"Running", "Basketball", "Training", "Casual"};
    const auto vectors = test::RandomVectors(kCount, kDim, 4321);
    proto::InsertRequest request;
    request.set_collection_name(kCollection);
    for (int i = 0; i < kCount; ++i) {
      Product p;
      p.vector = ToFloats(vectors[static_cast<size_t>(i)]);
      p.price = 50 + 2 * i;
      p.brand = brands[static_cast<size_t>(i % 4)];
      p.category = categories[static_cast<size_t>((i / 2) % 4)];
      p.rating = 3.0 + static_cast<double>(i % 20) * 0.1;
      p.in_stock = i % 3 != 0;

      const auto id = static_cast<uint64_t>(i + 1);
      auto* v = request.add_vectors();
      v->set_id(id);
      SetProtoVector(p.vector, v->mutable_vector());
      auto& fields = *v->mutable_metadata()->mutable_fields();
      fields["price"].set_int_value(p.price);
      fields["brand"].set_string_value(p.brand);
      fields["category"].set_string_value(p.category);
      fields["rating"].set_double_value(p.rating);
      fields["in_stock"].set_bool_value(p.in_stock);
      products_[id] = std::move(p);
    }
    proto::InsertResponse response;
    auto status = Insert(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.inserted_count(), static_cast<uint64_t>(kCount));
  }

  // Brute-force evaluation of a filter: the ids of the rows that satisfy it.
  template <typename Predicate>
  std::set<uint64_t> Matching(Predicate predicate) const {
    std::set<uint64_t> ids;
    for (const auto& [id, p] : products_) {
      if (predicate(p)) ids.insert(id);
    }
    return ids;
  }

  // Runs a filtered Search with top_k equal to the corpus size and checks
  // that it returns exactly the expected ids, each with its exact L2
  // distance. The result order is not checked: QueryExecutor::MergeResults
  // returns filtered results farthest first.
  proto::SearchResponse SearchExpecting(const std::string& filter, bool return_metadata,
                                        const std::set<uint64_t>& expected) {
    // The filter must select some rows but not all of them.
    REQUIRE_FALSE(expected.empty());
    REQUIRE_LT(expected.size(), static_cast<size_t>(kCount));

    proto::SearchRequest request;
    request.set_collection_name(kCollection);
    SetProtoVector(kQuery, request.mutable_query_vector());
    request.set_top_k(kCount);
    request.set_filter(filter);
    request.set_return_metadata(return_metadata);
    proto::SearchResponse response;
    auto status = Search(request, &response);
    INFO(filter);
    INFO(status.error_message());
    REQUIRE(status.ok());

    auto ids = Ids(response.results());
    CHECK_EQ(ids.size(), expected.size());
    CHECK_EQ(std::set<uint64_t>(ids.begin(), ids.end()), expected);
    for (const auto& r : response.results()) {
      REQUIRE(products_.count(r.id()) == 1);
      CHECK(r.distance() == doctest::Approx(L2(kQuery, products_.at(r.id()).vector)).epsilon(1e-4));
    }
    return response;
  }

  // Checks that a result carries the full row of its id, each field in its
  // own proto value type.
  void CheckMetadata(const proto::SearchResultEntry& result) const {
    INFO("id " << result.id());
    REQUIRE(result.has_metadata());
    const auto& p = products_.at(result.id());
    const auto& fields = result.metadata().fields();
    CHECK_EQ(fields.size(), 5u);

    const auto& price = fields.at("price");
    CHECK_EQ(price.value_case(), proto::MetadataValue::kIntValue);
    CHECK_EQ(price.int_value(), p.price);

    const auto& brand = fields.at("brand");
    CHECK_EQ(brand.value_case(), proto::MetadataValue::kStringValue);
    CHECK_EQ(brand.string_value(), p.brand);

    const auto& category = fields.at("category");
    CHECK_EQ(category.value_case(), proto::MetadataValue::kStringValue);
    CHECK_EQ(category.string_value(), p.category);

    const auto& rating = fields.at("rating");
    CHECK_EQ(rating.value_case(), proto::MetadataValue::kDoubleValue);
    CHECK_EQ(rating.double_value(), p.rating);

    const auto& in_stock = fields.at("in_stock");
    CHECK_EQ(in_stock.value_case(), proto::MetadataValue::kBoolValue);
    CHECK_EQ(in_stock.bool_value(), p.in_stock);
  }

  static inline const std::vector<float> kQuery = {0.3f,  -0.1f, 0.2f,  0.0f,
                                                   -0.4f, 0.1f,  0.25f, -0.2f};
  std::unordered_map<uint64_t, Product> products_;
};

TEST_CASE_FIXTURE(SearchFilterTest, "SearchFilterReturnsExactlyTheMatchingRows") {
  SUBCASE("int64 comparison AND string equality") {
    const auto expected =
        Matching([](const Product& p) { return p.price < 150 && p.brand == "Nike"; });
    auto response = SearchExpecting("price < 150 AND brand = 'Nike'", true, expected);
    for (const auto& r : response.results()) CheckMetadata(r);
  }

  SUBCASE("nested AND and OR with bool and double comparisons") {
    const auto nike =
        Matching([](const Product& p) { return p.in_stock && p.brand == "Nike" && p.price < 100; });
    const auto adidas = Matching(
        [](const Product& p) { return p.in_stock && p.brand == "Adidas" && p.rating > 4.0; });
    // Each OR branch matches rows, and the bool term removes some rows that
    // would match a branch without it.
    REQUIRE_FALSE(nike.empty());
    REQUIRE_FALSE(adidas.empty());
    std::set<uint64_t> expected = nike;
    expected.insert(adidas.begin(), adidas.end());
    const auto without_stock = Matching([](const Product& p) {
      return (p.brand == "Nike" && p.price < 100) || (p.brand == "Adidas" && p.rating > 4.0);
    });
    REQUIRE_GT(without_stock.size(), expected.size());

    auto response = SearchExpecting(
        "in_stock = true AND ((brand = 'Nike' AND price < 100) OR "
        "(brand = 'Adidas' AND rating > 4.0))",
        true, expected);
    for (const auto& r : response.results()) CheckMetadata(r);
  }

  SUBCASE("LIKE with a leading wildcard") {
    const auto expected = Matching([](const Product& p) { return p.category.ends_with("ing"); });
    auto response = SearchExpecting("category LIKE '%ing'", true, expected);
    for (const auto& r : response.results()) CheckMetadata(r);
  }

  SUBCASE("IN list") {
    const auto expected =
        Matching([](const Product& p) { return p.brand == "Nike" || p.brand == "Adidas"; });
    auto response = SearchExpecting("brand IN ('Nike', 'Adidas')", true, expected);
    for (const auto& r : response.results()) CheckMetadata(r);
  }
}

TEST_CASE_FIXTURE(SearchFilterTest, "SearchFilterWithoutReturnMetadata") {
  const auto expected = Matching([](const Product& p) { return p.price < 100; });
  auto response = SearchExpecting("price < 100", false, expected);
  for (const auto& r : response.results()) CHECK_FALSE(r.has_metadata());
}

// ============================================================================
// Index types and metrics in local mode
// ============================================================================
//
// No case here reaches the segment size cap, so every segment stays GROWING
// and every search is a brute-force scan with the collection's metric. The
// index type is stored at create time and used only when a segment seals:
// these cases cover create, list, insert and search for each type, not the
// index implementations.

namespace {

// Points for the index matrix, searched with the query (1, 0, ..., 0).
// Id 1 is the nearest point under L2, id 2 under COSINE and id 3 under
// INNER_PRODUCT. Every other point has a first coordinate <= 0, so it is
// never the nearest point under any of the three metrics. A collection that
// searched with the wrong metric therefore returns the wrong first id.
std::unordered_map<uint64_t, std::vector<float>> MatrixPoints(uint32_t dimension, int count) {
  std::unordered_map<uint64_t, std::vector<float>> points;
  std::vector<float> l2_nearest(dimension, 0.0f);
  l2_nearest[0] = 1.0f;
  l2_nearest[1] = 0.25f;
  std::vector<float> cosine_nearest(dimension, 0.0f);
  cosine_nearest[0] = 3.0f;
  cosine_nearest[1] = 0.03f;
  std::vector<float> ip_nearest(dimension, 0.0f);
  ip_nearest[0] = 5.0f;
  ip_nearest[1] = 4.0f;
  points[1] = l2_nearest;
  points[2] = cosine_nearest;
  points[3] = ip_nearest;

  // Ids 4 to count, seeded with the dimension.
  const auto random = test::RandomVectors(static_cast<size_t>(count - 3),
                                          static_cast<core::Dimension>(dimension), dimension);
  for (int i = 4; i <= count; ++i) {
    std::vector<float> values = ToFloats(random[static_cast<size_t>(i - 4)]);
    values[0] = -std::fabs(values[0]);
    points[static_cast<uint64_t>(i)] = values;
  }
  return points;
}

}  // namespace

TEST_CASE_FIXTURE(SearchServiceTest, "EveryIndexTypeAndMetricCreatesInsertsAndSearches") {
  struct Case {
    const char* name;
    proto::CreateCollectionRequest::IndexType index_type;
    core::IndexType stored_index_type;
    proto::CreateCollectionRequest::MetricType metric;
    const char* metric_name;
    uint32_t dimension;
    uint64_t nearest_id;
  };
  using Req = proto::CreateCollectionRequest;
  const std::vector<Case> cases = {
      {"auto_l2", Req::AUTO, core::IndexType::AUTO, Req::L2, "L2", 8, 1},
      {"hnsw_cosine", Req::HNSW, core::IndexType::HNSW, Req::COSINE, "COSINE", 12, 2},
      {"ivf_turboquant_cosine", Req::IVF_TURBOQUANT, core::IndexType::IVF_TURBOQUANT, Req::COSINE,
       "COSINE", 16, 2},
      {"ivf_pq_ip", Req::IVF_PQ, core::IndexType::IVF_PQ, Req::INNER_PRODUCT, "INNER_PRODUCT", 16,
       3},
      {"ivf_sq_l2", Req::IVF_SQ, core::IndexType::IVF_SQ, Req::L2, "L2", 10, 1},
      {"ivf_flat_l2", Req::IVF_FLAT, core::IndexType::IVF_FLAT, Req::L2, "L2", 6, 1},
      {"flat_cosine", Req::FLAT, core::IndexType::FLAT, Req::COSINE, "COSINE", 8, 2},
      {"flat_ip", Req::FLAT, core::IndexType::FLAT, Req::INNER_PRODUCT, "INNER_PRODUCT", 4, 3},
  };
  constexpr int kPoints = 30;
  constexpr uint32_t kTopK = 50;

  // Create every collection, check the stored index type, and insert.
  std::unordered_map<std::string, std::unordered_map<uint64_t, std::vector<float>>> points;
  for (const auto& c : cases) {
    CAPTURE(c.name);
    const uint32_t collection_id = CreateCollection(c.name, c.dimension, c.metric, c.index_type);
    const auto segment_ids =
        segment_store_->GetCollectionSegments(core::MakeCollectionId(collection_id));
    REQUIRE_FALSE(segment_ids.empty());
    for (auto segment_id : segment_ids) {
      const auto* segment = segment_store_->GetSegment(segment_id);
      REQUIRE(segment != nullptr);
      CHECK_EQ(segment->GetIndexType(), c.stored_index_type);
    }

    points[c.name] = MatrixPoints(c.dimension, kPoints);
    proto::InsertRequest request;
    request.set_collection_name(c.name);
    for (const auto& [id, values] : points[c.name]) {
      auto* v = request.add_vectors();
      v->set_id(id);
      SetProtoVector(values, v->mutable_vector());
    }
    proto::InsertResponse response;
    auto status = Insert(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.inserted_count(), static_cast<uint64_t>(kPoints));
  }

  // ListCollections reports every collection with its own dimension, metric
  // and vector count.
  proto::ListCollectionsResponse list;
  auto list_status = ListCollections(&list);
  INFO(list_status.error_message());
  REQUIRE(list_status.ok());
  REQUIRE_EQ(static_cast<size_t>(list.collections_size()), cases.size());
  for (const auto& c : cases) {
    CAPTURE(c.name);
    const auto it = std::find_if(
        list.collections().begin(), list.collections().end(),
        [&c](const proto::CollectionInfo& info) { return info.collection_name() == c.name; });
    REQUIRE(it != list.collections().end());
    CHECK_EQ(it->dimension(), c.dimension);
    CHECK_EQ(it->metric_type(), c.metric_name);
    CHECK_EQ(it->vector_count(), static_cast<uint64_t>(kPoints));
  }

  // Search with top_k above the point count returns every point once,
  // ranked as a brute-force scan with the collection's metric ranks them.
  for (const auto& c : cases) {
    CAPTURE(c.name);
    const auto& coll_points = points.at(c.name);
    std::vector<float> query(c.dimension, 0.0f);
    query[0] = 1.0f;

    std::vector<std::pair<double, uint64_t>> reference;
    for (const auto& [id, values] : coll_points) {
      reference.emplace_back(ReferenceDistance(c.metric, query, values), id);
    }
    std::sort(reference.begin(), reference.end());
    REQUIRE_EQ(reference.front().second, c.nearest_id);

    proto::SearchRequest request;
    request.set_collection_name(c.name);
    SetProtoVector(query, request.mutable_query_vector());
    request.set_top_k(kTopK);
    proto::SearchResponse response;
    auto status = Search(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());

    REQUIRE_EQ(response.results_size(), kPoints);
    CHECK_EQ(response.results(0).id(), c.nearest_id);
    auto ids = Ids(response.results());
    std::set<uint64_t> expected;
    for (const auto& [id, values] : coll_points) expected.insert(id);
    CHECK_EQ(std::set<uint64_t>(ids.begin(), ids.end()), expected);
    CheckDistancesNonDecreasing(response.results());

    // The order agrees with the reference ranking. The tolerance allows
    // float rounding between near-equal distances.
    for (size_t i = 1; i < ids.size(); ++i) {
      INFO("position " << i);
      CHECK(ReferenceDistance(c.metric, query, coll_points.at(ids[i - 1])) <=
            ReferenceDistance(c.metric, query, coll_points.at(ids[i])) + 1e-5);
    }
    // L2 and COSINE distances are the metric's distance itself.
    if (c.metric != Req::INNER_PRODUCT) {
      for (const auto& r : response.results()) {
        CHECK(r.distance() ==
              doctest::Approx(ReferenceDistance(c.metric, query, coll_points.at(r.id())))
                  .epsilon(1e-4));
      }
    }
  }
}

// ============================================================================
// Concurrent Search with the query cache
// ============================================================================

class ConcurrentSearchTest : public SearchServiceTest {
 public:
  static constexpr uint32_t kDim = 16;
  static constexpr int kCount = 200;
  static constexpr int kBatchSize = 50;
  static constexpr const char* kCollection = "concurrent_collection";

  // As in the single-node server, Search results go through a query cache.
  ConcurrentSearchTest() : SearchServiceTest(std::make_shared<compute::QueryCache>(10000)) {
    CreateCollection(kCollection, kDim);

    const auto vectors = test::RandomVectors(kCount, kDim, 97);
    proto::InsertRequest request;
    request.set_collection_name(kCollection);
    for (uint64_t id = 1; id <= static_cast<uint64_t>(kCount); ++id) {
      const std::vector<float> values = ToFloats(vectors[id - 1]);
      points_[id] = values;

      auto* v = request.add_vectors();
      v->set_id(id);
      SetProtoVector(values, v->mutable_vector());
      SetStringField(v->mutable_metadata(), "name", "p" + std::to_string(id));
      (*v->mutable_metadata()->mutable_fields())["batch"].set_int_value(BatchOf(id));
    }
    proto::InsertResponse response;
    auto status = Insert(request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.inserted_count(), static_cast<uint64_t>(kCount));
  }

  static int64_t BatchOf(uint64_t id) {
    return static_cast<int64_t>((id - 1) / static_cast<uint64_t>(kBatchSize));
  }

  std::unordered_map<uint64_t, std::vector<float>> points_;
};

TEST_CASE_FIXTURE(ConcurrentSearchTest, "ConcurrentSearchesReturnEachQuerysOwnIdFirst") {
  constexpr int kThreads = 8;
  constexpr int kSearchesPerThread = 25;
  // Fewer distinct queries than searches: threads repeat each other's
  // queries, so some searches are served from the cache while others fill it.
  constexpr uint64_t kDistinctQueries = 40;

  // What one search saw, recorded in the worker thread and checked after
  // the join.
  struct Outcome {
    uint64_t query_id = 0;
    grpc::StatusCode code = grpc::StatusCode::UNKNOWN;
    std::string error;
    int result_count = 0;
    uint64_t first_id = 0;
    float first_distance = -1.0f;
    std::string first_name;
    bool first_batch_is_int = false;
    int64_t first_batch = -1;
  };

  std::vector<std::vector<Outcome>> outcomes(kThreads);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([this, t, &outcomes] {
      auto& mine = outcomes[static_cast<size_t>(t)];
      for (int j = 0; j < kSearchesPerThread; ++j) {
        Outcome o;
        // Ids 1, 6, ..., 196: every batch is queried.
        o.query_id = 1 + (static_cast<uint64_t>(t * 5 + j * 3) % kDistinctQueries) * 5;

        proto::SearchRequest request;
        request.set_collection_name(kCollection);
        SetProtoVector(points_.at(o.query_id), request.mutable_query_vector());
        request.set_top_k(10);
        request.set_return_metadata(true);

        proto::SearchResponse response;
        auto status = Search(request, &response);
        o.code = status.error_code();
        o.error = status.error_message();
        o.result_count = response.results_size();
        if (response.results_size() > 0) {
          const auto& first = response.results(0);
          o.first_id = first.id();
          o.first_distance = first.distance();
          const auto& fields = first.metadata().fields();
          if (auto it = fields.find("name"); it != fields.end()) {
            o.first_name = it->second.string_value();
          }
          if (auto it = fields.find("batch"); it != fields.end()) {
            o.first_batch_is_int = it->second.has_int_value();
            o.first_batch = it->second.int_value();
          }
        }
        mine.push_back(std::move(o));
      }
    });
  }
  for (auto& thread : threads) thread.join();

  for (size_t t = 0; t < outcomes.size(); ++t) {
    REQUIRE_EQ(outcomes[t].size(), static_cast<size_t>(kSearchesPerThread));
    for (const auto& o : outcomes[t]) {
      INFO("thread " << t << ", query id " << o.query_id);
      INFO(o.error);
      CHECK_EQ(o.code, grpc::StatusCode::OK);
      CHECK_EQ(o.result_count, 10);
      CHECK_EQ(o.first_id, o.query_id);
      CHECK(o.first_distance == doctest::Approx(0.0f));
      CHECK_EQ(o.first_name, "p" + std::to_string(o.query_id));
      CHECK(o.first_batch_is_int);
      CHECK_EQ(o.first_batch, BatchOf(o.query_id));
    }
  }
}
