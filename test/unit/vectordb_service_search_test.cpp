// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/collection_resolver.h"
#include "network/vectordb_service.h"
#include "storage/segment_manager.h"

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

void SetStringField(proto::Metadata* metadata, const std::string& key,
                    const std::string& value) {
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

std::vector<uint64_t> Ids(const google::protobuf::RepeatedPtrField<proto::SearchResultEntry>& results) {
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

std::string UniqueSuffix() {
  static std::atomic<uint64_t> counter{0};
  std::random_device rd;
  auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  return std::to_string(now) + "_" + std::to_string(rd()) + "_" +
         std::to_string(counter.fetch_add(1));
}

}  // namespace

// ============================================================================
// Fixture: VectorDBService in local (single-node) mode
// ============================================================================

class SearchServiceTest {
 public:
  SearchServiceTest() {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("gvdb_service_search_test_" + UniqueSuffix());
    std::filesystem::create_directories(test_dir_);

    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>(
        test_dir_.string(), index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_store_.get());

    auto resolver = network::MakeLocalResolver(segment_store_);
    service_ = std::make_unique<network::VectorDBService>(
        segment_store_, query_executor_, std::move(resolver));
  }

  ~SearchServiceTest() {
    service_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();

    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }

  void CreateCollection(const std::string& name, uint32_t dimension) {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name(name);
    request.set_dimension(dimension);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
  }

  grpc::Status Insert(const proto::InsertRequest& request,
                      proto::InsertResponse* response) {
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

  grpc::Status Search(const proto::SearchRequest& request,
                      proto::SearchResponse* response) {
    grpc::ServerContext context;
    return service_->Search(&context, &request, response);
  }

  std::filesystem::path test_dir_;
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<network::VectorDBService> service_;
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

  RangeSearchTest() {
    CreateCollection(kCollection, kDim);

    std::mt19937 rng(1234);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    proto::InsertRequest request;
    request.set_collection_name(kCollection);
    for (int i = 1; i <= kCount; ++i) {
      std::vector<float> values(kDim);
      for (auto& x : values) x = dist(rng);
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

  // All points sorted by exact L2 distance to the query.
  std::vector<std::pair<double, uint64_t>> SortedByDistance(
      const std::vector<float>& query) const {
    std::vector<std::pair<double, uint64_t>> out;
    for (const auto& [id, values] : points_) out.emplace_back(L2(query, values), id);
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

  std::unordered_map<uint64_t, std::vector<float>> points_;
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
    CHECK(r.distance() ==
          doctest::Approx(L2(query, points_.at(r.id()))).epsilon(1e-4));
    REQUIRE(r.has_metadata());
    CHECK_EQ(r.metadata().fields().at("name").string_value(),
             "v" + std::to_string(r.id()));
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
  CHECK(response.results(0).distance() ==
        doctest::Approx(sorted[0].first).epsilon(1e-4));

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
    CHECK_EQ(RangeSearch(request, &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("missing query vector") {
    auto request = MakeRequest(query, 1.0f);
    request.clear_query_vector();
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("zero radius") {
    auto request = MakeRequest(query, 0.0f);
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("negative radius") {
    auto request = MakeRequest(query, -1.0f);
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("query vector whose dimension field disagrees with its values") {
    auto request = MakeRequest(query, 1.0f);
    request.mutable_query_vector()->set_dimension(kDim + 1);
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("unknown collection") {
    auto request = MakeRequest(query, 1.0f);
    request.set_collection_name("no_such_collection");
    proto::RangeSearchResponse response;
    CHECK_EQ(RangeSearch(request, &response).error_code(),
             grpc::StatusCode::NOT_FOUND);
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
  CHECK(search_response.results(1).distance() ==
        doctest::Approx(std::sqrt(0.02f)).epsilon(1e-4));
  CHECK(search_response.results(2).distance() ==
        doctest::Approx(std::sqrt(2.0f)).epsilon(1e-4));
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
    CHECK_EQ(HybridSearch(request, &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("unknown collection") {
    auto request = MakeRequest();
    request.set_collection_name("no_such_collection");
    request.set_text_query("running");
    request.set_top_k(5);
    proto::HybridSearchResponse response;
    CHECK_EQ(HybridSearch(request, &response).error_code(),
             grpc::StatusCode::NOT_FOUND);
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
    CHECK_EQ(HybridSearch(request, &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("sparse query with mismatched indices and values") {
    auto request = MakeRequest();
    request.mutable_sparse_query()->add_indices(0);
    request.mutable_sparse_query()->add_indices(1);
    request.mutable_sparse_query()->add_values(1.0f);
    request.set_sparse_weight(1.0f);
    request.set_top_k(5);
    proto::HybridSearchResponse response;
    CHECK_EQ(HybridSearch(request, &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
  }

  SUBCASE("sparse query with unsorted indices") {
    auto request = MakeRequest();
    SetSparse({5, 0}, {1.0f, 1.0f}, request.mutable_sparse_query());
    request.set_sparse_weight(1.0f);
    request.set_top_k(5);
    proto::HybridSearchResponse response;
    CHECK_EQ(HybridSearch(request, &response).error_code(),
             grpc::StatusCode::INVALID_ARGUMENT);
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
                                        const std::vector<float>& values,
                                        uint32_t top_k) const {
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
      SetSparse({0, static_cast<uint32_t>(100 + id)},
                {0.01f * static_cast<float>(id), 1.0f}, v->mutable_sparse_vector());
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
