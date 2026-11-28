// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// ReadPath without a gRPC server. The handler tests in
// vectordb_service_search_test.cpp and network_test.cpp cover the results;
// this file covers the query counters that GetStats reports and the paging
// of ListVectors.

#include <doctest/doctest.h>

#include <memory>
#include <vector>

#include "compute/query_executor.h"
#include "core/types.h"
#include "index/index_factory.h"
#include "rpc/proto_conversions.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/read_path.h"
#include "service/vectordb/segment_locator.h"
#include "storage/segment/segment_manager.h"
#include "support/temp_dir.h"
#include "support/vectors.h"

namespace {

using namespace gvdb;

constexpr core::Dimension kDimension = 4;
constexpr size_t kVectorCount = 25;

// A single-node store with one collection of one shard that holds the
// vectors 1..25.
struct ReadPathFixture {
  ReadPathFixture() {
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ =
        std::make_shared<storage::SegmentManager>(temp_dir_.str(), index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(*segment_store_);
    resolver_ = service::MakeLocalResolver(segment_store_);
    REQUIRE(
        resolver_->CreateCollection("c", kDimension, core::MetricType::L2, core::IndexType::FLAT)
            .ok());
    auto segments = resolver_->GetSegmentIds("c");
    REQUIRE(segments.ok());
    vectors_ = test::RandomVectors(kVectorCount, kDimension, 7);
    REQUIRE(
        segment_store_->WriteVectors((*segments)[0], vectors_, test::SequentialIds(kVectorCount))
            .ok());
    locator_ = std::make_unique<service::SegmentLocator>(segment_store_, *resolver_);
    read_path_ =
        std::make_unique<service::ReadPath>(segment_store_, query_executor_, *resolver_, *locator_);
  }

  proto::Vector Query() const {
    proto::Vector query;
    rpc::ToProto(vectors_[0], &query);
    return query;
  }

  test::ScopedTempDir temp_dir_{"gvdb_read_path_test"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<service::ICollectionResolver> resolver_;
  std::vector<core::Vector> vectors_;
  std::unique_ptr<service::SegmentLocator> locator_;
  std::unique_ptr<service::ReadPath> read_path_;
};

TEST_CASE_FIXTURE(ReadPathFixture, "ReadPath counts searches and range searches") {
  CHECK_EQ(read_path_->total_queries(), 0);

  proto::SearchRequest search;
  search.set_collection_name("c");
  *search.mutable_query_vector() = Query();
  search.set_top_k(3);
  for (int i = 0; i < 2; ++i) {
    proto::SearchResponse response;
    auto status = read_path_->Search(search, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.results_size(), 3);
    CHECK_EQ(response.results(0).id(), 1);
  }

  proto::RangeSearchRequest range;
  range.set_collection_name("c");
  *range.mutable_query_vector() = Query();
  range.set_radius(1e-3F);
  proto::RangeSearchResponse range_response;
  REQUIRE(read_path_->RangeSearch(range, &range_response).ok());
  CHECK_EQ(range_response.results_size(), 1);

  // A failed search is not counted.
  search.set_collection_name("missing");
  proto::SearchResponse missing;
  CHECK_FALSE(read_path_->Search(search, &missing).ok());

  CHECK_EQ(read_path_->total_queries(), 3);
}

TEST_CASE_FIXTURE(ReadPathFixture, "ReadPath::ListVectors pages through the collection") {
  struct Row {
    uint32_t limit;
    uint64_t offset;
    int expected_count;
    bool has_more;
  };
  const Row rows[] = {
      {0, 0, 20, true},    // the default limit is 20
      {10, 20, 5, false},  // the last page is short
      {10, 30, 0, false},  // an offset past the end gives an empty page
  };
  for (const Row& row : rows) {
    CAPTURE(row.limit);
    CAPTURE(row.offset);
    proto::ListVectorsRequest request;
    request.set_collection_name("c");
    request.set_limit(row.limit);
    request.set_offset(row.offset);
    proto::ListVectorsResponse response;
    REQUIRE(read_path_->ListVectors(request, &response).ok());
    CHECK_EQ(response.total_count(), kVectorCount);
    CHECK_EQ(response.vectors_size(), row.expected_count);
    CHECK_EQ(response.has_more(), row.has_more);
  }
}

}  // namespace
