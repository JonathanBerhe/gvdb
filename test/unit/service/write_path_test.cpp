// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// WritePath without a gRPC server. The handler tests in
// vectordb_service_write_test.cpp and network_test.cpp cover the writes; this
// file covers the write gate, which only the integration tests reached
// before.

#include <doctest/doctest.h>

#include <memory>
#include <string>

#include "cluster/datanode/primary_term_tracker.h"
#include "cluster/datanode/shard_write_gate.h"
#include "compute/query_executor.h"
#include "core/types.h"
#include "index/index_factory.h"
#include "rpc/primary_term_header.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/segment_locator.h"
#include "service/vectordb/write_path.h"
#include "storage/segment/segment_manager.h"
#include "support/temp_dir.h"

namespace {

using namespace gvdb;

// A single-node store with one collection of one shard, so every vector id
// maps to shard 0.
struct WritePathFixture {
  WritePathFixture() {
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ =
        std::make_shared<storage::SegmentManager>(temp_dir_.str(), index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(*segment_store_);
    resolver_ = service::MakeLocalResolver(segment_store_);
    REQUIRE(resolver_->CreateCollection("c", 4, core::MetricType::L2, core::IndexType::FLAT).ok());
    locator_ = std::make_unique<service::SegmentLocator>(segment_store_, *resolver_);
  }

  service::WritePath MakeWritePath(cluster::PrimaryTermTracker* tracker,
                                   cluster::ShardWriteGate* gate) {
    return {segment_store_, query_executor_, *resolver_, *locator_, tracker, gate};
  }

  test::ScopedTempDir temp_dir_{"gvdb_write_path_test"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<service::ICollectionResolver> resolver_;
  std::unique_ptr<service::SegmentLocator> locator_;
};

enum class Tracker { kNone, kEmpty, kPrimaryAt5, kNotPrimaryAt7 };

TEST_CASE_FIXTURE(WritePathFixture, "WritePath::CheckGate decisions") {
  const rpc::PrimaryTermFromHeader no_header{};
  const rpc::PrimaryTermFromHeader malformed{.has_term = true, .parse_error = true, .term = 0};
  auto term = [](uint64_t t) { return rpc::PrimaryTermFromHeader{.has_term = true, .term = t}; };

  struct Row {
    const char* name;
    Tracker tracker;
    bool freeze_gate;  // a freeze gate is plumbed in
    bool frozen;       // shard 0 is frozen for a backup
    const char* collection;
    rpc::PrimaryTermFromHeader header;
    grpc::StatusCode expected;
  };
  const Row rows[] = {
      {"no tracker and no freeze gate", Tracker::kNone, false, false, "c", term(1),
       grpc::StatusCode::OK},
      {"freeze gate, shard not frozen", Tracker::kNone, true, false, "c", no_header,
       grpc::StatusCode::OK},
      {"freeze gate, shard frozen", Tracker::kNone, true, true, "c", no_header,
       grpc::StatusCode::RESOURCE_EXHAUSTED},
      {"the freeze wins over a valid term", Tracker::kPrimaryAt5, true, true, "c", term(5),
       grpc::StatusCode::RESOURCE_EXHAUSTED},
      {"unknown collection defers to the handler", Tracker::kPrimaryAt5, false, false, "missing",
       term(4), grpc::StatusCode::OK},
      {"no term header is accepted", Tracker::kPrimaryAt5, false, false, "c", no_header,
       grpc::StatusCode::OK},
      {"malformed term header", Tracker::kPrimaryAt5, false, false, "c", malformed,
       grpc::StatusCode::INVALID_ARGUMENT},
      {"primary at the request term", Tracker::kPrimaryAt5, false, false, "c", term(5),
       grpc::StatusCode::OK},
      {"stale term", Tracker::kPrimaryAt5, false, false, "c", term(4), grpc::StatusCode::ABORTED},
      {"not the primary", Tracker::kNotPrimaryAt7, false, false, "c", term(7),
       grpc::StatusCode::ABORTED},
      {"unknown shard", Tracker::kEmpty, false, false, "c", term(3),
       grpc::StatusCode::FAILED_PRECONDITION},
  };

  for (const Row& row : rows) {
    CAPTURE(row.name);
    cluster::PrimaryTermTracker tracker;
    if (row.tracker == Tracker::kPrimaryAt5) REQUIRE(tracker.RecordPrimary(0, 5));
    if (row.tracker == Tracker::kNotPrimaryAt7) REQUIRE(tracker.RecordNotPrimary(0, 7));
    cluster::ShardWriteGate gate;
    if (row.frozen) REQUIRE(gate.Freeze(core::MakeShardId(0), "backup", 60'000));

    auto write_path = MakeWritePath(row.tracker == Tracker::kNone ? nullptr : &tracker,
                                    row.freeze_gate ? &gate : nullptr);
    auto status = write_path.CheckGate(row.header, row.collection, /*sample_vector_id=*/42);
    CHECK_EQ(status.error_code(), row.expected);
  }
}

}  // namespace
