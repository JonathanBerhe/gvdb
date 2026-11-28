// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Collection resolver tests. The contract cases run against the local
// resolver (a single node with its own segment store) and the coordinator
// resolver (the coordinator's collection registry), so the two cannot drift
// apart. The cases after the contract cover behavior that only one resolver
// has.

#include <doctest/doctest.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "cluster/coordinator/coordinator.h"
#include "cluster/coordinator/node_registry.h"
#include "cluster/shard_manager.h"
#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"
#include "service/vectordb/collection_resolver.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_manager.h"
#include "support/temp_dir.h"

namespace {

using namespace gvdb;

// A single node: the resolver reads and writes its own segment store.
struct LocalResolverFixture {
  LocalResolverFixture() {
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>(test_dir_, index_factory_.get());
    resolver_ = service::MakeLocalResolver(segment_store_);
  }

  ~LocalResolverFixture() {
    resolver_.reset();
    segment_store_.reset();
    index_factory_.reset();
  }

  service::ICollectionResolver& resolver() { return *resolver_; }

  // Removed after the destructor body has reset everything that uses it.
  test::ScopedTempDir temp_dir_{"gvdb_local_resolver_test"};
  std::string test_dir_ = temp_dir_.str();
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::unique_ptr<service::ICollectionResolver> resolver_;
};

// The coordinator: the resolver uses the coordinator's collection registry.
// The coordinator has no client factory, so it creates no segments on data
// nodes.
struct CoordinatorResolverFixture {
  CoordinatorResolverFixture() {
    shard_manager_ = std::make_shared<cluster::ShardManager>(16, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30));
    coordinator_ = std::make_shared<cluster::Coordinator>(shard_manager_, node_registry_);

    // CreateCollection needs a ready data node to assign shards to.
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(1);
    proto_node.set_grpc_address("localhost:50099");
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node.set_memory_total_bytes(1000000000);
    proto_node.set_memory_used_bytes(0);
    proto_node.set_disk_total_bytes(10000000000);
    proto_node.set_disk_used_bytes(0);
    node_registry_->UpdateNode(proto_node);

    resolver_ = service::MakeCoordinatorResolver(coordinator_);
  }

  ~CoordinatorResolverFixture() {
    resolver_.reset();
    coordinator_.reset();
    node_registry_.reset();
    shard_manager_.reset();
  }

  service::ICollectionResolver& resolver() { return *resolver_; }

  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
  std::unique_ptr<service::ICollectionResolver> resolver_;
};

}  // namespace

TYPE_TO_STRING_AS("local", LocalResolverFixture);
TYPE_TO_STRING_AS("coordinator", CoordinatorResolverFixture);

namespace {

// ============================================================================
// Contract: every resolver
// ============================================================================

#define RESOLVER_FIXTURES LocalResolverFixture, CoordinatorResolverFixture

TEST_CASE_TEMPLATE("CollectionResolver contract: CreateCollection then GetCollectionId", F,
                   RESOLVER_FIXTURES) {
  F f;
  auto created =
      f.resolver().CreateCollection("vectors", 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(created.ok());
  CHECK_NE(*created, core::kInvalidCollectionId);

  auto id = f.resolver().GetCollectionId("vectors");
  REQUIRE(id.ok());
  CHECK_EQ(*id, *created);
}

TEST_CASE_TEMPLATE("CollectionResolver contract: CreateCollection rejects a duplicate name", F,
                   RESOLVER_FIXTURES) {
  F f;
  REQUIRE(f.resolver()
              .CreateCollection("vectors", 128, core::MetricType::L2, core::IndexType::FLAT)
              .ok());

  auto second =
      f.resolver().CreateCollection("vectors", 64, core::MetricType::COSINE, core::IndexType::HNSW);
  REQUIRE_FALSE(second.ok());
  CHECK_EQ(second.status().code(), absl::StatusCode::kAlreadyExists);
}

TEST_CASE_TEMPLATE("CollectionResolver contract: GetCollectionId of an unknown name is NotFound", F,
                   RESOLVER_FIXTURES) {
  F f;
  auto id = f.resolver().GetCollectionId("nonexistent");
  REQUIRE_FALSE(id.ok());
  CHECK_EQ(id.status().code(), absl::StatusCode::kNotFound);
}

TEST_CASE_TEMPLATE("CollectionResolver contract: DropCollection removes the collection", F,
                   RESOLVER_FIXTURES) {
  F f;
  REQUIRE(f.resolver()
              .CreateCollection("drop_me", 32, core::MetricType::L2, core::IndexType::FLAT)
              .ok());

  CHECK(f.resolver().DropCollection("drop_me").ok());

  auto id = f.resolver().GetCollectionId("drop_me");
  REQUIRE_FALSE(id.ok());
  CHECK_EQ(id.status().code(), absl::StatusCode::kNotFound);
}

TEST_CASE_TEMPLATE("CollectionResolver contract: DropCollection of an unknown name is NotFound", F,
                   RESOLVER_FIXTURES) {
  F f;
  auto status = f.resolver().DropCollection("no_such_collection");
  REQUIRE_FALSE(status.ok());
  CHECK_EQ(status.code(), absl::StatusCode::kNotFound);
}

TEST_CASE_TEMPLATE("CollectionResolver contract: ListCollections returns every collection", F,
                   RESOLVER_FIXTURES) {
  F f;
  CHECK(f.resolver().ListCollections().empty());

  REQUIRE(
      f.resolver().CreateCollection("col_a", 64, core::MetricType::L2, core::IndexType::FLAT).ok());
  REQUIRE(f.resolver()
              .CreateCollection("col_b", 128, core::MetricType::COSINE, core::IndexType::HNSW)
              .ok());

  auto list = f.resolver().ListCollections();
  CHECK_EQ(list.size(), 2);

  // The order is unspecified, so the entries are found by name.
  bool found_a = false;
  bool found_b = false;
  for (const auto& info : list) {
    if (info.collection_name == "col_a") {
      found_a = true;
      CHECK_EQ(info.dimension, 64);
      CHECK_EQ(info.metric_type, core::MetricType::L2);
    }
    if (info.collection_name == "col_b") {
      found_b = true;
      CHECK_EQ(info.dimension, 128);
      CHECK_EQ(info.metric_type, core::MetricType::COSINE);
    }
  }
  CHECK(found_a);
  CHECK(found_b);
}

TEST_CASE_TEMPLATE("CollectionResolver contract: CollectionCount follows create and drop", F,
                   RESOLVER_FIXTURES) {
  F f;
  CHECK_EQ(f.resolver().CollectionCount(), 0);

  REQUIRE(
      f.resolver().CreateCollection("c1", 32, core::MetricType::L2, core::IndexType::FLAT).ok());
  CHECK_EQ(f.resolver().CollectionCount(), 1);

  REQUIRE(
      f.resolver().CreateCollection("c2", 32, core::MetricType::L2, core::IndexType::FLAT).ok());
  CHECK_EQ(f.resolver().CollectionCount(), 2);

  REQUIRE(f.resolver().DropCollection("c1").ok());
  CHECK_EQ(f.resolver().CollectionCount(), 1);
}

#undef RESOLVER_FIXTURES

// ============================================================================
// Local resolver only
// ============================================================================

TEST_CASE_FIXTURE(LocalResolverFixture, "Local_DropAndRecreateGetsNewIdAndParams") {
  constexpr size_t kOldShards = 2;
  auto old_id = resolver_->CreateCollection("recreated", 128, core::MetricType::L2,
                                            core::IndexType::FLAT, kOldShards);
  REQUIRE(old_id.ok());
  auto old_segments = resolver_->GetSegmentIds("recreated");
  REQUIRE(old_segments.ok());
  REQUIRE_EQ(old_segments->size(), kOldShards);

  // Give the old collection data, so a leak into the new one would show up
  // in its vector count.
  std::vector<core::Vector> vectors = {core::Vector(128), core::Vector(128)};
  std::vector<core::VectorId> ids = {core::MakeVectorId(1), core::MakeVectorId(2)};
  REQUIRE(segment_store_->WriteVectors((*old_segments)[0], vectors, ids).ok());

  REQUIRE(resolver_->DropCollection("recreated").ok());

  auto new_id = resolver_->CreateCollection("recreated", 1024, core::MetricType::COSINE,
                                            core::IndexType::HNSW);
  REQUIRE(new_id.ok());
  CHECK_NE(*new_id, *old_id);

  auto looked_up = resolver_->GetCollectionId("recreated");
  REQUIRE(looked_up.ok());
  CHECK_EQ(*looked_up, *new_id);

  auto list = resolver_->ListCollections();
  REQUIRE_EQ(list.size(), 1);
  CHECK_EQ(list[0].collection_id, *new_id);
  CHECK_EQ(list[0].collection_name, "recreated");
  CHECK_EQ(list[0].dimension, 1024);
  CHECK_EQ(list[0].metric_type, core::MetricType::COSINE);
  CHECK_EQ(list[0].index_type, core::IndexType::HNSW);
  CHECK_EQ(list[0].vector_count, 0);

  // The old segments are gone from the store, and the new collection owns
  // only its own segment, created with the new parameters.
  for (const auto& old_segment : *old_segments) {
    CHECK_EQ(segment_store_->GetSegment(old_segment), nullptr);
  }
  CHECK(segment_store_->GetCollectionSegments(*old_id).empty());

  auto new_segments = resolver_->GetSegmentIds("recreated");
  REQUIRE(new_segments.ok());
  REQUIRE_EQ(new_segments->size(), 1);
  CHECK_EQ((*new_segments)[0], core::ShardSegmentId(*new_id, 0));
  CHECK_EQ(segment_store_->GetSegmentCount(), 1);

  const auto* new_segment = segment_store_->GetSegment((*new_segments)[0]);
  REQUIRE_NE(new_segment, nullptr);
  CHECK_EQ(new_segment->GetCollectionId(), *new_id);
  CHECK_EQ(new_segment->GetDimension(), 1024);
  CHECK_EQ(new_segment->GetMetric(), core::MetricType::COSINE);
  CHECK_EQ(new_segment->GetIndexType(), core::IndexType::HNSW);
  CHECK_EQ(new_segment->GetVectorCount(), 0);
}

TEST_CASE_FIXTURE(LocalResolverFixture, "Local_GetSegmentIdsMultiShard") {
  auto created = resolver_->CreateCollection("multi_shard", 64, core::MetricType::L2,
                                             core::IndexType::FLAT, 3);
  REQUIRE(created.ok());

  auto segs = resolver_->GetSegmentIds("multi_shard");
  REQUIRE(segs.ok());
  CHECK_EQ(segs->size(), 3);

  // Each segment ID is ShardSegmentId(collection_id, shard_index).
  for (uint32_t i = 0; i < 3; ++i) {
    CHECK_EQ((*segs)[i], core::ShardSegmentId(*created, i));
  }
}

TEST_CASE_FIXTURE(LocalResolverFixture, "Local_GetCoordinatorStubIsNull") {
  CHECK_EQ(resolver_->GetCoordinatorStub(), nullptr);
}

// ============================================================================
// Coordinator resolver only
// ============================================================================

TEST_CASE_FIXTURE(CoordinatorResolverFixture, "Coord_GetSegmentIdsUnimplemented") {
  REQUIRE(
      resolver_->CreateCollection("no_data", 64, core::MetricType::L2, core::IndexType::FLAT).ok());

  auto segs = resolver_->GetSegmentIds("no_data");
  REQUIRE_FALSE(segs.ok());
  CHECK_EQ(segs.status().code(), absl::StatusCode::kUnimplemented);
}

}  // namespace
