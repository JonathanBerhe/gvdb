// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include <string>
#include <vector>

#include "network/collection_resolver.h"
#include "storage/segment.h"
#include "storage/segment_manager.h"
#include "index/index_factory.h"
#include "cluster/coordinator.h"
#include "cluster/node_registry.h"
#include "cluster/shard_manager.h"
#include "core/types.h"
#include "core/vector.h"
#include "support/temp_dir.h"

using namespace gvdb;

// ============================================================================
// Fixture A: LocalResolverTest
// ============================================================================

struct LocalResolverTest {
  LocalResolverTest() {
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>(
        test_dir_, index_factory_.get());
    resolver_ = network::MakeLocalResolver(segment_store_);
  }

  ~LocalResolverTest() {
    resolver_.reset();
    segment_store_.reset();
    index_factory_.reset();
  }

  // Removed after the destructor body has reset everything that uses it.
  test::ScopedTempDir temp_dir_{"gvdb_local_resolver_test"};
  std::string test_dir_ = temp_dir_.str();
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::unique_ptr<network::ICollectionResolver> resolver_;
};

// ---------------------------------------------------------------------------
// Local: CreateCollection
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(LocalResolverTest, "Local_CreateCollection") {
  auto result = resolver_->CreateCollection(
      "vectors", 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(result.ok());
  CHECK_NE(*result, core::kInvalidCollectionId);
}

TEST_CASE_FIXTURE(LocalResolverTest, "Local_CreateCollectionDuplicate") {
  auto first = resolver_->CreateCollection(
      "vectors", 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(first.ok());

  auto second = resolver_->CreateCollection(
      "vectors", 64, core::MetricType::COSINE, core::IndexType::HNSW);
  CHECK_FALSE(second.ok());
  CHECK_EQ(second.status().code(), absl::StatusCode::kAlreadyExists);
}

// ---------------------------------------------------------------------------
// Local: GetCollectionId
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(LocalResolverTest, "Local_GetCollectionIdSuccess") {
  auto created = resolver_->CreateCollection(
      "my_col", 64, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(created.ok());

  auto id = resolver_->GetCollectionId("my_col");
  REQUIRE(id.ok());
  CHECK_EQ(*id, *created);
}

TEST_CASE_FIXTURE(LocalResolverTest, "Local_GetCollectionIdUnknown") {
  auto id = resolver_->GetCollectionId("nonexistent");
  CHECK_FALSE(id.ok());
  CHECK_EQ(id.status().code(), absl::StatusCode::kNotFound);
}

// ---------------------------------------------------------------------------
// Local: DropCollection
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(LocalResolverTest, "Local_DropCollectionSuccess") {
  auto created = resolver_->CreateCollection(
      "drop_me", 32, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(created.ok());

  auto status = resolver_->DropCollection("drop_me");
  CHECK(status.ok());

  // Verify it is gone
  auto id = resolver_->GetCollectionId("drop_me");
  CHECK_FALSE(id.ok());
  CHECK_EQ(id.status().code(), absl::StatusCode::kNotFound);
}

TEST_CASE_FIXTURE(LocalResolverTest, "Local_DropCollectionUnknown") {
  auto status = resolver_->DropCollection("no_such_collection");
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.code(), absl::StatusCode::kNotFound);
}

TEST_CASE_FIXTURE(LocalResolverTest, "Local_DropAndRecreateGetsNewIdAndParams") {
  constexpr size_t kOldShards = 2;
  auto old_id = resolver_->CreateCollection(
      "recreated", 128, core::MetricType::L2, core::IndexType::FLAT,
      kOldShards);
  REQUIRE(old_id.ok());
  auto old_segments = resolver_->GetSegmentIds("recreated");
  REQUIRE(old_segments.ok());
  REQUIRE_EQ(old_segments->size(), kOldShards);

  // Give the old collection data, so a leak into the new one would show up
  // in its vector count.
  std::vector<core::Vector> vectors = {core::Vector(128), core::Vector(128)};
  std::vector<core::VectorId> ids = {core::MakeVectorId(1),
                                     core::MakeVectorId(2)};
  REQUIRE(segment_store_->WriteVectors((*old_segments)[0], vectors, ids).ok());

  REQUIRE(resolver_->DropCollection("recreated").ok());

  auto new_id = resolver_->CreateCollection(
      "recreated", 1024, core::MetricType::COSINE, core::IndexType::HNSW);
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
  CHECK_EQ((*new_segments)[0], cluster::ShardSegmentId(*new_id, 0));
  CHECK_EQ(segment_store_->GetSegmentCount(), 1);

  const auto* new_segment = segment_store_->GetSegment((*new_segments)[0]);
  REQUIRE_NE(new_segment, nullptr);
  CHECK_EQ(new_segment->GetCollectionId(), *new_id);
  CHECK_EQ(new_segment->GetDimension(), 1024);
  CHECK_EQ(new_segment->GetMetric(), core::MetricType::COSINE);
  CHECK_EQ(new_segment->GetIndexType(), core::IndexType::HNSW);
  CHECK_EQ(new_segment->GetVectorCount(), 0);
}

// ---------------------------------------------------------------------------
// Local: ListCollections
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(LocalResolverTest, "Local_ListCollectionsEmpty") {
  auto list = resolver_->ListCollections();
  CHECK(list.empty());
}

TEST_CASE_FIXTURE(LocalResolverTest, "Local_ListCollectionsMultiple") {
  (void)resolver_->CreateCollection(
      "col_a", 64, core::MetricType::L2, core::IndexType::FLAT);
  (void)resolver_->CreateCollection(
      "col_b", 128, core::MetricType::COSINE, core::IndexType::HNSW);

  auto list = resolver_->ListCollections();
  CHECK_EQ(list.size(), 2);

  // Verify contents (order is unspecified, so check by name)
  bool found_a = false, found_b = false;
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

// ---------------------------------------------------------------------------
// Local: CollectionCount
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(LocalResolverTest, "Local_CollectionCount") {
  CHECK_EQ(resolver_->CollectionCount(), 0);

  (void)resolver_->CreateCollection(
      "c1", 32, core::MetricType::L2, core::IndexType::FLAT);
  CHECK_EQ(resolver_->CollectionCount(), 1);

  (void)resolver_->CreateCollection(
      "c2", 32, core::MetricType::L2, core::IndexType::FLAT);
  CHECK_EQ(resolver_->CollectionCount(), 2);

  (void)resolver_->DropCollection("c1");
  CHECK_EQ(resolver_->CollectionCount(), 1);
}

// ---------------------------------------------------------------------------
// Local: GetSegmentIds (multi-shard)
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(LocalResolverTest, "Local_GetSegmentIdsMultiShard") {
  auto created = resolver_->CreateCollection(
      "multi_shard", 64, core::MetricType::L2, core::IndexType::FLAT, 3);
  REQUIRE(created.ok());

  auto segs = resolver_->GetSegmentIds("multi_shard");
  REQUIRE(segs.ok());
  CHECK_EQ(segs->size(), 3);

  // Each segment ID should be ShardSegmentId(collection_id, shard_index)
  for (uint32_t i = 0; i < 3; ++i) {
    core::SegmentId expected = cluster::ShardSegmentId(*created, i);
    CHECK_EQ((*segs)[i], expected);
  }
}

// ---------------------------------------------------------------------------
// Local: SupportsDataOps
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(LocalResolverTest, "Local_SupportsDataOps") {
  CHECK(resolver_->SupportsDataOps());
}

// ---------------------------------------------------------------------------
// Local: GetCoordinatorStub (base default returns nullptr)
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(LocalResolverTest, "Local_GetCoordinatorStubIsNull") {
  CHECK_EQ(resolver_->GetCoordinatorStub(), nullptr);
}

// ============================================================================
// Fixture B: CoordinatorResolverTest
// ============================================================================

struct CoordinatorResolverTest {
  CoordinatorResolverTest() {
    shard_manager_ = std::make_shared<cluster::ShardManager>(
        16, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(
        std::chrono::seconds(30));

    // Create coordinator with no client factory (test mode)
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_, nullptr);

    // Register a fake data node so that CreateCollection succeeds
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

    resolver_ = network::MakeCoordinatorResolver(coordinator_);
  }

  ~CoordinatorResolverTest() {
    resolver_.reset();
    coordinator_.reset();
    node_registry_.reset();
    shard_manager_.reset();
  }

  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
  std::unique_ptr<network::ICollectionResolver> resolver_;
};

// ---------------------------------------------------------------------------
// Coordinator: CreateCollection
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_CreateCollection") {
  auto result = resolver_->CreateCollection(
      "coord_col", 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(result.ok());
  CHECK_NE(*result, core::kInvalidCollectionId);
}

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_CreateCollectionDuplicate") {
  auto first = resolver_->CreateCollection(
      "dup_col", 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(first.ok());

  auto second = resolver_->CreateCollection(
      "dup_col", 64, core::MetricType::COSINE, core::IndexType::HNSW);
  CHECK_FALSE(second.ok());
  CHECK_EQ(second.status().code(), absl::StatusCode::kAlreadyExists);
}

// ---------------------------------------------------------------------------
// Coordinator: GetCollectionId
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_GetCollectionIdSuccess") {
  auto created = resolver_->CreateCollection(
      "lookup_col", 64, core::MetricType::COSINE, core::IndexType::FLAT);
  REQUIRE(created.ok());

  auto id = resolver_->GetCollectionId("lookup_col");
  REQUIRE(id.ok());
  CHECK_EQ(*id, *created);
}

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_GetCollectionIdUnknown") {
  auto id = resolver_->GetCollectionId("missing");
  CHECK_FALSE(id.ok());
  CHECK_EQ(id.status().code(), absl::StatusCode::kNotFound);
}

// ---------------------------------------------------------------------------
// Coordinator: DropCollection
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_DropCollection") {
  auto created = resolver_->CreateCollection(
      "drop_coord", 32, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(created.ok());

  auto status = resolver_->DropCollection("drop_coord");
  CHECK(status.ok());

  // Verify it is gone
  auto id = resolver_->GetCollectionId("drop_coord");
  CHECK_FALSE(id.ok());
}

// ---------------------------------------------------------------------------
// Coordinator: ListCollections
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_ListCollections") {
  (void)resolver_->CreateCollection(
      "coord_a", 64, core::MetricType::L2, core::IndexType::FLAT);
  (void)resolver_->CreateCollection(
      "coord_b", 128, core::MetricType::COSINE, core::IndexType::HNSW);

  auto list = resolver_->ListCollections();
  CHECK_EQ(list.size(), 2);
}

// ---------------------------------------------------------------------------
// Coordinator: CollectionCount
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_CollectionCount") {
  CHECK_EQ(resolver_->CollectionCount(), 0);

  (void)resolver_->CreateCollection(
      "cc1", 32, core::MetricType::L2, core::IndexType::FLAT);
  CHECK_EQ(resolver_->CollectionCount(), 1);

  (void)resolver_->CreateCollection(
      "cc2", 32, core::MetricType::L2, core::IndexType::FLAT);
  CHECK_EQ(resolver_->CollectionCount(), 2);
}

// ---------------------------------------------------------------------------
// Coordinator: GetSegmentIds returns Unimplemented
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_GetSegmentIdsUnimplemented") {
  (void)resolver_->CreateCollection(
      "no_data", 64, core::MetricType::L2, core::IndexType::FLAT);

  auto segs = resolver_->GetSegmentIds("no_data");
  CHECK_FALSE(segs.ok());
  CHECK_EQ(segs.status().code(), absl::StatusCode::kUnimplemented);
}

// ---------------------------------------------------------------------------
// Coordinator: SupportsDataOps is false
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_SupportsDataOpsFalse") {
  CHECK_FALSE(resolver_->SupportsDataOps());
}
