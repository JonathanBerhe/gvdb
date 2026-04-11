// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include <filesystem>

#include "network/collection_resolver.h"
#include "storage/segment_manager.h"
#include "index/index_factory.h"
#include "cluster/coordinator.h"
#include "cluster/node_registry.h"
#include "cluster/shard_manager.h"
#include "core/types.h"

using namespace gvdb;

// ============================================================================
// Fixture A: LocalResolverTest
// ============================================================================

struct LocalResolverTest {
  LocalResolverTest() {
    test_dir_ = "/tmp/gvdb_local_resolver_test";
    std::filesystem::create_directories(test_dir_);

    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>(
        test_dir_, index_factory_.get());
    resolver_ = network::MakeLocalResolver(segment_store_);
  }

  ~LocalResolverTest() {
    resolver_.reset();
    segment_store_.reset();
    index_factory_.reset();
    std::filesystem::remove_all(test_dir_);
  }

  std::string test_dir_;
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
// Local: GetSegmentId
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(LocalResolverTest, "Local_GetSegmentId") {
  auto created = resolver_->CreateCollection(
      "seg_test", 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(created.ok());

  auto seg = resolver_->GetSegmentId("seg_test");
  REQUIRE(seg.ok());

  // The segment should be the ShardSegmentId for shard 0
  core::SegmentId expected = cluster::ShardSegmentId(*created, 0);
  CHECK_EQ(*seg, expected);
}

TEST_CASE_FIXTURE(LocalResolverTest, "Local_GetSegmentIdUnknown") {
  auto seg = resolver_->GetSegmentId("no_such");
  CHECK_FALSE(seg.ok());
  CHECK_EQ(seg.status().code(), absl::StatusCode::kNotFound);
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
// Local: GetNumShards
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(LocalResolverTest, "Local_GetNumShardsDefault") {
  (void)resolver_->CreateCollection(
      "one_shard", 64, core::MetricType::L2, core::IndexType::FLAT);
  CHECK_EQ(resolver_->GetNumShards("one_shard"), 1);
}

TEST_CASE_FIXTURE(LocalResolverTest, "Local_GetNumShardsMulti") {
  (void)resolver_->CreateCollection(
      "four_shards", 64, core::MetricType::L2, core::IndexType::FLAT, 4);
  CHECK_EQ(resolver_->GetNumShards("four_shards"), 4);
}

TEST_CASE_FIXTURE(LocalResolverTest, "Local_GetNumShardsUnknown") {
  // Unknown collection returns default 1
  CHECK_EQ(resolver_->GetNumShards("ghost"), 1);
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
// Coordinator: GetSegmentId returns Unimplemented
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_GetSegmentIdUnimplemented") {
  (void)resolver_->CreateCollection(
      "no_data", 64, core::MetricType::L2, core::IndexType::FLAT);

  auto seg = resolver_->GetSegmentId("no_data");
  CHECK_FALSE(seg.ok());
  CHECK_EQ(seg.status().code(), absl::StatusCode::kUnimplemented);
}

// ---------------------------------------------------------------------------
// Coordinator: SupportsDataOps is false
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE(CoordinatorResolverTest, "Coord_SupportsDataOpsFalse") {
  CHECK_FALSE(resolver_->SupportsDataOps());
}
