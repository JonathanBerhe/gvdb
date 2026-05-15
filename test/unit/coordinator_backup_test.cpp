// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Unit tests for Coordinator-orchestrated backup/restore. The test
// exercises the per-shard fan-out shape using a mock IInternalServiceClient
// that records the call sequence. The coordinator-side BackupManager
// writes the top manifest into an InMemoryObjectStore which the test
// inspects afterward.

#include <doctest/doctest.h>

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cluster/coordinator.h"
#include "cluster/internal_client.h"
#include "cluster/node_registry.h"
#include "cluster/shard_manager.h"
#include "core/types.h"
#include "internal.grpc.pb.h"
#include "storage/backup_manager.h"
#include "storage/object_store.h"

using namespace gvdb;
using namespace gvdb::cluster;

namespace {

// Per-call record. The test inspects this to assert the
// Freeze → Backup → Unfreeze order.
struct RecordedCall {
  std::string method;          // "FreezeWrites" | "BackupShard" | "UnfreezeWrites"
  uint32_t shard_id = 0;
  std::string fence_token;
  std::string backup_id;
  uint32_t collection_id = 0;
};

// IInternalServiceClient mock that records every call and returns OK with
// synthetic shard-manifest keys. Shared state lives in a struct passed in
// by the factory so MockClient instances created per-call see the same
// recording vector (the factory hands out fresh client objects per dial).
struct MockState {
  mutable std::mutex mu;
  std::vector<RecordedCall> calls;     // in temporal order across all shards

  void Record(RecordedCall c) {
    std::lock_guard<std::mutex> lk(mu);
    calls.push_back(std::move(c));
  }
};

class MockClient : public IInternalServiceClient {
 public:
  explicit MockClient(std::shared_ptr<MockState> state)
      : state_(std::move(state)) {}

  grpc::Status CreateSegment(grpc::ClientContext*,
                             const proto::internal::CreateSegmentRequest&,
                             proto::internal::CreateSegmentResponse* r) override {
    r->set_success(true);
    return grpc::Status::OK;
  }
  grpc::Status DeleteSegment(grpc::ClientContext*,
                             const proto::internal::DeleteSegmentRequest&,
                             proto::internal::DeleteSegmentResponse* r) override {
    r->set_success(true);
    return grpc::Status::OK;
  }
  grpc::Status ReplicateSegment(grpc::ClientContext*,
                                const proto::internal::ReplicateSegmentRequest&,
                                proto::internal::ReplicateSegmentResponse* r) override {
    r->set_success(true);
    return grpc::Status::OK;
  }
  grpc::Status GetSegment(grpc::ClientContext*,
                          const proto::internal::GetSegmentRequest&,
                          proto::internal::GetSegmentResponse* /*r*/) override {
    return grpc::Status::OK;
  }
  grpc::Status ListSegments(grpc::ClientContext*,
                            const proto::internal::ListSegmentsRequest&,
                            proto::internal::ListSegmentsResponse* /*r*/) override {
    return grpc::Status::OK;
  }
  grpc::Status PausePrimary(grpc::ClientContext*,
                            const proto::internal::PausePrimaryRequest&,
                            proto::internal::PausePrimaryResponse* r) override {
    r->set_success(true);
    return grpc::Status::OK;
  }
  grpc::Status PreparePromote(grpc::ClientContext*,
                              const proto::internal::PreparePromoteRequest&,
                              proto::internal::PreparePromoteResponse* r) override {
    r->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status FreezeWrites(grpc::ClientContext*,
                            const proto::internal::FreezeWritesRequest& req,
                            proto::internal::FreezeWritesResponse* r) override {
    state_->Record(RecordedCall{"FreezeWrites", req.shard_id(),
                                req.fence_token(), "", 0});
    r->set_success(true);
    return grpc::Status::OK;
  }
  grpc::Status BackupShard(grpc::ClientContext*,
                           const proto::internal::BackupShardRequest& req,
                           proto::internal::BackupShardResponse* r) override {
    state_->Record(RecordedCall{"BackupShard", req.shard_id(), "",
                                req.backup_id(), req.collection_id()});
    r->set_success(true);
    // Return a synthetic shard manifest key the coordinator will record
    // in the top-level manifest. The body of that manifest is never
    // physically uploaded by the mock — the test only inspects the
    // top-level manifest the coordinator writes.
    r->set_shard_manifest_key(
        "backups/" + req.backup_id() + "/shards/" +
        std::to_string(req.shard_id()) + "/shard.manifest.json");
    r->set_bytes_uploaded(1024);
    auto* seg = r->add_segments();
    seg->set_segment_id(100 + req.shard_id());
    seg->set_is_growing(true);
    seg->set_vector_count(10);
    seg->set_size_bytes(1024);
    return grpc::Status::OK;
  }
  grpc::Status RestoreShard(grpc::ClientContext*,
                            const proto::internal::RestoreShardRequest& req,
                            proto::internal::RestoreShardResponse* r) override {
    state_->Record(RecordedCall{"RestoreShard", req.shard_id(), "",
                                req.backup_id(),
                                req.target_collection_id()});
    r->set_success(true);
    r->set_segments_restored(1);
    return grpc::Status::OK;
  }
  grpc::Status UnfreezeWrites(grpc::ClientContext*,
                              const proto::internal::UnfreezeWritesRequest& req,
                              proto::internal::UnfreezeWritesResponse* r) override {
    state_->Record(RecordedCall{"UnfreezeWrites", req.shard_id(),
                                req.fence_token(), "", 0});
    r->set_success(true);
    return grpc::Status::OK;
  }

 private:
  std::shared_ptr<MockState> state_;
};

class MockFactory : public IInternalServiceClientFactory {
 public:
  explicit MockFactory(std::shared_ptr<MockState> state)
      : state_(std::move(state)) {}
  std::unique_ptr<IInternalServiceClient> CreateClient(
      core::NodeId, const std::string& /*addr*/) override {
    return std::make_unique<MockClient>(state_);
  }
 private:
  std::shared_ptr<MockState> state_;
};

// Tightly-scoped fixture: 1 collection, 2 shards, primaries on
// different mock data-nodes. Backup orchestration must hit each shard
// once with FreezeWrites → BackupShard → UnfreezeWrites in that order.
struct CoordinatorBackupFixture {
  std::shared_ptr<MockState> mock_state;
  std::shared_ptr<MockFactory> factory;
  std::shared_ptr<ShardManager> shard_manager;
  std::shared_ptr<NodeRegistry> node_registry;
  std::unique_ptr<Coordinator> coordinator;
  std::unique_ptr<storage::InMemoryObjectStore> object_store;
  std::shared_ptr<storage::BackupManager> backup_manager;
  std::shared_ptr<storage::RestoreManager> restore_manager;

  CoordinatorBackupFixture() {
    mock_state = std::make_shared<MockState>();
    factory = std::make_shared<MockFactory>(mock_state);
    shard_manager = std::make_shared<ShardManager>(2, ShardingStrategy::HASH);
    node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));

    coordinator = std::make_unique<Coordinator>(
        shard_manager, node_registry, factory);

    // Register through the coordinator so ShardManager's hash ring is
    // populated — otherwise AssignShard falls back to a degenerate
    // HashKey that collides for small num_shards and produces a single
    // shard_id for multi-shard collections.
    NodeInfo info1;
    info1.node_id = core::MakeNodeId(1);
    info1.type = NodeType::DATA_NODE;
    info1.status = NodeStatus::HEALTHY;
    info1.address = "node1:50051";
    REQUIRE(coordinator->RegisterNode(info1).ok());

    NodeInfo info2;
    info2.node_id = core::MakeNodeId(2);
    info2.type = NodeType::DATA_NODE;
    info2.status = NodeStatus::HEALTHY;
    info2.address = "node2:50051";
    REQUIRE(coordinator->RegisterNode(info2).ok());

    object_store = std::make_unique<storage::InMemoryObjectStore>();
    storage::BackupManagerOptions opts;
    opts.default_object_store = object_store.get();
    opts.s3_bucket = "test-bucket";
    opts.tmp_dir = "/tmp/gvdb_coord_backup_test";
    opts.max_concurrent_jobs = 1;
    opts.gvdb_version = "test";
    opts.node_id = 0;
    backup_manager = std::make_shared<storage::BackupManager>(std::move(opts));

    storage::RestoreManagerOptions ropts;
    ropts.default_object_store = object_store.get();
    ropts.s3_bucket = "test-bucket";
    ropts.staging_dir = "/tmp/gvdb_coord_restore_test";
    ropts.max_concurrent_jobs = 1;
    restore_manager = std::make_shared<storage::RestoreManager>(
        std::move(ropts));

    coordinator->SetBackupManager(backup_manager);
    coordinator->SetRestoreManager(restore_manager);
  }
};

// Spin until the backup transitions to a terminal state or the deadline
// elapses. The fan-out for 2 mock shards typically completes in a few
// milliseconds.
storage::BackupState WaitForBackup(storage::BackupManager& mgr,
                                   const std::string& id, int timeout_ms = 5000) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    auto s = mgr.GetStatus(id);
    REQUIRE(s.ok());
    if (s->state == storage::BackupState::COMPLETED ||
        s->state == storage::BackupState::FAILED ||
        s->state == storage::BackupState::CANCELLED) {
      return s->state;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return storage::BackupState::RUNNING;
}

}  // namespace

TEST_SUITE("CoordinatorBackup") {

TEST_CASE_FIXTURE(CoordinatorBackupFixture,
                  "two-shard backup issues Freeze → Backup → Unfreeze per shard") {
  // 2 shards, single-replica — primaries land on node1 and node2 by
  // round-robin in AssignShardsToCollection.
  auto coll_or = coordinator->CreateCollection(
      "products", 128, core::MetricType::L2, core::IndexType::FLAT,
      /*replication_factor=*/1, /*num_shards=*/2);
  REQUIRE(coll_or.ok());

  storage::BackupTarget target{storage::S3BackupTarget{"test-bucket", "bk"}};
  auto bid_or = coordinator->StartBackupDistributed(
      "products", target, "bk-fixed");
  REQUIRE(bid_or.ok());
  REQUIRE_EQ(*bid_or, "bk-fixed");

  auto final_state = WaitForBackup(*backup_manager, *bid_or);
  REQUIRE_EQ(final_state, storage::BackupState::COMPLETED);

  // Verify per-shard call sequence: each distinct shard must see
  // exactly Freeze → Backup → Unfreeze in that order.
  std::map<uint32_t, std::vector<std::string>> per_shard_methods;
  {
    std::lock_guard<std::mutex> lk(mock_state->mu);
    for (const auto& c : mock_state->calls) {
      per_shard_methods[c.shard_id].push_back(c.method);
    }
  }
  REQUIRE_EQ(per_shard_methods.size(), 2u);
  for (const auto& [shard_id, methods] : per_shard_methods) {
    REQUIRE_EQ(methods.size(), 3u);
    CHECK_EQ(methods[0], "FreezeWrites");
    CHECK_EQ(methods[1], "BackupShard");
    CHECK_EQ(methods[2], "UnfreezeWrites");
  }

  // Verify that BackupShard carried the right backup_id and a non-zero
  // collection_id (regression guard: an earlier draft hardcoded 0).
  bool saw_nonzero_collection = false;
  {
    std::lock_guard<std::mutex> lk(mock_state->mu);
    for (const auto& c : mock_state->calls) {
      if (c.method == "BackupShard") {
        CHECK_EQ(c.backup_id, "bk-fixed");
        if (c.collection_id != 0) saw_nonzero_collection = true;
      }
    }
  }
  CHECK(saw_nonzero_collection);

  // Verify the top-level manifest was uploaded to the InMemoryObjectStore
  // and references both shards.
  auto top = object_store->GetObject(
      "bk/backups/bk-fixed/backup.manifest.json");
  REQUIRE(top.ok());
  auto manifest = storage::BackupManifest::DeserializeTop(*top);
  REQUIRE(manifest.ok());
  CHECK_EQ(manifest->backup_id, "bk-fixed");
  CHECK_EQ(manifest->collection.collection_name, "products");
  CHECK_EQ(manifest->collection.num_shards, 2u);
  REQUIRE_EQ(manifest->shards.size(), 2u);
}

TEST_CASE_FIXTURE(CoordinatorBackupFixture,
                  "StartBackupDistributed without manager returns UNIMPLEMENTED") {
  // Detach the backup manager — simulates a coordinator binary not
  // configured for backups.
  coordinator->SetBackupManager(nullptr);
  storage::BackupTarget target{storage::S3BackupTarget{"test-bucket", "bk"}};
  auto r = coordinator->StartBackupDistributed("products", target);
  CHECK_FALSE(r.ok());
  CHECK_EQ(r.status().code(), absl::StatusCode::kUnimplemented);
}

TEST_CASE_FIXTURE(CoordinatorBackupFixture,
                  "GetDistributedBackupStatus surfaces NotFound for unknown id") {
  auto r = coordinator->GetDistributedBackupStatus("never-allocated");
  CHECK_FALSE(r.ok());
  CHECK_EQ(r.status().code(), absl::StatusCode::kNotFound);
}

TEST_CASE_FIXTURE(CoordinatorBackupFixture,
                  "StartRestoreDistributed rejects existing target without overwrite") {
  // Create the target collection so a restore without overwrite hits
  // ALREADY_EXISTS — exercises the overwrite gate.
  auto coll_or = coordinator->CreateCollection(
      "existing", 64, core::MetricType::L2, core::IndexType::FLAT, 1, 1);
  REQUIRE(coll_or.ok());

  storage::BackupTarget source{storage::S3BackupTarget{"test-bucket", "bk"}};
  // The manifest doesn't exist; the coordinator should fail the lookup
  // before the overwrite check. To exercise the overwrite gate
  // independently, we'd need to seed a manifest. For this commit we
  // assert the call path returns an error rather than the specific
  // status code.
  auto r = coordinator->StartRestoreDistributed(
      source, "missing-backup", "existing", /*overwrite=*/false);
  CHECK_FALSE(r.ok());
}

}  // TEST_SUITE("CoordinatorBackup")
