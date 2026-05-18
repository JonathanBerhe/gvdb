// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Unit tests for BackupManager and RestoreManager. The exercised path is
// the single-shard, single-node configuration that ships in this commit;
// multi-shard coordinator orchestration lands in a later commit and
// reuses RunShardBackup / RunShardRestore directly. The tests rely on
// InMemoryObjectStore as the IObjectStore so they're hermetic and fast.

#include <doctest/doctest.h>

#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>

#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"
#include "storage/backup_manager.h"
#include "storage/backup_manifest.h"
#include "storage/object_store.h"
#include "storage/segment_manager.h"

namespace fs = std::filesystem;
using namespace gvdb;
using namespace gvdb::storage;

namespace {

std::string MakeTestDir(const char* tag) {
  return std::string("/tmp/gvdb_backup_test_") + tag + "_" +
         std::to_string(std::chrono::steady_clock::now()
                            .time_since_epoch()
                            .count());
}

// Wait for a backup to terminate or for `timeout_ms` to elapse. The
// async pool typically finishes a 3-vector backup in well under 100 ms,
// but the loop is bounded so the suite never deadlocks on CI.
BackupState WaitForBackup(BackupManager& mgr, const std::string& id,
                          int timeout_ms = 5000) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    auto s = mgr.GetStatus(id);
    REQUIRE(s.ok());
    if (s->state == BackupState::COMPLETED ||
        s->state == BackupState::FAILED ||
        s->state == BackupState::CANCELLED) {
      return s->state;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return BackupState::RUNNING;  // timed out
}

BackupState WaitForRestore(RestoreManager& mgr, const std::string& id,
                           int timeout_ms = 5000) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    auto s = mgr.GetStatus(id);
    REQUIRE(s.ok());
    if (s->state == BackupState::COMPLETED ||
        s->state == BackupState::FAILED) {
      return s->state;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return BackupState::RUNNING;
}

struct BackupFixture {
  std::string test_dir;
  std::string flushed_root;
  std::string tmp_root;
  std::unique_ptr<index::IndexFactory> factory;
  std::shared_ptr<SegmentManager> store;
  std::unique_ptr<InMemoryObjectStore> object_store;
  core::CollectionId collection_id{1};
  core::Dimension dimension = 4;
  core::MetricType metric = core::MetricType::L2;
  core::IndexType index_type = core::IndexType::FLAT;

  BackupFixture() {
    test_dir = MakeTestDir("backup");
    flushed_root = test_dir + "/segments";
    tmp_root = test_dir + "/tmp";
    fs::create_directories(flushed_root);
    fs::create_directories(tmp_root);
    factory = std::make_unique<index::IndexFactory>();
    store = std::make_shared<SegmentManager>(flushed_root, factory.get());
    object_store = std::make_unique<InMemoryObjectStore>();
  }

  ~BackupFixture() {
    std::error_code ec;
    fs::remove_all(test_dir, ec);
  }

  // Seed a growing segment with `n` deterministic vectors.
  core::SegmentId SeedGrowingSegment(size_t n) {
    auto sid_or = store->CreateSegment(collection_id, dimension, metric);
    REQUIRE(sid_or.ok());
    auto seg = store->GetSegment(*sid_or);
    REQUIRE(seg != nullptr);
    std::vector<core::Vector> vectors;
    std::vector<core::VectorId> ids;
    vectors.reserve(n);
    ids.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      std::vector<float> v(dimension);
      for (size_t d = 0; d < dimension; ++d) {
        v[d] = static_cast<float>(i * 10 + d);
      }
      vectors.emplace_back(std::move(v));
      ids.push_back(core::MakeVectorId(i + 1));
    }
    auto add = seg->AddVectors(vectors, ids);
    REQUIRE(add.ok());
    return *sid_or;
  }

  BackupManagerOptions MakeBackupOptions() {
    BackupManagerOptions opts;
    opts.default_object_store = object_store.get();
    opts.s3_bucket = "test-bucket";
    opts.flushed_segments_root = flushed_root;
    opts.tmp_dir = tmp_root + "/backup";
    opts.gvdb_version = "test";
    opts.node_id = 1;
    opts.max_concurrent_jobs = 1;
    return opts;
  }

  RestoreManagerOptions MakeRestoreOptions() {
    RestoreManagerOptions opts;
    opts.default_object_store = object_store.get();
    opts.s3_bucket = "test-bucket";
    opts.staging_dir = tmp_root + "/restore";
    opts.max_concurrent_jobs = 1;
    return opts;
  }

  BackupTarget MakeS3Target(const std::string& prefix = "bk") {
    return BackupTarget{S3BackupTarget{"test-bucket", prefix}};
  }
};

}  // namespace

TEST_SUITE("BackupManager") {

TEST_CASE_FIXTURE(BackupFixture, "single-shard backup writes manifests") {
  SeedGrowingSegment(3);

  BackupManager mgr(MakeBackupOptions());
  auto id_or = mgr.StartBackupSingleShard(
      "products", collection_id, dimension, metric, index_type,
      /*num_shards=*/1, /*replication_factor=*/1,
      core::MakeShardId(0), /*primary_term=*/1, MakeS3Target("bk"), store,
      "bk-fixed");
  REQUIRE(id_or.ok());
  CHECK_EQ(*id_or, "bk-fixed");

  auto final_state = WaitForBackup(mgr, *id_or);
  REQUIRE_EQ(final_state, BackupState::COMPLETED);

  // The two manifest objects must exist at their canonical keys.
  auto top_key = "bk/" + BackupManifest::TopManifestKey("bk-fixed");
  auto shard_key = "bk/" + BackupManifest::ShardManifestKey("bk-fixed", 0);
  auto exists_top = object_store->ObjectExists(top_key);
  REQUIRE(exists_top.ok());
  CHECK(*exists_top);
  auto exists_shard = object_store->ObjectExists(shard_key);
  REQUIRE(exists_shard.ok());
  CHECK(*exists_shard);

  // The top manifest must report a single shard pointing at the shard manifest.
  auto top_json = object_store->GetObject(top_key);
  REQUIRE(top_json.ok());
  auto parsed = BackupManifest::DeserializeTop(*top_json);
  REQUIRE(parsed.ok());
  CHECK_EQ(parsed->backup_id, "bk-fixed");
  CHECK_EQ(parsed->collection.collection_name, "products");
  CHECK_EQ(parsed->collection.dimension, 4);
  REQUIRE_EQ(parsed->shards.size(), 1u);
  CHECK_EQ(parsed->shards[0].shard_id, 0u);
  CHECK_EQ(parsed->shards[0].manifest_key, shard_key);
}

TEST_CASE_FIXTURE(BackupFixture, "requested_backup_id is idempotent") {
  SeedGrowingSegment(2);

  BackupManager mgr(MakeBackupOptions());
  auto first = mgr.StartBackupSingleShard(
      "c", collection_id, dimension, metric, index_type, 1, 1,
      core::MakeShardId(0), 1, MakeS3Target(), store, "bk-same");
  REQUIRE(first.ok());
  REQUIRE_EQ(WaitForBackup(mgr, *first), BackupState::COMPLETED);

  // Second call with the same requested id must not start a fresh run.
  auto second = mgr.StartBackupSingleShard(
      "c", collection_id, dimension, metric, index_type, 1, 1,
      core::MakeShardId(0), 1, MakeS3Target(), store, "bk-same");
  REQUIRE(second.ok());
  CHECK_EQ(*first, *second);
  // Same status reported (no FAILED transition from re-queue).
  auto status = mgr.GetStatus(*second);
  REQUIRE(status.ok());
  CHECK_EQ(status->state, BackupState::COMPLETED);
}

TEST_CASE_FIXTURE(BackupFixture, "rejects unset target") {
  SeedGrowingSegment(1);
  BackupManager mgr(MakeBackupOptions());
  BackupTarget unset;
  auto r = mgr.StartBackupSingleShard(
      "c", collection_id, dimension, metric, index_type, 1, 1,
      core::MakeShardId(0), 1, unset, store);
  CHECK_FALSE(r.ok());
}

TEST_CASE_FIXTURE(BackupFixture, "rejects S3 bucket mismatch") {
  SeedGrowingSegment(1);
  BackupManager mgr(MakeBackupOptions());
  BackupTarget wrong{S3BackupTarget{"other-bucket", "p"}};
  auto r = mgr.StartBackupSingleShard(
      "c", collection_id, dimension, metric, index_type, 1, 1,
      core::MakeShardId(0), 1, wrong, store);
  CHECK_FALSE(r.ok());
}

TEST_CASE_FIXTURE(BackupFixture, "GetStatus surfaces NotFound for unknown id") {
  BackupManager mgr(MakeBackupOptions());
  auto r = mgr.GetStatus("never-allocated");
  CHECK_FALSE(r.ok());
}

TEST_CASE_FIXTURE(BackupFixture,
                  "LocalBackupTarget rejects paths outside the allowlist") {
  SeedGrowingSegment(1);
  auto opts = MakeBackupOptions();
  opts.local_root_allowlist = test_dir + "/allowed";
  fs::create_directories(opts.local_root_allowlist);
  BackupManager mgr(std::move(opts));
  BackupTarget escape{LocalBackupTarget{"/tmp/somewhere/else"}};
  auto r = mgr.StartBackupSingleShard(
      "c", collection_id, dimension, metric, index_type, 1, 1,
      core::MakeShardId(0), 1, escape, store);
  CHECK_FALSE(r.ok());
}

}  // TEST_SUITE("BackupManager")

TEST_SUITE("RestoreManager") {

TEST_CASE_FIXTURE(BackupFixture,
                  "round-trip: backup, restore into fresh store, vectors match") {
  // 1. Build a collection on `store` and back it up.
  auto src_seg_id = SeedGrowingSegment(5);
  (void)src_seg_id;

  BackupManager bmgr(MakeBackupOptions());
  auto bid = bmgr.StartBackupSingleShard(
      "c", collection_id, dimension, metric, index_type, 1, 1,
      core::MakeShardId(0), 1, MakeS3Target("p"), store, "bk-rt");
  REQUIRE(bid.ok());
  REQUIRE_EQ(WaitForBackup(bmgr, *bid), BackupState::COMPLETED);

  // 2. Construct a *separate* SegmentManager rooted at a different
  //    flushed dir to play the role of the restore target. We share the
  //    InMemoryObjectStore so the restore can fetch what the backup
  //    wrote — same instance, different consumers.
  auto restore_factory = std::make_unique<index::IndexFactory>();
  auto restore_dir = test_dir + "/restored";
  fs::create_directories(restore_dir);
  auto restore_store = std::make_shared<SegmentManager>(
      restore_dir, restore_factory.get());

  // Sanity: target store starts empty.
  CHECK_EQ(restore_store->GetCollectionSegments(collection_id).size(), 0u);

  // 3. Restore.
  RestoreManager rmgr(MakeRestoreOptions());
  auto rid = rmgr.StartRestoreSingleShard(
      MakeS3Target("p"), "bk-rt", collection_id,
      core::MakeShardId(0), restore_store);
  REQUIRE(rid.ok());
  REQUIRE_EQ(WaitForRestore(rmgr, *rid), BackupState::COMPLETED);

  // 4. The restored store should now have a segment with our 5 vectors.
  auto segs = restore_store->GetCollectionSegments(collection_id);
  REQUIRE_EQ(segs.size(), 1u);
  auto* restored = restore_store->GetSegment(segs[0]);
  REQUIRE(restored != nullptr);
  CHECK_EQ(restored->GetVectorCount(), 5u);
  CHECK_EQ(restored->GetDimension(), dimension);
}

TEST_CASE_FIXTURE(BackupFixture, "ReadManifest returns the parsed top manifest") {
  SeedGrowingSegment(1);
  BackupManager bmgr(MakeBackupOptions());
  auto bid = bmgr.StartBackupSingleShard(
      "c", collection_id, dimension, metric, index_type, 1, 1,
      core::MakeShardId(0), 1, MakeS3Target("p"), store, "bk-read");
  REQUIRE(bid.ok());
  REQUIRE_EQ(WaitForBackup(bmgr, *bid), BackupState::COMPLETED);

  RestoreManager rmgr(MakeRestoreOptions());
  auto m = rmgr.ReadManifest(MakeS3Target("p"), "bk-read");
  REQUIRE(m.ok());
  CHECK_EQ(m->backup_id, "bk-read");
  CHECK_EQ(m->collection.dimension, dimension);
  CHECK_EQ(m->collection.metric, static_cast<int32_t>(metric));
}

}  // TEST_SUITE("RestoreManager")
