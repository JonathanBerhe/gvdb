// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// TieredSegmentManager behavior that SegmentManager does not have: uploads,
// the manifest, retries and remote copies. The ISegmentStore behavior that
// both stores share is in segment_store_contract_test.cpp.

#include "storage/segment/tiered_segment_manager.h"
#include "index/index_factory.h"
#include "storage/objstore/object_store.h"
#include "support/temp_dir.h"
#include "support/vectors.h"
#include "support/wait.h"

#include <doctest/doctest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace gvdb::storage {
namespace {

const std::string kPrefix = "test-prefix";
const std::string kManifestKey = kPrefix + "/manifest.json";

// Object store that fails the first N Put operations, then behaves like the
// in-memory store. Models a transient object-store outage (e.g. MinIO
// XMinioStorageFull) that later recovers. Each failed upload attempt consumes
// exactly one failing Put because UploadSegmentToS3 returns on the first error.
class FlakyObjectStore : public InMemoryObjectStore {
 public:
  explicit FlakyObjectStore(int put_failures) : remaining_failures_(put_failures) {}

  // Ends the outage: every later Put succeeds.
  void StopFailing() { remaining_failures_.store(0); }

  // How many Put operations the store has rejected so far. Each rejected
  // Put is one failed upload attempt.
  int RejectedPuts() const { return rejected_puts_.load(); }

  absl::Status PutObject(const std::string& key, const std::string& data) override {
    if (ConsumeFailure()) {
      return absl::InternalError("injected object-store put failure");
    }
    return InMemoryObjectStore::PutObject(key, data);
  }

  absl::Status PutObjectFromFile(const std::string& key,
                                 const std::string& local_file_path) override {
    if (ConsumeFailure()) {
      return absl::InternalError("injected object-store put failure");
    }
    return InMemoryObjectStore::PutObjectFromFile(key, local_file_path);
  }

 private:
  // Returns true (and burns one failure) while failures remain.
  bool ConsumeFailure() {
    int cur = remaining_failures_.load();
    while (cur > 0) {
      if (remaining_failures_.compare_exchange_weak(cur, cur - 1)) {
        rejected_puts_.fetch_add(1);
        return true;
      }
    }
    return false;
  }

  std::atomic<int> remaining_failures_;
  std::atomic<int> rejected_puts_{0};
};

// In-memory object store that rejects every file upload of one segment, so
// that segment's upload always fails and GetFailedUploads() reports it.
class SegmentRejectingObjectStore : public InMemoryObjectStore {
 public:
  // Call before any segment is flushed. Upload threads read the value only
  // after FlushSegment hands them the task through the pool's queue lock.
  void RejectSegment(core::SegmentId id) {
    rejected_key_part_ = "/segments/" + std::to_string(core::ToUInt32(id)) + "/";
  }

  absl::Status PutObjectFromFile(const std::string& key,
                                 const std::string& local_file_path) override {
    if (!rejected_key_part_.empty() && key.contains(rejected_key_part_)) {
      return absl::InternalError("injected put failure for a rejected segment");
    }
    return InMemoryObjectStore::PutObjectFromFile(key, local_file_path);
  }

 private:
  std::string rejected_key_part_;
};

struct TieredTest {
  // Holds the local and the cache folder, and removes both at the end.
  test::ScopedTempDir temp_dir_{"gvdb_tiered_test"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  core::Dimension dimension_ = static_cast<core::Dimension>(128);
  core::MetricType metric_ = core::MetricType::L2;
  core::CollectionId collection_id_ = core::CollectionId(1);
  const std::string test_dir_ = (temp_dir_ / "local").string();
  const std::string cache_dir_ = (temp_dir_ / "cache").string();
  std::uint32_t next_seed_ = 42;

  TieredTest() {
    std::filesystem::create_directories(test_dir_);
    index_factory_ = std::make_unique<index::IndexFactory>();
  }

  // A tiered manager on the fixture's dirs, and a non-owning pointer to its
  // object store for inspection.
  template <typename Store>
  struct TieredWith {
    std::unique_ptr<TieredSegmentManager> tiered;
    Store* store;  // non-owning
  };

  template <typename Store>
  TieredWith<Store> MakeTieredOver(std::unique_ptr<Store> object_store, int upload_threads,
                                   std::chrono::milliseconds retry_delay) {
    auto* store_ptr = object_store.get();
    auto local = std::make_unique<SegmentManager>(test_dir_, index_factory_.get());
    auto cache = std::make_unique<SegmentCache>(cache_dir_, 256 * 1024 * 1024);
    auto tiered = std::make_unique<TieredSegmentManager>(std::move(local), std::move(object_store),
                                                         std::move(cache), kPrefix, upload_threads,
                                                         retry_delay);
    return {std::move(tiered), store_ptr};
  }

  // A healthy in-memory store never fails an upload, so no retry runs and
  // the retry delay does not matter.
  TieredWith<InMemoryObjectStore> MakeTieredWithStore(int upload_threads = 1) {
    return MakeTieredOver(std::make_unique<InMemoryObjectStore>(), upload_threads,
                          std::chrono::seconds(3));
  }

  // Tiered manager backed by a FlakyObjectStore with a millisecond-scale retry
  // delay so background retries can be observed quickly in tests.
  TieredWith<FlakyObjectStore> MakeTieredFlaky(int put_failures,
                                               std::chrono::milliseconds retry_delay) {
    return MakeTieredOver(std::make_unique<FlakyObjectStore>(put_failures),
                          /*upload_threads=*/1, retry_delay);
  }

  core::IndexConfig FlatIndexConfig() const {
    core::IndexConfig config;
    config.index_type = core::IndexType::FLAT;
    config.dimension = dimension_;
    config.metric_type = metric_;
    return config;
  }

  // Create, populate and seal a segment; returns its id.
  core::SegmentId SealOneSegment(TieredSegmentManager& tiered, size_t count,
                                 uint64_t first_id = 1) {
    auto seg_id = *tiered.CreateSegment(collection_id_, dimension_, metric_);
    auto vectors = MakeVectors(count);
    auto ids = MakeIds(count, first_id);
    REQUIRE(tiered.WriteVectors(seg_id, vectors, ids).ok());
    REQUIRE(tiered.SealSegment(seg_id, FlatIndexConfig()).ok());
    return seg_id;
  }

  // Create, populate, seal, and flush a segment; returns its id.
  core::SegmentId FlushOneSegment(TieredSegmentManager& tiered, size_t count) {
    auto seg_id = SealOneSegment(tiered, count);
    REQUIRE(tiered.FlushSegment(seg_id).ok());
    return seg_id;
  }

  // Each call uses the next seed, so two calls in one case give different
  // vectors and every run gives the same ones.
  std::vector<core::Vector> MakeVectors(size_t count) {
    return test::RandomVectors(count, dimension_, next_seed_++);
  }

  std::vector<core::VectorId> MakeIds(size_t count, uint64_t start = 1) {
    return test::SequentialIds(count, start);
  }

  // Object-store key prefix of a segment in the fixture's collection.
  std::string SegmentKeyPrefix(core::SegmentId id) const {
    return kPrefix + "/collections/" + std::to_string(core::ToUInt32(collection_id_)) +
           "/segments/" + std::to_string(core::ToUInt32(id)) + "/";
  }

  // Polls the manifest until it lists `expected` entries. Returns the entries
  // of the last successful read, so a timeout shows up as a size mismatch.
  static std::vector<ManifestEntry> WaitForManifestEntries(InMemoryObjectStore& store,
                                                           size_t expected) {
    std::vector<ManifestEntry> entries;
    (void)test::WaitFor([&] {
      auto manifest = store.GetObject(kManifestKey);
      if (!manifest.ok()) return false;
      auto parsed = SegmentManifest::Deserialize(*manifest);
      if (!parsed.ok()) return false;
      entries = std::move(*parsed);
      return entries.size() == expected;
    });
    return entries;
  }
};

TEST_CASE_FIXTURE(TieredTest, "ManifestDiscovery") {
  // Phase 1: Create, write, seal, flush to populate S3
  auto [tiered1, store1] = MakeTieredWithStore();
  auto seg_id = *tiered1->CreateSegment(collection_id_, dimension_, metric_);

  auto vectors = MakeVectors(50);
  auto ids = MakeIds(50);
  REQUIRE(tiered1->WriteVectors(seg_id, vectors, ids).ok());

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;
  REQUIRE(tiered1->SealSegment(seg_id, config).ok());
  REQUIRE(tiered1->FlushSegment(seg_id).ok());

  // Wait for the upload. The helper reads and deserializes the manifest, so
  // one entry back means the manifest was created and parses.
  auto entries = WaitForManifestEntries(*store1, 1);
  REQUIRE_EQ(entries.size(), 1);
  CHECK_EQ(entries[0].vector_count, 50);
}

TEST_CASE_FIXTURE(TieredTest, "DropUploadedSegmentRemovesRemoteCopy") {
  // A long retry delay keeps the marker's failed upload in place for the
  // whole test.
  auto parts = MakeTieredOver(std::make_unique<SegmentRejectingObjectStore>(),
                              /*upload_threads=*/1, std::chrono::minutes(5));
  auto& tiered = parts.tiered;
  auto* store = parts.store;

  auto seg_id = SealOneSegment(*tiered, 30);
  auto marker_id = SealOneSegment(*tiered, 10, /*first_id=*/1001);
  store->RejectSegment(marker_id);
  REQUIRE(tiered->FlushSegment(seg_id).ok());
  REQUIRE(tiered->FlushSegment(marker_id).ok());

  // DropSegment removes the remote copy only of a segment it already tracks
  // as remote, and an upload writes the manifest before it starts tracking
  // the segment. So the manifest entry of seg_id is not a safe signal. The
  // failed marker upload is: with one upload thread, uploads run one at a
  // time in flush order, so once GetFailedUploads() lists the marker, the
  // upload of seg_id has returned, tracking included.
  bool marker_failed = test::WaitFor([&] {
    auto failed = tiered->GetFailedUploads();
    return std::ranges::find(failed, marker_id) != failed.end();
  });
  REQUIRE(marker_failed);
  CHECK_EQ(tiered->GetFailedUploads().size(), 1);

  auto before = WaitForManifestEntries(*store, 1);
  REQUIRE_EQ(before.size(), 1);
  CHECK_EQ(before[0].segment_id, core::ToUInt32(seg_id));
  auto objects_before = store->ListObjects(SegmentKeyPrefix(seg_id));
  REQUIRE(objects_before.ok());
  CHECK_FALSE(objects_before->empty());

  REQUIRE(tiered->DropSegment(seg_id, /*delete_files=*/true).ok());

  // The manifest stays and still parses, with no entries left.
  auto manifest = store->GetObject(kManifestKey);
  REQUIRE(manifest.ok());
  auto entries = SegmentManifest::Deserialize(*manifest);
  REQUIRE(entries.ok());
  CHECK(entries->empty());

  // The segment's files are gone from the object store and from local disk.
  auto objects_after = store->ListObjects(SegmentKeyPrefix(seg_id));
  REQUIRE(objects_after.ok());
  CHECK(objects_after->empty());
  CHECK_FALSE(
      std::filesystem::exists(test_dir_ + "/segment_" + std::to_string(core::ToUInt32(seg_id))));

  // Only the marker is left. The dropped segment is not tracked as remote any
  // more, so it is not counted as a remote-only segment.
  CHECK_EQ(tiered->GetSegmentCount(), 1);
}

TEST_CASE_FIXTURE(TieredTest, "MultipleCollectionsUploadAndSearch") {
  auto [tiered, store] = MakeTieredWithStore();
  const auto collection2 = core::CollectionId(2);

  auto seg1 = *tiered->CreateSegment(collection_id_, dimension_, metric_);
  auto vectors1 = MakeVectors(50);
  auto ids1 = MakeIds(50, 1);
  REQUIRE(tiered->WriteVectors(seg1, vectors1, ids1).ok());

  auto seg2 = *tiered->CreateSegment(collection2, dimension_, metric_);
  auto vectors2 = MakeVectors(50);
  auto ids2 = MakeIds(50, 1000);
  REQUIRE(tiered->WriteVectors(seg2, vectors2, ids2).ok());

  REQUIRE(tiered->SealSegment(seg1, FlatIndexConfig()).ok());
  REQUIRE(tiered->SealSegment(seg2, FlatIndexConfig()).ok());
  REQUIRE(tiered->FlushSegment(seg1).ok());
  REQUIRE(tiered->FlushSegment(seg2).ok());

  // Both uploads land in the one manifest, one entry per collection.
  auto entries = WaitForManifestEntries(*store, 2);
  REQUIRE_EQ(entries.size(), 2);
  std::vector<uint32_t> collections = {entries[0].collection_id, entries[1].collection_id};
  std::ranges::sort(collections);
  const std::vector<uint32_t> expected_collections = {1, 2};
  CHECK_EQ(collections, expected_collections);

  // Each collection still searches on its own after the upload: the query's
  // own id is the top hit, and no hit comes from the other collection.
  auto r1 = tiered->SearchCollection(collection_id_, vectors1[0], 3);
  REQUIRE(r1.ok());
  REQUIRE_FALSE(r1->entries.empty());
  CHECK_EQ(r1->entries[0].id, ids1[0]);
  for (const auto& entry : r1->entries) {
    CHECK_LE(core::ToUInt64(entry.id), uint64_t{50});
  }

  auto r2 = tiered->SearchCollection(collection2, vectors2[0], 3);
  REQUIRE(r2.ok());
  REQUIRE_FALSE(r2->entries.empty());
  CHECK_EQ(r2->entries[0].id, ids2[0]);
  for (const auto& entry : r2->entries) {
    CHECK_GE(core::ToUInt64(entry.id), uint64_t{1000});
  }
}

TEST_CASE_FIXTURE(TieredTest, "ConcurrentFlushesAllReachManifest") {
  // Three upload threads, so the uploads and their manifest updates can
  // overlap.
  auto [tiered, store] = MakeTieredWithStore(/*upload_threads=*/3);

  // Seal all three before flushing any, so the uploads are queued together.
  std::vector<core::SegmentId> seg_ids;
  seg_ids.reserve(3);
  for (uint64_t i = 0; i < 3; ++i) {
    seg_ids.push_back(SealOneSegment(*tiered, 50, /*first_id=*/i * 100 + 1));
  }
  for (auto seg_id : seg_ids) {
    REQUIRE(tiered->FlushSegment(seg_id).ok());
  }

  auto entries = WaitForManifestEntries(*store, 3);
  REQUIRE_EQ(entries.size(), 3);
  std::vector<uint32_t> listed;
  for (const auto& entry : entries) {
    listed.push_back(entry.segment_id);
    CHECK_EQ(entry.vector_count, 50);
  }
  std::vector<uint32_t> expected;
  expected.reserve(seg_ids.size());
  for (auto seg_id : seg_ids) {
    expected.push_back(core::ToUInt32(seg_id));
  }
  std::ranges::sort(listed);
  std::ranges::sort(expected);
  CHECK_EQ(listed, expected);
  CHECK(tiered->GetFailedUploads().empty());
}

TEST_CASE_FIXTURE(TieredTest, "FailedUploadRetriesAndRecovers") {
  // The store rejects every upload until the test has seen the failed state
  // and two rejected attempts (so one retry ran with a doubled backoff),
  // then succeeds. So the failed state cannot come and go between two polls.
  // A short retry base delay keeps the exponential backoff inside the test.
  auto [tiered, store] = MakeTieredFlaky(
      /*put_failures=*/1000000, std::chrono::milliseconds(25));
  auto seg_id = FlushOneSegment(*tiered, 50);

  // The first attempt fails, so the segment must surface as a failed upload.
  std::vector<core::SegmentId> failed;
  bool saw_failure = test::WaitFor([&] {
    failed = tiered->GetFailedUploads();
    return !failed.empty() && store->RejectedPuts() >= 2;
  });
  REQUIRE(saw_failure);
  CHECK_EQ(failed.size(), 1);
  CHECK_EQ(core::ToUInt32(failed[0]), core::ToUInt32(seg_id));

  // The outage ends.
  store->StopFailing();

  // The background retry thread eventually re-drives the upload to success.
  // The manifest is written after the segment files, so wait for it too.
  bool recovered = test::WaitFor([&] {
    return tiered->GetFailedUploads().empty() && store->GetObject("test-prefix/manifest.json").ok();
  });
  CHECK(recovered);
  CHECK(tiered->GetFailedUploads().empty());
  CHECK(store->ObjectCount() > 0);
  CHECK(store->GetObject("test-prefix/manifest.json").ok());
}

TEST_CASE_FIXTURE(TieredTest, "DestructorWithPendingFailedUploadShutsDownCleanly") {
  // Store never recovers, so the segment stays in the failed set indefinitely.
  auto parts = MakeTieredFlaky(
      /*put_failures=*/1000000, std::chrono::milliseconds(25));
  auto& tiered = parts.tiered;
  FlushOneSegment(*tiered, 20);

  // Wait until the upload has failed often enough to reach the error
  // threshold of the retry loop (5 attempts), and is queued for retry again.
  bool failed = test::WaitFor(
      [&] { return parts.store->RejectedPuts() >= 5 && !tiered->GetFailedUploads().empty(); });
  CHECK(failed);

  // Destroying the manager while a failed upload is pending must not hang or
  // crash: the retry thread and upload pool shut down cleanly.
  tiered.reset();
  CHECK(tiered == nullptr);
}

TEST_CASE_FIXTURE(TieredTest, "StartupRedrivesUnuploadedSegments") {
  // Phase 1: flush a segment while every upload is rejected, so it lands on
  // local disk but never reaches the remote manifest.
  {
    auto parts = MakeTieredFlaky(
        /*put_failures=*/1000000, std::chrono::milliseconds(25));
    FlushOneSegment(*parts.tiered, 20);
    bool failed = test::WaitFor([&] { return !parts.tiered->GetFailedUploads().empty(); });
    CHECK(failed);
  }

  // Phase 2: a fresh manager with a healthy store on the same local dirs.
  // Startup reconciliation detects the FLUSHED-but-unlisted segment and
  // re-drives its upload.
  auto [tiered2, store2] = MakeTieredWithStore();
  REQUIRE(tiered2->LoadAllSegments().ok());

  bool uploaded =
      test::WaitFor([&] { return store2->GetObject("test-prefix/manifest.json").ok(); });
  CHECK(uploaded);
  CHECK(store2->ObjectCount() > 0);
  CHECK(tiered2->GetFailedUploads().empty());
}

}  // namespace
}  // namespace gvdb::storage
