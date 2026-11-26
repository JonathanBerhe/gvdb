// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/tiered_segment_manager.h"
#include "storage/object_store.h"
#include "index/index_factory.h"

#include <doctest/doctest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace gvdb {
namespace storage {
namespace {

const std::string kPrefix = "test-prefix";
const std::string kManifestKey = kPrefix + "/manifest.json";

// A per-fixture directory under the system temp dir, unique across
// processes and fixture instances.
std::string UniqueTestDir(const std::string& prefix) {
  std::random_device rd;
  std::mt19937_64 gen(rd());
  return (std::filesystem::temp_directory_path() /
          (prefix + "_" + std::to_string(gen())))
      .string();
}

// Object store that fails the first N Put operations, then behaves like the
// in-memory store. Models a transient object-store outage (e.g. MinIO
// XMinioStorageFull) that later recovers. Each failed upload attempt consumes
// exactly one failing Put because UploadSegmentToS3 returns on the first error.
class FlakyObjectStore : public InMemoryObjectStore {
 public:
  explicit FlakyObjectStore(int put_failures)
      : remaining_failures_(put_failures) {}

  core::Status PutObject(
      const std::string& key, const std::string& data) override {
    if (ConsumeFailure()) {
      return core::InternalError("injected object-store put failure");
    }
    return InMemoryObjectStore::PutObject(key, data);
  }

  core::Status PutObjectFromFile(
      const std::string& key, const std::string& local_file_path) override {
    if (ConsumeFailure()) {
      return core::InternalError("injected object-store put failure");
    }
    return InMemoryObjectStore::PutObjectFromFile(key, local_file_path);
  }

 private:
  // Returns true (and burns one failure) while failures remain.
  bool ConsumeFailure() {
    int cur = remaining_failures_.load();
    while (cur > 0) {
      if (remaining_failures_.compare_exchange_weak(cur, cur - 1)) return true;
    }
    return false;
  }

  std::atomic<int> remaining_failures_;
};

// In-memory object store that rejects every file upload of one segment, so
// that segment's upload always fails and GetFailedUploads() reports it.
class SegmentRejectingObjectStore : public InMemoryObjectStore {
 public:
  // Call before any segment is flushed. Upload threads read the value only
  // after FlushSegment hands them the task through the pool's queue lock.
  void RejectSegment(core::SegmentId id) {
    rejected_key_part_ =
        "/segments/" + std::to_string(core::ToUInt32(id)) + "/";
  }

  core::Status PutObjectFromFile(
      const std::string& key, const std::string& local_file_path) override {
    if (!rejected_key_part_.empty() &&
        key.find(rejected_key_part_) != std::string::npos) {
      return core::InternalError("injected put failure for a rejected segment");
    }
    return InMemoryObjectStore::PutObjectFromFile(key, local_file_path);
  }

 private:
  std::string rejected_key_part_;
};

struct TieredTest {
  std::unique_ptr<index::IndexFactory> index_factory_;
  core::Dimension dimension_ = core::Dimension(128);
  core::MetricType metric_ = core::MetricType::L2;
  core::CollectionId collection_id_ = core::CollectionId(1);
  const std::string test_dir_ = UniqueTestDir("gvdb_tiered_test");
  const std::string cache_dir_ = test_dir_ + "_cache";

  TieredTest() {
    std::filesystem::create_directories(test_dir_);
    index_factory_ = std::make_unique<index::IndexFactory>();
  }

  ~TieredTest() {
    std::filesystem::remove_all(test_dir_);
    std::filesystem::remove_all(cache_dir_);
  }

  // A tiered manager on the fixture's dirs, and a non-owning pointer to its
  // object store for inspection.
  template <typename Store>
  struct TieredWith {
    std::unique_ptr<TieredSegmentManager> tiered;
    Store* store;  // non-owning
  };

  template <typename Store>
  TieredWith<Store> MakeTieredOver(
      std::unique_ptr<Store> object_store, int upload_threads,
      std::chrono::milliseconds retry_delay) {
    auto* store_ptr = object_store.get();
    auto local = std::make_unique<SegmentManager>(
        test_dir_, index_factory_.get());
    auto cache = std::make_unique<SegmentCache>(cache_dir_, 256 * 1024 * 1024);
    auto tiered = std::make_unique<TieredSegmentManager>(
        std::move(local), std::move(object_store), std::move(cache), kPrefix,
        upload_threads, retry_delay);
    return {std::move(tiered), store_ptr};
  }

  std::unique_ptr<TieredSegmentManager> MakeTiered() {
    return MakeTieredWithStore().tiered;
  }

  // A healthy in-memory store never fails an upload, so no retry runs and
  // the retry delay does not matter.
  TieredWith<InMemoryObjectStore> MakeTieredWithStore(int upload_threads = 1) {
    return MakeTieredOver(std::make_unique<InMemoryObjectStore>(),
                          upload_threads, std::chrono::seconds(3));
  }

  // Tiered manager backed by a FlakyObjectStore with a millisecond-scale retry
  // delay so background retries can be observed quickly in tests.
  TieredWith<FlakyObjectStore> MakeTieredFlaky(
      int put_failures, std::chrono::milliseconds retry_delay) {
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
  core::SegmentId SealOneSegment(
      TieredSegmentManager& tiered, size_t count, uint64_t first_id = 1) {
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

  std::vector<core::Vector> MakeVectors(size_t count) {
    std::vector<core::Vector> vectors;
    vectors.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      vectors.push_back(core::RandomVector(dimension_));
    }
    return vectors;
  }

  std::vector<core::VectorId> MakeIds(size_t count, uint64_t start = 1) {
    std::vector<core::VectorId> ids;
    ids.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      ids.push_back(core::VectorId(start + i));
    }
    return ids;
  }

  // Object-store key prefix of a segment in the fixture's collection.
  std::string SegmentKeyPrefix(core::SegmentId id) const {
    return kPrefix + "/collections/" +
           std::to_string(core::ToUInt32(collection_id_)) + "/segments/" +
           std::to_string(core::ToUInt32(id)) + "/";
  }

  // Calls `done` every 5 ms until it returns true or 10 s pass. Returns
  // whether `done` returned true.
  template <typename Predicate>
  static bool PollUntil(Predicate done) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!done()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return true;
  }

  // Polls the manifest until it lists `expected` entries. Returns the entries
  // of the last successful read, so a timeout shows up as a size mismatch.
  static std::vector<ManifestEntry> WaitForManifestEntries(
      InMemoryObjectStore& store, size_t expected) {
    std::vector<ManifestEntry> entries;
    (void)PollUntil([&] {
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

TEST_CASE_FIXTURE(TieredTest, "CreateSegment") {
  auto tiered = MakeTiered();
  auto result = tiered->CreateSegment(collection_id_, dimension_, metric_);
  REQUIRE(result.ok());
}

TEST_CASE_FIXTURE(TieredTest, "WriteAndReadVectors") {
  auto tiered = MakeTiered();
  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);

  auto vectors = MakeVectors(10);
  auto ids = MakeIds(10);
  REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

  auto* segment = tiered->GetSegment(seg_id);
  REQUIRE(segment != nullptr);
  auto read_result = segment->ReadVectors({ids[0]});
  REQUIRE(read_result.ok());
  CHECK_EQ(read_result->size(), 1);
}

TEST_CASE_FIXTURE(TieredTest, "FlushTriggersUpload") {
  auto [tiered, store] = MakeTieredWithStore();
  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);

  auto vectors = MakeVectors(100);
  auto ids = MakeIds(100);
  REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

  // Seal the segment (required before flush)
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;
  REQUIRE(tiered->SealSegment(seg_id, config).ok());

  // Flush — should trigger async upload
  REQUIRE(tiered->FlushSegment(seg_id).ok());

  // Wait for the async upload: it writes the manifest after the files.
  REQUIRE_EQ(WaitForManifestEntries(*store, 1).size(), 1);

  // Verify objects were uploaded to the mock store
  CHECK(store->ObjectCount() > 0);

  // Check manifest exists
  auto manifest = store->GetObject("test-prefix/manifest.json");
  CHECK(manifest.ok());
}

TEST_CASE_FIXTURE(TieredTest, "SealAndSearch") {
  auto tiered = MakeTiered();
  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);

  auto vectors = MakeVectors(50);
  auto ids = MakeIds(50);
  REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;
  REQUIRE(tiered->SealSegment(seg_id, config).ok());

  auto* segment = tiered->GetSegment(seg_id);
  REQUIRE(segment != nullptr);
  auto search_result = segment->Search(vectors[0], 5);
  REQUIRE(search_result.ok());
  CHECK(search_result->entries.size() > 0);
}

TEST_CASE_FIXTURE(TieredTest, "SearchCollection") {
  auto tiered = MakeTiered();
  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);

  auto vectors = MakeVectors(50);
  auto ids = MakeIds(50);
  REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;
  REQUIRE(tiered->SealSegment(seg_id, config).ok());

  auto result = tiered->SearchCollection(collection_id_, vectors[0], 5);
  REQUIRE(result.ok());
  CHECK(result->entries.size() > 0);
}

TEST_CASE_FIXTURE(TieredTest, "GetSegmentCount") {
  auto tiered = MakeTiered();
  CHECK_EQ(tiered->GetSegmentCount(), 0);
  REQUIRE(tiered->CreateSegment(collection_id_, dimension_, metric_).ok());
  CHECK_EQ(tiered->GetSegmentCount(), 1);
}

TEST_CASE_FIXTURE(TieredTest, "GetCollectionSegments") {
  auto tiered = MakeTiered();
  REQUIRE(tiered->CreateSegment(collection_id_, dimension_, metric_).ok());
  auto col2 = core::CollectionId(2);
  REQUIRE(tiered->CreateSegment(col2, dimension_, metric_).ok());

  auto col1_segs = tiered->GetCollectionSegments(collection_id_);
  CHECK_EQ(col1_segs.size(), 1);

  auto col2_segs = tiered->GetCollectionSegments(col2);
  CHECK_EQ(col2_segs.size(), 1);
}

TEST_CASE_FIXTURE(TieredTest, "Clear") {
  auto tiered = MakeTiered();
  REQUIRE(tiered->CreateSegment(collection_id_, dimension_, metric_).ok());
  CHECK_EQ(tiered->GetSegmentCount(), 1);
  tiered->Clear();
  CHECK_EQ(tiered->GetSegmentCount(), 0);
}

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

TEST_CASE_FIXTURE(TieredTest, "DropSegment") {
  auto tiered = MakeTiered();
  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);
  CHECK_EQ(tiered->GetSegmentCount(), 1);

  REQUIRE(tiered->DropSegment(seg_id).ok());
  CHECK_EQ(tiered->GetSegmentCount(), 0);
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
  bool marker_failed = PollUntil([&] {
    auto failed = tiered->GetFailedUploads();
    return std::find(failed.begin(), failed.end(), marker_id) != failed.end();
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
  CHECK_FALSE(std::filesystem::exists(
      test_dir_ + "/segment_" + std::to_string(core::ToUInt32(seg_id))));

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
  std::vector<uint32_t> collections = {entries[0].collection_id,
                                       entries[1].collection_id};
  std::sort(collections.begin(), collections.end());
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
  for (auto seg_id : seg_ids) {
    expected.push_back(core::ToUInt32(seg_id));
  }
  std::sort(listed.begin(), listed.end());
  std::sort(expected.begin(), expected.end());
  CHECK_EQ(listed, expected);
  CHECK(tiered->GetFailedUploads().empty());
}

TEST_CASE_FIXTURE(TieredTest, "SetSealCallback") {
  auto tiered = MakeTiered();
  bool callback_called = false;
  tiered->SetSealCallback([&](core::SegmentId, core::IndexType) {
    callback_called = true;
  });

  // GetWritableSegment with a segment that's full should trigger callback
  // This test just verifies the callback is wired through
  CHECK_FALSE(callback_called);
}

TEST_CASE_FIXTURE(TieredTest, "LoadAllSegmentsLocal") {
  // Create and flush a segment
  {
    auto tiered = MakeTiered();
    auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);
    auto vectors = MakeVectors(20);
    auto ids = MakeIds(20);
    REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

    core::IndexConfig config;
    config.index_type = core::IndexType::FLAT;
    config.dimension = dimension_;
    config.metric_type = metric_;
    REQUIRE(tiered->SealSegment(seg_id, config).ok());
    REQUIRE(tiered->FlushSegment(seg_id).ok());
  }

  // Create fresh tiered manager and load
  auto tiered2 = MakeTiered();
  REQUIRE(tiered2->LoadAllSegments().ok());

  // Should have recovered the segment from local disk
  CHECK(tiered2->GetSegmentCount() >= 1);
}

TEST_CASE_FIXTURE(TieredTest, "FailedUploadRetriesAndRecovers") {
  // Store rejects the first two upload attempts, then succeeds. A short retry
  // base delay keeps the exponential backoff (25ms, 50ms) inside the test.
  auto [tiered, store] = MakeTieredFlaky(
      /*put_failures=*/2, std::chrono::milliseconds(25));
  auto seg_id = FlushOneSegment(*tiered, 50);

  // The first attempt fails, so the segment must surface as a failed upload.
  bool saw_failure = false;
  for (int i = 0; i < 200; ++i) {
    auto failed = tiered->GetFailedUploads();
    if (!failed.empty()) {
      CHECK_EQ(failed.size(), 1);
      CHECK_EQ(core::ToUInt32(failed[0]), core::ToUInt32(seg_id));
      saw_failure = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  CHECK(saw_failure);

  // The background retry thread eventually re-drives the upload to success.
  // The manifest is written after the segment files, so wait for it too.
  bool recovered = false;
  for (int i = 0; i < 300; ++i) {
    if (tiered->GetFailedUploads().empty() &&
        store->GetObject("test-prefix/manifest.json").ok()) {
      recovered = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
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

  // Wait until the upload has failed and is queued for retry.
  bool failed = false;
  for (int i = 0; i < 200; ++i) {
    if (!tiered->GetFailedUploads().empty()) {
      failed = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
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
    bool failed = false;
    for (int i = 0; i < 200; ++i) {
      if (!parts.tiered->GetFailedUploads().empty()) {
        failed = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    CHECK(failed);
  }

  // Phase 2: a fresh manager with a healthy store on the same local dirs.
  // Startup reconciliation detects the FLUSHED-but-unlisted segment and
  // re-drives its upload.
  auto [tiered2, store2] = MakeTieredWithStore();
  REQUIRE(tiered2->LoadAllSegments().ok());

  bool uploaded = false;
  for (int i = 0; i < 300; ++i) {
    if (store2->GetObject("test-prefix/manifest.json").ok()) {
      uploaded = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  CHECK(uploaded);
  CHECK(store2->ObjectCount() > 0);
  CHECK(tiered2->GetFailedUploads().empty());
}

}  // namespace
}  // namespace storage
}  // namespace gvdb
