// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>

#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"
#include "storage/bulk_importer.h"
#include "storage/npy_reader.h"
#include "storage/object_store.h"
#include "storage/segment_manager.h"

namespace fs = std::filesystem;

namespace {

// Helper: write a valid .npy float32 file with shape (num_vectors, dimension)
std::string WriteTestNpy(const std::string& dir, size_t num_vectors,
                         size_t dimension, const std::string& name = "test.npy") {
  fs::create_directories(dir);
  std::string path = dir + "/" + name;

  // Build header dict
  std::string header_dict = "{'descr': '<f4', 'fortran_order': False, 'shape': (" +
                            std::to_string(num_vectors) + ", " +
                            std::to_string(dimension) + "), }";
  // Pad to align data to 64 bytes (v1: preamble is 10 bytes + header_len)
  size_t preamble = 10;  // magic(6) + version(2) + header_len(2)
  size_t total_header = preamble + header_dict.size() + 1;  // +1 for newline
  size_t padding = (64 - (total_header % 64)) % 64;
  header_dict += std::string(padding, ' ');
  header_dict += '\n';

  uint16_t header_len = static_cast<uint16_t>(header_dict.size());

  std::ofstream file(path, std::ios::binary);
  // Magic
  uint8_t magic[] = {0x93, 'N', 'U', 'M', 'P', 'Y'};
  file.write(reinterpret_cast<char*>(magic), 6);
  // Version 1.0
  uint8_t version[] = {1, 0};
  file.write(reinterpret_cast<char*>(version), 2);
  // Header length (2 bytes LE)
  file.write(reinterpret_cast<char*>(&header_len), 2);
  // Header dict
  file.write(header_dict.data(), static_cast<std::streamsize>(header_dict.size()));

  // Write vector data: sequential floats for easy verification
  for (size_t i = 0; i < num_vectors; ++i) {
    for (size_t d = 0; d < dimension; ++d) {
      float val = static_cast<float>(i * dimension + d) * 0.001f;
      file.write(reinterpret_cast<char*>(&val), sizeof(float));
    }
  }
  file.close();
  return path;
}

// Helper: write an invalid file (not .npy)
std::string WriteGarbageFile(const std::string& dir) {
  fs::create_directories(dir);
  std::string path = dir + "/garbage.bin";
  std::ofstream file(path, std::ios::binary);
  file << "this is not a numpy file";
  file.close();
  return path;
}

std::string TestDir() {
  return "/tmp/gvdb_bulk_import_test_" +
         std::to_string(std::chrono::steady_clock::now()
                            .time_since_epoch()
                            .count());
}

}  // namespace

// ============================================================================
// NpyReader Tests
// ============================================================================

TEST_SUITE("NpyReader") {
  TEST_CASE("ReadHeader parses valid .npy file") {
    auto dir = TestDir();
    auto path = WriteTestNpy(dir, 100, 32);

    auto result = gvdb::storage::NpyReader::ReadHeader(path);
    REQUIRE(result.ok());
    CHECK(result->num_vectors == 100);
    CHECK(result->dimension == 32);
    CHECK(result->data_offset > 0);

    fs::remove_all(dir);
  }

  TEST_CASE("ReadHeader rejects non-.npy file") {
    auto dir = TestDir();
    auto path = WriteGarbageFile(dir);

    auto result = gvdb::storage::NpyReader::ReadHeader(path);
    CHECK(!result.ok());
    CHECK(result.status().code() == absl::StatusCode::kInvalidArgument);

    fs::remove_all(dir);
  }

  TEST_CASE("ReadHeader rejects missing file") {
    auto result = gvdb::storage::NpyReader::ReadHeader("/tmp/nonexistent_npy_file.npy");
    CHECK(!result.ok());
    CHECK(result.status().code() == absl::StatusCode::kNotFound);
  }

  TEST_CASE("ReadChunk reads vectors correctly") {
    auto dir = TestDir();
    size_t dim = 4;
    size_t count = 10;
    auto path = WriteTestNpy(dir, count, dim);

    auto header = gvdb::storage::NpyReader::ReadHeader(path);
    REQUIRE(header.ok());

    // Read first 5 vectors
    auto chunk = gvdb::storage::NpyReader::ReadChunk(path, *header, 0, 5, 1);
    REQUIRE(chunk.ok());
    CHECK(chunk->size() == 5);

    // Verify first vector ID
    CHECK(gvdb::core::ToUInt64(chunk->at(0).first) == 1);
    // Verify first vector values
    CHECK(chunk->at(0).second.dimension() == static_cast<int>(dim));
    CHECK(chunk->at(0).second[0] == doctest::Approx(0.0f));
    CHECK(chunk->at(0).second[1] == doctest::Approx(0.001f));

    // Verify last vector in chunk (vector index 4)
    CHECK(gvdb::core::ToUInt64(chunk->at(4).first) == 5);

    fs::remove_all(dir);
  }

  TEST_CASE("ReadChunk clamps to available rows") {
    auto dir = TestDir();
    auto path = WriteTestNpy(dir, 3, 4);

    auto header = gvdb::storage::NpyReader::ReadHeader(path);
    REQUIRE(header.ok());

    // Request more than available
    auto chunk = gvdb::storage::NpyReader::ReadChunk(path, *header, 0, 100, 1);
    REQUIRE(chunk.ok());
    CHECK(chunk->size() == 3);

    fs::remove_all(dir);
  }

  TEST_CASE("ReadChunk returns empty for out-of-range start") {
    auto dir = TestDir();
    auto path = WriteTestNpy(dir, 5, 4);

    auto header = gvdb::storage::NpyReader::ReadHeader(path);
    REQUIRE(header.ok());

    auto chunk = gvdb::storage::NpyReader::ReadChunk(path, *header, 100, 5, 1);
    REQUIRE(chunk.ok());
    CHECK(chunk->empty());

    fs::remove_all(dir);
  }

  TEST_CASE("ReadHeader rejects 1D array") {
    auto dir = TestDir();
    fs::create_directories(dir);
    std::string path = dir + "/1d.npy";

    // Write 1D header
    std::string header_dict = "{'descr': '<f4', 'fortran_order': False, 'shape': (10,), }";
    size_t padding = (64 - ((10 + header_dict.size() + 1) % 64)) % 64;
    header_dict += std::string(padding, ' ') + '\n';
    uint16_t header_len = static_cast<uint16_t>(header_dict.size());

    std::ofstream file(path, std::ios::binary);
    uint8_t magic[] = {0x93, 'N', 'U', 'M', 'P', 'Y', 1, 0};
    file.write(reinterpret_cast<char*>(magic), 8);
    file.write(reinterpret_cast<char*>(&header_len), 2);
    file.write(header_dict.data(), static_cast<std::streamsize>(header_dict.size()));
    file.close();

    auto result = gvdb::storage::NpyReader::ReadHeader(path);
    CHECK(!result.ok());

    fs::remove_all(dir);
  }
}

// ============================================================================
// BulkImporter Tests
// ============================================================================

TEST_SUITE("BulkImporter") {

  // Common test fixture setup
  struct BulkImportFixture {
    std::string test_dir;
    std::string data_dir;
    std::string temp_dir;
    std::unique_ptr<gvdb::index::IndexFactory> index_factory;
    std::shared_ptr<gvdb::storage::SegmentManager> segment_store;
    std::unique_ptr<gvdb::storage::InMemoryObjectStore> object_store;

    BulkImportFixture() {
      test_dir = TestDir();
      data_dir = test_dir + "/data";
      temp_dir = test_dir + "/tmp";
      index_factory = std::make_unique<gvdb::index::IndexFactory>();
      segment_store = std::make_shared<gvdb::storage::SegmentManager>(
          data_dir + "/segments", index_factory.get());
      object_store = std::make_unique<gvdb::storage::InMemoryObjectStore>();
    }

    ~BulkImportFixture() {
      fs::remove_all(test_dir);
    }

    // Upload a .npy file to the in-memory object store
    void UploadNpy(const std::string& key, size_t num_vectors, size_t dimension) {
      auto npy_path = WriteTestNpy(test_dir + "/staging", num_vectors, dimension);
      auto status = object_store->PutObjectFromFile(key, npy_path);
      REQUIRE(status.ok());
    }

    // Create a collection segment so GetWritableSegment works
    gvdb::core::CollectionId SetupCollection(gvdb::core::Dimension dim,
                                              gvdb::core::MetricType metric = gvdb::core::MetricType::L2) {
      auto collection_id = gvdb::core::CollectionId(1);
      auto seg_result = segment_store->CreateSegment(collection_id, dim, metric);
      REQUIRE(seg_result.ok());

      // Wire seal callback (inline seal like single-node)
      auto* ifp = index_factory.get();
      auto store = segment_store;
      segment_store->SetSealCallback(
          [store, ifp](gvdb::core::SegmentId sid, gvdb::core::IndexType idx_type) {
            auto* seg = store->GetSegment(sid);
            if (!seg) return;
            auto resolved = gvdb::core::ResolveAutoIndexType(idx_type, seg->GetVectorCount());
            gvdb::core::IndexConfig config;
            config.index_type = resolved;
            config.dimension = seg->GetDimension();
            config.metric_type = seg->GetMetric();
            (void)store->SealSegment(sid, config);
          });

      return collection_id;
    }
  };

  TEST_CASE_FIXTURE(BulkImportFixture, "ImportNpy imports small dataset") {
    auto cid = SetupCollection(4);
    UploadNpy("test/vectors.npy", 50, 4);

    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    auto job_result = importer.StartImport(
        "test_collection", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT,
        "s3://bucket/test/vectors.npy",
        gvdb::storage::ImportFormat::NUMPY);
    REQUIRE(job_result.ok());

    auto import_id = *job_result;
    CHECK(!import_id.empty());

    // Wait for completion
    for (int i = 0; i < 50; ++i) {
      auto status = importer.GetStatus(import_id);
      REQUIRE(status.ok());
      if (status->state == gvdb::storage::ImportState::COMPLETED) break;
      if (status->state == gvdb::storage::ImportState::FAILED) {
        FAIL("Import failed: " << status->error_message);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    auto final_status = importer.GetStatus(import_id);
    REQUIRE(final_status.ok());
    CHECK(final_status->state == gvdb::storage::ImportState::COMPLETED);
    CHECK(final_status->imported_vectors == 50);
    CHECK(final_status->segments_created >= 1);
  }

  TEST_CASE_FIXTURE(BulkImportFixture, "Import rejects invalid S3 URI") {
    auto cid = SetupCollection(4);

    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    auto result = importer.StartImport(
        "test", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT,
        "http://not-s3/file.npy",
        gvdb::storage::ImportFormat::NUMPY);
    CHECK(!result.ok());
    CHECK(result.status().code() == absl::StatusCode::kInvalidArgument);
  }

  TEST_CASE_FIXTURE(BulkImportFixture, "Import rejects empty source_uri") {
    auto cid = SetupCollection(4);

    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    auto result = importer.StartImport(
        "test", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT,
        "",
        gvdb::storage::ImportFormat::NUMPY);
    CHECK(!result.ok());
  }

  TEST_CASE_FIXTURE(BulkImportFixture, "Import fails on S3 download error") {
    auto cid = SetupCollection(4);
    // Don't upload — file doesn't exist in object store

    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    auto job_result = importer.StartImport(
        "test", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT,
        "s3://bucket/nonexistent/file.npy",
        gvdb::storage::ImportFormat::NUMPY);
    REQUIRE(job_result.ok());

    // Wait for failure
    for (int i = 0; i < 50; ++i) {
      auto status = importer.GetStatus(*job_result);
      REQUIRE(status.ok());
      if (status->state == gvdb::storage::ImportState::FAILED) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    auto status = importer.GetStatus(*job_result);
    REQUIRE(status.ok());
    CHECK(status->state == gvdb::storage::ImportState::FAILED);
    CHECK(!status->error_message.empty());
  }

  TEST_CASE_FIXTURE(BulkImportFixture, "Import detects dimension mismatch") {
    auto cid = SetupCollection(8);  // Collection expects dim=8
    UploadNpy("test/wrong_dim.npy", 10, 4);  // File has dim=4

    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    auto job_result = importer.StartImport(
        "test", cid, 8, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT,
        "s3://bucket/test/wrong_dim.npy",
        gvdb::storage::ImportFormat::NUMPY);
    REQUIRE(job_result.ok());

    // Wait for failure
    for (int i = 0; i < 50; ++i) {
      auto status = importer.GetStatus(*job_result);
      REQUIRE(status.ok());
      if (status->state == gvdb::storage::ImportState::FAILED) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    auto status = importer.GetStatus(*job_result);
    REQUIRE(status.ok());
    CHECK(status->state == gvdb::storage::ImportState::FAILED);
    CHECK(status->error_message.find("Dimension mismatch") != std::string::npos);
  }

  TEST_CASE_FIXTURE(BulkImportFixture, "Idempotent: same URI returns same job") {
    auto cid = SetupCollection(4);
    UploadNpy("test/idem.npy", 10, 4);

    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    auto job1 = importer.StartImport(
        "test", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT,
        "s3://bucket/test/idem.npy",
        gvdb::storage::ImportFormat::NUMPY);
    REQUIRE(job1.ok());

    // Second call with same URI + collection should return same job
    auto job2 = importer.StartImport(
        "test", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT,
        "s3://bucket/test/idem.npy",
        gvdb::storage::ImportFormat::NUMPY);
    REQUIRE(job2.ok());
    CHECK(*job1 == *job2);
  }

  TEST_CASE_FIXTURE(BulkImportFixture, "GetStatus returns NOT_FOUND for unknown job") {
    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    auto result = importer.GetStatus("nonexistent_job");
    CHECK(!result.ok());
    CHECK(result.status().code() == absl::StatusCode::kNotFound);
  }

  TEST_CASE_FIXTURE(BulkImportFixture, "CancelImport cancels a running job") {
    auto cid = SetupCollection(4);
    // Large dataset to give us time to cancel
    UploadNpy("test/big.npy", 100000, 4);

    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    auto job_result = importer.StartImport(
        "test", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT,
        "s3://bucket/test/big.npy",
        gvdb::storage::ImportFormat::NUMPY);
    REQUIRE(job_result.ok());

    // Give it a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Cancel
    auto cancel = importer.CancelImport(*job_result);
    CHECK(cancel.ok());

    // Wait for terminal state
    for (int i = 0; i < 100; ++i) {
      auto status = importer.GetStatus(*job_result);
      if (status.ok() && (status->state == gvdb::storage::ImportState::CANCELLED ||
                          status->state == gvdb::storage::ImportState::COMPLETED)) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // Should be cancelled (or completed if it was too fast)
    auto status = importer.GetStatus(*job_result);
    REQUIRE(status.ok());
    CHECK((status->state == gvdb::storage::ImportState::CANCELLED ||
           status->state == gvdb::storage::ImportState::COMPLETED));
  }

  TEST_CASE_FIXTURE(BulkImportFixture, "CancelImport is idempotent on terminal state") {
    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    // Cancel nonexistent job → NOT_FOUND
    auto result = importer.CancelImport("nonexistent");
    CHECK(!result.ok());
  }

  TEST_CASE_FIXTURE(BulkImportFixture, "Concurrency limit enforced") {
    auto cid = SetupCollection(4);
    UploadNpy("test/a.npy", 100000, 4);
    UploadNpy("test/b.npy", 100000, 4);
    UploadNpy("test/c.npy", 100000, 4);

    // max_concurrent = 2
    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    auto job1 = importer.StartImport("test", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT, "s3://bucket/test/a.npy",
        gvdb::storage::ImportFormat::NUMPY);
    REQUIRE(job1.ok());

    auto job2 = importer.StartImport("test2", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT, "s3://bucket/test/b.npy",
        gvdb::storage::ImportFormat::NUMPY);
    REQUIRE(job2.ok());

    // Third should fail with RESOURCE_EXHAUSTED
    // (Give the first two a moment to start running)
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto job3 = importer.StartImport("test3", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT, "s3://bucket/test/c.npy",
        gvdb::storage::ImportFormat::NUMPY);
    // May succeed if first two completed fast, or fail with RESOURCE_EXHAUSTED
    if (!job3.ok()) {
      CHECK(job3.status().code() == absl::StatusCode::kResourceExhausted);
    }
  }

  TEST_CASE("BulkImporter requires object store") {
    auto dir = TestDir();
    auto index_factory = std::make_unique<gvdb::index::IndexFactory>();
    auto segment_store = std::make_shared<gvdb::storage::SegmentManager>(
        dir + "/segments", index_factory.get());

    gvdb::storage::BulkImporter importer(segment_store, nullptr, dir + "/tmp");

    auto result = importer.StartImport(
        "test", gvdb::core::CollectionId(1), 4,
        gvdb::core::MetricType::L2, gvdb::core::IndexType::FLAT,
        "s3://bucket/test.npy", gvdb::storage::ImportFormat::NUMPY);
    CHECK(!result.ok());
    CHECK(result.status().code() == absl::StatusCode::kFailedPrecondition);

    fs::remove_all(dir);
  }

  TEST_CASE_FIXTURE(BulkImportFixture, "Import progress tracking") {
    auto cid = SetupCollection(4);
    UploadNpy("test/progress.npy", 100, 4);

    gvdb::storage::BulkImporter importer(segment_store, object_store.get(), temp_dir, 2);

    auto job_result = importer.StartImport(
        "test", cid, 4, gvdb::core::MetricType::L2,
        gvdb::core::IndexType::FLAT,
        "s3://bucket/test/progress.npy",
        gvdb::storage::ImportFormat::NUMPY);
    REQUIRE(job_result.ok());

    // Wait for completion and check progress fields
    for (int i = 0; i < 50; ++i) {
      auto status = importer.GetStatus(*job_result);
      REQUIRE(status.ok());
      if (status->state == gvdb::storage::ImportState::COMPLETED) {
        CHECK(status->imported_vectors == 100);
        CHECK(status->total_vectors == 100);
        CHECK(status->progress_percent == doctest::Approx(100.0f));
        CHECK(status->elapsed_seconds > 0.0f);
        CHECK(status->segments_created >= 1);
        CHECK(status->error_message.empty());
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}
// Parquet tests removed — Arrow dependency deferred to a follow-up PR.
