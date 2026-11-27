// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <algorithm>
#include <cctype>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/vectordb_service.h"
#include "storage/import/bulk_importer.h"
#include "storage/objstore/object_store.h"
#include "storage/segment/segment_manager.h"
#include "support/temp_dir.h"
#include "support/wait.h"

using namespace gvdb;

namespace {

constexpr uint32_t kDim = 8;
constexpr size_t kNumVectors = 300;

// Value of column d in row r of the test matrix. Rows are far apart so the
// nearest neighbor of a row is always the row itself.
float RowValue(size_t row, size_t d) {
  return static_cast<float>(row) + static_cast<float>(d) * 0.01f;
}

std::vector<float> Row(size_t row, size_t dimension) {
  std::vector<float> values(dimension);
  for (size_t d = 0; d < dimension; ++d) values[d] = RowValue(row, d);
  return values;
}

// Builds the bytes of a float32 .npy v1.0 file with shape
// (num_vectors, dimension), filled with RowValue.
std::string BuildNpy(size_t num_vectors, size_t dimension) {
  std::string header_dict = "{'descr': '<f4', 'fortran_order': False, 'shape': (" +
                            std::to_string(num_vectors) + ", " +
                            std::to_string(dimension) + "), }";
  // Pad so that the data starts on a 64 byte boundary. The preamble is the
  // magic (6), the version (2) and the header length (2).
  const size_t preamble = 10;
  const size_t total_header = preamble + header_dict.size() + 1;
  header_dict += std::string((64 - (total_header % 64)) % 64, ' ');
  header_dict += '\n';
  const auto header_len = static_cast<uint16_t>(header_dict.size());

  std::string out;
  out.append("\x93NUMPY", 6);
  out.push_back(static_cast<char>(1));
  out.push_back(static_cast<char>(0));
  out.push_back(static_cast<char>(header_len & 0xFF));
  out.push_back(static_cast<char>((header_len >> 8) & 0xFF));
  out += header_dict;
  for (size_t r = 0; r < num_vectors; ++r) {
    for (size_t d = 0; d < dimension; ++d) {
      float v = RowValue(r, d);
      char bytes[sizeof(float)];
      std::memcpy(bytes, &v, sizeof(float));
      out.append(bytes, sizeof(float));
    }
  }
  return out;
}

std::string ToLower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

bool IsTerminal(proto::ImportState state) {
  return state == proto::IMPORT_COMPLETED || state == proto::IMPORT_FAILED ||
         state == proto::IMPORT_CANCELLED;
}

// An in-memory object store whose downloads can be held. While held,
// GetObjectToFile blocks until Release() is called, which keeps an import
// job in the RUNNING state for as long as a test needs it.
class GatedObjectStore : public storage::IObjectStore {
 public:
  void Hold() {
    std::lock_guard lock(mutex_);
    held_ = true;
  }

  void Release() {
    {
      std::lock_guard lock(mutex_);
      held_ = false;
    }
    cv_.notify_all();
  }

  absl::Status PutObject(const std::string& key, const std::string& data) override {
    return inner_.PutObject(key, data);
  }
  absl::StatusOr<std::string> GetObject(const std::string& key) override {
    return inner_.GetObject(key);
  }
  absl::Status DeleteObject(const std::string& key) override {
    return inner_.DeleteObject(key);
  }
  absl::StatusOr<std::vector<std::string>> ListObjects(
      const std::string& prefix) override {
    return inner_.ListObjects(prefix);
  }
  absl::StatusOr<bool> ObjectExists(const std::string& key) override {
    return inner_.ObjectExists(key);
  }
  absl::Status PutObjectFromFile(const std::string& key,
                                 const std::string& local_file_path) override {
    return inner_.PutObjectFromFile(key, local_file_path);
  }
  absl::Status GetObjectToFile(const std::string& key,
                               const std::string& local_file_path) override {
    {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return !held_; });
    }
    return inner_.GetObjectToFile(key, local_file_path);
  }

 private:
  storage::InMemoryObjectStore inner_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool held_ = false;
};

// Local (single-node) VectorDBService. When with_importer is true the
// service gets a BulkImporter that reads from a GatedObjectStore.
class ImportServiceFixtureBase {
 public:
  explicit ImportServiceFixtureBase(bool with_importer) {
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>(
        (test_dir_ / "data").string(), index_factory_.get());
    query_executor_ = std::make_shared<compute::QueryExecutor>(
        *segment_store_);

    object_store_ = std::make_unique<GatedObjectStore>();
    if (with_importer) {
      importer_ = std::make_shared<storage::BulkImporter>(
          segment_store_, object_store_.get(), (test_dir_ / "import_tmp").string(),
          2);
    }

    auto resolver = service::MakeLocalResolver(segment_store_);
    service_ = std::make_unique<service::VectorDBService>(
        segment_store_, query_executor_, std::move(resolver),
        service::VectorDBServiceOptions{.bulk_importer = importer_});
  }

  ~ImportServiceFixtureBase() {
    // Unblock any held download so the import thread pool can join.
    object_store_->Release();
    service_.reset();
    importer_.reset();
    object_store_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();
    // test_dir_ is the first member, so it removes the folder last.
  }

  void CreateCollection(const std::string& name, uint32_t dimension) {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name(name);
    request.set_dimension(dimension);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
  }

  void PutNpy(const std::string& key, size_t num_vectors, size_t dimension) {
    auto status = object_store_->PutObject(key, BuildNpy(num_vectors, dimension));
    REQUIRE(status.ok());
  }

  grpc::Status StartImport(const std::string& collection,
                           const std::string& source_uri,
                           proto::BulkImportResponse* response,
                           proto::ImportFormat format = proto::NUMPY) {
    grpc::ServerContext context;
    proto::BulkImportRequest request;
    request.set_collection_name(collection);
    request.set_source_uri(source_uri);
    request.set_format(format);
    return service_->BulkImport(&context, &request, response);
  }

  grpc::Status GetImportStatus(const std::string& import_id,
                               proto::GetImportStatusResponse* response) {
    grpc::ServerContext context;
    proto::GetImportStatusRequest request;
    request.set_import_id(import_id);
    return service_->GetImportStatus(&context, &request, response);
  }

  grpc::Status CancelImport(const std::string& import_id,
                            proto::CancelImportResponse* response) {
    grpc::ServerContext context;
    proto::CancelImportRequest request;
    request.set_import_id(import_id);
    return service_->CancelImport(&context, &request, response);
  }

  // Polls GetImportStatus until the predicate holds or the WaitFor timeout
  // passes, and returns the last response. The callers check its state.
  template <typename Pred>
  proto::GetImportStatusResponse PollStatus(const std::string& import_id,
                                            Pred pred) {
    proto::GetImportStatusResponse response;
    test::WaitFor([&] {
      response.Clear();
      auto status = GetImportStatus(import_id, &response);
      INFO(status.error_message());
      REQUIRE(status.ok());
      return pred(response);
    });
    return response;
  }

  proto::GetImportStatusResponse WaitForTerminal(const std::string& import_id) {
    auto response = PollStatus(import_id, [](const auto& r) {
      return IsTerminal(r.state());
    });
    INFO("state=" << proto::ImportState_Name(response.state())
                  << " error=" << response.error_message());
    REQUIRE(IsTerminal(response.state()));
    return response;
  }

  proto::SearchResponse Search(const std::string& collection,
                               const std::vector<float>& query,
                               uint32_t top_k) {
    grpc::ServerContext context;
    proto::SearchRequest request;
    request.set_collection_name(collection);
    request.mutable_query_vector()->set_dimension(
        static_cast<uint32_t>(query.size()));
    for (float v : query) request.mutable_query_vector()->add_values(v);
    request.set_top_k(top_k);
    proto::SearchResponse response;
    auto status = service_->Search(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return response;
  }

  proto::GetResponse Get(const std::string& collection,
                         const std::vector<uint64_t>& ids) {
    grpc::ServerContext context;
    proto::GetRequest request;
    request.set_collection_name(collection);
    for (auto id : ids) request.add_ids(id);
    proto::GetResponse response;
    auto status = service_->Get(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return response;
  }

  uint64_t CollectionVectorCount(const std::string& collection) {
    grpc::ServerContext context;
    proto::ListCollectionsRequest request;
    proto::ListCollectionsResponse response;
    auto status = service_->ListCollections(&context, &request, &response);
    REQUIRE(status.ok());
    for (const auto& c : response.collections()) {
      if (c.collection_name() == collection) return c.vector_count();
    }
    FAIL("collection not listed: " << collection);
    return 0;
  }

  test::ScopedTempDir test_dir_{"gvdb_import_service_test"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<GatedObjectStore> object_store_;
  std::shared_ptr<storage::BulkImporter> importer_;
  std::unique_ptr<service::VectorDBService> service_;
};

class ImportServiceTest : public ImportServiceFixtureBase {
 public:
  ImportServiceTest() : ImportServiceFixtureBase(/*with_importer=*/true) {}
};

class NoImporterServiceTest : public ImportServiceFixtureBase {
 public:
  NoImporterServiceTest() : ImportServiceFixtureBase(/*with_importer=*/false) {}
};

}  // namespace

// ============================================================================
// Argument validation
// ============================================================================

TEST_CASE_FIXTURE(ImportServiceTest, "BulkImportRejectsMissingCollectionName") {
  PutNpy("data/vectors.npy", 10, kDim);
  proto::BulkImportResponse response;
  auto status = StartImport("", "s3://bucket/data/vectors.npy", &response);
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  CHECK(response.import_id().empty());
}

TEST_CASE_FIXTURE(ImportServiceTest, "BulkImportRejectsMissingSourceUri") {
  CreateCollection("coll", kDim);
  proto::BulkImportResponse response;
  auto status = StartImport("coll", "", &response);
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  CHECK(response.import_id().empty());
}

TEST_CASE_FIXTURE(ImportServiceTest, "BulkImportUnknownCollectionIsNotFound") {
  PutNpy("data/vectors.npy", 10, kDim);
  proto::BulkImportResponse response;
  auto status = StartImport("missing", "s3://bucket/data/vectors.npy", &response);
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
  CHECK(response.import_id().empty());
}

TEST_CASE_FIXTURE(ImportServiceTest, "BulkImportRejectsUnsupportedFormat") {
  CreateCollection("coll", kDim);
  PutNpy("data/vectors.npy", 10, kDim);
  proto::BulkImportResponse response;
  auto status = StartImport("coll", "s3://bucket/data/vectors.npy", &response,
                            static_cast<proto::ImportFormat>(42));
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  CHECK(response.import_id().empty());
}

TEST_CASE_FIXTURE(ImportServiceTest, "BulkImportRejectsNonS3SourceUri") {
  CreateCollection("coll", kDim);
  proto::BulkImportResponse response;
  auto status = StartImport("coll", "http://host/data/vectors.npy", &response);
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  CHECK(response.import_id().empty());

  // A bucket with no key is rejected too.
  proto::BulkImportResponse no_key;
  status = StartImport("coll", "s3://bucket/", &no_key);
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  CHECK(no_key.import_id().empty());
}

TEST_CASE_FIXTURE(ImportServiceTest, "ImportStatusAndCancelRequireImportId") {
  proto::GetImportStatusResponse status_response;
  CHECK_EQ(GetImportStatus("", &status_response).error_code(),
           grpc::StatusCode::INVALID_ARGUMENT);

  proto::CancelImportResponse cancel_response;
  CHECK_EQ(CancelImport("", &cancel_response).error_code(),
           grpc::StatusCode::INVALID_ARGUMENT);
  CHECK_FALSE(cancel_response.success());
}

TEST_CASE_FIXTURE(ImportServiceTest, "GetImportStatusUnknownIdIsNotFound") {
  proto::GetImportStatusResponse response;
  auto status = GetImportStatus("imp_does_not_exist", &response);
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST_CASE_FIXTURE(ImportServiceTest, "CancelImportUnknownIdIsNotFound") {
  proto::CancelImportResponse response;
  auto status = CancelImport("imp_does_not_exist", &response);
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
  CHECK_FALSE(response.success());
}

// ============================================================================
// No importer configured
// ============================================================================

TEST_CASE_FIXTURE(NoImporterServiceTest, "ImportEndpointsUnimplementedWithoutImporter") {
  CreateCollection("coll", kDim);
  PutNpy("data/vectors.npy", 10, kDim);

  proto::BulkImportResponse start_response;
  CHECK_EQ(StartImport("coll", "s3://bucket/data/vectors.npy", &start_response)
               .error_code(),
           grpc::StatusCode::UNIMPLEMENTED);
  CHECK(start_response.import_id().empty());

  proto::GetImportStatusResponse status_response;
  CHECK_EQ(GetImportStatus("imp_any", &status_response).error_code(),
           grpc::StatusCode::UNIMPLEMENTED);

  proto::CancelImportResponse cancel_response;
  CHECK_EQ(CancelImport("imp_any", &cancel_response).error_code(),
           grpc::StatusCode::UNIMPLEMENTED);
  CHECK_FALSE(cancel_response.success());
}

// ============================================================================
// NPY import end to end
// ============================================================================

TEST_CASE_FIXTURE(ImportServiceTest, "NpyImportCompletesAndVectorsAreSearchable") {
  CreateCollection("coll", kDim);
  PutNpy("data/vectors.npy", kNumVectors, kDim);

  proto::BulkImportResponse start;
  auto status = StartImport("coll", "s3://bucket/data/vectors.npy", &start);
  INFO(status.error_message());
  REQUIRE(status.ok());
  REQUIRE_FALSE(start.import_id().empty());
  CHECK_FALSE(start.message().empty());

  auto final_status = WaitForTerminal(start.import_id());
  INFO("error=" << final_status.error_message());
  REQUIRE_EQ(final_status.state(), proto::IMPORT_COMPLETED);
  CHECK_EQ(final_status.import_id(), start.import_id());
  CHECK_EQ(final_status.total_vectors(), kNumVectors);
  CHECK_EQ(final_status.imported_vectors(), kNumVectors);
  CHECK(final_status.progress_percent() == doctest::Approx(100.0f));
  CHECK(final_status.error_message().empty());
  CHECK_GE(final_status.segments_created(), 1u);
  CHECK_GE(final_status.elapsed_seconds(), 0.0f);

  CHECK_EQ(CollectionVectorCount("coll"), kNumVectors);

  // Rows get ids 1..N in file order. Each row's nearest neighbor is itself
  // at distance zero (FLAT + L2).
  for (size_t row : {size_t{0}, size_t{150}, kNumVectors - 1}) {
    CAPTURE(row);
    auto search = Search("coll", Row(row, kDim), 3);
    REQUIRE_EQ(search.results_size(), 3);
    CHECK_EQ(search.results(0).id(), row + 1);
    CHECK(search.results(0).distance() == doctest::Approx(0.0f));
    CHECK_GT(search.results(1).distance(), search.results(0).distance());
  }

  // Every imported id can be fetched with the values from the file, and the
  // id after the last row does not exist.
  std::vector<uint64_t> ids;
  for (uint64_t id = 1; id <= kNumVectors; ++id) ids.push_back(id);
  auto fetched = Get("coll", ids);
  CHECK_EQ(fetched.vectors_size(), static_cast<int>(kNumVectors));
  CHECK_EQ(fetched.not_found_ids_size(), 0);
  for (const auto& v : fetched.vectors()) {
    REQUIRE_GE(v.id(), 1u);
    const size_t row = v.id() - 1;
    REQUIRE_EQ(v.vector().values_size(), static_cast<int>(kDim));
    for (size_t d = 0; d < kDim; ++d) {
      CHECK(v.vector().values(static_cast<int>(d)) ==
            doctest::Approx(RowValue(row, d)));
    }
  }

  auto past_end = Get("coll", {kNumVectors + 1});
  CHECK_EQ(past_end.vectors_size(), 0);
  REQUIRE_EQ(past_end.not_found_ids_size(), 1);
  CHECK_EQ(past_end.not_found_ids(0), kNumVectors + 1);
}

TEST_CASE_FIXTURE(ImportServiceTest, "BulkImportSameSourceReturnsExistingJob") {
  CreateCollection("coll", kDim);
  PutNpy("data/vectors.npy", 20, kDim);

  proto::BulkImportResponse first;
  REQUIRE(StartImport("coll", "s3://bucket/data/vectors.npy", &first).ok());
  auto done = WaitForTerminal(first.import_id());
  REQUIRE_EQ(done.state(), proto::IMPORT_COMPLETED);
  CHECK_EQ(done.imported_vectors(), 20u);

  // Importing the same source into the same collection again does not start
  // a second job and does not duplicate the data.
  proto::BulkImportResponse second;
  REQUIRE(StartImport("coll", "s3://bucket/data/vectors.npy", &second).ok());
  CHECK_EQ(second.import_id(), first.import_id());
  auto again = WaitForTerminal(second.import_id());
  CHECK_EQ(again.state(), proto::IMPORT_COMPLETED);
  CHECK_EQ(again.imported_vectors(), 20u);
  CHECK_EQ(CollectionVectorCount("coll"), 20u);
}

TEST_CASE_FIXTURE(ImportServiceTest, "NpyImportWithWrongDimensionFails") {
  CreateCollection("coll", kDim);
  PutNpy("data/wide.npy", 5, kDim * 2);

  proto::BulkImportResponse start;
  REQUIRE(StartImport("coll", "s3://bucket/data/wide.npy", &start).ok());

  auto final_status = WaitForTerminal(start.import_id());
  CHECK_EQ(final_status.state(), proto::IMPORT_FAILED);
  CHECK_EQ(final_status.imported_vectors(), 0u);
  CHECK(ToLower(final_status.error_message()).find("dimension") !=
        std::string::npos);
  CHECK_EQ(CollectionVectorCount("coll"), 0u);
}

TEST_CASE_FIXTURE(ImportServiceTest, "ImportOfMissingObjectFails") {
  CreateCollection("coll", kDim);

  proto::BulkImportResponse start;
  REQUIRE(StartImport("coll", "s3://bucket/data/absent.npy", &start).ok());

  auto final_status = WaitForTerminal(start.import_id());
  CHECK_EQ(final_status.state(), proto::IMPORT_FAILED);
  CHECK_FALSE(final_status.error_message().empty());
  CHECK_EQ(final_status.imported_vectors(), 0u);
}

// ============================================================================
// CancelImport
// ============================================================================

TEST_CASE_FIXTURE(ImportServiceTest, "CancelImportOnFinishedJobKeepsItCompleted") {
  CreateCollection("coll", kDim);
  PutNpy("data/vectors.npy", 10, kDim);

  proto::BulkImportResponse start;
  REQUIRE(StartImport("coll", "s3://bucket/data/vectors.npy", &start).ok());
  REQUIRE_EQ(WaitForTerminal(start.import_id()).state(), proto::IMPORT_COMPLETED);

  proto::CancelImportResponse cancel;
  auto status = CancelImport(start.import_id(), &cancel);
  INFO(status.error_message());
  CHECK(status.ok());
  CHECK(cancel.success());
  CHECK_FALSE(cancel.message().empty());

  proto::GetImportStatusResponse after;
  REQUIRE(GetImportStatus(start.import_id(), &after).ok());
  CHECK_EQ(after.state(), proto::IMPORT_COMPLETED);
  CHECK_EQ(after.imported_vectors(), 10u);
  CHECK_EQ(CollectionVectorCount("coll"), 10u);
}

TEST_CASE_FIXTURE(ImportServiceTest, "CancelImportStopsRunningJob") {
  CreateCollection("coll", kDim);
  PutNpy("data/vectors.npy", 50, kDim);

  // Hold the download so the job stays RUNNING until the cancel is in.
  object_store_->Hold();

  proto::BulkImportResponse start;
  REQUIRE(StartImport("coll", "s3://bucket/data/vectors.npy", &start).ok());

  auto running = PollStatus(start.import_id(), [](const auto& r) {
    return r.state() == proto::IMPORT_RUNNING;
  });
  REQUIRE_EQ(running.state(), proto::IMPORT_RUNNING);

  proto::CancelImportResponse cancel;
  auto status = CancelImport(start.import_id(), &cancel);
  INFO(status.error_message());
  REQUIRE(status.ok());
  CHECK(cancel.success());

  object_store_->Release();

  auto final_status = WaitForTerminal(start.import_id());
  CHECK_EQ(final_status.state(), proto::IMPORT_CANCELLED);
  CHECK_EQ(final_status.imported_vectors(), 0u);
  CHECK_EQ(CollectionVectorCount("coll"), 0u);

  // A cancelled source can be imported again; it gets a new job.
  proto::BulkImportResponse retry;
  REQUIRE(StartImport("coll", "s3://bucket/data/vectors.npy", &retry).ok());
  CHECK_NE(retry.import_id(), start.import_id());
  auto retried = WaitForTerminal(retry.import_id());
  CHECK_EQ(retried.state(), proto::IMPORT_COMPLETED);
  CHECK_EQ(retried.imported_vectors(), 50u);
  CHECK_EQ(CollectionVectorCount("coll"), 50u);
}
