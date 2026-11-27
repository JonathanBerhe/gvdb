// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Backup and restore handlers of VectorDBService in local (single-node)
// mode. The service is wired like the gvdb-single-node binary wires it when
// storage.object_store and storage.local_backup_dir are configured: a
// TieredSegmentManager over the object store, a BackupManager and a
// RestoreManager that use the same object store for S3 targets, and the
// local backup directory as the allowlist root for local targets. An
// InMemoryObjectStore stands in for S3.

#include <doctest/doctest.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "service/collection_resolver.h"
#include "service/vectordb_service.h"
#include "storage/backup/backup_manager.h"
#include "storage/objstore/object_store.h"
#include "storage/segment/segment_cache.h"
#include "storage/segment/segment_manager.h"
#include "storage/segment/tiered_segment_manager.h"
#include "support/temp_dir.h"
#include "support/wait.h"

namespace fs = std::filesystem;
using namespace gvdb;

namespace {

constexpr uint32_t kDim = 8;
constexpr uint64_t kNumVectors = 32;
constexpr const char* kBucket = "gvdb-test";
constexpr const char* kS3Prefix = "e2e-backups";
constexpr const char* kSource = "bkp_source";
constexpr const char* kRestored = "bkp_restored";

// Value of dimension d of the vector with the given id. Vectors are far
// apart, so the nearest neighbor of a stored vector is the vector itself.
float VectorValue(uint64_t id, uint32_t d) {
  return static_cast<float>((id - 1) * 10 + d);
}

std::vector<float> VectorFor(uint64_t id) {
  std::vector<float> values(kDim);
  for (uint32_t d = 0; d < kDim; ++d) values[d] = VectorValue(id, d);
  return values;
}

bool IsTerminal(proto::BackupState state) {
  return state == proto::BACKUP_COMPLETED || state == proto::BACKUP_FAILED ||
         state == proto::BACKUP_CANCELLED;
}

proto::BackupTarget S3Target() {
  proto::BackupTarget target;
  target.mutable_s3()->set_bucket(kBucket);
  target.mutable_s3()->set_prefix(kS3Prefix);
  return target;
}

proto::BackupTarget LocalTarget(const std::string& path) {
  proto::BackupTarget target;
  target.mutable_local()->set_path(path);
  return target;
}

// Local VectorDBService with backup and restore. When with_backup_storage
// is false the server has neither an object store nor a local backup
// directory, which is the default gvdb-single-node configuration.
class BackupServiceFixtureBase {
 public:
  explicit BackupServiceFixtureBase(bool with_backup_storage) {
    data_dir_ = (test_dir_ / "data").string();
    fs::create_directories(data_dir_);

    index_factory_ = std::make_unique<index::IndexFactory>();
    auto local = std::make_unique<storage::SegmentManager>(
        data_dir_ + "/segments", index_factory_.get());
    std::string bucket;
    if (with_backup_storage) {
      backup_root_ = (test_dir_ / "backups").string();
      fs::create_directories(backup_root_);
      bucket = kBucket;

      auto object_store = std::make_unique<storage::InMemoryObjectStore>();
      object_store_ = object_store.get();
      auto cache = std::make_unique<storage::SegmentCache>(
          data_dir_ + "/cache", size_t{64} * 1024 * 1024);
      segment_store_ = std::make_shared<storage::TieredSegmentManager>(
          std::move(local), std::move(object_store), std::move(cache), "gvdb");
    } else {
      segment_store_ =
          std::shared_ptr<storage::SegmentManager>(std::move(local));
    }
    query_executor_ =
        std::make_shared<compute::QueryExecutor>(segment_store_.get());

    storage::BackupManagerOptions backup_options;
    backup_options.default_object_store = object_store_;
    backup_options.s3_bucket = bucket;
    backup_options.local_root_allowlist = backup_root_;
    backup_options.flushed_segments_root = data_dir_ + "/segments";
    backup_options.tmp_dir = data_dir_ + "/tmp/backup";
    backup_options.gvdb_version = "single-node";
    backup_options.node_id = 1;
    backup_manager_ =
        std::make_shared<storage::BackupManager>(std::move(backup_options));

    storage::RestoreManagerOptions restore_options;
    restore_options.default_object_store = object_store_;
    restore_options.s3_bucket = bucket;
    restore_options.local_root_allowlist = backup_root_;
    restore_options.staging_dir = data_dir_ + "/tmp/restore";
    restore_manager_ =
        std::make_shared<storage::RestoreManager>(std::move(restore_options));

    service_ = std::make_unique<service::VectorDBService>(
        segment_store_, query_executor_,
        service::MakeLocalResolver(segment_store_, data_dir_),
        /*rbac_store=*/nullptr, /*bulk_importer=*/nullptr, backup_manager_,
        restore_manager_);
  }

  ~BackupServiceFixtureBase() {
    // The managers' worker threads use the segment store, and the tiered
    // store owns the object store the managers point at, so the managers
    // go first.
    service_.reset();
    backup_manager_.reset();
    restore_manager_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();
    // test_dir_ is the first member, so it removes the folder last.
  }

  void CreateCollection(const std::string& name) {
    grpc::ServerContext context;
    proto::CreateCollectionRequest request;
    request.set_collection_name(name);
    request.set_dimension(kDim);
    request.set_metric(proto::CreateCollectionRequest::L2);
    request.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse response;
    auto status = service_->CreateCollection(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
  }

  // Inserts the vectors with ids 1..kNumVectors.
  void InsertVectors(const std::string& name) {
    grpc::ServerContext context;
    proto::InsertRequest request;
    request.set_collection_name(name);
    for (uint64_t id = 1; id <= kNumVectors; ++id) {
      auto* vector = request.add_vectors();
      vector->set_id(id);
      vector->mutable_vector()->set_dimension(kDim);
      for (float v : VectorFor(id)) vector->mutable_vector()->add_values(v);
    }
    proto::InsertResponse response;
    auto status = service_->Insert(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_EQ(response.inserted_count(), kNumVectors);
  }

  void CreatePopulatedSource() {
    CreateCollection(kSource);
    InsertVectors(kSource);
  }

  grpc::Status DropCollection(const std::string& name) {
    grpc::ServerContext context;
    proto::DropCollectionRequest request;
    request.set_collection_name(name);
    proto::DropCollectionResponse response;
    return service_->DropCollection(&context, &request, &response);
  }

  grpc::Status Backup(const std::string& collection,
                      const proto::BackupTarget& target,
                      proto::BackupCollectionResponse* response) {
    grpc::ServerContext context;
    proto::BackupCollectionRequest request;
    request.set_collection_name(collection);
    *request.mutable_target() = target;
    return service_->BackupCollection(&context, &request, response);
  }

  grpc::Status Restore(const proto::BackupTarget& source,
                       const std::string& backup_id,
                       const std::string& target_collection, bool overwrite,
                       proto::RestoreCollectionResponse* response) {
    grpc::ServerContext context;
    proto::RestoreCollectionRequest request;
    *request.mutable_source() = source;
    request.set_backup_id(backup_id);
    request.set_target_collection_name(target_collection);
    request.set_overwrite(overwrite);
    return service_->RestoreCollection(&context, &request, response);
  }

  // Polls GetBackupStatus until the job is terminal or the WaitFor timeout
  // passes.
  proto::GetBackupStatusResponse WaitForBackup(const std::string& backup_id) {
    proto::GetBackupStatusResponse response;
    test::WaitFor([&] {
      grpc::ServerContext context;
      proto::GetBackupStatusRequest request;
      request.set_backup_id(backup_id);
      response.Clear();
      auto status = service_->GetBackupStatus(&context, &request, &response);
      INFO(status.error_message());
      REQUIRE(status.ok());
      return IsTerminal(response.state());
    });
    INFO("state=" << proto::BackupState_Name(response.state())
                  << " error=" << response.error_message());
    REQUIRE(IsTerminal(response.state()));
    return response;
  }

  // Polls GetRestoreStatus until the job is terminal or the WaitFor timeout
  // passes.
  proto::GetRestoreStatusResponse WaitForRestore(const std::string& restore_id) {
    proto::GetRestoreStatusResponse response;
    test::WaitFor([&] {
      grpc::ServerContext context;
      proto::GetRestoreStatusRequest request;
      request.set_restore_id(restore_id);
      response.Clear();
      auto status = service_->GetRestoreStatus(&context, &request, &response);
      INFO(status.error_message());
      REQUIRE(status.ok());
      return IsTerminal(response.state());
    });
    INFO("state=" << proto::BackupState_Name(response.state())
                  << " error=" << response.error_message());
    REQUIRE(IsTerminal(response.state()));
    return response;
  }

  // Starts a backup of the source collection and waits until it completes.
  proto::GetBackupStatusResponse BackupAndWait(
      const proto::BackupTarget& target) {
    proto::BackupCollectionResponse backup;
    auto status = Backup(kSource, target, &backup);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_FALSE(backup.backup_id().empty());

    auto done = WaitForBackup(backup.backup_id());
    INFO("backup error: " << done.error_message());
    REQUIRE_EQ(done.state(), proto::BACKUP_COMPLETED);
    CHECK_EQ(done.backup_id(), backup.backup_id());
    CHECK_EQ(done.shards_total(), 1u);
    CHECK_EQ(done.shards_completed(), 1u);
    CHECK_GT(done.bytes_uploaded(), 0u);
    CHECK(done.error_message().empty());
    return done;
  }

  proto::SearchResponse SearchTop1(const std::string& collection,
                                   uint64_t id) {
    grpc::ServerContext context;
    proto::SearchRequest request;
    request.set_collection_name(collection);
    request.mutable_query_vector()->set_dimension(kDim);
    for (float v : VectorFor(id)) request.mutable_query_vector()->add_values(v);
    request.set_top_k(1);
    proto::SearchResponse response;
    auto status = service_->Search(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    return response;
  }

  // Vector count of a collection from ListCollections, or nullopt when the
  // collection does not exist.
  std::optional<uint64_t> VectorCount(const std::string& collection) {
    grpc::ServerContext context;
    proto::ListCollectionsRequest request;
    proto::ListCollectionsResponse response;
    auto status = service_->ListCollections(&context, &request, &response);
    REQUIRE(status.ok());
    for (const auto& c : response.collections()) {
      if (c.collection_name() == collection) return c.vector_count();
    }
    return std::nullopt;
  }

  // Checks that the collection holds exactly the inserted vectors: each one
  // is its own nearest neighbor, and Get returns the inserted values.
  void CheckHoldsInsertedVectors(const std::string& collection) {
    auto count = VectorCount(collection);
    REQUIRE(count.has_value());
    CHECK_EQ(*count, kNumVectors);

    for (uint64_t id : {uint64_t{1}, uint64_t{7}, kNumVectors}) {
      CAPTURE(id);
      auto search = SearchTop1(collection, id);
      REQUIRE_EQ(search.results_size(), 1);
      CHECK_EQ(search.results(0).id(), id);
      CHECK(search.results(0).distance() == doctest::Approx(0.0f));
    }

    grpc::ServerContext context;
    proto::GetRequest request;
    request.set_collection_name(collection);
    for (uint64_t id = 1; id <= kNumVectors; ++id) request.add_ids(id);
    proto::GetResponse response;
    auto status = service_->Get(&context, &request, &response);
    INFO(status.error_message());
    REQUIRE(status.ok());
    CHECK_EQ(response.vectors_size(), static_cast<int>(kNumVectors));
    CHECK_EQ(response.not_found_ids_size(), 0);
    for (const auto& v : response.vectors()) {
      CAPTURE(v.id());
      REQUIRE_GE(v.id(), 1u);
      REQUIRE_EQ(v.vector().values_size(), static_cast<int>(kDim));
      for (uint32_t d = 0; d < kDim; ++d) {
        CHECK_EQ(v.vector().values(static_cast<int>(d)), VectorValue(v.id(), d));
      }
    }
  }

  // Backs up the populated source collection to `target`, drops the source,
  // restores the backup from `target` under a new name and checks that the
  // restored collection holds the original vectors. Returns the status of
  // the completed backup.
  proto::GetBackupStatusResponse BackupDropRestoreAndCheck(
      const proto::BackupTarget& target) {
    CreatePopulatedSource();
    auto backup = BackupAndWait(target);

    // The restore must not depend on the source collection.
    auto drop = DropCollection(kSource);
    INFO(drop.error_message());
    REQUIRE(drop.ok());
    REQUIRE_FALSE(VectorCount(kSource).has_value());

    proto::RestoreCollectionResponse restore;
    auto status = Restore(target, backup.backup_id(), kRestored,
                          /*overwrite=*/false, &restore);
    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE_FALSE(restore.restore_id().empty());

    auto done = WaitForRestore(restore.restore_id());
    INFO("restore error: " << done.error_message());
    REQUIRE_EQ(done.state(), proto::BACKUP_COMPLETED);
    CHECK_EQ(done.restore_id(), restore.restore_id());
    CHECK_EQ(done.shards_total(), 1u);
    CHECK_EQ(done.shards_completed(), 1u);
    CHECK(done.error_message().empty());

    CheckHoldsInsertedVectors(kRestored);
    return backup;
  }

  test::ScopedTempDir test_dir_{"gvdb_backup_service_test"};
  std::string data_dir_;
  std::string backup_root_;  // empty without backup storage
  std::unique_ptr<index::IndexFactory> index_factory_;
  storage::IObjectStore* object_store_ = nullptr;  // owned by segment_store_
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::shared_ptr<storage::BackupManager> backup_manager_;
  std::shared_ptr<storage::RestoreManager> restore_manager_;
  std::unique_ptr<service::VectorDBService> service_;
};

class BackupServiceTest : public BackupServiceFixtureBase {
 public:
  BackupServiceTest() : BackupServiceFixtureBase(/*with_backup_storage=*/true) {}
};

class NoBackupStorageServiceTest : public BackupServiceFixtureBase {
 public:
  NoBackupStorageServiceTest()
      : BackupServiceFixtureBase(/*with_backup_storage=*/false) {}
};

}  // namespace

// ============================================================================
// Round trips
// ============================================================================

TEST_CASE_FIXTURE(BackupServiceTest, "S3TargetBackupDropRestoreAndSearch") {
  auto backup = BackupDropRestoreAndCheck(S3Target());

  // The backup went to the configured object store, under the requested
  // prefix.
  const std::string manifest_key = std::string(kS3Prefix) + "/backups/" +
                                   backup.backup_id() + "/backup.manifest.json";
  CHECK_EQ(backup.manifest_uri(),
           std::string("s3://") + kBucket + "/" + manifest_key);
  auto exists = object_store_->ObjectExists(manifest_key);
  REQUIRE(exists.ok());
  CHECK(*exists);
}

TEST_CASE_FIXTURE(BackupServiceTest, "LocalTargetBackupDropRestoreAndSearch") {
  std::string path;
  SUBCASE("path is the allowlist root") { path = backup_root_; }
  SUBCASE("path is a subdirectory of the allowlist root") {
    path = backup_root_ + "/nightly/products";
  }
  CAPTURE(path);

  auto backup = BackupDropRestoreAndCheck(LocalTarget(path));

  // The backup was written under the requested directory.
  const std::string manifest_file =
      path + "/backups/" + backup.backup_id() + "/backup.manifest.json";
  CHECK_EQ(backup.manifest_uri(), "file://" + manifest_file);
  CHECK(fs::is_regular_file(manifest_file));
}

// ============================================================================
// Restore onto an existing collection
// ============================================================================

TEST_CASE_FIXTURE(BackupServiceTest,
                  "RestoreOntoExistingCollectionWithoutOverwriteIsAlreadyExists") {
  CreatePopulatedSource();
  auto backup = BackupAndWait(S3Target());

  // An empty target name restores under the name recorded in the backup,
  // which is the source collection, so both requests hit the existing one.
  for (const std::string& target_name : {std::string(kSource), std::string()}) {
    CAPTURE(target_name);
    proto::RestoreCollectionResponse restore;
    auto status = Restore(S3Target(), backup.backup_id(), target_name,
                          /*overwrite=*/false, &restore);
    INFO(status.error_message());
    CHECK_EQ(status.error_code(), grpc::StatusCode::ALREADY_EXISTS);
    CHECK(restore.restore_id().empty());
  }

  // The existing collection is left as it was.
  CheckHoldsInsertedVectors(kSource);
}

// ============================================================================
// Local target allowlist
// ============================================================================

TEST_CASE_FIXTURE(BackupServiceTest, "LocalTargetOutsideAllowlistIsRejected") {
  CreatePopulatedSource();

  // Every path is inside this fixture's own temp folder, so no other test
  // or run can create it, and the copies made below go with the folder.
  const std::vector<std::string> outside = {
      (test_dir_ / "elsewhere").string(),  // a sibling of the allowlist root
      backup_root_ + "_evil",              // starts with the root's path as text
      backup_root_ + "/../escaped",        // climbs out of the root with ..
  };

  for (const auto& path : outside) {
    CAPTURE(path);
    proto::BackupCollectionResponse backup;
    auto status = Backup(kSource, LocalTarget(path), &backup);
    INFO(status.error_message());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
    CHECK(backup.backup_id().empty());
    CHECK_FALSE(fs::exists(path));
  }

  // Put a copy of a complete backup at each outside path, so that only the
  // allowlist check can stop a restore from there.
  auto backup = BackupAndWait(LocalTarget(backup_root_));
  for (const auto& path : outside) {
    CAPTURE(path);
    fs::create_directories(path);
    fs::copy(backup_root_, path, fs::copy_options::recursive);
    REQUIRE(fs::is_regular_file(path + "/backups/" + backup.backup_id() +
                                "/backup.manifest.json"));

    proto::RestoreCollectionResponse restore;
    auto status = Restore(LocalTarget(path), backup.backup_id(), kRestored,
                          /*overwrite=*/false, &restore);
    INFO(status.error_message());
    CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
    CHECK(restore.restore_id().empty());
    CHECK_FALSE(VectorCount(kRestored).has_value());
  }
}

// ============================================================================
// No backup storage configured
// ============================================================================

TEST_CASE_FIXTURE(NoBackupStorageServiceTest,
                  "BackupAndRestoreRejectedWithoutBackupStorage") {
  CreatePopulatedSource();
  const std::string local_path = (test_dir_ / "backups").string();

  // Only the failure is checked, not the status code: the managers return
  // FailedPrecondition, which toGrpcStatus does not map yet.
  for (const auto& target : {S3Target(), LocalTarget(local_path)}) {
    CAPTURE(target.has_s3());

    proto::BackupCollectionResponse backup;
    auto backup_status = Backup(kSource, target, &backup);
    CHECK_FALSE(backup_status.ok());
    CHECK(backup.backup_id().empty());

    proto::RestoreCollectionResponse restore;
    auto restore_status = Restore(target, "bk-any", kRestored,
                                  /*overwrite=*/false, &restore);
    CHECK_FALSE(restore_status.ok());
    CHECK(restore.restore_id().empty());
    CHECK_FALSE(VectorCount(kRestored).has_value());
  }
  CHECK_FALSE(fs::exists(local_path));
  CheckHoldsInsertedVectors(kSource);
}
