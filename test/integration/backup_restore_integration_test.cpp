// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// End-to-end integration test for backup/restore. Spins up a real
// coordinator + data-node + proxy, calls BackupCollection /
// RestoreCollection via a gRPC client, and verifies that vectors
// inserted before the backup are queryable after the restore.
//
// Storage layer: shared InMemoryObjectStore between the coordinator's
// BackupManager (for top-manifest writes) and the data-node's
// BackupManager (for per-shard uploads). The same store is reused on
// restore so the data-node's RestoreManager reads back what the
// coordinator wrote.

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

#include "cluster/coordinator.h"
#include "cluster/internal_client.h"
#include "cluster/node_registry.h"
#include "cluster/shard_manager.h"
#include "cluster/shard_write_gate.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/collection_resolver.h"
#include "network/internal_service.h"
#include "network/proxy_service.h"
#include "network/vectordb_service.h"
#include "storage/backup_manager.h"
#include "storage/object_store.h"
#include "storage/segment_manager.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace test {

namespace {

constexpr const char* kTestDir = "/tmp/gvdb-backup-restore-integration-test";

// Wait for a backup or restore job to reach a terminal state. Returns
// the final phase or BACKUP_RUNNING on timeout.
proto::BackupState WaitForBackup(
    proto::VectorDBService::Stub* client, const std::string& backup_id,
    int timeout_ms = 10'000) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    proto::GetBackupStatusRequest req;
    req.set_backup_id(backup_id);
    proto::GetBackupStatusResponse resp;
    grpc::ClientContext ctx;
    auto status = client->GetBackupStatus(&ctx, req, &resp);
    if (!status.ok()) return proto::BACKUP_FAILED;
    if (resp.state() == proto::BACKUP_COMPLETED ||
        resp.state() == proto::BACKUP_FAILED ||
        resp.state() == proto::BACKUP_CANCELLED) {
      return resp.state();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return proto::BACKUP_RUNNING;
}

proto::BackupState WaitForRestore(
    proto::VectorDBService::Stub* client, const std::string& restore_id,
    int timeout_ms = 10'000) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    proto::GetRestoreStatusRequest req;
    req.set_restore_id(restore_id);
    proto::GetRestoreStatusResponse resp;
    grpc::ClientContext ctx;
    auto status = client->GetRestoreStatus(&ctx, req, &resp);
    if (!status.ok()) return proto::BACKUP_FAILED;
    if (resp.state() == proto::BACKUP_COMPLETED ||
        resp.state() == proto::BACKUP_FAILED) {
      return resp.state();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return proto::BACKUP_RUNNING;
}

}  // namespace

class BackupRestoreIntegrationTest {
 public:
  BackupRestoreIntegrationTest() {
    std::filesystem::remove_all(kTestDir);
    std::filesystem::create_directories(
        std::string(kTestDir) + "/coordinator/segments");
    std::filesystem::create_directories(
        std::string(kTestDir) + "/data_node/segments");

    index_factory_ = std::make_unique<index::IndexFactory>();
    shared_object_store_ = std::make_unique<storage::InMemoryObjectStore>();

    // --- Coordinator: shard manager, registry, factory, services ---
    shard_manager_ = std::make_shared<cluster::ShardManager>(
        8, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(
        std::chrono::seconds(30));
    auto client_factory =
        std::make_shared<cluster::GrpcInternalServiceClientFactory>();
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_, client_factory);

    coord_segment_store_ = std::make_shared<storage::SegmentManager>(
        std::string(kTestDir) + "/coordinator/segments",
        index_factory_.get());
    coord_query_executor_ = std::make_shared<compute::QueryExecutor>(
        coord_segment_store_.get());

    // Wire BackupManager + RestoreManager on the coordinator. The
    // coordinator writes the top manifest into the shared object store.
    {
      storage::BackupManagerOptions opts;
      opts.default_object_store = shared_object_store_.get();
      opts.s3_bucket = "test-bucket";
      opts.tmp_dir = std::string(kTestDir) + "/coordinator/tmp";
      opts.gvdb_version = "integration";
      opts.node_id = 1;
      coord_backup_mgr_ =
          std::make_shared<storage::BackupManager>(std::move(opts));
    }
    {
      storage::RestoreManagerOptions opts;
      opts.default_object_store = shared_object_store_.get();
      opts.s3_bucket = "test-bucket";
      opts.staging_dir = std::string(kTestDir) + "/coordinator/restore";
      coord_restore_mgr_ =
          std::make_shared<storage::RestoreManager>(std::move(opts));
    }
    coordinator_->SetBackupManager(coord_backup_mgr_);
    coordinator_->SetRestoreManager(coord_restore_mgr_);

    coord_internal_service_ = std::make_unique<network::InternalService>(
        shard_manager_, coord_segment_store_, coord_query_executor_,
        node_registry_, /*timestamp_oracle=*/nullptr, coordinator_);
    auto coord_resolver = network::MakeCoordinatorResolver(coordinator_);
    coord_vectordb_service_ = std::make_unique<network::VectorDBService>(
        coord_segment_store_, coord_query_executor_,
        std::move(coord_resolver));
    coord_vectordb_service_->SetCoordinator(coordinator_);

    {
      grpc::ServerBuilder builder;
      int port = 0;
      builder.AddListeningPort("localhost:0",
                                grpc::InsecureServerCredentials(), &port);
      builder.RegisterService(coord_internal_service_.get());
      builder.RegisterService(coord_vectordb_service_.get());
      coord_server_ = builder.BuildAndStart();
      REQUIRE_NE(coord_server_, nullptr);
      coord_address_ = "localhost:" + std::to_string(port);
    }

    // --- Data node ---
    dn_segment_store_ = std::make_shared<storage::SegmentManager>(
        std::string(kTestDir) + "/data_node/segments",
        index_factory_.get());
    dn_query_executor_ = std::make_shared<compute::QueryExecutor>(
        dn_segment_store_.get());
    auto dn_shard_manager = std::make_shared<cluster::ShardManager>(
        8, cluster::ShardingStrategy::HASH);
    dn_internal_service_ = std::make_unique<network::InternalService>(
        dn_shard_manager, dn_segment_store_, dn_query_executor_);

    // Backup/restore engines on the data-node — same object store so
    // the per-shard uploads end up where the coordinator's top
    // manifest references.
    {
      storage::BackupManagerOptions opts;
      opts.default_object_store = shared_object_store_.get();
      opts.s3_bucket = "test-bucket";
      opts.flushed_segments_root =
          std::string(kTestDir) + "/data_node/segments";
      opts.tmp_dir = std::string(kTestDir) + "/data_node/tmp/backup";
      opts.gvdb_version = "integration";
      opts.node_id = 100;
      dn_backup_mgr_ =
          std::make_shared<storage::BackupManager>(std::move(opts));
    }
    {
      storage::RestoreManagerOptions opts;
      opts.default_object_store = shared_object_store_.get();
      opts.s3_bucket = "test-bucket";
      opts.staging_dir = std::string(kTestDir) + "/data_node/tmp/restore";
      dn_restore_mgr_ =
          std::make_shared<storage::RestoreManager>(std::move(opts));
    }
    shard_write_gate_ = std::make_unique<cluster::ShardWriteGate>();
    dn_internal_service_->SetShardWriteGate(shard_write_gate_.get());
    dn_internal_service_->SetBackupManager(dn_backup_mgr_);
    dn_internal_service_->SetRestoreManager(dn_restore_mgr_);

    auto dn_resolver = network::MakeCachedCoordinatorResolver(coord_address_);
    dn_vectordb_service_ = std::make_unique<network::VectorDBService>(
        dn_segment_store_, dn_query_executor_, std::move(dn_resolver));
    dn_vectordb_service_->SetShardWriteGate(shard_write_gate_.get());

    {
      grpc::ServerBuilder builder;
      int port = 0;
      builder.AddListeningPort("localhost:0",
                                grpc::InsecureServerCredentials(), &port);
      builder.RegisterService(dn_internal_service_.get());
      builder.RegisterService(dn_vectordb_service_.get());
      dn_server_ = builder.BuildAndStart();
      REQUIRE_NE(dn_server_, nullptr);
      dn_address_ = "localhost:" + std::to_string(port);
    }

    // Register the data node with the coordinator so the orchestration
    // can dial it for BackupShard / RestoreShard.
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(100);
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node.set_grpc_address(dn_address_);
    node_registry_->UpdateNode(proto_node);
    REQUIRE(shard_manager_->RegisterNode(core::MakeNodeId(100)).ok());

    // --- Proxy in front of both ---
    proxy_service_ = std::make_unique<network::ProxyService>(
        std::vector<std::string>{coord_address_}, dn_address_);
    {
      grpc::ServerBuilder builder;
      int port = 0;
      builder.AddListeningPort("localhost:0",
                                grpc::InsecureServerCredentials(), &port);
      builder.RegisterService(proxy_service_.get());
      proxy_server_ = builder.BuildAndStart();
      REQUIRE_NE(proxy_server_, nullptr);
      proxy_address_ = "localhost:" + std::to_string(port);
    }

    auto channel = grpc::CreateChannel(
        proxy_address_, grpc::InsecureChannelCredentials());
    client_ = proto::VectorDBService::NewStub(channel);
  }

  ~BackupRestoreIntegrationTest() {
    client_.reset();
    if (proxy_server_) { proxy_server_->Shutdown(); proxy_server_->Wait(); }
    if (dn_server_)    { dn_server_->Shutdown();    dn_server_->Wait();    }
    if (coord_server_) { coord_server_->Shutdown(); coord_server_->Wait(); }

    proxy_service_.reset();
    dn_vectordb_service_.reset();
    dn_internal_service_.reset();
    coord_vectordb_service_.reset();
    coord_internal_service_.reset();
    // Drop managers / gate before the segment stores so background
    // worker threads finish before the stores they reference die.
    coord_backup_mgr_.reset();
    coord_restore_mgr_.reset();
    dn_backup_mgr_.reset();
    dn_restore_mgr_.reset();
    shard_write_gate_.reset();
    coordinator_.reset();

    std::filesystem::remove_all(kTestDir);
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  void CreateCollection(const std::string& name, uint32_t dim) {
    proto::CreateCollectionRequest req;
    req.set_collection_name(name);
    req.set_dimension(dim);
    req.set_metric(proto::CreateCollectionRequest::L2);
    req.set_index_type(proto::CreateCollectionRequest::FLAT);
    proto::CreateCollectionResponse resp;
    grpc::ClientContext ctx;
    auto s = client_->CreateCollection(&ctx, req, &resp);
    INFO("CreateCollection: " << s.error_message());
    REQUIRE(s.ok());
  }

  void InsertVectors(const std::string& name, uint32_t dim, size_t count) {
    proto::InsertRequest req;
    req.set_collection_name(name);
    for (size_t i = 1; i <= count; ++i) {
      auto* vec = req.add_vectors();
      vec->set_id(i);
      auto* v = vec->mutable_vector();
      v->set_dimension(dim);
      for (uint32_t d = 0; d < dim; ++d) {
        v->add_values(static_cast<float>(i * 10 + d));
      }
    }
    proto::InsertResponse resp;
    grpc::ClientContext ctx;
    auto s = client_->Insert(&ctx, req, &resp);
    INFO("Insert: " << s.error_message());
    REQUIRE(s.ok());
    CHECK_EQ(resp.inserted_count(), count);
  }

  // Run a one-NN search for an inserted vector by id; returns the
  // top result's id (which should be the same as `expected_id` if the
  // vector is present and unique).
  uint64_t SearchTopMatch(const std::string& name, uint32_t dim,
                          uint64_t expected_id) {
    proto::SearchRequest req;
    req.set_collection_name(name);
    req.set_top_k(1);
    auto* q = req.mutable_query_vector();
    q->set_dimension(dim);
    for (uint32_t d = 0; d < dim; ++d) {
      q->add_values(static_cast<float>(expected_id * 10 + d));
    }
    proto::SearchResponse resp;
    grpc::ClientContext ctx;
    auto s = client_->Search(&ctx, req, &resp);
    INFO("Search: " << s.error_message());
    REQUIRE(s.ok());
    if (resp.results_size() == 0) return 0;
    return resp.results(0).id();
  }

  // Members (public for in-test introspection).
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::unique_ptr<storage::InMemoryObjectStore> shared_object_store_;

  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;

  std::shared_ptr<storage::SegmentManager> coord_segment_store_;
  std::shared_ptr<compute::QueryExecutor> coord_query_executor_;
  std::shared_ptr<storage::BackupManager> coord_backup_mgr_;
  std::shared_ptr<storage::RestoreManager> coord_restore_mgr_;
  std::unique_ptr<network::InternalService> coord_internal_service_;
  std::unique_ptr<network::VectorDBService> coord_vectordb_service_;
  std::unique_ptr<grpc::Server> coord_server_;
  std::string coord_address_;

  std::shared_ptr<storage::SegmentManager> dn_segment_store_;
  std::shared_ptr<compute::QueryExecutor> dn_query_executor_;
  std::shared_ptr<storage::BackupManager> dn_backup_mgr_;
  std::shared_ptr<storage::RestoreManager> dn_restore_mgr_;
  std::unique_ptr<cluster::ShardWriteGate> shard_write_gate_;
  std::unique_ptr<network::InternalService> dn_internal_service_;
  std::unique_ptr<network::VectorDBService> dn_vectordb_service_;
  std::unique_ptr<grpc::Server> dn_server_;
  std::string dn_address_;

  std::unique_ptr<network::ProxyService> proxy_service_;
  std::unique_ptr<grpc::Server> proxy_server_;
  std::string proxy_address_;
  std::unique_ptr<proto::VectorDBService::Stub> client_;
};

TEST_CASE_FIXTURE(BackupRestoreIntegrationTest,
                  "backup → restore round-trip preserves inserted vectors") {
  constexpr uint32_t kDim = 4;
  constexpr size_t kCount = 10;
  const std::string kCollection = "products";

  CreateCollection(kCollection, kDim);
  InsertVectors(kCollection, kDim, kCount);

  // Sanity: at least one vector is queryable before the backup.
  CHECK_EQ(SearchTopMatch(kCollection, kDim, 3), 3u);

  // Backup the collection to the shared in-memory store via S3Target.
  std::string backup_id;
  {
    proto::BackupCollectionRequest req;
    req.set_collection_name(kCollection);
    auto* s3 = req.mutable_target()->mutable_s3();
    s3->set_bucket("test-bucket");
    s3->set_prefix("backups-test");
    proto::BackupCollectionResponse resp;
    grpc::ClientContext ctx;
    auto s = client_->BackupCollection(&ctx, req, &resp);
    INFO("BackupCollection: " << s.error_message());
    REQUIRE(s.ok());
    REQUIRE_FALSE(resp.backup_id().empty());
    backup_id = resp.backup_id();
  }

  auto bkp_state = WaitForBackup(client_.get(), backup_id);
  REQUIRE_EQ(bkp_state, proto::BACKUP_COMPLETED);

  // Restore into a fresh collection name with overwrite=false. The
  // coordinator's orchestration creates the target collection from
  // the manifest's recorded metadata, then fans out RestoreShard to
  // the data-node.
  const std::string kRestoredName = "products_restored";
  std::string restore_id;
  {
    proto::RestoreCollectionRequest req;
    auto* s3 = req.mutable_source()->mutable_s3();
    s3->set_bucket("test-bucket");
    s3->set_prefix("backups-test");
    req.set_backup_id(backup_id);
    req.set_target_collection_name(kRestoredName);
    req.set_overwrite(false);
    proto::RestoreCollectionResponse resp;
    grpc::ClientContext ctx;
    auto s = client_->RestoreCollection(&ctx, req, &resp);
    INFO("RestoreCollection: " << s.error_message());
    REQUIRE(s.ok());
    REQUIRE_FALSE(resp.restore_id().empty());
    restore_id = resp.restore_id();
  }
  auto rst_state = WaitForRestore(client_.get(), restore_id);
  REQUIRE_EQ(rst_state, proto::BACKUP_COMPLETED);

  // The restored collection should expose the same vectors.
  CHECK_EQ(SearchTopMatch(kRestoredName, kDim, 3), 3u);
  CHECK_EQ(SearchTopMatch(kRestoredName, kDim, 7), 7u);
  CHECK_EQ(SearchTopMatch(kRestoredName, kDim, kCount), kCount);
}

}  // namespace test
}  // namespace gvdb
