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
#include <set>
#include <string>
#include <vector>

#include "cluster/coordinator/coordinator.h"
#include "cluster/coordinator/internal_client.h"
#include "cluster/coordinator/node_registry.h"
#include "cluster/shard_manager.h"
#include "cluster/datanode/shard_write_gate.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "service/vectordb/collection_resolver.h"
#include "service/internal/internal_service.h"
#include "service/proxy/proxy_service.h"
#include "service/vectordb/vectordb_service.h"
#include "storage/backup/backup_manager.h"
#include "storage/backup/restore_manager.h"
#include "storage/objstore/object_store.h"
#include "storage/segment/segment_manager.h"
#include "support/temp_dir.h"
#include "support/wait.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace test {

namespace {

// Polls a backup job until it reaches a terminal state. Returns that
// state, BACKUP_FAILED when the status RPC fails, or the last state seen
// when the job has not finished within WaitFor's timeout.
proto::BackupState WaitForBackup(
    proto::VectorDBService::Stub* client, const std::string& backup_id) {
  proto::BackupState state = proto::BACKUP_RUNNING;
  WaitFor([&] {
    proto::GetBackupStatusRequest req;
    req.set_backup_id(backup_id);
    proto::GetBackupStatusResponse resp;
    grpc::ClientContext ctx;
    if (!client->GetBackupStatus(&ctx, req, &resp).ok()) {
      state = proto::BACKUP_FAILED;
      return true;
    }
    state = resp.state();
    return state == proto::BACKUP_COMPLETED ||
           state == proto::BACKUP_FAILED ||
           state == proto::BACKUP_CANCELLED;
  });
  return state;
}

// The same for a restore job.
proto::BackupState WaitForRestore(
    proto::VectorDBService::Stub* client, const std::string& restore_id) {
  proto::BackupState state = proto::BACKUP_RUNNING;
  WaitFor([&] {
    proto::GetRestoreStatusRequest req;
    req.set_restore_id(restore_id);
    proto::GetRestoreStatusResponse resp;
    grpc::ClientContext ctx;
    if (!client->GetRestoreStatus(&ctx, req, &resp).ok()) {
      state = proto::BACKUP_FAILED;
      return true;
    }
    state = resp.state();
    return state == proto::BACKUP_COMPLETED || state == proto::BACKUP_FAILED;
  });
  return state;
}

}  // namespace

class BackupRestoreIntegrationTest {
 public:
  BackupRestoreIntegrationTest() {
    std::filesystem::create_directories(dir_ / "coordinator" / "segments");
    std::filesystem::create_directories(dir_ / "data_node" / "segments");

    index_factory_ = std::make_unique<index::IndexFactory>();
    shared_object_store_ = std::make_unique<storage::InMemoryObjectStore>();

    // --- Coordinator: shard manager, registry, factory, services ---
    shard_manager_ = std::make_shared<cluster::ShardManager>(
        8, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(
        std::chrono::seconds(30));

    coord_segment_store_ = std::make_shared<storage::SegmentManager>(
        (dir_ / "coordinator" / "segments").string(), index_factory_.get());
    coord_query_executor_ = std::make_shared<compute::QueryExecutor>(
        *coord_segment_store_);

    // BackupManager + RestoreManager of the coordinator. The coordinator
    // writes the top manifest into the shared object store.
    {
      storage::BackupManagerOptions opts;
      opts.default_object_store = shared_object_store_.get();
      opts.s3_bucket = "test-bucket";
      opts.tmp_dir = (dir_ / "coordinator" / "tmp").string();
      opts.gvdb_version = "integration";
      opts.node_id = 1;
      coord_backup_mgr_ =
          std::make_shared<storage::BackupManager>(std::move(opts));
    }
    {
      storage::RestoreManagerOptions opts;
      opts.default_object_store = shared_object_store_.get();
      opts.s3_bucket = "test-bucket";
      opts.staging_dir = (dir_ / "coordinator" / "restore").string();
      coord_restore_mgr_ =
          std::make_shared<storage::RestoreManager>(std::move(opts));
    }
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_,
        cluster::CoordinatorOptions{
            .client_factory =
                std::make_shared<cluster::GrpcInternalServiceClientFactory>(),
            .backup_manager = coord_backup_mgr_,
            .restore_manager = coord_restore_mgr_});

    coord_internal_service_ = std::make_unique<service::InternalService>(
        shard_manager_, coord_segment_store_, coord_query_executor_,
        service::InternalServiceOptions{.node_registry = node_registry_,
                                        .coordinator = coordinator_});
    auto coord_resolver = service::MakeCoordinatorResolver(coordinator_);
    coord_vectordb_service_ = std::make_unique<service::VectorDBService>(
        coord_segment_store_, coord_query_executor_,
        std::move(coord_resolver),
        service::VectorDBServiceOptions{.coordinator = coordinator_});

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
        (dir_ / "data_node" / "segments").string(), index_factory_.get());
    dn_query_executor_ = std::make_shared<compute::QueryExecutor>(
        *dn_segment_store_);
    auto dn_shard_manager = std::make_shared<cluster::ShardManager>(
        8, cluster::ShardingStrategy::HASH);

    // Backup/restore engines on the data-node, same object store so
    // the per-shard uploads end up where the coordinator's top
    // manifest references.
    {
      storage::BackupManagerOptions opts;
      opts.default_object_store = shared_object_store_.get();
      opts.s3_bucket = "test-bucket";
      opts.flushed_segments_root = (dir_ / "data_node" / "segments").string();
      opts.tmp_dir = (dir_ / "data_node" / "tmp" / "backup").string();
      opts.gvdb_version = "integration";
      opts.node_id = 100;
      dn_backup_mgr_ =
          std::make_shared<storage::BackupManager>(std::move(opts));
    }
    {
      storage::RestoreManagerOptions opts;
      opts.default_object_store = shared_object_store_.get();
      opts.s3_bucket = "test-bucket";
      opts.staging_dir = (dir_ / "data_node" / "tmp" / "restore").string();
      dn_restore_mgr_ =
          std::make_shared<storage::RestoreManager>(std::move(opts));
    }
    shard_write_gate_ = std::make_unique<cluster::ShardWriteGate>();
    dn_internal_service_ = std::make_unique<service::InternalService>(
        dn_shard_manager, dn_segment_store_, dn_query_executor_,
        service::InternalServiceOptions{
            .backup_manager = dn_backup_mgr_,
            .restore_manager = dn_restore_mgr_,
            .shard_write_gate = shard_write_gate_.get()});

    auto dn_resolver = service::MakeCachedCoordinatorResolver(coord_address_);
    dn_vectordb_service_ = std::make_unique<service::VectorDBService>(
        dn_segment_store_, dn_query_executor_, std::move(dn_resolver),
        service::VectorDBServiceOptions{.shard_write_gate =
                                            shard_write_gate_.get()});

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
    proxy_service_ = std::make_unique<service::ProxyService>(
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
    // Close every client channel before the server that it dials. A
    // graceful server shutdown waits until each open client connection
    // answers the server's GOAWAY, and on macOS an idle in-process channel
    // answers only at gRPC's next client backup poll (every 5 s).
    client_.reset();
    proxy_server_->Shutdown();
    proxy_server_.reset();
    proxy_service_.reset();  // Its channels dial the coordinator and the data node.

    // The coordinator and the data node dial each other: the coordinator's
    // clients dial the data node, and the data node's resolver dials the
    // coordinator. A deadline that has passed makes the coordinator close
    // its connections at once instead of waiting for the data node's
    // channel. The tests wait for every backup and restore job to finish,
    // so no call is expected to run here; one that still runs is cancelled.
    coord_server_->Shutdown(std::chrono::system_clock::now());
    coord_server_.reset();
    coord_vectordb_service_.reset();
    coord_internal_service_.reset();
    coordinator_.reset();  // Its clients dial the data node.
    dn_server_->Shutdown();
    dn_server_.reset();

    dn_vectordb_service_.reset();
    dn_internal_service_.reset();
    // Drop managers / gate before the segment stores so background
    // worker threads finish before the stores they reference die.
    coord_backup_mgr_.reset();
    coord_restore_mgr_.reset();
    dn_backup_mgr_.reset();
    dn_restore_mgr_.reset();
    shard_write_gate_.reset();
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
  ScopedTempDir dir_{"gvdb_backup_restore"};
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::unique_ptr<storage::InMemoryObjectStore> shared_object_store_;

  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;

  std::shared_ptr<storage::SegmentManager> coord_segment_store_;
  std::shared_ptr<compute::QueryExecutor> coord_query_executor_;
  std::shared_ptr<storage::BackupManager> coord_backup_mgr_;
  std::shared_ptr<storage::RestoreManager> coord_restore_mgr_;
  std::unique_ptr<service::InternalService> coord_internal_service_;
  std::unique_ptr<service::VectorDBService> coord_vectordb_service_;
  std::unique_ptr<grpc::Server> coord_server_;
  std::string coord_address_;

  std::shared_ptr<storage::SegmentManager> dn_segment_store_;
  std::shared_ptr<compute::QueryExecutor> dn_query_executor_;
  std::shared_ptr<storage::BackupManager> dn_backup_mgr_;
  std::shared_ptr<storage::RestoreManager> dn_restore_mgr_;
  std::unique_ptr<cluster::ShardWriteGate> shard_write_gate_;
  std::unique_ptr<service::InternalService> dn_internal_service_;
  std::unique_ptr<service::VectorDBService> dn_vectordb_service_;
  std::unique_ptr<grpc::Server> dn_server_;
  std::string dn_address_;

  std::unique_ptr<service::ProxyService> proxy_service_;
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

TEST_CASE_FIXTURE(BackupRestoreIntegrationTest,
                  "backup → restore round-trip preserves SEALED segments") {
  // The growing-segment round-trip above serializes in-memory segments;
  // sealed segments take a different path (Segment::Load from the staged
  // sealed layout, which restores vectors but no live index and relies on
  // the segment store rebuilding it). This is the path a production
  // backup of a sealed, flushed collection exercises.
  constexpr uint32_t kDim = 4;
  constexpr size_t kCount = 10;
  const std::string kCollection = "products_sealed";

  CreateCollection(kCollection, kDim);
  InsertVectors(kCollection, kDim, kCount);

  // Seal and flush every growing segment on the data node, as the
  // auto-seal callback and flush-after-seal do in the server mains.
  size_t sealed = 0;
  for (auto sid : dn_segment_store_->GetAllSegmentIds()) {
    auto* seg = dn_segment_store_->GetSegment(sid);
    if (!seg || seg->GetState() != core::SegmentState::GROWING ||
        seg->GetVectorCount() == 0) {
      continue;
    }
    core::IndexConfig config;
    config.index_type = core::IndexType::FLAT;
    config.dimension = seg->GetDimension();
    config.metric_type = seg->GetMetric();
    REQUIRE(dn_segment_store_->SealSegment(sid, config).ok());
    REQUIRE(dn_segment_store_->FlushSegment(sid).ok());
    ++sealed;
  }
  REQUIRE_GE(sealed, 1);

  CHECK_EQ(SearchTopMatch(kCollection, kDim, 3), 3u);

  std::string backup_id;
  {
    proto::BackupCollectionRequest req;
    req.set_collection_name(kCollection);
    auto* s3 = req.mutable_target()->mutable_s3();
    s3->set_bucket("test-bucket");
    s3->set_prefix("backups-sealed-test");
    proto::BackupCollectionResponse resp;
    grpc::ClientContext ctx;
    auto s = client_->BackupCollection(&ctx, req, &resp);
    INFO("BackupCollection: " << s.error_message());
    REQUIRE(s.ok());
    backup_id = resp.backup_id();
  }
  REQUIRE_EQ(WaitForBackup(client_.get(), backup_id), proto::BACKUP_COMPLETED);

  // Snapshot the data node's segments so the restored ones can be singled
  // out afterwards.
  std::set<uint32_t> before_restore;
  for (auto sid : dn_segment_store_->GetAllSegmentIds()) {
    before_restore.insert(core::ToUInt32(sid));
  }

  const std::string kRestoredName = "products_sealed_restored";
  std::string restore_id;
  {
    proto::RestoreCollectionRequest req;
    auto* s3 = req.mutable_source()->mutable_s3();
    s3->set_bucket("test-bucket");
    s3->set_prefix("backups-sealed-test");
    req.set_backup_id(backup_id);
    req.set_target_collection_name(kRestoredName);
    req.set_overwrite(false);
    proto::RestoreCollectionResponse resp;
    grpc::ClientContext ctx;
    auto s = client_->RestoreCollection(&ctx, req, &resp);
    INFO("RestoreCollection: " << s.error_message());
    REQUIRE(s.ok());
    restore_id = resp.restore_id();
  }
  REQUIRE_EQ(WaitForRestore(client_.get(), restore_id),
             proto::BACKUP_COMPLETED);

  // Search must work against the restored sealed segments: the segment
  // store rebuilds their indexes when they are installed.
  CHECK_EQ(SearchTopMatch(kRestoredName, kDim, 3), 3u);
  CHECK_EQ(SearchTopMatch(kRestoredName, kDim, 7), 7u);
  CHECK_EQ(SearchTopMatch(kRestoredName, kDim, kCount), kCount);

  // Durability: restored sealed segments must be flushed to the data node's
  // local disk, not just installed in memory, or a restart loses the
  // restored collection.
  const std::string dn_segments_dir = (dir_ / "data_node" / "segments").string();
  std::vector<uint32_t> restored_ids;
  for (auto sid : dn_segment_store_->GetAllSegmentIds()) {
    auto key = core::ToUInt32(sid);
    if (before_restore.count(key) > 0) continue;
    auto* seg = dn_segment_store_->GetSegment(sid);
    if (!seg || seg->GetState() == core::SegmentState::GROWING) continue;
    restored_ids.push_back(key);
    auto seg_dir = dn_segments_dir + "/segment_" + std::to_string(key);
    INFO("restored segment dir: " << seg_dir);
    CHECK(std::filesystem::exists(seg_dir + "/vectors.bin"));
  }
  REQUIRE_GE(restored_ids.size(), 1);

  // Simulate a data-node restart: a fresh SegmentManager over the same
  // directory must reload the restored segments with their vectors.
  {
    storage::SegmentManager reloaded(dn_segments_dir, index_factory_.get());
    REQUIRE(reloaded.LoadAllSegments().ok());
    for (auto key : restored_ids) {
      auto* seg = reloaded.GetSegment(static_cast<core::SegmentId>(key));
      REQUIRE(seg != nullptr);
      CHECK_GT(seg->GetVectorCount(), 0);
    }
  }
}

}  // namespace test
}  // namespace gvdb
