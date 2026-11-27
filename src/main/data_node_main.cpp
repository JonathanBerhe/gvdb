// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// gvdb-data-node: stores the segments of its shards, builds their indexes
// and runs the shard-local part of searches, backups and restores.

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cluster/datanode/data_node.h"
#include "cluster/datanode/primary_term_tracker.h"
#include "cluster/shard_manager.h"
#include "cluster/datanode/shard_write_gate.h"
#include "common/logger.h"
#include "compute/query_cache.h"
#include "compute/query_executor.h"
#include "config/config.h"
#include "core/types.h"
#include "internal.pb.h"
#include "core/status.h"
#include "server/bootstrap.h"
#include "server/builders.h"
#include "server/cluster_steps.h"
#include "server/flags.h"
#include "service/vectordb/collection_resolver.h"
#include "service/internal/internal_service.h"
#include "service/vectordb/vectordb_service.h"

namespace gvdb {
namespace {

struct Args {
  int node_id = 101;
  std::string bind_address = "0.0.0.0:50060";
  std::string advertise_address;
  std::string data_dir = "/tmp/gvdb/data_node";
  std::string config_file;
  std::vector<std::string> coordinator_addresses;
  size_t memory_limit_gb = 8;
  // How long shutdown waits for the coordinator to move the shards off this
  // node. In Kubernetes, terminationGracePeriodSeconds bounds it.
  int drain_wait_seconds = 15;
};

absl::Status Run(const Args& args) {
  server::InstallSignalHandlers();
  server::StartLogging(args.data_dir, "data_node.log");
  common::Logger::Instance().Info("Starting GVDB Data Node (ID: {})", args.node_id);
  const int metrics_port = 9100 + (args.node_id - 100);
  server::StartMetricsServer(metrics_port);

  const config::GVDBConfig config = server::LoadConfigOrDefault(args.config_file);
  const bool in_cluster = !args.coordinator_addresses.empty();

  ASSIGN_OR_RETURN(server::SegmentStoreParts storage,
                   server::BuildSegmentStore(config.storage, args.data_dir,
                                                 server::ObjectStoreFailure::kLocalOnly));
  auto segment_store = storage.segment_store;
  auto bulk_importer = server::MakeBulkImporter(storage, args.data_dir);
  auto data_node =
      std::make_shared<cluster::DataNode>(std::move(storage.index_factory), segment_store);
  // A full segment goes to the background index builder.
  segment_store->SetSealCallback([data_node](core::SegmentId sid, core::IndexType idx_type) {
    (void)data_node->ScheduleBuildTask({sid, idx_type, 100});
  });
  auto query_executor = std::make_shared<compute::QueryExecutor>(
      *segment_store, nullptr, std::make_shared<compute::QueryCache>(10000));

  // Both services and the heartbeat keep raw pointers to the write fence and
  // to the primary-term tracker, so these two outlive all three. The fence
  // blocks writes to a shard while the coordinator backs it up. The tracker
  // tells the write path whether this node is the primary of a shard and at
  // which term. The heartbeat fills it from the coordinator, and the primary
  // swap RPCs change it.
  cluster::ShardWriteGate shard_write_gate;
  cluster::PrimaryTermTracker primary_term_tracker;

  auto backup = server::BuildBackupEngines(config.storage, {.object_store = storage.object_store,
                                                            .data_dir = args.data_dir,
                                                            .version = "data-node",
                                                            .node_id = args.node_id});
  // The services get the tracker only in a cluster: without a coordinator
  // no heartbeat fills it, and every shard would be unknown to it.
  cluster::PrimaryTermTracker* tracker = in_cluster ? &primary_term_tracker : nullptr;
  auto internal_service = std::make_unique<service::InternalService>(
      std::make_shared<cluster::ShardManager>(16, cluster::ShardingStrategy::HASH), segment_store,
      query_executor,
      service::InternalServiceOptions{.backup_manager = backup.backup,
                                      .restore_manager = backup.restore,
                                      .primary_term_tracker = tracker,
                                      .shard_write_gate = &shard_write_gate});

  auto resolver = in_cluster
                      ? service::MakeCachedCoordinatorResolver(args.coordinator_addresses[0])
                      : service::MakeLocalResolver(segment_store, args.data_dir);
  auto vectordb_service = std::make_unique<service::VectorDBService>(
      segment_store, query_executor, std::move(resolver),
      service::VectorDBServiceOptions{.bulk_importer = bulk_importer,
                                      .backup_manager = backup.backup,
                                      .restore_manager = backup.restore,
                                      .primary_term_tracker = tracker,
                                      .shard_write_gate = &shard_write_gate});

  server::InterceptorFactories interceptors;
  server::AddAuditInterceptor(config.logging.audit, interceptors);
  ASSIGN_OR_RETURN(auto grpc_server,
                   server::StartGrpcServer(args.bind_address, {internal_service.get(), vectordb_service.get()}, grpc::InsecureServerCredentials(), std::move(interceptors)));

  std::thread build_thread([data_node] { data_node->RunBuildLoop(server::ShutdownFlag()); });
  std::thread ttl_sweep_thread(
      [data_node] { data_node->RunTTLSweepLoop(server::ShutdownFlag()); });

  server::PrintBanner("GVDB Data Node",
                      {
                          "Node ID: " + std::to_string(args.node_id),
                          "gRPC Service: " + args.bind_address,
                          "Metrics: http://0.0.0.0:" + std::to_string(metrics_port) + "/metrics",
                          "Data Directory: " + args.data_dir,
                          "Memory Limit: " + std::to_string(args.memory_limit_gb) + " GB",
                      });

  auto heartbeat = server::StartHeartbeat(
      args.coordinator_addresses, args.node_id, args.bind_address, args.advertise_address,
      proto::internal::NodeType::NODE_TYPE_DATA_NODE, &primary_term_tracker);
  server::WaitForShutdown();

  // Tell the coordinator that this node drains, so it stops routing new
  // work at once. DrainAndStop joins the heartbeat loop first, so no READY
  // heartbeat can overwrite the DRAINING one.
  if (heartbeat) heartbeat->DrainAndStop();
  build_thread.join();
  ttl_sweep_thread.join();
  heartbeat.reset();

  // Keep serving until the coordinator moved the shards off this node:
  // other data nodes still pull segments from it to promote replicas. After
  // the timeout the coordinator evicts the node when its heartbeat times out.
  if (in_cluster) {
    server::WaitForDrainCompletion(args.coordinator_addresses[0], args.node_id,
                                   std::chrono::seconds(args.drain_wait_seconds));
  }
  server::StopServing(*grpc_server);
  return absl::OkStatus();
}

}  // namespace
}  // namespace gvdb

int main(int argc, char** argv) {
  gvdb::Args args;
  gvdb::server::FlagSet flags;
  flags.AddInt("--node-id", "ID", "Data node ID (default: 101)", &args.node_id);
  flags.AddString("--bind-address", "ADDR", "gRPC bind address (default: 0.0.0.0:50060)",
                  &args.bind_address, "GVDB_BIND_ADDRESS");
  flags.AddString("--advertise-address", "ADDR",
                  "Address advertised to coordinator (default: bind-address)",
                  &args.advertise_address, "GVDB_ADVERTISE_ADDRESS");
  flags.AddString("--data-dir", "PATH", "Data directory (default: /tmp/gvdb/data_node)",
                  &args.data_dir, "GVDB_DATA_DIR");
  flags.AddList("--coordinator", "ADDR", "Coordinator addresses (comma-separated)",
                &args.coordinator_addresses);
  flags.AddString("--config", "FILE", "YAML config file (for S3/MinIO settings)",
                  &args.config_file);
  flags.AddSize("--memory-limit-gb", "SIZE", "Memory limit in GB (default: 8)",
                &args.memory_limit_gb);
  flags.AddInt("--drain-wait-seconds", "SEC",
               "Max seconds to wait for shard drain on shutdown (default: 15)",
               &args.drain_wait_seconds);
  if (auto exit_code = flags.Parse(argc, argv)) return *exit_code;
  return gvdb::server::RunMain([&] { return gvdb::Run(args); });
}
