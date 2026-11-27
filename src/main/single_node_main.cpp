// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// gvdb-single-node: every role in one process. Raft runs in single-node
// mode, and the segment store and the service share one data folder.

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "absl/strings/str_cat.h"
#include "cluster/coordinator/coordinator.h"
#include "cluster/coordinator/node_registry.h"
#include "cluster/shard_manager.h"
#include "cluster/datanode/shard_write_gate.h"
#include "common/logger.h"
#include "compute/query_cache.h"
#include "compute/query_executor.h"
#include "config/config.h"
#include "consensus/raft_config.h"
#include "consensus/raft_node.h"
#include "core/status.h"
#include "server/bootstrap.h"
#include "server/builders.h"
#include "server/cluster_steps.h"
#include "server/flags.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/vectordb_service.h"

namespace gvdb {
namespace {

// A flag that is not set keeps the value from the YAML configuration.
struct Args {
  std::string config_file;
  int port = -1;
  std::string data_dir;
  int node_id = -1;
};

absl::Status Run(const Args& args) {
  server::InstallSignalHandlers();

  ASSIGN_OR_RETURN(config::GVDBConfig config,
                   server::LoadConfig(args.config_file));
  if (args.port != -1) config.server.grpc_port = args.port;
  if (!args.data_dir.empty()) config.storage.data_dir = args.data_dir;
  if (args.node_id != -1) config.consensus.node_id = args.node_id;
  if (auto status = config::Config::Validate(config); !status.ok()) {
    return server::WithPrefix("Invalid configuration: ", status);
  }
  const std::string data_dir = config.storage.data_dir;
  const int node_id = config.consensus.node_id;

  server::StartLogging(data_dir, "gvdb.log", server::ParseLogLevel(config.logging.level));
  common::Logger::Instance().Info("Starting GVDB All-in-One Server");
  server::StartMetricsServer(9090);

  consensus::RaftConfig raft_config;
  raft_config.node_id = node_id;
  raft_config.single_node_mode = true;
  raft_config.data_dir = data_dir + "/raft";
  ASSIGN_OR_RETURN(std::unique_ptr<consensus::RaftNode> raft_node,
                   server::StartRaft(raft_config));

  ASSIGN_OR_RETURN(server::SegmentStoreParts storage,
                   server::BuildSegmentStore(config.storage, data_dir, server::ObjectStoreFailure::kStop));
  auto segment_store = storage.segment_store;
  auto query_executor = std::make_shared<compute::QueryExecutor>(*segment_store);
  query_executor->SetCache(std::make_shared<compute::QueryCache>(10000));
  server::SealFullSegmentsInline(segment_store);

  // The coordinator restores the collection registry from the data folder.
  // Without it, a restart orphans every collection, although the segments
  // load.
  auto coordinator = std::make_unique<cluster::Coordinator>(
      std::make_shared<cluster::ShardManager>(16, cluster::ShardingStrategy::HASH),
      std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30)));
  coordinator->SetRegistryDir(data_dir);
  if (auto status = coordinator->LoadRegistry(); !status.ok()) {
    return server::WithPrefix("Failed to load coordinator registry: ", status);
  }

  server::InterceptorFactories interceptors;
  ASSIGN_OR_RETURN(auto rbac_store, server::AddAuthInterceptor(config.server.auth, interceptors));
  server::AddAuditInterceptor(config.logging.audit, interceptors);

  // Single node runs the single-shard backup against its own segment store.
  // S3 targets need the object store, local targets need local_backup_dir.
  auto backup = server::BuildBackupEngines(config.storage, {.object_store = storage.object_store,
                                                            .data_dir = data_dir,
                                                            .version = "single-node",
                                                            .node_id = node_id});
  // The backup write fence. Single node has one shard, and the fence works
  // the same way.
  cluster::ShardWriteGate shard_write_gate;

  auto vectordb_service = std::make_unique<service::VectorDBService>(
      segment_store, query_executor, service::MakeLocalResolver(segment_store, data_dir),
      rbac_store, server::MakeBulkImporter(storage, data_dir), backup.backup, backup.restore);
  vectordb_service->SetShardWriteGate(&shard_write_gate);

  const std::string address = absl::StrCat("0.0.0.0:", config.server.grpc_port);
  ASSIGN_OR_RETURN(auto grpc_server,
                   server::StartGrpcServer(address, {vectordb_service.get()}, server::MakeServerCredentials(config.server.tls), std::move(interceptors)));

  std::thread ttl_sweep_thread(
      [segment_store] { segment_store->RunTTLSweepLoop(server::ShutdownFlag()); });

  server::PrintBanner("GVDB All-in-One Server", {
                                                     "gRPC Service: " + address,
                                                     "Metrics: http://0.0.0.0:9090/metrics",
                                                     "Node ID: " + std::to_string(node_id),
                                                     "Data Directory: " + data_dir,
                                                 });
  server::WaitForShutdown();

  ttl_sweep_thread.join();
  server::StopServing(*grpc_server);
  (void)raft_node->Shutdown();
  return absl::OkStatus();
}

}  // namespace
}  // namespace gvdb

int main(int argc, char** argv) {
  gvdb::Args args;
  gvdb::server::FlagSet flags;
  flags.AddString("--config", "FILE", "YAML config file (optional)", &args.config_file);
  flags.AddInt("--port", "PORT", "gRPC server port (overrides config, default: 50051)", &args.port);
  flags.AddString("--data-dir", "PATH", "Data directory (overrides config, default: /tmp/gvdb)",
                  &args.data_dir);
  flags.AddInt("--node-id", "ID", "Node ID (overrides config, default: 1)", &args.node_id);
  if (auto exit_code = flags.Parse(argc, argv)) return *exit_code;
  return gvdb::server::RunMain([&] { return gvdb::Run(args); });
}
