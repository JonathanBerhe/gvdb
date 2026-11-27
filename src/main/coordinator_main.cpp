// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// gvdb-coordinator: holds the collection registry and the shard map in a
// Raft group, and fans backups and restores out to the data nodes.

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cluster/coordinator.h"
#include "cluster/internal_client.h"
#include "cluster/node_registry.h"
#include "cluster/shard_manager.h"
#include "common/logger.h"
#include "compute/query_executor.h"
#include "config/config.h"
#include "consensus/raft_config.h"
#include "consensus/raft_node.h"
#include "index/index_factory.h"
#include "server/bootstrap.h"
#include "server/builders.h"
#include "server/cluster_steps.h"
#include "server/flags.h"
#include "service/collection_resolver.h"
#include "service/internal_service.h"
#include "service/vectordb_service.h"
#include "storage/objstore/object_store_factory.h"
#include "storage/segment/segment_manager.h"

namespace gvdb {
namespace {

struct Args {
  int node_id = 1;
  std::string bind_address = "0.0.0.0:50051";
  std::string advertise_address;
  std::string raft_address = "0.0.0.0:8300";
  // The Raft address that peers dial. Empty means raft_address. In
  // Kubernetes it is the pod FQDN, so peers reach the pod after its IP
  // changes.
  std::string raft_advertise_address;
  std::vector<std::string> raft_peers;
  // The gRPC addresses of the coordinators, for the startup peer probe and
  // the self-remove on shutdown. With fewer than two entries the node does
  // not auto-join and starts from the initial seed.
  std::vector<std::string> coordinator_grpc_peers;
  std::string data_dir = "/tmp/gvdb/coordinator";
  std::string config_file;
  bool single_node_mode = true;
};

absl::Status Run(const Args& args) {
  server::InstallSignalHandlers();
  server::StartLogging(args.data_dir, "coordinator.log");
  common::Logger::Instance().Info("Starting GVDB Coordinator Node");
  common::Logger::Instance().Info("  Node ID: {}, Mode: {}", args.node_id,
                                  args.single_node_mode ? "Single-Node" : "Multi-Node");
  const int metrics_port = 9090 + args.node_id;
  server::StartMetricsServer(metrics_port);

  consensus::RaftConfig raft_config;
  raft_config.node_id = args.node_id;
  raft_config.single_node_mode = args.single_node_mode;
  raft_config.listen_address = args.raft_address;
  raft_config.advertise_address = args.raft_advertise_address;
  raft_config.peers = args.raft_peers;
  raft_config.coordinator_grpc_peers = args.coordinator_grpc_peers;
  raft_config.data_dir = args.data_dir + "/raft";
  auto started_raft = server::StartRaft(raft_config);
  if (!started_raft.ok()) return started_raft.status();
  std::unique_ptr<consensus::RaftNode> raft_node = std::move(*started_raft);
  if (!args.single_node_mode) {
    server::WaitForLeaderElection(*raft_node, std::chrono::seconds(60));
  }

  auto shard_manager = std::make_shared<cluster::ShardManager>(16, cluster::ShardingStrategy::HASH);
  auto node_registry = std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30));
  auto coordinator = std::make_shared<cluster::Coordinator>(
      shard_manager, node_registry, std::make_shared<cluster::GrpcInternalServiceClientFactory>());
  // Restore the collection registry and the shard map before serving.
  // Without it, a restart orphans every collection, although the data
  // nodes keep the segments.
  coordinator->SetRegistryDir(args.data_dir);
  if (auto status = coordinator->LoadRegistry(); !status.ok()) {
    return server::WithPrefix("Failed to load coordinator registry: ", status);
  }

  // The coordinator keeps no data, but its services need a segment store
  // and a query executor.
  auto index_factory = std::make_unique<index::IndexFactory>();
  auto segment_manager =
      std::make_shared<storage::SegmentManager>(args.data_dir + "/segments", index_factory.get());
  auto query_executor = std::make_shared<compute::QueryExecutor>(segment_manager.get());

  // The service shares the Raft node without owning it. raft_node outlives
  // the service.
  std::shared_ptr<consensus::RaftNode> raft_ref(raft_node.get(), [](consensus::RaftNode*) {});
  auto internal_service = std::make_unique<service::InternalService>(
      shard_manager, segment_manager, query_executor, node_registry, coordinator, raft_ref);

  const config::GVDBConfig config = server::LoadConfigOrDefault(args.config_file);

  // The coordinator writes the top-level backup manifest into the bucket
  // where the data nodes upload their shards. Without an object store, only
  // local backup targets work.
  std::unique_ptr<storage::IObjectStore> object_store;
  auto created_store = storage::CreateObjectStore(config.storage.object_store);
  if (created_store.ok()) {
    object_store = std::move(*created_store);
  } else {
    common::Logger::Instance().Warn(
        "Coordinator object store creation failed ({}); backups limited to local targets",
        created_store.status().message());
  }
  auto backup = server::BuildBackupEngines(config.storage, {.object_store = object_store.get(),
                                                            .data_dir = args.data_dir,
                                                            .version = "coordinator",
                                                            .node_id = args.node_id,
                                                            .local_segments = false});
  coordinator->SetBackupManager(backup.backup);
  coordinator->SetRestoreManager(backup.restore);

  server::InterceptorFactories interceptors;
  auto rbac_store = server::AddAuthInterceptor(config.server.auth, interceptors);
  if (!rbac_store.ok()) return rbac_store.status();
  server::AddAuditInterceptor(config.logging.audit, interceptors);

  auto vectordb_service = std::make_unique<service::VectorDBService>(
      segment_manager, query_executor, service::MakeCoordinatorResolver(coordinator), *rbac_store);
  // Client backup and restore calls fan out to the shards through the
  // coordinator. Without it, they would back up the empty local store. The
  // service logs "Backup disabled" because it has no local backup manager,
  // so the next line says how backups run here.
  vectordb_service->SetCoordinator(coordinator);
  common::Logger::Instance().Info(
      "Coordinator backup/restore served via distributed shard orchestration");

  auto grpc_server = server::StartGrpcServer(
      args.bind_address, {internal_service.get(), vectordb_service.get()},
      server::MakeServerCredentials(config.server.tls), std::move(interceptors));
  if (!grpc_server.ok()) return grpc_server.status();

  server::PrintBanner(
      "GVDB Coordinator Node",
      {
          "Node ID: " + std::to_string(args.node_id),
          "Mode: " + std::string(args.single_node_mode ? "Single-Node" : "Multi-Node"),
          "gRPC Services: " + args.bind_address,
          "Raft Address: " + args.raft_address,
          "Metrics: http://0.0.0.0:" + std::to_string(metrics_port) + "/metrics",
          "Role: " + std::string(raft_node->IsLeader() ? "LEADER" : "FOLLOWER"),
      });

  bool was_leader = raft_node->IsLeader();
  server::WaitForShutdown([&] {
    if (args.single_node_mode) return;
    const bool is_leader = raft_node->IsLeader();
    if (is_leader != was_leader) {
      std::cout << ">>> " << (is_leader ? "Became LEADER" : "Lost leadership") << " <<<"
                << std::endl;
      was_leader = is_leader;
    }
  });

  // Leave the Raft group before the gRPC server stops, while the leader can
  // still reach this node.
  if (!args.single_node_mode) {
    server::RemoveSelfFromRaft(*raft_node, args.node_id, args.coordinator_grpc_peers);
  }
  server::StopServing(**grpc_server);
  (void)raft_node->Shutdown();
  return absl::OkStatus();
}

}  // namespace
}  // namespace gvdb

int main(int argc, char** argv) {
  gvdb::Args args;
  gvdb::server::FlagSet flags;
  flags.AddInt("--node-id", "ID", "Node ID (default: 1)", &args.node_id);
  flags.AddString("--bind-address", "ADDR", "gRPC bind address (default: 0.0.0.0:50051)",
                  &args.bind_address, "GVDB_BIND_ADDRESS");
  flags.AddString("--advertise-address", "ADDR",
                  "Address advertised to peers (default: bind-address)", &args.advertise_address,
                  "GVDB_ADVERTISE_ADDRESS");
  flags.AddString("--raft-address", "ADDR",
                  "Raft BIND address (default: 0.0.0.0:8300).\n"
                  "Only the port is used; NuRaft binds 0.0.0.0.",
                  &args.raft_address, "GVDB_RAFT_ADDRESS");
  flags.AddString("--raft-advertise-address", "ADDR",
                  "Peer-facing Raft endpoint (host:port).\n"
                  "Set to the pod FQDN in K8s. Falls back to\n"
                  "--raft-address when empty.",
                  &args.raft_advertise_address, "GVDB_RAFT_ADVERTISE_ADDRESS");
  flags.AddValue("--raft-peers", "PEERS",
                 "Comma-separated Raft peers in 'id:host:port' format\n"
                 "(e.g. '1:host1:8300,2:host2:8300,3:host3:8300').\n"
                 "Passing this flag implies multi-node mode.",
                 [&](const std::string& value) {
                   for (auto& peer : gvdb::server::SplitList(value)) {
                     args.raft_peers.push_back(std::move(peer));
                   }
                   args.single_node_mode = false;
                 });
  flags.AddList("--coordinator-grpc-peers", "PEERS",
                "Comma-separated coordinator gRPC endpoints\n"
                "('host1:50051,host2:50051,host3:50051') used for\n"
                "the startup peer-probe and self-remove RPC.\n"
                "Order must match --raft-peers (node_id=ordinal+1).",
                &args.coordinator_grpc_peers, "GVDB_COORDINATOR_GRPC_PEERS");
  flags.AddString("--data-dir", "PATH", "Data directory (default: /tmp/gvdb/coordinator)",
                  &args.data_dir, "GVDB_DATA_DIR");
  flags.AddString("--config", "FILE", "YAML configuration file", &args.config_file);
  flags.AddSwitch("--single-node", "Run in single-node mode",
                  [&] { args.single_node_mode = true; });
  if (auto exit_code = flags.Parse(argc, argv)) return *exit_code;
  return gvdb::server::RunMain([&] { return gvdb::Run(args); });
}
