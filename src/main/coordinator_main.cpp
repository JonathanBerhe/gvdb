// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "absl/strings/str_cat.h"
#include "consensus/raft_node.h"
#include "consensus/raft_config.h"
#include "consensus/timestamp_oracle.h"
#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/node_registry.h"
#include "cluster/internal_client.h"
#include "storage/backup_manager.h"
#include "storage/s3_object_store.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/internal_service.h"
#include "network/vectordb_service.h"
#include "internal.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "network/collection_resolver.h"
#include "network/auth_processor.h"
#include "network/audit_interceptor.h"
#include "utils/audit_logger.h"
#include "auth/rbac.h"
#include "utils/server_bootstrap.h"
#include "utils/config.h"
#include "utils/env_flags.h"

struct CoordinatorArgs {
  int node_id = 1;
  std::string bind_address = "0.0.0.0:50051";
  std::string advertise_address;
  std::string raft_address = "0.0.0.0:8300";
  // Peer-facing Raft endpoint. Optional — when empty, raft_address is
  // used. Set to the pod FQDN in K8s so other Raft replicas can reach
  // us regardless of pod IP churn (roadmap 0b.4).
  std::string raft_advertise_address;
  std::vector<std::string> raft_peers;
  // Coordinator gRPC InternalService endpoints (host:50051) for the 1.7b
  // startup peer probe and self-remove RPC on shutdown. Rendered by the
  // operator alongside --raft-peers. Comma-separated; empty or single-
  // entry list disables auto-join (falls back to the initial-bootstrap
  // seed path; single-node clusters don't need peer probing anyway).
  std::vector<std::string> coordinator_grpc_peers;
  std::string data_dir = "/tmp/gvdb/coordinator";
  std::string config_file;
  bool single_node_mode = true;
};

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [options]\n"
            << "Options:\n"
            << "  --node-id ID             Node ID (default: 1)\n"
            << "  --bind-address ADDR      gRPC bind address (default: 0.0.0.0:50051)\n"
            << "  --advertise-address ADDR Address advertised to peers (default: bind-address)\n"
            << "  --raft-address ADDR      Raft BIND address (default: 0.0.0.0:8300).\n"
            << "                           Only the port is used; NuRaft binds 0.0.0.0.\n"
            << "  --raft-advertise-address ADDR  Peer-facing Raft endpoint (host:port).\n"
            << "                           Set to the pod FQDN in K8s. Falls back to\n"
            << "                           --raft-address when empty (roadmap 0b.4).\n"
            << "  --raft-peers PEERS       Comma-separated Raft peers in 'id:host:port' format\n"
            << "                           (e.g. '1:host1:8300,2:host2:8300,3:host3:8300').\n"
            << "                           Passing this flag implies --multi-node.\n"
            << "  --coordinator-grpc-peers PEERS  Comma-separated coordinator gRPC endpoints\n"
            << "                           ('host1:50051,host2:50051,host3:50051') used for\n"
            << "                           the 1.7b startup peer-probe and self-remove RPC.\n"
            << "                           Order must match --raft-peers (node_id=ordinal+1).\n"
            << "  --data-dir PATH          Data directory (default: /tmp/gvdb/coordinator)\n"
            << "  --config FILE            YAML configuration file\n"
            << "  --single-node            Run in single-node mode\n"
            << "  --help                   Show this help message\n";
}

bool ParseArgs(int argc, char** argv, CoordinatorArgs& args) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      return false;
    } else if (arg == "--node-id" && i + 1 < argc) {
      args.node_id = std::stoi(argv[++i]);
    } else if (arg == "--bind-address" && i + 1 < argc) {
      args.bind_address = argv[++i];
    } else if (arg == "--advertise-address" && i + 1 < argc) {
      args.advertise_address = argv[++i];
    } else if (arg == "--raft-address" && i + 1 < argc) {
      args.raft_address = argv[++i];
    } else if (arg == "--raft-advertise-address" && i + 1 < argc) {
      args.raft_advertise_address = argv[++i];
    } else if (arg == "--raft-peers" && i + 1 < argc) {
      std::string peers_str = argv[++i];
      size_t start = 0;
      size_t end = peers_str.find(',');
      while (end != std::string::npos) {
        args.raft_peers.push_back(peers_str.substr(start, end - start));
        start = end + 1;
        end = peers_str.find(',', start);
      }
      args.raft_peers.push_back(peers_str.substr(start));
      args.single_node_mode = false;
    } else if (arg == "--coordinator-grpc-peers" && i + 1 < argc) {
      std::string peers_str = argv[++i];
      size_t start = 0;
      size_t end = peers_str.find(',');
      while (end != std::string::npos) {
        args.coordinator_grpc_peers.push_back(peers_str.substr(start, end - start));
        start = end + 1;
        end = peers_str.find(',', start);
      }
      args.coordinator_grpc_peers.push_back(peers_str.substr(start));
    } else if (arg == "--data-dir" && i + 1 < argc) {
      args.data_dir = argv[++i];
    } else if (arg == "--config" && i + 1 < argc) {
      args.config_file = argv[++i];
    } else if (arg == "--single-node") {
      args.single_node_mode = true;
    } else {
      std::cerr << "Unknown argument: " << arg << std::endl;
      PrintUsage(argv[0]);
      return false;
    }
  }
  return true;
}

int main(int argc, char** argv) {
  CoordinatorArgs args;
  if (!ParseArgs(argc, argv, args)) return 1;

  using namespace gvdb;

  // Env vars override CLI flags
  args.bind_address = utils::ResolveFlag("GVDB_BIND_ADDRESS", args.bind_address);
  args.advertise_address = utils::ResolveFlag("GVDB_ADVERTISE_ADDRESS", args.advertise_address);
  args.data_dir = utils::ResolveFlag("GVDB_DATA_DIR", args.data_dir);
  args.raft_address = utils::ResolveFlag("GVDB_RAFT_ADDRESS", args.raft_address);
  args.raft_advertise_address = utils::ResolveFlag(
      "GVDB_RAFT_ADVERTISE_ADDRESS", args.raft_advertise_address);
  // GVDB_COORDINATOR_GRPC_PEERS overrides --coordinator-grpc-peers (parity
  // with the other peer-list flags). Comma-separated host:port list.
  {
    std::string override_peers = utils::ResolveFlag(
        "GVDB_COORDINATOR_GRPC_PEERS", std::string{});
    if (!override_peers.empty()) {
      args.coordinator_grpc_peers.clear();
      size_t start = 0;
      size_t end = override_peers.find(',');
      while (end != std::string::npos) {
        args.coordinator_grpc_peers.push_back(override_peers.substr(start, end - start));
        start = end + 1;
        end = override_peers.find(',', start);
      }
      args.coordinator_grpc_peers.push_back(override_peers.substr(start));
    }
  }
  utils::ServerBootstrap::InstallSignalHandlers();

  auto log_status = utils::ServerBootstrap::InitializeLogger(
      args.data_dir, "coordinator.log");
  if (!log_status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << log_status.message() << std::endl;
  }

  utils::Logger::Instance().Info("Starting GVDB Coordinator Node");
  utils::Logger::Instance().Info("  Node ID: {}, Mode: {}",
      args.node_id, args.single_node_mode ? "Single-Node" : "Multi-Node");

  int metrics_port = 9090 + args.node_id;
  utils::ServerBootstrap::StartMetricsServer(metrics_port);

  try {
    // 1. Raft consensus
    consensus::RaftConfig raft_config;
    raft_config.node_id = args.node_id;
    raft_config.single_node_mode = args.single_node_mode;
    raft_config.listen_address = args.raft_address;
    raft_config.advertise_address = args.raft_advertise_address;
    raft_config.peers = args.raft_peers;
    raft_config.coordinator_grpc_peers = args.coordinator_grpc_peers;
    raft_config.data_dir = args.data_dir + "/raft";

    auto raft_node = std::make_unique<consensus::RaftNode>(raft_config);
    auto status = raft_node->Start();
    if (!status.ok()) {
      std::cerr << "Failed to start Raft: " << status.message() << std::endl;
      return 1;
    }

    if (!args.single_node_mode) {
      // Bootstrap-tolerant leader-election wait (roadmap 0b.4). During a
      // cold start of an HA cluster, peer pods may take tens of seconds
      // to become reachable (DNS propagation, scheduling, image pulls) —
      // a short hard timeout would cause every coordinator pod to
      // crashloop forever. Wait up to kElectionWaitSeconds; if no leader
      // by then, log a warning and continue so the gRPC server comes up.
      // Readiness/health reporting reflects the not-leader state, and
      // Raft can still converge after the election wait elapses.
      constexpr int kElectionWaitSeconds = 60;
      utils::Logger::Instance().Info(
          "Waiting up to {}s for leader election...", kElectionWaitSeconds);
      auto start_time = std::chrono::steady_clock::now();
      while (!raft_node->IsLeader() && raft_node->GetLeaderId() < 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (utils::ServerBootstrap::ShutdownFlag().load()) break;
        if (std::chrono::steady_clock::now() - start_time >
            std::chrono::seconds(kElectionWaitSeconds)) {
          utils::Logger::Instance().Warn(
              "No Raft leader elected after {}s — continuing startup; "
              "election will complete when enough peers are reachable",
              kElectionWaitSeconds);
          break;
        }
      }
    }

    // 2. Cluster components
    auto shard_manager = std::make_shared<cluster::ShardManager>(
        16, cluster::ShardingStrategy::HASH);
    auto node_registry = std::make_shared<cluster::NodeRegistry>(
        std::chrono::seconds(30));
    auto client_factory = std::make_shared<cluster::GrpcInternalServiceClientFactory>();
    auto coordinator = std::make_shared<cluster::Coordinator>(
        shard_manager, node_registry, client_factory);

    // 3. TimestampOracle
    auto* tso_ptr = raft_node->GetTimestampOracle();
    std::shared_ptr<consensus::TimestampOracle> timestamp_oracle;
    if (tso_ptr) {
      timestamp_oracle = std::shared_ptr<consensus::TimestampOracle>(
          tso_ptr, [](consensus::TimestampOracle*){});
    }

    // 4. Storage/compute stubs (coordinator doesn't store data)
    auto index_factory = std::make_unique<index::IndexFactory>();
    auto segment_manager = std::make_shared<storage::SegmentManager>(
        args.data_dir + "/segments", index_factory.get());
    auto query_executor = std::make_shared<compute::QueryExecutor>(
        segment_manager.get());

    // 5. Services
    // Non-owning alias-shared_ptr mirror of the unique_ptr raft_node — the
    // local variable outlives internal_service so this is safe. Same pattern
    // we use for timestamp_oracle above.
    std::shared_ptr<consensus::RaftNode> raft_node_ptr(
        raft_node.get(), [](consensus::RaftNode*){});
    auto internal_service = std::make_unique<network::InternalService>(
        shard_manager, segment_manager, query_executor,
        node_registry, timestamp_oracle, coordinator, raft_node_ptr);
    // Load config for auth (optional)
    utils::GVDBConfig config = utils::Config::get_default();
    if (!args.config_file.empty()) {
      auto cfg_result = utils::Config::load_from_file(args.config_file);
      if (cfg_result.ok()) config = std::move(*cfg_result);
    }

    // 5.1. Backup / restore engines on the coordinator. The coordinator
    // needs its own IObjectStore to write the top-level manifest — same
    // bucket the data-nodes upload per-shard objects to. When no S3
    // store is configured, only LocalBackupTarget is available (and
    // only if local_backup_dir is set).
    std::unique_ptr<storage::IObjectStore> coord_object_store;
#ifdef GVDB_HAS_S3
    if (!config.storage.object_store_endpoint.empty()) {
      storage::S3Config s3_config;
      s3_config.endpoint = config.storage.object_store_endpoint;
      s3_config.access_key = config.storage.object_store_access_key;
      s3_config.secret_key = config.storage.object_store_secret_key;
      s3_config.bucket = config.storage.object_store_bucket;
      s3_config.region = config.storage.object_store_region;
      s3_config.use_ssl = config.storage.object_store_use_ssl;
      s3_config.path_style = (config.storage.object_store_type == "minio");
      auto s3_result = storage::S3ObjectStore::Create(s3_config);
      if (s3_result.ok()) {
        coord_object_store = std::move(*s3_result);
      }
    }
#endif
    std::shared_ptr<storage::BackupManager> backup_manager;
    std::shared_ptr<storage::RestoreManager> restore_manager;
    {
      storage::BackupManagerOptions bopts;
      bopts.default_object_store = coord_object_store.get();
      bopts.s3_bucket = config.storage.object_store_bucket;
      bopts.local_root_allowlist = config.storage.local_backup_dir;
      // flushed_segments_root is unused on the coordinator (the
      // coordinator never runs RunShardBackup against a local store —
      // it dispatches BackupShard via RPC to data-nodes).
      bopts.tmp_dir = args.data_dir + "/tmp/backup";
      bopts.gvdb_version = "coordinator";
      bopts.node_id = args.node_id;
      backup_manager = std::make_shared<storage::BackupManager>(std::move(bopts));

      storage::RestoreManagerOptions ropts;
      ropts.default_object_store = coord_object_store.get();
      ropts.s3_bucket = config.storage.object_store_bucket;
      ropts.local_root_allowlist = config.storage.local_backup_dir;
      ropts.staging_dir = args.data_dir + "/tmp/restore";
      restore_manager = std::make_shared<storage::RestoreManager>(
          std::move(ropts));
    }
    coordinator->SetBackupManager(backup_manager);
    coordinator->SetRestoreManager(restore_manager);

    // RBAC
    std::shared_ptr<auth::RbacStore> rbac_store;
    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> interceptors;
    if (config.server.auth.enabled) {
      auto rbac_result = auth::RbacStore::Create(config.server.auth);
      if (!rbac_result.ok()) {
        std::cerr << "Invalid auth config: " << rbac_result.status().message() << std::endl;
        return 1;
      }
      rbac_store = std::move(*rbac_result);
      interceptors.push_back(
          std::make_unique<network::ApiKeyAuthInterceptorFactory>(rbac_store));
    }

    // Audit logging
    if (config.logging.audit.enabled) {
      utils::AuditLogger::Initialize(config.logging.audit);
      interceptors.push_back(
          std::make_unique<network::AuditInterceptorFactory>());
    }

    auto coord_resolver = network::MakeCoordinatorResolver(coordinator);
    auto vectordb_service = std::make_unique<network::VectorDBService>(
        segment_manager, query_executor, std::move(coord_resolver), rbac_store);
    // Route the client-facing backup/restore RPCs through the
    // coordinator's per-shard fan-out instead of the local single-shard
    // path. Without this, BackupCollection on the coordinator binary
    // would try to back up the coordinator's empty local segment store.
    vectordb_service->SetCoordinator(coordinator);

    // 6. Start gRPC server
    auto credentials = utils::ServerBootstrap::MakeServerCredentials(config.server.tls);
    auto grpc_server = utils::ServerBootstrap::StartGrpcServer(
        args.bind_address,
        {internal_service.get(), vectordb_service.get()},
        credentials, std::move(interceptors));
    if (!grpc_server) {
      std::cerr << "Failed to start gRPC server on " << args.bind_address << std::endl;
      return 1;
    }

    std::string role = raft_node->IsLeader() ? "LEADER" : "FOLLOWER";
    utils::ServerBootstrap::PrintBanner("GVDB Coordinator Node", {
        "Node ID: " + std::to_string(args.node_id),
        "Mode: " + std::string(args.single_node_mode ? "Single-Node" : "Multi-Node"),
        "gRPC Services: " + args.bind_address,
        "Raft Address: " + args.raft_address,
        "Metrics: http://0.0.0.0:" + std::to_string(metrics_port) + "/metrics",
        "Role: " + role,
    });

    // Wait loop with leader status monitoring
    while (!utils::ServerBootstrap::ShutdownFlag().load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      if (!args.single_node_mode) {
        static bool was_leader = raft_node->IsLeader();
        bool is_leader = raft_node->IsLeader();
        if (was_leader != is_leader) {
          std::cout << ">>> " << (is_leader ? "Became LEADER" : "Lost leadership") << " <<<" << std::endl;
          was_leader = is_leader;
        }
      }
    }

    // Graceful shutdown
    std::cout << "\nShutting down gracefully..." << std::endl;

    // Self-remove from Raft membership BEFORE shutting down the gRPC
    // server (roadmap 1.7b). Only relevant in multi-node mode and only
    // if we're a follower — NuRaft refuses to remove the current leader
    // (CANNOT_REMOVE_LEADER) and letting the cluster re-elect after we
    // exit is the clean path. Failures here are non-fatal: a lingering
    // peer in cluster_config is a degraded state that recovers on the
    // next reconcile or follow-up operator RemovePeer call.
    //
    // One retry on denial: leadership can flip between our GetLeaderId()
    // snapshot and the RemovePeer RPC. If the first call returns a
    // current_leader_id pointing at a different pod, retry against that
    // target once. Bounded by K8s terminationGracePeriodSeconds — two
    // attempts × 3s deadline is well within a 30s grace window.
    if (!args.single_node_mode && !args.coordinator_grpc_peers.empty() &&
        !raft_node->IsLeader()) {
      auto resolve_target = [&](int id) -> std::string {
        if (id <= 0) return "";
        size_t idx = static_cast<size_t>(id - 1);
        if (idx >= args.coordinator_grpc_peers.size()) return "";
        return args.coordinator_grpc_peers[idx];
      };
      int leader_id = raft_node->GetLeaderId();
      std::string leader_target = resolve_target(leader_id);
      for (int attempt = 0; attempt < 2 && !leader_target.empty(); ++attempt) {
        utils::Logger::Instance().Info(
            "Self-removing node_id={} via leader {} (attempt {})",
            args.node_id, leader_target, attempt);
        auto channel = grpc::CreateChannel(
            leader_target, grpc::InsecureChannelCredentials());
        auto stub = proto::internal::InternalService::NewStub(channel);
        proto::internal::RemovePeerRequest req;
        req.set_node_id(static_cast<uint32_t>(args.node_id));
        proto::internal::RemovePeerResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(3));
        auto st = stub->RemovePeer(&ctx, req, &resp);
        if (!st.ok()) {
          utils::Logger::Instance().Warn(
              "Self-remove RPC failed: {}", st.error_message());
          break;  // transport failure; no point retrying same target
        }
        if (resp.success()) {
          utils::Logger::Instance().Info("Self-remove succeeded");
          break;
        }
        utils::Logger::Instance().Warn(
            "Self-remove denied: {} (leader_id={})",
            resp.message(), resp.current_leader_id());
        // Leadership flipped between snapshot and RPC — follow the redirect
        // for one retry.
        std::string new_target = resolve_target(resp.current_leader_id());
        if (new_target.empty() || new_target == leader_target) break;
        leader_target = new_target;
      }
    }

    grpc_server->Shutdown();
    utils::ServerBootstrap::StopMetricsServer();
    (void)raft_node->Shutdown();
    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    return 1;
  }
}