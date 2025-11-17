#include <signal.h>
#include <atomic>
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
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/internal_service.h"
#include "utils/logger.h"
#include "utils/metrics.h"
#include "utils/config.h"

#include <grpcpp/grpcpp.h>

// Command-line arguments
struct CoordinatorArgs {
  int node_id = 1;
  std::string bind_address = "0.0.0.0:50051";
  std::string raft_address = "0.0.0.0:8300";
  std::vector<std::string> raft_peers;  // Other coordinator addresses
  std::string data_dir = "/tmp/gvdb/coordinator";
  std::string config_file;
  bool single_node_mode = true;  // Default to single-node for now
};

// Global shutdown flag
std::atomic<bool> g_shutdown{false};

void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    std::cout << "\nReceived shutdown signal, gracefully shutting down..." << std::endl;
    g_shutdown.store(true);
  }
}

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [options]\n"
            << "Options:\n"
            << "  --node-id ID             Node ID (default: 1)\n"
            << "  --bind-address ADDR      gRPC bind address (default: 0.0.0.0:50051)\n"
            << "  --raft-address ADDR      Raft listen address (default: 0.0.0.0:8300)\n"
            << "  --raft-peers PEERS       Comma-separated list of Raft peer addresses\n"
            << "                           Example: host1:8300,host2:8300,host3:8300\n"
            << "  --data-dir PATH          Data directory (default: /tmp/gvdb/coordinator)\n"
            << "  --config FILE            YAML configuration file\n"
            << "  --single-node            Run in single-node mode (no Raft cluster)\n"
            << "  --help                   Show this help message\n"
            << "\nExamples:\n"
            << "  # Single-node mode (for testing)\n"
            << "  " << program_name << " --single-node\n"
            << "\n"
            << "  # Multi-node mode (3-node cluster)\n"
            << "  " << program_name << " --node-id 1 --bind-address 0.0.0.0:50051 \\\n"
            << "    --raft-address 0.0.0.0:8300 \\\n"
            << "    --raft-peers coord2:8300,coord3:8300\n";
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
    } else if (arg == "--raft-address" && i + 1 < argc) {
      args.raft_address = argv[++i];
    } else if (arg == "--raft-peers" && i + 1 < argc) {
      std::string peers_str = argv[++i];
      // Split by comma
      size_t start = 0;
      size_t end = peers_str.find(',');
      while (end != std::string::npos) {
        args.raft_peers.push_back(peers_str.substr(start, end - start));
        start = end + 1;
        end = peers_str.find(',', start);
      }
      args.raft_peers.push_back(peers_str.substr(start));
      args.single_node_mode = false;  // Multi-node mode
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

  // Parse command-line arguments
  if (!ParseArgs(argc, argv, args)) {
    return 1;
  }

  // Setup signal handlers
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // Initialize logger
  gvdb::utils::LogConfig log_config;
  log_config.file_path = args.data_dir + "/logs/coordinator.log";
  log_config.console_enabled = true;
  log_config.file_enabled = true;
  log_config.level = gvdb::utils::LogLevel::INFO;

  auto log_status = gvdb::utils::Logger::Initialize(log_config);
  if (!log_status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << log_status.message() << std::endl;
  }

  gvdb::utils::Logger::Instance().Info("Starting GVDB Coordinator Node");
  gvdb::utils::Logger::Instance().Info("  Node ID: {}", args.node_id);
  gvdb::utils::Logger::Instance().Info("  gRPC Address: {}", args.bind_address);
  gvdb::utils::Logger::Instance().Info("  Raft Address: {}", args.raft_address);
  gvdb::utils::Logger::Instance().Info("  Data Directory: {}", args.data_dir);
  gvdb::utils::Logger::Instance().Info("  Mode: {}",
                                       args.single_node_mode ? "Single-Node" : "Multi-Node");

  if (!args.single_node_mode && !args.raft_peers.empty()) {
    std::string peers_str;
    for (size_t i = 0; i < args.raft_peers.size(); ++i) {
      if (i > 0) peers_str += ", ";
      peers_str += args.raft_peers[i];
    }
    gvdb::utils::Logger::Instance().Info("  Raft Peers: {}", peers_str);
  }

  // Start metrics server
  int metrics_port = 9090 + args.node_id;  // Different port per coordinator
  if (!gvdb::utils::MetricsRegistry::Instance().StartMetricsServer(metrics_port)) {
    std::cerr << "Warning: Failed to start metrics server on :" << metrics_port << std::endl;
  } else {
    gvdb::utils::Logger::Instance().Info("Metrics server started on :{}metrics", metrics_port);
  }

  try {
    // 1. Create Raft consensus node
    gvdb::consensus::RaftConfig raft_config;
    raft_config.node_id = args.node_id;
    raft_config.single_node_mode = args.single_node_mode;
    raft_config.listen_address = args.raft_address;
    raft_config.peers = args.raft_peers;
    raft_config.data_dir = args.data_dir + "/raft";

    auto raft_node = std::make_unique<gvdb::consensus::RaftNode>(raft_config);
    auto status = raft_node->Start();
    if (!status.ok()) {
      std::cerr << "Failed to start Raft: " << status.message() << std::endl;
      return 1;
    }

    if (args.single_node_mode) {
      gvdb::utils::Logger::Instance().Info("Consensus node started (single-node mode)");
    } else {
      gvdb::utils::Logger::Instance().Info("Consensus node started (multi-node mode)");
      gvdb::utils::Logger::Instance().Info("Waiting for leader election...");

      // Wait for leader election (timeout after 10 seconds)
      auto start_time = std::chrono::steady_clock::now();
      while (!raft_node->IsLeader() && raft_node->GetLeaderId() < 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        auto elapsed = std::chrono::steady_clock::now() - start_time;
        if (elapsed > std::chrono::seconds(10)) {
          std::cerr << "Leader election timeout. Check network connectivity to peers." << std::endl;
          return 1;
        }
      }

      if (raft_node->IsLeader()) {
        gvdb::utils::Logger::Instance().Info("This node is the leader");
      } else {
        gvdb::utils::Logger::Instance().Info("Leader elected: node_id={}",
                                             raft_node->GetLeaderId());
      }
    }

    // 2. Create cluster coordinator
    auto shard_manager = std::make_shared<gvdb::cluster::ShardManager>(
        16, gvdb::cluster::ShardingStrategy::HASH);
    auto coordinator = std::make_unique<gvdb::cluster::Coordinator>(shard_manager);
    gvdb::utils::Logger::Instance().Info("ShardManager initialized with 16 shards");
    gvdb::utils::Logger::Instance().Info("Coordinator initialized");

    // 3. Create NodeRegistry for cluster-wide node tracking
    auto node_registry = std::make_shared<gvdb::cluster::NodeRegistry>(
        std::chrono::seconds(30));  // 30s heartbeat timeout
    gvdb::utils::Logger::Instance().Info("NodeRegistry initialized (timeout=30s)");

    // 4. Get TimestampOracle from RaftNode (wrap in non-owning shared_ptr)
    auto* timestamp_oracle_ptr = raft_node->GetTimestampOracle();
    std::shared_ptr<gvdb::consensus::TimestampOracle> timestamp_oracle;
    if (timestamp_oracle_ptr) {
      // Wrap in shared_ptr with no-op deleter (RaftNode owns the oracle)
      timestamp_oracle = std::shared_ptr<gvdb::consensus::TimestampOracle>(
          timestamp_oracle_ptr, [](gvdb::consensus::TimestampOracle*){});
      gvdb::utils::Logger::Instance().Info("TimestampOracle available from Raft");
    } else {
      gvdb::utils::Logger::Instance().Info("TimestampOracle not available (fallback mode)");
    }

    // 5. Create storage and compute layers for coordinator
    // Note: Coordinators don't store data, but need these for InternalService
    auto index_factory = std::make_unique<gvdb::index::IndexFactory>();
    auto segment_manager = std::make_shared<gvdb::storage::SegmentManager>(
        args.data_dir + "/segments", index_factory.get());
    auto query_executor = std::make_shared<gvdb::compute::QueryExecutor>(
        segment_manager.get());
    gvdb::utils::Logger::Instance().Info("Storage/Compute stubs created for InternalService");

    // 6. Create InternalService for node-to-node communication
    auto internal_service = std::make_unique<gvdb::network::InternalService>(
        shard_manager, segment_manager, query_executor, node_registry, timestamp_oracle);
    gvdb::utils::Logger::Instance().Info("InternalService created");

    // 7. Start gRPC server for internal communication
    grpc::ServerBuilder builder;
    builder.AddListeningPort(args.bind_address, grpc::InsecureServerCredentials());
    builder.RegisterService(internal_service.get());

    // Set message size limits (256 MB for segment transfers)
    builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
    builder.SetMaxSendMessageSize(256 * 1024 * 1024);

    auto grpc_server = builder.BuildAndStart();
    if (!grpc_server) {
      std::cerr << "Failed to start gRPC server on " << args.bind_address << std::endl;
      return 1;
    }
    gvdb::utils::Logger::Instance().Info("InternalService gRPC server started on {}", args.bind_address);

    std::cout << "\n========================================" << std::endl;
    std::cout << "GVDB Coordinator Node Started" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Node ID: " << args.node_id << std::endl;
    std::cout << "Mode: " << (args.single_node_mode ? "Single-Node" : "Multi-Node") << std::endl;
    std::cout << "InternalService gRPC: " << args.bind_address << std::endl;
    std::cout << "Raft Address: " << args.raft_address << std::endl;
    std::cout << "Metrics: http://0.0.0.0:" << metrics_port << "/metrics" << std::endl;
    std::cout << "Data Directory: " << args.data_dir << std::endl;
    std::cout << "NodeRegistry: Active (30s timeout)" << std::endl;

    if (raft_node->IsLeader()) {
      std::cout << "Role: LEADER" << std::endl;
    } else if (!args.single_node_mode) {
      std::cout << "Role: FOLLOWER (leader: node_id=" << raft_node->GetLeaderId() << ")" << std::endl;
    }

    std::cout << "\nPress Ctrl+C to shutdown..." << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Wait for shutdown signal
    while (!g_shutdown.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      // In multi-node mode, periodically check leader status
      if (!args.single_node_mode) {
        static bool was_leader = raft_node->IsLeader();
        bool is_leader = raft_node->IsLeader();

        if (was_leader != is_leader) {
          if (is_leader) {
            gvdb::utils::Logger::Instance().Info("This node became the leader");
            std::cout << ">>> This node became the LEADER <<<" << std::endl;
          } else {
            gvdb::utils::Logger::Instance().Info("This node lost leadership");
            std::cout << ">>> This node lost leadership <<<" << std::endl;
          }
          was_leader = is_leader;
        }
      }
    }

    // Graceful shutdown
    std::cout << "\nShutting down gracefully..." << std::endl;
    gvdb::utils::Logger::Instance().Info("Shutting down...");

    grpc_server->Shutdown();
    gvdb::utils::Logger::Instance().Info("gRPC server stopped");

    gvdb::utils::MetricsRegistry::Instance().StopMetricsServer();
    gvdb::utils::Logger::Instance().Info("Metrics server stopped");

    auto shutdown_status = raft_node->Shutdown();
    if (!shutdown_status.ok()) {
      gvdb::utils::Logger::Instance().Warn("Raft shutdown returned: {}",
                                           shutdown_status.message());
    }
    gvdb::utils::Logger::Instance().Info("Consensus node stopped");

    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    gvdb::utils::Logger::Instance().Error("Fatal error: {}", e.what());
    return 1;
  }
}
