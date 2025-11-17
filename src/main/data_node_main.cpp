#include <signal.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "absl/strings/str_cat.h"
#include "cluster/data_node.h"
#include "storage/segment_manager.h"
#include "index/index_factory.h"
#include "network/vectordb_service.h"
#include "utils/logger.h"
#include "utils/metrics.h"
#include "internal.grpc.pb.h"

#include <grpcpp/grpcpp.h>

// Command-line arguments
struct DataNodeArgs {
  int node_id = 101;  // Data nodes start from 101
  std::string bind_address = "0.0.0.0:50060";
  std::string data_dir = "/tmp/gvdb/data_node";
  std::vector<std::string> coordinator_addresses;
  std::vector<int> assigned_shards;  // Shard IDs assigned to this node
  size_t memory_limit_gb = 8;  // Memory limit in GB
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
            << "  --node-id ID                Data node ID (default: 101)\n"
            << "  --bind-address ADDR         gRPC bind address (default: 0.0.0.0:50060)\n"
            << "  --data-dir PATH             Data directory (default: /tmp/gvdb/data_node)\n"
            << "  --coordinator ADDR          Coordinator address (can be specified multiple times)\n"
            << "                              Example: coord1:50051,coord2:50051,coord3:50051\n"
            << "  --shards SHARD_IDS          Comma-separated shard IDs assigned to this node\n"
            << "                              Example: 1,2,3,4,5,6,7,8\n"
            << "  --memory-limit-gb SIZE      Memory limit in GB (default: 8)\n"
            << "  --help                      Show this help message\n"
            << "\nExamples:\n"
            << "  # Data node 1 handling shards 1-8\n"
            << "  " << program_name << " --node-id 101 --bind-address 0.0.0.0:50060 \\\n"
            << "    --coordinator coord1:50051,coord2:50051,coord3:50051 \\\n"
            << "    --shards 1,2,3,4,5,6,7,8 --memory-limit-gb 16\n"
            << "\n"
            << "  # Data node 2 handling shards 9-16\n"
            << "  " << program_name << " --node-id 102 --bind-address 0.0.0.0:50061 \\\n"
            << "    --coordinator coord1:50051 \\\n"
            << "    --shards 9,10,11,12,13,14,15,16\n";
}

std::vector<int> ParseShardIds(const std::string& shard_str) {
  std::vector<int> shards;
  size_t start = 0;
  size_t end = shard_str.find(',');

  while (end != std::string::npos) {
    shards.push_back(std::stoi(shard_str.substr(start, end - start)));
    start = end + 1;
    end = shard_str.find(',', start);
  }
  shards.push_back(std::stoi(shard_str.substr(start)));

  return shards;
}

// Send periodic heartbeats to coordinator
void HeartbeatSender(const std::string& coordinator_address,
                     int node_id,
                     const std::string& grpc_address,
                     std::atomic<bool>& shutdown_flag) {
  using namespace gvdb::proto::internal;

  // Create gRPC channel to coordinator
  auto channel = grpc::CreateChannel(coordinator_address, grpc::InsecureChannelCredentials());
  auto stub = InternalService::NewStub(channel);

  gvdb::utils::Logger::Instance().Info("Heartbeat sender started (coordinator={})", coordinator_address);

  while (!shutdown_flag.load()) {
    try {
      // Build heartbeat request
      HeartbeatRequest request;
      auto* node_info = request.mutable_node_info();
      node_info->set_node_id(node_id);
      node_info->set_node_type(NodeType::NODE_TYPE_DATA_NODE);
      node_info->set_status(NodeStatus::NODE_STATUS_READY);
      node_info->set_grpc_address(grpc_address);

      // Send heartbeat
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

      HeartbeatResponse response;
      auto status = stub->Heartbeat(&context, request, &response);

      if (status.ok() && response.acknowledged()) {
        gvdb::utils::Logger::Instance().Debug("Heartbeat acknowledged by coordinator");
      } else {
        gvdb::utils::Logger::Instance().Warn("Heartbeat failed: {}", status.error_message());
      }

    } catch (const std::exception& e) {
      gvdb::utils::Logger::Instance().Error("Heartbeat error: {}", e.what());
    }

    // Wait 10 seconds before next heartbeat (100ms intervals for fast shutdown)
    for (int i = 0; i < 100 && !shutdown_flag.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  gvdb::utils::Logger::Instance().Info("Heartbeat sender stopped");
}

bool ParseArgs(int argc, char** argv, DataNodeArgs& args) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      return false;
    } else if (arg == "--node-id" && i + 1 < argc) {
      args.node_id = std::stoi(argv[++i]);
    } else if (arg == "--bind-address" && i + 1 < argc) {
      args.bind_address = argv[++i];
    } else if (arg == "--data-dir" && i + 1 < argc) {
      args.data_dir = argv[++i];
    } else if (arg == "--coordinator" && i + 1 < argc) {
      std::string coords_str = argv[++i];
      size_t start = 0;
      size_t end = coords_str.find(',');
      while (end != std::string::npos) {
        args.coordinator_addresses.push_back(coords_str.substr(start, end - start));
        start = end + 1;
        end = coords_str.find(',', start);
      }
      args.coordinator_addresses.push_back(coords_str.substr(start));
    } else if (arg == "--shards" && i + 1 < argc) {
      args.assigned_shards = ParseShardIds(argv[++i]);
    } else if (arg == "--memory-limit-gb" && i + 1 < argc) {
      args.memory_limit_gb = std::stoull(argv[++i]);
    } else {
      std::cerr << "Unknown argument: " << arg << std::endl;
      PrintUsage(argv[0]);
      return false;
    }
  }

  return true;
}

int main(int argc, char** argv) {
  DataNodeArgs args;

  // Parse command-line arguments
  if (!ParseArgs(argc, argv, args)) {
    return 1;
  }

  // Setup signal handlers
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // Initialize logger
  gvdb::utils::LogConfig log_config;
  log_config.file_path = args.data_dir + "/logs/data_node.log";
  log_config.console_enabled = true;
  log_config.file_enabled = true;
  log_config.level = gvdb::utils::LogLevel::INFO;

  auto log_status = gvdb::utils::Logger::Initialize(log_config);
  if (!log_status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << log_status.message() << std::endl;
  }

  gvdb::utils::Logger::Instance().Info("Starting GVDB Data Node");
  gvdb::utils::Logger::Instance().Info("  Node ID: {}", args.node_id);
  gvdb::utils::Logger::Instance().Info("  gRPC Address: {}", args.bind_address);
  gvdb::utils::Logger::Instance().Info("  Data Directory: {}", args.data_dir);
  gvdb::utils::Logger::Instance().Info("  Memory Limit: {} GB", args.memory_limit_gb);

  if (!args.coordinator_addresses.empty()) {
    std::string coords_str;
    for (size_t i = 0; i < args.coordinator_addresses.size(); ++i) {
      if (i > 0) coords_str += ", ";
      coords_str += args.coordinator_addresses[i];
    }
    gvdb::utils::Logger::Instance().Info("  Coordinators: {}", coords_str);
  }

  if (!args.assigned_shards.empty()) {
    std::string shards_str;
    for (size_t i = 0; i < args.assigned_shards.size(); ++i) {
      if (i > 0) shards_str += ",";
      shards_str += std::to_string(args.assigned_shards[i]);
    }
    gvdb::utils::Logger::Instance().Info("  Assigned Shards: [{}]", shards_str);
  } else {
    gvdb::utils::Logger::Instance().Info("  Assigned Shards: None (will accept dynamic assignment)");
  }

  // Start metrics server
  int metrics_port = 9100 + (args.node_id - 100);  // 9101, 9102, etc.
  if (!gvdb::utils::MetricsRegistry::Instance().StartMetricsServer(metrics_port)) {
    std::cerr << "Warning: Failed to start metrics server on :" << metrics_port << std::endl;
  } else {
    gvdb::utils::Logger::Instance().Info("Metrics server started on :{}metrics", metrics_port);
  }

  try {
    // 1. Create storage layer
    auto index_factory = std::make_unique<gvdb::index::IndexFactory>();
    auto segment_manager = std::make_shared<gvdb::storage::SegmentManager>(
        args.data_dir + "/segments", index_factory.get());
    gvdb::utils::Logger::Instance().Info("Storage layer initialized");

    // 2. Create data node (for index building and compaction)
    auto data_node = std::make_unique<gvdb::cluster::DataNode>(std::move(index_factory));
    gvdb::utils::Logger::Instance().Info("DataNode initialized");

    // 3. Create compute layer
    auto query_executor = std::make_shared<gvdb::compute::QueryExecutor>(
        segment_manager.get());
    gvdb::utils::Logger::Instance().Info("Compute layer initialized");

    // 4. Create gRPC service (data nodes serve client requests directly)
    auto service = std::make_unique<gvdb::network::VectorDBService>(
        segment_manager, query_executor);
    gvdb::utils::Logger::Instance().Info("gRPC service created");

    // 5. Start gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(args.bind_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());

    // Set message size limits (256 MB)
    builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
    builder.SetMaxSendMessageSize(256 * 1024 * 1024);

    auto server = builder.BuildAndStart();
    if (!server) {
      std::cerr << "Failed to start gRPC server on " << args.bind_address << std::endl;
      return 1;
    }

    std::cout << "\n========================================" << std::endl;
    std::cout << "GVDB Data Node Started" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Node ID: " << args.node_id << std::endl;
    std::cout << "gRPC Service: " << args.bind_address << std::endl;
    std::cout << "Metrics: http://0.0.0.0:" << metrics_port << "/metrics" << std::endl;
    std::cout << "Data Directory: " << args.data_dir << std::endl;
    std::cout << "Memory Limit: " << args.memory_limit_gb << " GB" << std::endl;

    if (!args.assigned_shards.empty()) {
      std::cout << "Assigned Shards: [";
      for (size_t i = 0; i < args.assigned_shards.size(); ++i) {
        if (i > 0) std::cout << ",";
        std::cout << args.assigned_shards[i];
      }
      std::cout << "]" << std::endl;
    }

    std::cout << "\nPress Ctrl+C to shutdown..." << std::endl;
    std::cout << "========================================\n" << std::endl;

    gvdb::utils::Logger::Instance().Info("Server listening on {}", args.bind_address);

    // Start heartbeat sender if coordinator addresses provided
    std::unique_ptr<std::thread> heartbeat_thread;
    if (!args.coordinator_addresses.empty()) {
      std::string coordinator_addr = args.coordinator_addresses[0];  // Use first coordinator
      gvdb::utils::Logger::Instance().Info("Starting heartbeat to coordinator: {}", coordinator_addr);

      heartbeat_thread = std::make_unique<std::thread>(
          HeartbeatSender, coordinator_addr, args.node_id, args.bind_address, std::ref(g_shutdown));
    } else {
      gvdb::utils::Logger::Instance().Info("No coordinator specified - running standalone");
    }

    // Wait for shutdown signal
    while (!g_shutdown.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Graceful shutdown
    std::cout << "\nShutting down gracefully..." << std::endl;
    gvdb::utils::Logger::Instance().Info("Shutting down...");

    // Wait for heartbeat thread to finish
    if (heartbeat_thread && heartbeat_thread->joinable()) {
      heartbeat_thread->join();
      gvdb::utils::Logger::Instance().Info("Heartbeat thread stopped");
    }

    server->Shutdown();
    gvdb::utils::Logger::Instance().Info("gRPC server stopped");

    gvdb::utils::MetricsRegistry::Instance().StopMetricsServer();
    gvdb::utils::Logger::Instance().Info("Metrics server stopped");

    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    gvdb::utils::Logger::Instance().Error("Fatal error: {}", e.what());
    return 1;
  }
}
