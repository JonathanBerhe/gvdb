// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// gvdb-query-node: serves reads from a segment cache in its data folder.

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cluster/query_node.h"
#include "common/logger.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "internal.pb.h"
#include "core/status.h"
#include "server/bootstrap.h"
#include "server/cluster_steps.h"
#include "server/flags.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/vectordb_service.h"
#include "storage/segment/segment_manager.h"

namespace gvdb {
namespace {

struct Args {
  int node_id = 201;
  std::string bind_address = "0.0.0.0:50070";
  std::string advertise_address;
  std::string data_dir = "/tmp/gvdb/query_node";
  std::vector<std::string> coordinator_addresses;
  size_t memory_limit_gb = 16;
};

absl::Status Run(const Args& args) {
  server::InstallSignalHandlers();
  server::StartLogging(args.data_dir, "query_node.log");
  common::Logger::Instance().Info("Starting GVDB Query Node (ID: {})", args.node_id);
  const int metrics_port = 9200 + (args.node_id - 200);
  server::StartMetricsServer(metrics_port);

  auto index_factory = std::make_unique<index::IndexFactory>();
  auto segment_manager =
      std::make_shared<storage::SegmentManager>(args.data_dir + "/cache", index_factory.get());
  auto query_executor = std::make_shared<compute::QueryExecutor>(segment_manager.get());
  // The service does not use the query node yet. Only it and the banner
  // read the memory limit.
  auto query_node = std::make_unique<cluster::QueryNode>(
      segment_manager, query_executor, args.memory_limit_gb * 1024ULL * 1024ULL * 1024ULL);

  auto resolver = args.coordinator_addresses.empty()
                      ? service::MakeLocalResolver(segment_manager)
                      : service::MakeCachedCoordinatorResolver(args.coordinator_addresses[0]);
  auto vectordb_service = std::make_unique<service::VectorDBService>(
      segment_manager, query_executor, std::move(resolver));

  ASSIGN_OR_RETURN(auto grpc_server,
                   server::StartGrpcServer(args.bind_address, {vectordb_service.get()}, grpc::InsecureServerCredentials()));

  server::PrintBanner(
      "GVDB Query Node",
      {
          "Node ID: " + std::to_string(args.node_id),
          "gRPC Service: " + args.bind_address,
          "Metrics: http://0.0.0.0:" + std::to_string(metrics_port) + "/metrics",
          "Cache Directory: " + args.data_dir,
          "Cache Memory Limit: " + std::to_string(args.memory_limit_gb) + " GB",
      });

  auto heartbeat =
      server::StartHeartbeat(args.coordinator_addresses, args.node_id, args.bind_address,
                             args.advertise_address, proto::internal::NodeType::NODE_TYPE_QUERY_NODE);
  server::WaitForShutdown();

  // Tell the coordinator that this node drains, so it stops routing new
  // work at once. DrainAndStop joins the heartbeat loop first, so no READY
  // heartbeat can overwrite the DRAINING one.
  if (heartbeat) heartbeat->DrainAndStop();
  heartbeat.reset();
  server::StopServing(*grpc_server);
  return absl::OkStatus();
}

}  // namespace
}  // namespace gvdb

int main(int argc, char** argv) {
  gvdb::Args args;
  gvdb::server::FlagSet flags;
  flags.AddInt("--node-id", "ID", "Query node ID (default: 201)", &args.node_id);
  flags.AddString("--bind-address", "ADDR", "gRPC bind address (default: 0.0.0.0:50070)",
                  &args.bind_address, "GVDB_BIND_ADDRESS");
  flags.AddString("--advertise-address", "ADDR",
                  "Address advertised to coordinator (default: bind-address)",
                  &args.advertise_address, "GVDB_ADVERTISE_ADDRESS");
  flags.AddString("--data-dir", "PATH", "Data directory for cache (default: /tmp/gvdb/query_node)",
                  &args.data_dir, "GVDB_DATA_DIR");
  flags.AddList("--coordinator", "ADDR", "Coordinator addresses (comma-separated)",
                &args.coordinator_addresses);
  flags.AddSize("--memory-limit-gb", "SIZE", "Memory limit for caching in GB (default: 16)",
                &args.memory_limit_gb);
  if (auto exit_code = flags.Parse(argc, argv)) return *exit_code;
  return gvdb::server::RunMain([&] { return gvdb::Run(args); });
}
