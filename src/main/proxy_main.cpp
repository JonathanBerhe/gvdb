// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// gvdb-proxy: the client entry point of a cluster. It checks API keys and
// forwards each call to the coordinators, the data nodes or the query nodes.

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/logger.h"
#include "config/config.h"
#include "core/status.h"
#include "server/bootstrap.h"
#include "server/builders.h"
#include "server/flags.h"
#include "service/proxy/proxy_service.h"

namespace gvdb {
namespace {

struct Args {
  int node_id = 1;
  std::string bind_address = "0.0.0.0:50050";
  std::string data_dir = "/tmp/gvdb/proxy";
  std::string config_file;
  std::vector<std::string> coordinator_addresses;
  // One URI for all query nodes. In Kubernetes, a dns:/// URI of the
  // headless service makes gRPC balance across every pod and pick up new
  // pods without a proxy restart. A bare host:port also works.
  std::string query_node_uri;
};

absl::Status Run(const Args& args) {
  server::InstallSignalHandlers();
  server::StartLogging(args.data_dir, "proxy.log");
  common::Logger::Instance().Info("Starting GVDB Proxy (ID: {})", args.node_id);
  const int metrics_port = 9050;
  server::StartMetricsServer(metrics_port);

  const config::GVDBConfig config = server::LoadConfigOrDefault(args.config_file);
  server::InterceptorFactories interceptors;
  if (auto rbac_store = server::AddAuthInterceptor(config.server.auth, interceptors);
      !rbac_store.ok()) {
    return rbac_store.status();
  }
  server::AddAuditInterceptor(config.logging.audit, interceptors);

  auto proxy_service =
      std::make_unique<service::ProxyService>(args.coordinator_addresses, args.query_node_uri);
  ASSIGN_OR_RETURN(auto grpc_server,
                   server::StartGrpcServer(args.bind_address, {proxy_service.get()}, server::MakeServerCredentials(config.server.tls), std::move(interceptors)));

  std::vector<std::string> banner_lines = {
      "Node ID: " + std::to_string(args.node_id),
      "gRPC Service: " + args.bind_address,
      "Metrics: http://0.0.0.0:" + std::to_string(metrics_port) + "/metrics",
      "Coordinators: " + std::to_string(args.coordinator_addresses.size()) + " node(s)",
  };
  if (!args.query_node_uri.empty()) {
    banner_lines.push_back("Query Nodes URI: " + args.query_node_uri);
  }
  server::PrintBanner("GVDB Proxy", banner_lines);

  server::WaitForShutdown();
  server::StopServing(*grpc_server);
  return absl::OkStatus();
}

}  // namespace
}  // namespace gvdb

int main(int argc, char** argv) {
  gvdb::Args args;
  gvdb::server::FlagSet flags;
  flags.AddInt("--node-id", "ID", "Proxy node ID (default: 1)", &args.node_id);
  flags.AddString("--bind-address", "ADDR", "gRPC bind address (default: 0.0.0.0:50050)",
                  &args.bind_address, "GVDB_BIND_ADDRESS");
  flags.AddString("--data-dir", "PATH", "Data directory (default: /tmp/gvdb/proxy)",
                  &args.data_dir, "GVDB_DATA_DIR");
  flags.AddString("--config", "FILE", "YAML config file (optional, for auth)", &args.config_file);
  flags.AddList("--coordinators", "ADDRS", "Coordinator addresses (comma-separated)",
                &args.coordinator_addresses);
  // One URI, not a list: a dns:/// URI has no commas, and gRPC round robin
  // on the DNS resolver balances the calls.
  flags.AddString("--query-nodes", "URI", "Query node target URI (dns:///svc:port or host:port)",
                  &args.query_node_uri);
  if (auto exit_code = flags.Parse(argc, argv)) return *exit_code;
  if (args.coordinator_addresses.empty()) {
    std::cerr << "Error: At least one coordinator address must be specified" << std::endl;
    return 1;
  }
  return gvdb::server::RunMain([&] { return gvdb::Run(args); });
}
