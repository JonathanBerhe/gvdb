// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "utils/server_bootstrap.h"

#include <signal.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

namespace gvdb {
namespace utils {

namespace {
std::atomic<bool> g_shutdown{false};

void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    std::cout << "\nReceived shutdown signal, gracefully shutting down..." << std::endl;
    g_shutdown.store(true);
  }
}
}  // namespace

std::atomic<bool>& ServerBootstrap::ShutdownFlag() {
  return g_shutdown;
}

void ServerBootstrap::InstallSignalHandlers() {
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);
}

absl::Status ServerBootstrap::InitializeLogger(const std::string& data_dir,
                                                const std::string& log_file_name,
                                                LogLevel level) {
  LogConfig log_config;
  log_config.file_path = data_dir + "/logs/" + log_file_name;
  log_config.console_enabled = true;
  log_config.file_enabled = true;
  log_config.level = level;

  return Logger::Initialize(log_config);
}

bool ServerBootstrap::StartMetricsServer(int port) {
  return MetricsRegistry::Instance().StartMetricsServer(port);
}

std::unique_ptr<grpc::Server> ServerBootstrap::StartGrpcServer(
    const std::string& bind_address,
    const std::vector<grpc::Service*>& services) {

  grpc::ServerBuilder builder;
  builder.AddListeningPort(bind_address, grpc::InsecureServerCredentials());

  for (auto* service : services) {
    builder.RegisterService(service);
  }

  // 256 MB message limits for large vector batches
  builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
  builder.SetMaxSendMessageSize(256 * 1024 * 1024);

  return builder.BuildAndStart();
}

void ServerBootstrap::WaitForShutdown() {
  while (!g_shutdown.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

void ServerBootstrap::PrintBanner(const std::string& node_name,
                                   const std::vector<std::string>& info_lines) {
  std::cout << "\n========================================" << std::endl;
  std::cout << node_name << " Started" << std::endl;
  std::cout << "========================================" << std::endl;

  for (const auto& line : info_lines) {
    std::cout << line << std::endl;
  }

  std::cout << "\nPress Ctrl+C to shutdown..." << std::endl;
  std::cout << "========================================\n" << std::endl;
}

void ServerBootstrap::StopMetricsServer() {
  MetricsRegistry::Instance().StopMetricsServer();
}

namespace {
std::string ReadFile(const std::string& path) {
  std::ifstream file(path);
  if (!file.is_open()) return "";
  return std::string(std::istreambuf_iterator<char>(file),
                     std::istreambuf_iterator<char>());
}
}  // namespace

std::shared_ptr<grpc::ServerCredentials> ServerBootstrap::MakeServerCredentials(
    const TlsConfig& tls_config) {

  if (tls_config.enabled && !tls_config.cert_path.empty() &&
      !tls_config.key_path.empty()) {
    std::string cert = ReadFile(tls_config.cert_path);
    std::string key = ReadFile(tls_config.key_path);

    if (cert.empty() || key.empty()) {
      Logger::Instance().Error("TLS enabled but cert/key files could not be read");
      return grpc::InsecureServerCredentials();
    }

    grpc::SslServerCredentialsOptions ssl_opts;
    ssl_opts.pem_key_cert_pairs.push_back({key, cert});

    if (tls_config.mutual_tls && !tls_config.ca_cert_path.empty()) {
      ssl_opts.pem_root_certs = ReadFile(tls_config.ca_cert_path);
      ssl_opts.client_certificate_request =
          GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
    }

    Logger::Instance().Info("gRPC server using TLS (mutual_tls={})", tls_config.mutual_tls);
    return grpc::SslServerCredentials(ssl_opts);
  }

  return grpc::InsecureServerCredentials();
}

std::unique_ptr<grpc::Server> ServerBootstrap::StartGrpcServer(
    const std::string& bind_address,
    const std::vector<grpc::Service*>& services,
    const TlsConfig& tls_config) {

  return StartGrpcServer(bind_address, services, MakeServerCredentials(tls_config));
}

std::unique_ptr<grpc::Server> ServerBootstrap::StartGrpcServer(
    const std::string& bind_address,
    const std::vector<grpc::Service*>& services,
    std::shared_ptr<grpc::ServerCredentials> credentials) {

  grpc::ServerBuilder builder;
  builder.AddListeningPort(bind_address, credentials);

  for (auto* service : services) {
    builder.RegisterService(service);
  }

  builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
  builder.SetMaxSendMessageSize(256 * 1024 * 1024);

  return builder.BuildAndStart();
}

std::unique_ptr<grpc::Server> ServerBootstrap::StartGrpcServer(
    const std::string& bind_address,
    const std::vector<grpc::Service*>& services,
    std::shared_ptr<grpc::ServerCredentials> credentials,
    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> interceptors) {

  grpc::ServerBuilder builder;
  builder.AddListeningPort(bind_address, credentials);

  for (auto* service : services) {
    builder.RegisterService(service);
  }

  builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
  builder.SetMaxSendMessageSize(256 * 1024 * 1024);

  if (!interceptors.empty()) {
    builder.experimental().SetInterceptorCreators(std::move(interceptors));
  }

  return builder.BuildAndStart();
}

std::shared_ptr<grpc::ChannelCredentials> ServerBootstrap::MakeChannelCredentials(
    const TlsConfig& tls_config) {

  if (!tls_config.enabled) {
    return grpc::InsecureChannelCredentials();
  }

  grpc::SslCredentialsOptions ssl_opts;

  if (!tls_config.ca_cert_path.empty()) {
    ssl_opts.pem_root_certs = ReadFile(tls_config.ca_cert_path);
  }

  if (tls_config.mutual_tls) {
    if (!tls_config.cert_path.empty()) {
      ssl_opts.pem_cert_chain = ReadFile(tls_config.cert_path);
    }
    if (!tls_config.key_path.empty()) {
      ssl_opts.pem_private_key = ReadFile(tls_config.key_path);
    }
  }

  return grpc::SslCredentials(ssl_opts);
}

}  // namespace utils
}  // namespace gvdb