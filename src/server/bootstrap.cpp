// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "server/bootstrap.h"

#include <signal.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <exception>
#include <fstream>
#include <iostream>
#include <iterator>
#include <thread>
#include <utility>

#include "absl/strings/str_cat.h"
#include "common/metrics.h"

namespace gvdb {
namespace server {

namespace {

constexpr int kMaxMessageBytes = 256 * 1024 * 1024;

std::atomic<bool> g_shutdown{false};

void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    std::cout << "\nReceived shutdown signal, gracefully shutting down..." << std::endl;
    g_shutdown.store(true);
  }
}

std::string ReadFile(const std::string& path) {
  std::ifstream file(path);
  if (!file.is_open()) return "";
  return std::string(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
}

}  // namespace

std::atomic<bool>& ShutdownFlag() { return g_shutdown; }

void InstallSignalHandlers() {
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);
}

common::LogLevel ParseLogLevel(const std::string& name) {
  std::string lower = name;
  std::transform(lower.begin(), lower.end(), lower.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  if (lower == "debug") return common::LogLevel::DEBUG;
  if (lower == "warn" || lower == "warning") return common::LogLevel::WARN;
  if (lower == "error") return common::LogLevel::ERROR;
  return common::LogLevel::INFO;
}

void StartLogging(const std::string& data_dir, const std::string& file_name,
                  common::LogLevel level) {
  common::LogConfig log_config;
  log_config.file_path = data_dir + "/logs/" + file_name;
  log_config.console_enabled = true;
  log_config.file_enabled = true;
  log_config.level = level;

  auto status = common::Logger::Initialize(log_config);
  if (!status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << status.message() << std::endl;
  }
}

void StartMetricsServer(int port) {
  common::MetricsRegistry::Instance().StartMetricsServer(port);
}

std::shared_ptr<grpc::ServerCredentials> MakeServerCredentials(const config::TlsConfig& tls) {
  if (tls.enabled && !tls.cert_path.empty() && !tls.key_path.empty()) {
    std::string cert = ReadFile(tls.cert_path);
    std::string key = ReadFile(tls.key_path);

    if (cert.empty() || key.empty()) {
      common::Logger::Instance().Error("TLS enabled but cert/key files could not be read");
      return grpc::InsecureServerCredentials();
    }

    grpc::SslServerCredentialsOptions ssl_opts;
    ssl_opts.pem_key_cert_pairs.push_back({key, cert});

    if (tls.mutual_tls && !tls.ca_cert_path.empty()) {
      ssl_opts.pem_root_certs = ReadFile(tls.ca_cert_path);
      ssl_opts.client_certificate_request =
          GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
    }

    common::Logger::Instance().Info("gRPC server using TLS (mutual_tls={})", tls.mutual_tls);
    return grpc::SslServerCredentials(ssl_opts);
  }

  return grpc::InsecureServerCredentials();
}

absl::StatusOr<std::unique_ptr<grpc::Server>> StartGrpcServer(
    const std::string& bind_address, const std::vector<grpc::Service*>& services,
    std::shared_ptr<grpc::ServerCredentials> credentials, InterceptorFactories interceptors) {
  grpc::ServerBuilder builder;
  builder.AddListeningPort(bind_address, std::move(credentials));
  for (auto* service : services) {
    builder.RegisterService(service);
  }
  builder.SetMaxReceiveMessageSize(kMaxMessageBytes);
  builder.SetMaxSendMessageSize(kMaxMessageBytes);
  if (!interceptors.empty()) {
    builder.experimental().SetInterceptorCreators(std::move(interceptors));
  }

  auto server = builder.BuildAndStart();
  if (!server) {
    return absl::UnavailableError(absl::StrCat("Failed to start gRPC server on ", bind_address));
  }
  return server;
}

void PrintBanner(const std::string& node_name, const std::vector<std::string>& info_lines) {
  std::cout << "\n========================================" << std::endl;
  std::cout << node_name << " Started" << std::endl;
  std::cout << "========================================" << std::endl;
  for (const auto& line : info_lines) {
    std::cout << line << std::endl;
  }
  std::cout << "\nPress Ctrl+C to shutdown..." << std::endl;
  std::cout << "========================================\n" << std::endl;
}

void WaitForShutdown(const std::function<void()>& every_second) {
  while (!g_shutdown.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (every_second) every_second();
  }
  std::cout << "\nShutting down gracefully..." << std::endl;
}

void StopServing(grpc::Server& server) {
  server.Shutdown();
  common::MetricsRegistry::Instance().StopMetricsServer();
}

absl::Status WithPrefix(std::string_view prefix, const absl::Status& status) {
  return absl::Status(status.code(), absl::StrCat(prefix, status.message()));
}

int RunMain(const std::function<absl::Status()>& body) {
  try {
    auto status = body();
    if (!status.ok()) {
      std::cerr << status.message() << std::endl;
      return 1;
    }
    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    return 1;
  }
}

}  // namespace server
}  // namespace gvdb
