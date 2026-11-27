// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/server_interceptor.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/logger.h"
#include "config/config.h"

// The startup and shutdown steps that every server program shares: signal
// handlers, logger, metrics endpoint, gRPC server and the exit code.

namespace gvdb::server {

using InterceptorFactories =
    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>>;

// The flag that SIGINT and SIGTERM set. Background loops stop when it is true.
std::atomic<bool>& ShutdownFlag();

// Makes SIGINT and SIGTERM set ShutdownFlag().
void InstallSignalHandlers();

// The log level for a level name from the configuration: "debug", "warn",
// "warning" or "error", in any case. Any other name gives INFO.
common::LogLevel ParseLogLevel(const std::string& name);

// Starts the logger. It writes to the console and to
// <data_dir>/logs/<file_name>. When the logger does not start, the function
// prints a warning to stderr and the program continues.
void StartLogging(const std::string& data_dir, const std::string& file_name,
                  common::LogLevel level = common::LogLevel::INFO);

// Starts the Prometheus endpoint on all interfaces. When the port is in
// use, the error goes to the log and the program continues.
void StartMetricsServer(int port);

// TLS credentials when the configuration turns TLS on. Insecure credentials
// when TLS is off, or when the certificate or the key cannot be read.
std::shared_ptr<grpc::ServerCredentials> MakeServerCredentials(const config::TlsConfig& tls);

// Starts a gRPC server with 256 MB message limits for large vector batches.
absl::StatusOr<std::unique_ptr<grpc::Server>> StartGrpcServer(
    const std::string& bind_address, const std::vector<grpc::Service*>& services,
    std::shared_ptr<grpc::ServerCredentials> credentials, InterceptorFactories interceptors = {});

// Prints the startup banner to stdout.
void PrintBanner(const std::string& node_name, const std::vector<std::string>& info_lines);

// Blocks until ShutdownFlag() is set. `every_second`, when set, runs once a
// second while the program waits. Prints the shutdown notice at the end.
void WaitForShutdown(const std::function<void()>& every_second = {});

// Stops the gRPC server, then the metrics endpoint.
void StopServing(grpc::Server& server);

// The same status with `prefix` before its message.
absl::Status WithPrefix(std::string_view prefix, const absl::Status& status);

// Runs the body of a server program and returns the exit code. When `body`
// returns OK, the function prints the goodbye line and returns 0. When
// `body` returns an error or throws, the function prints the error to
// stderr and returns 1.
int RunMain(const std::function<absl::Status()>& body);

}  // namespace gvdb::server
