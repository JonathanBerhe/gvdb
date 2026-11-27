// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/server_interceptor.h>

#include <chrono>

namespace gvdb {
namespace network {

// gRPC server interceptor that emits structured audit log entries.
// Captures timing at POST_RECV_INITIAL_METADATA and emits JSON at PRE_SEND_STATUS.
// Reads AuthContext (set by auth interceptor) and AuditContext (set by service handlers).
class AuditInterceptor : public grpc::experimental::Interceptor {
 public:
  explicit AuditInterceptor(grpc::experimental::ServerRpcInfo* info);

  void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override;

 private:
  grpc::experimental::ServerRpcInfo* info_;
  std::chrono::steady_clock::time_point start_time_;
  std::string api_key_;  // Captured at POST_RECV_INITIAL_METADATA before auth clears it
  bool is_public_ = false;
};

// Factory that creates AuditInterceptor for each RPC
class AuditInterceptorFactory
    : public grpc::experimental::ServerInterceptorFactoryInterface {
 public:
  grpc::experimental::Interceptor* CreateServerInterceptor(
      grpc::experimental::ServerRpcInfo* info) override;
};

}  // namespace network
}  // namespace gvdb
