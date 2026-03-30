// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "utils/config.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/server_interceptor.h>
#include <string>
#include <unordered_set>
#include <vector>

namespace gvdb {
namespace network {

// gRPC server interceptor that validates API keys from the
// "authorization: Bearer <key>" metadata header.
class ApiKeyAuthInterceptor : public grpc::experimental::Interceptor {
 public:
  ApiKeyAuthInterceptor(grpc::experimental::ServerRpcInfo* info,
                         const std::unordered_set<std::string>* valid_keys);

  void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override;

 private:
  grpc::experimental::ServerRpcInfo* info_;
  const std::unordered_set<std::string>* valid_keys_;
  bool rejected_ = false;
};

// Factory that creates ApiKeyAuthInterceptor for each RPC
class ApiKeyAuthInterceptorFactory
    : public grpc::experimental::ServerInterceptorFactoryInterface {
 public:
  explicit ApiKeyAuthInterceptorFactory(const utils::AuthConfig& config);

  grpc::experimental::Interceptor* CreateServerInterceptor(
      grpc::experimental::ServerRpcInfo* info) override;

 private:
  std::unordered_set<std::string> valid_keys_;
};

}  // namespace network
}  // namespace gvdb