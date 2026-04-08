// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "auth/rbac.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/server_interceptor.h>
#include <memory>
#include <string>

namespace gvdb {
namespace network {

// gRPC server interceptor that validates API keys from the
// "authorization: Bearer <key>" metadata header and sets thread-local auth context.
// Skips auth for HealthCheck and GetStats RPCs.
class ApiKeyAuthInterceptor : public grpc::experimental::Interceptor {
 public:
  ApiKeyAuthInterceptor(grpc::experimental::ServerRpcInfo* info,
                         const auth::RbacStore* rbac_store);

  void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override;

 private:
  grpc::experimental::ServerRpcInfo* info_;
  const auth::RbacStore* rbac_store_;
  bool rejected_ = false;
};

// Factory that creates ApiKeyAuthInterceptor for each RPC
class ApiKeyAuthInterceptorFactory
    : public grpc::experimental::ServerInterceptorFactoryInterface {
 public:
  explicit ApiKeyAuthInterceptorFactory(std::shared_ptr<auth::RbacStore> rbac_store);

  grpc::experimental::Interceptor* CreateServerInterceptor(
      grpc::experimental::ServerRpcInfo* info) override;

 private:
  std::shared_ptr<auth::RbacStore> rbac_store_;
};

}  // namespace network
}  // namespace gvdb
