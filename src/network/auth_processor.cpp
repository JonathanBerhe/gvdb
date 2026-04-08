// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/auth_processor.h"
#include "auth/auth_context.h"
#include "utils/logger.h"

#include <string>

namespace gvdb {
namespace network {

namespace {
bool IsPublicMethod(const std::string& method) {
  // HealthCheck and GetStats are always allowed without auth
  return method.find("/HealthCheck") != std::string::npos ||
         method.find("/GetStats") != std::string::npos;
}
}  // namespace

ApiKeyAuthInterceptor::ApiKeyAuthInterceptor(
    grpc::experimental::ServerRpcInfo* info,
    const auth::RbacStore* rbac_store)
    : info_(info), rbac_store_(rbac_store) {}

void ApiKeyAuthInterceptor::Intercept(
    grpc::experimental::InterceptorBatchMethods* methods) {

  if (!rejected_ && methods->QueryInterceptionHookPoint(
          grpc::experimental::InterceptionHookPoints::
              POST_RECV_INITIAL_METADATA)) {

    // Skip auth for public endpoints
    std::string method(info_->method());
    if (IsPublicMethod(method)) {
      methods->Proceed();
      return;
    }

    auto* context = info_->server_context();
    const auto& metadata = context->client_metadata();

    grpc::Status reject_status;
    bool should_reject = false;

    auto it = metadata.find("authorization");
    if (it == metadata.end()) {
      should_reject = true;
      reject_status = grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
          "Missing authorization header. Use: authorization: Bearer <api-key>");
    } else {
      std::string auth_value(it->second.data(), it->second.length());
      const std::string prefix = "Bearer ";

      if (auth_value.length() <= prefix.length() ||
          auth_value.substr(0, prefix.length()) != prefix) {
        should_reject = true;
        reject_status = grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
            "Invalid authorization format. Expected: Bearer <api-key>");
      } else {
        std::string key = auth_value.substr(prefix.length());
        auto* role = rbac_store_->Lookup(key);
        if (!role) {
          should_reject = true;
          reject_status = grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
              "Invalid API key");
        } else {
          // Authentication passed — store key in thread-local for service to read
          auth::AuthContext::SetCurrentKey(key);
        }
      }
    }

    if (should_reject) {
      rejected_ = true;
      context->TryCancel();
    }
  }

  methods->Proceed();
}

ApiKeyAuthInterceptorFactory::ApiKeyAuthInterceptorFactory(
    std::shared_ptr<auth::RbacStore> rbac_store)
    : rbac_store_(std::move(rbac_store)) {
  utils::Logger::Instance().Info("Auth interceptor factory initialized with {} API key(s)",
                                  rbac_store_->Size());
}

grpc::experimental::Interceptor*
ApiKeyAuthInterceptorFactory::CreateServerInterceptor(
    grpc::experimental::ServerRpcInfo* info) {
  return new ApiKeyAuthInterceptor(info, rbac_store_.get());
}

}  // namespace network
}  // namespace gvdb
