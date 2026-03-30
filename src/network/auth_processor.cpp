// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/auth_processor.h"
#include "utils/logger.h"

namespace gvdb {
namespace network {

ApiKeyAuthInterceptor::ApiKeyAuthInterceptor(
    grpc::experimental::ServerRpcInfo* info,
    const std::unordered_set<std::string>* valid_keys)
    : info_(info), valid_keys_(valid_keys) {}

void ApiKeyAuthInterceptor::Intercept(
    grpc::experimental::InterceptorBatchMethods* methods) {

  if (!rejected_ && methods->QueryInterceptionHookPoint(
          grpc::experimental::InterceptionHookPoints::
              POST_RECV_INITIAL_METADATA)) {

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
        if (valid_keys_->find(key) == valid_keys_->end()) {
          should_reject = true;
          reject_status = grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
              "Invalid API key");
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
    const utils::AuthConfig& config)
    : valid_keys_(config.api_keys.begin(), config.api_keys.end()) {
  utils::Logger::Instance().Info("Auth interceptor factory initialized with {} API key(s)",
                                  valid_keys_.size());
}

grpc::experimental::Interceptor*
ApiKeyAuthInterceptorFactory::CreateServerInterceptor(
    grpc::experimental::ServerRpcInfo* info) {
  return new ApiKeyAuthInterceptor(info, &valid_keys_);
}

}  // namespace network
}  // namespace gvdb