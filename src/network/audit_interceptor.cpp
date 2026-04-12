// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/audit_interceptor.h"
#include "network/audit_context.h"
#include "utils/audit_logger.h"

#include <spdlog/fmt/fmt.h>

#include <chrono>
#include <string>

namespace gvdb {
namespace network {

namespace {

bool EndsWith(const std::string& str, const std::string& suffix) {
  if (suffix.size() > str.size()) return false;
  return str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool IsPublicMethod(const std::string& method) {
  return EndsWith(method, "/HealthCheck") || EndsWith(method, "/GetStats");
}

// Extract method name from full gRPC method string.
// "/gvdb.VectorDBService/Insert" → "Insert"
std::string ExtractMethodName(const std::string& full_method) {
  auto pos = full_method.rfind('/');
  if (pos != std::string::npos && pos + 1 < full_method.size()) {
    return full_method.substr(pos + 1);
  }
  return full_method;
}

// Map gRPC status code to human-readable string
const char* StatusCodeName(grpc::StatusCode code) {
  switch (code) {
    case grpc::StatusCode::OK:                  return "OK";
    case grpc::StatusCode::CANCELLED:           return "CANCELLED";
    case grpc::StatusCode::UNKNOWN:             return "UNKNOWN";
    case grpc::StatusCode::INVALID_ARGUMENT:    return "INVALID_ARGUMENT";
    case grpc::StatusCode::DEADLINE_EXCEEDED:   return "DEADLINE_EXCEEDED";
    case grpc::StatusCode::NOT_FOUND:           return "NOT_FOUND";
    case grpc::StatusCode::ALREADY_EXISTS:      return "ALREADY_EXISTS";
    case grpc::StatusCode::PERMISSION_DENIED:   return "PERMISSION_DENIED";
    case grpc::StatusCode::RESOURCE_EXHAUSTED:  return "RESOURCE_EXHAUSTED";
    case grpc::StatusCode::FAILED_PRECONDITION: return "FAILED_PRECONDITION";
    case grpc::StatusCode::ABORTED:             return "ABORTED";
    case grpc::StatusCode::OUT_OF_RANGE:        return "OUT_OF_RANGE";
    case grpc::StatusCode::UNIMPLEMENTED:       return "UNIMPLEMENTED";
    case grpc::StatusCode::INTERNAL:            return "INTERNAL";
    case grpc::StatusCode::UNAVAILABLE:         return "UNAVAILABLE";
    case grpc::StatusCode::DATA_LOSS:           return "DATA_LOSS";
    case grpc::StatusCode::UNAUTHENTICATED:     return "UNAUTHENTICATED";
    default:                                    return "UNKNOWN";
  }
}

// Escape a string for safe JSON embedding (handles " and \)
std::string JsonEscape(const std::string& s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    if (c == '"') {
      out += "\\\"";
    } else if (c == '\\') {
      out += "\\\\";
    } else {
      out += c;
    }
  }
  return out;
}

// Format current UTC time as ISO 8601
std::string NowISO8601() {
  auto now = std::chrono::system_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()) % 1000;
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  struct tm utc;
  gmtime_r(&time_t_now, &utc);

  return fmt::format("{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:03d}Z",
                     utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday,
                     utc.tm_hour, utc.tm_min, utc.tm_sec,
                     static_cast<int>(ms.count()));
}

}  // namespace

AuditInterceptor::AuditInterceptor(grpc::experimental::ServerRpcInfo* info)
    : info_(info) {
  std::string method(info_->method());
  is_public_ = IsPublicMethod(method);
}

void AuditInterceptor::Intercept(
    grpc::experimental::InterceptorBatchMethods* methods) {

  if (!is_public_ && methods->QueryInterceptionHookPoint(
          grpc::experimental::InterceptionHookPoints::
              POST_RECV_INITIAL_METADATA)) {
    start_time_ = std::chrono::steady_clock::now();
    // Extract API key directly from gRPC metadata — self-contained, no dependency
    // on AuthContext thread-local which is cleared by intermediate hook dispatches.
    auto* ctx = info_->server_context();
    const auto& metadata = ctx->client_metadata();
    auto it = metadata.find("authorization");
    if (it != metadata.end()) {
      std::string val(it->second.data(), it->second.length());
      if (val.size() > 7 && val.substr(0, 7) == "Bearer ") {
        api_key_ = val.substr(7);
      }
    }
    AuditContext::Clear();
  }

  if (!is_public_ && utils::AuditLogger::IsEnabled() &&
      methods->QueryInterceptionHookPoint(
          grpc::experimental::InterceptionHookPoints::PRE_SEND_STATUS)) {
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time_);

    auto send_status = methods->GetSendStatus();
    grpc::StatusCode code = send_status.error_code();

    std::string operation = ExtractMethodName(std::string(info_->method()));
    const auto& collection = AuditContext::GetCollection();
    int64_t item_count = AuditContext::GetItemCount();

    std::string json = fmt::format(
        R"({{"timestamp":"{}","api_key_id":"{}","operation":"{}","collection":"{}","status":"{}","grpc_code":{},"latency_ms":{},"item_count":{}}})",
        NowISO8601(),
        JsonEscape(api_key_),
        JsonEscape(operation),
        JsonEscape(collection),
        StatusCodeName(code),
        static_cast<int>(code),
        elapsed.count(),
        item_count);

    utils::AuditLogger::Log(json);
  }

  methods->Proceed();
}

grpc::experimental::Interceptor*
AuditInterceptorFactory::CreateServerInterceptor(
    grpc::experimental::ServerRpcInfo* info) {
  return new AuditInterceptor(info);
}

}  // namespace network
}  // namespace gvdb
