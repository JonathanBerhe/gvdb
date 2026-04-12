// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <cstdint>
#include <string>

namespace gvdb {
namespace network {

// Thread-local storage for audit enrichment fields.
// Set by service handlers, read by the audit interceptor in PRE_SEND_STATUS.
// Safe because gRPC sync server runs each unary/streaming RPC on a single thread.
class AuditContext {
 public:
  static void SetCollection(const std::string& name);
  static const std::string& GetCollection();
  static void SetItemCount(int64_t count);
  static int64_t GetItemCount();
  static void Clear();
};

}  // namespace network
}  // namespace gvdb
