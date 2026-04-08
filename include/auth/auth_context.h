// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <string>

namespace gvdb {
namespace auth {

// Thread-local storage for the authenticated API key.
// Set by the auth interceptor, read by service methods.
// Safe because gRPC sync server runs each unary/streaming RPC on a single thread.
class AuthContext {
 public:
  static void SetCurrentKey(const std::string& key);
  static const std::string& GetCurrentKey();
  static void Clear();
};

}  // namespace auth
}  // namespace gvdb
