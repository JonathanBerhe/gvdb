// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "auth/auth_context.h"

namespace gvdb {
namespace auth {

namespace {
thread_local std::string g_current_key;
}  // namespace

void AuthContext::SetCurrentKey(const std::string& key) {
  g_current_key = key;
}

const std::string& AuthContext::GetCurrentKey() {
  return g_current_key;
}

void AuthContext::Clear() {
  g_current_key.clear();
}

}  // namespace auth
}  // namespace gvdb
