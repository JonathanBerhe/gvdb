// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/audit_context.h"

namespace gvdb {
namespace network {

namespace {
thread_local std::string g_audit_collection;
thread_local int64_t g_audit_item_count = 0;
}  // namespace

void AuditContext::SetCollection(const std::string& name) {
  g_audit_collection = name;
}

const std::string& AuditContext::GetCollection() {
  return g_audit_collection;
}

void AuditContext::SetItemCount(int64_t count) {
  g_audit_item_count = count;
}

int64_t AuditContext::GetItemCount() {
  return g_audit_item_count;
}

void AuditContext::Clear() {
  g_audit_collection.clear();
  g_audit_item_count = 0;
}

}  // namespace network
}  // namespace gvdb
