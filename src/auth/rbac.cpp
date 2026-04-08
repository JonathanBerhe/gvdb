// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "auth/rbac.h"
#include "utils/config.h"

#include <algorithm>

namespace gvdb {
namespace auth {

bool HasPermission(Role role, Permission permission) {
  switch (role) {
    case Role::ADMIN:
      return true;

    case Role::READWRITE:
      switch (permission) {
        case Permission::CREATE_COLLECTION:
        case Permission::DROP_COLLECTION:
          return false;
        default:
          return true;
      }

    case Role::READONLY:
      switch (permission) {
        case Permission::SEARCH:
        case Permission::RANGE_SEARCH:
        case Permission::HYBRID_SEARCH:
        case Permission::GET:
        case Permission::LIST_COLLECTIONS:
        case Permission::LIST_VECTORS:
          return true;
        default:
          return false;
      }

    case Role::COLLECTION_ADMIN:
      switch (permission) {
        case Permission::CREATE_COLLECTION:
        case Permission::DROP_COLLECTION:
          return false;
        default:
          return true;
      }
  }
  return false;
}

bool HasCollectionAccess(const ApiKeyRole& key_role,
                         const std::string& collection) {
  if (key_role.role == Role::ADMIN) return true;
  if (collection.empty()) return true;
  if (key_role.collections.empty()) return true;

  for (const auto& c : key_role.collections) {
    if (c == "*" || c == collection) return true;
  }
  return false;
}

Role RoleFromString(const std::string& role_str) {
  if (role_str == "admin") return Role::ADMIN;
  if (role_str == "readwrite") return Role::READWRITE;
  if (role_str == "readonly") return Role::READONLY;
  if (role_str == "collection_admin") return Role::COLLECTION_ADMIN;
  return Role::ADMIN;  // safe default for unrecognized
}

RbacStore::RbacStore(const utils::AuthConfig& config) {
  // New role-based keys
  for (const auto& r : config.roles) {
    ApiKeyRole entry;
    entry.key = r.key;
    entry.role = RoleFromString(r.role);
    entry.collections = r.collections;
    keys_[r.key] = std::move(entry);
  }

  // Legacy flat api_keys treated as admin (backward compat)
  for (const auto& key : config.api_keys) {
    if (keys_.find(key) != keys_.end()) continue;  // role-based takes precedence
    ApiKeyRole entry;
    entry.key = key;
    entry.role = Role::ADMIN;
    keys_[key] = std::move(entry);
  }
}

const ApiKeyRole* RbacStore::Lookup(const std::string& key) const {
  std::shared_lock lock(mutex_);
  auto it = keys_.find(key);
  if (it == keys_.end()) return nullptr;
  return &it->second;
}

size_t RbacStore::Size() const {
  std::shared_lock lock(mutex_);
  return keys_.size();
}

}  // namespace auth
}  // namespace gvdb
