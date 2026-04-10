// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "auth/rbac.h"
#include "utils/config.h"
#include "utils/logger.h"

#include "absl/strings/str_cat.h"

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

    default:
      return false;
  }
}

bool HasCollectionAccess(const ApiKeyRole& key_role,
                         const std::string& collection) {
  if (key_role.role == Role::ADMIN) return true;
  if (collection.empty()) return true;

  for (const auto& c : key_role.collections) {
    if (c == "*" || c == collection) return true;
  }
  return false;
}

absl::StatusOr<Role> RoleFromString(const std::string& role_str) {
  if (role_str == "admin") return Role::ADMIN;
  if (role_str == "readwrite") return Role::READWRITE;
  if (role_str == "readonly") return Role::READONLY;
  if (role_str == "collection_admin") return Role::COLLECTION_ADMIN;
  return absl::InvalidArgumentError(
      absl::StrCat("Unknown role: '", role_str,
                    "'. Must be: admin, readwrite, readonly, collection_admin"));
}

absl::StatusOr<std::shared_ptr<RbacStore>> RbacStore::Create(
    const utils::AuthConfig& config) {
  auto store = std::shared_ptr<RbacStore>(new RbacStore());

  // Role-based keys
  for (const auto& r : config.roles) {
    if (r.key.empty()) {
      return absl::InvalidArgumentError("Auth role entry has empty key");
    }

    auto role_result = RoleFromString(r.role);
    if (!role_result.ok()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Auth key '", r.key, "': ", role_result.status().message()));
    }

    if (*role_result != Role::ADMIN && r.collections.empty()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Auth key '", r.key, "' with role '", r.role,
                        "' must specify collections (use [\"*\"] for all)"));
    }

    if (store->keys_.contains(r.key)) {
      utils::Logger::Instance().Warn(
          "Duplicate auth key '{}' in config — later entry wins", r.key);
    }

    ApiKeyRole entry;
    entry.key = r.key;
    entry.role = *role_result;
    entry.collections = r.collections;
    store->keys_[r.key] = std::move(entry);
  }

  // Legacy flat api_keys treated as admin (backward compat)
  for (const auto& key : config.api_keys) {
    if (key.empty()) {
      return absl::InvalidArgumentError("Auth api_keys contains empty key");
    }
    if (store->keys_.contains(key)) continue;  // role-based takes precedence
    ApiKeyRole entry;
    entry.key = key;
    entry.role = Role::ADMIN;
    store->keys_[key] = std::move(entry);
  }

  if (store->keys_.empty()) {
    return absl::InvalidArgumentError(
        "Auth is enabled but no API keys configured (add api_keys or roles)");
  }

  return store;
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
