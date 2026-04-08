// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <shared_mutex>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"

namespace gvdb {

namespace utils { struct AuthConfig; }

namespace auth {

enum class Role {
  ADMIN,
  READWRITE,
  READONLY,
  COLLECTION_ADMIN
};

enum class Permission {
  CREATE_COLLECTION,
  DROP_COLLECTION,
  LIST_COLLECTIONS,
  INSERT,
  STREAM_INSERT,
  SEARCH,
  RANGE_SEARCH,
  HYBRID_SEARCH,
  GET,
  DELETE,
  UPDATE_METADATA,
  UPSERT,
  LIST_VECTORS
};

struct ApiKeyRole {
  std::string key;
  Role role;
  std::vector<std::string> collections;  // empty or ["*"] = all
};

// Check if a role has a specific permission.
bool HasPermission(Role role, Permission permission);

// Check if a key-role grants access to a specific collection.
// Returns true if collections is empty, contains "*", or contains the name.
bool HasCollectionAccess(const ApiKeyRole& key_role,
                         const std::string& collection);

// Parse a role string ("admin", "readwrite", "readonly", "collection_admin").
// Returns ADMIN for unrecognized strings.
Role RoleFromString(const std::string& role_str);

// Thread-safe store of API key to role mappings.
// Constructed from AuthConfig at startup.
class RbacStore {
 public:
  explicit RbacStore(const utils::AuthConfig& config);

  // Look up role info for an API key. Returns nullptr if not found.
  const ApiKeyRole* Lookup(const std::string& key) const;

  size_t Size() const;

 private:
  absl::flat_hash_map<std::string, ApiKeyRole> keys_;
  mutable std::shared_mutex mutex_;
};

}  // namespace auth
}  // namespace gvdb
