// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

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
  std::vector<std::string> collections;  // ["*"] = all collections
};

// Check if a role has a specific permission.
bool HasPermission(Role role, Permission permission);

// Check if a key-role grants access to a specific collection.
// Returns true if role is ADMIN, collections contains "*", or contains the name.
bool HasCollectionAccess(const ApiKeyRole& key_role,
                         const std::string& collection);

// Parse a role string ("admin", "readwrite", "readonly", "collection_admin").
// Returns InvalidArgumentError for unrecognized strings.
absl::StatusOr<Role> RoleFromString(const std::string& role_str);

// Thread-safe store of API key to role mappings.
// Use Create() factory which validates the config.
class RbacStore {
 public:
  // Validate config and build the store. Returns error on:
  //   - empty key
  //   - unrecognized role string
  //   - non-admin role with empty collections (must specify ["*"] or explicit list)
  static absl::StatusOr<std::shared_ptr<RbacStore>> Create(
      const utils::AuthConfig& config);

  // Look up role info for an API key. Returns nullptr if not found.
  const ApiKeyRole* Lookup(const std::string& key) const;

  size_t Size() const;

 private:
  RbacStore() = default;
  absl::flat_hash_map<std::string, ApiKeyRole> keys_;
  mutable std::shared_mutex mutex_;
};

}  // namespace auth
}  // namespace gvdb
