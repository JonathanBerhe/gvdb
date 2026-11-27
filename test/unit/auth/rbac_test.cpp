// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <cstddef>
#include <iterator>

#include "auth/auth_context.h"
#include "auth/rbac.h"
#include "config/config.h"

namespace {

using namespace gvdb;

// ============================================================================
// Permission matrix
// ============================================================================

TEST_CASE("HasPermission - every role against every permission") {
  using auth::Permission;
  using auth::Role;

  struct Row {
    Permission permission;
    const char* name;
    bool admin;
    bool readwrite;
    bool readonly;
    bool collection_admin;
  };
  // One row per Permission value, in declaration order. Columns: admin,
  // readwrite, readonly, collection_admin.
  static constexpr Row kMatrix[] = {
      {Permission::CREATE_COLLECTION, "CREATE_COLLECTION", true, false, false, false},
      {Permission::DROP_COLLECTION, "DROP_COLLECTION", true, false, false, false},
      {Permission::LIST_COLLECTIONS, "LIST_COLLECTIONS", true, true, true, true},
      {Permission::INSERT, "INSERT", true, true, false, true},
      {Permission::STREAM_INSERT, "STREAM_INSERT", true, true, false, true},
      {Permission::SEARCH, "SEARCH", true, true, true, true},
      {Permission::RANGE_SEARCH, "RANGE_SEARCH", true, true, true, true},
      {Permission::HYBRID_SEARCH, "HYBRID_SEARCH", true, true, true, true},
      {Permission::GET, "GET", true, true, true, true},
      {Permission::DELETE, "DELETE", true, true, false, true},
      {Permission::UPDATE_METADATA, "UPDATE_METADATA", true, true, false, true},
      {Permission::UPSERT, "UPSERT", true, true, false, true},
      {Permission::LIST_VECTORS, "LIST_VECTORS", true, true, true, true},
      // BACKUP reads every segment and ships the bytes off the node. Roles
      // with write authority on a collection may back it up; readonly may
      // not, and reads the data through Search, Get or ListVectors instead.
      {Permission::BACKUP, "BACKUP", true, true, false, true},
      // RESTORE creates a collection or replaces one, so it is gated like
      // CREATE_COLLECTION and DROP_COLLECTION: admin only.
      {Permission::RESTORE, "RESTORE", true, false, false, false},
  };
  // RESTORE is the last Permission value.
  static_assert(std::size(kMatrix) == static_cast<size_t>(Permission::RESTORE) + 1,
                "every Permission value needs a row");

  for (size_t i = 0; i < std::size(kMatrix); ++i) {
    const Row& row = kMatrix[i];
    CAPTURE(row.name);
    CHECK_EQ(static_cast<size_t>(row.permission), i);
    CHECK_EQ(auth::HasPermission(Role::ADMIN, row.permission), row.admin);
    CHECK_EQ(auth::HasPermission(Role::READWRITE, row.permission), row.readwrite);
    CHECK_EQ(auth::HasPermission(Role::READONLY, row.permission), row.readonly);
    CHECK_EQ(auth::HasPermission(Role::COLLECTION_ADMIN, row.permission), row.collection_admin);
  }
}

// ============================================================================
// Collection access tests
// ============================================================================

TEST_CASE("HasCollectionAccess") {
  struct Row {
    const char* name;
    auth::Role role;
    std::vector<std::string> collections;
    std::string collection;
    bool allowed;
  };
  const std::vector<Row> rows = {
      {"admin always has access", auth::Role::ADMIN, {}, "anything", true},
      {"wildcard grants all", auth::Role::READWRITE, {"*"}, "products", true},
      {"wildcard grants all", auth::Role::READWRITE, {"*"}, "embeddings", true},
      {"specific collection", auth::Role::READWRITE, {"products"}, "products", true},
      {"specific collection", auth::Role::READWRITE, {"products"}, "embeddings", false},
      {"multiple collections", auth::Role::READWRITE, {"products", "embeddings"}, "products", true},
      {"multiple collections",
       auth::Role::READWRITE,
       {"products", "embeddings"},
       "embeddings",
       true},
      {"multiple collections", auth::Role::READWRITE, {"products", "embeddings"}, "other", false},
      {"empty collection name always passes", auth::Role::READWRITE, {"products"}, "", true},
  };
  for (const Row& row : rows) {
    CAPTURE(row.name);
    CAPTURE(row.collection);
    const auth::ApiKeyRole role{"key", row.role, row.collections};
    CHECK_EQ(auth::HasCollectionAccess(role, row.collection), row.allowed);
  }
}

// ============================================================================
// RoleFromString tests
// ============================================================================

TEST_CASE("RoleFromString - valid roles") {
  CHECK_EQ(*auth::RoleFromString("admin"), auth::Role::ADMIN);
  CHECK_EQ(*auth::RoleFromString("readwrite"), auth::Role::READWRITE);
  CHECK_EQ(*auth::RoleFromString("readonly"), auth::Role::READONLY);
  CHECK_EQ(*auth::RoleFromString("collection_admin"), auth::Role::COLLECTION_ADMIN);
}

TEST_CASE("RoleFromString - unknown role returns error") {
  auto result = auth::RoleFromString("unknown");
  CHECK_FALSE(result.ok());

  result = auth::RoleFromString("readwritee");
  CHECK_FALSE(result.ok());

  result = auth::RoleFromString("");
  CHECK_FALSE(result.ok());

  result = auth::RoleFromString("ADMIN");
  CHECK_FALSE(result.ok());  // case-sensitive
}

// ============================================================================
// RbacStore::Create tests: valid configs
// ============================================================================

TEST_CASE("RbacStore - lookup found") {
  auth::AuthConfig config;
  config.roles = {{"my-key", "readwrite", {"products"}}};
  auto store = auth::RbacStore::Create(config);
  REQUIRE(store.ok());

  auto* role = (*store)->Lookup("my-key");
  REQUIRE(role != nullptr);
  CHECK_EQ(role->role, auth::Role::READWRITE);
  CHECK_EQ(role->collections.size(), 1);
  CHECK_EQ(role->collections[0], "products");
}

TEST_CASE("RbacStore - lookup not found") {
  auth::AuthConfig config;
  config.roles = {{"my-key", "admin", {}}};
  auto store = auth::RbacStore::Create(config);
  REQUIRE(store.ok());

  CHECK((*store)->Lookup("nonexistent") == nullptr);
}

TEST_CASE("RbacStore - legacy api_keys treated as admin") {
  auth::AuthConfig config;
  config.api_keys = {"old-key-1", "old-key-2"};
  auto store = auth::RbacStore::Create(config);
  REQUIRE(store.ok());

  auto* role1 = (*store)->Lookup("old-key-1");
  REQUIRE(role1 != nullptr);
  CHECK_EQ(role1->role, auth::Role::ADMIN);

  auto* role2 = (*store)->Lookup("old-key-2");
  REQUIRE(role2 != nullptr);
  CHECK_EQ(role2->role, auth::Role::ADMIN);
}

TEST_CASE("RbacStore - role-based takes precedence over legacy") {
  auth::AuthConfig config;
  config.api_keys = {"shared-key"};
  config.roles = {{"shared-key", "readonly", {"*"}}};
  auto store = auth::RbacStore::Create(config);
  REQUIRE(store.ok());

  auto* role = (*store)->Lookup("shared-key");
  REQUIRE(role != nullptr);
  CHECK_EQ(role->role, auth::Role::READONLY);
}

TEST_CASE("RbacStore - size") {
  auth::AuthConfig config;
  config.api_keys = {"key1"};
  config.roles = {{"key2", "readwrite", {"*"}}, {"key3", "readonly", {"*"}}};
  auto store = auth::RbacStore::Create(config);
  REQUIRE(store.ok());
  CHECK_EQ((*store)->Size(), 3);
}

// ============================================================================
// RbacStore::Create: invalid configs (validation)
// ============================================================================

TEST_CASE("RbacStore::Create validates the auth configuration") {
  struct Row {
    const char* name;
    std::vector<std::string> api_keys;
    std::vector<auth::ApiKeyConfig> roles;
    // A substring of the error message, or empty when the configuration is valid.
    std::string error;
  };
  const std::vector<Row> rows = {
      {"unknown role string", {}, {{"key1", "readwritee", {"*"}}}, "Unknown role"},
      {"empty key", {}, {{"", "admin", {}}}, "empty key"},
      {"empty legacy key", {""}, {}, "empty key"},
      {"readwrite with empty collections",
       {},
       {{"key1", "readwrite", {}}},
       "must specify collections"},
      {"readonly with empty collections",
       {},
       {{"key1", "readonly", {}}},
       "must specify collections"},
      {"collection_admin with empty collections",
       {},
       {{"key1", "collection_admin", {}}},
       "must specify collections"},
      {"admin with empty collections", {}, {{"key1", "admin", {}}}, ""},
      {"enabled auth with no keys", {}, {}, "no API keys configured"},
  };
  for (const Row& row : rows) {
    CAPTURE(row.name);
    auth::AuthConfig config;
    config.api_keys = row.api_keys;
    config.roles = row.roles;
    auto result = auth::RbacStore::Create(config);
    if (row.error.empty()) {
      CHECK(result.ok());
    } else {
      REQUIRE_FALSE(result.ok());
      CHECK(result.status().message().find(row.error) != std::string::npos);
    }
  }
}

// ============================================================================
// AuthContext tests
// ============================================================================

TEST_CASE("AuthContext - set and get") {
  auth::AuthContext::SetCurrentKey("test-key");
  CHECK_EQ(auth::AuthContext::GetCurrentKey(), "test-key");
  auth::AuthContext::Clear();
  CHECK(auth::AuthContext::GetCurrentKey().empty());
}

}  // namespace
