#include <doctest/doctest.h>

#include "auth/rbac.h"
#include "auth/auth_context.h"
#include "utils/config.h"

using namespace gvdb;

// ============================================================================
// Permission matrix tests
// ============================================================================

TEST_CASE("HasPermission - admin has all permissions") {
  CHECK(auth::HasPermission(auth::Role::ADMIN, auth::Permission::CREATE_COLLECTION));
  CHECK(auth::HasPermission(auth::Role::ADMIN, auth::Permission::DROP_COLLECTION));
  CHECK(auth::HasPermission(auth::Role::ADMIN, auth::Permission::INSERT));
  CHECK(auth::HasPermission(auth::Role::ADMIN, auth::Permission::SEARCH));
  CHECK(auth::HasPermission(auth::Role::ADMIN, auth::Permission::DELETE));
  CHECK(auth::HasPermission(auth::Role::ADMIN, auth::Permission::LIST_COLLECTIONS));
}

TEST_CASE("HasPermission - readwrite can insert/search but not create/drop") {
  CHECK(auth::HasPermission(auth::Role::READWRITE, auth::Permission::INSERT));
  CHECK(auth::HasPermission(auth::Role::READWRITE, auth::Permission::SEARCH));
  CHECK(auth::HasPermission(auth::Role::READWRITE, auth::Permission::DELETE));
  CHECK(auth::HasPermission(auth::Role::READWRITE, auth::Permission::UPSERT));
  CHECK(auth::HasPermission(auth::Role::READWRITE, auth::Permission::LIST_COLLECTIONS));
  CHECK_FALSE(auth::HasPermission(auth::Role::READWRITE, auth::Permission::CREATE_COLLECTION));
  CHECK_FALSE(auth::HasPermission(auth::Role::READWRITE, auth::Permission::DROP_COLLECTION));
}

TEST_CASE("HasPermission - readonly can only read") {
  CHECK(auth::HasPermission(auth::Role::READONLY, auth::Permission::SEARCH));
  CHECK(auth::HasPermission(auth::Role::READONLY, auth::Permission::GET));
  CHECK(auth::HasPermission(auth::Role::READONLY, auth::Permission::RANGE_SEARCH));
  CHECK(auth::HasPermission(auth::Role::READONLY, auth::Permission::HYBRID_SEARCH));
  CHECK(auth::HasPermission(auth::Role::READONLY, auth::Permission::LIST_VECTORS));
  CHECK(auth::HasPermission(auth::Role::READONLY, auth::Permission::LIST_COLLECTIONS));
  CHECK_FALSE(auth::HasPermission(auth::Role::READONLY, auth::Permission::INSERT));
  CHECK_FALSE(auth::HasPermission(auth::Role::READONLY, auth::Permission::DELETE));
  CHECK_FALSE(auth::HasPermission(auth::Role::READONLY, auth::Permission::UPSERT));
  CHECK_FALSE(auth::HasPermission(auth::Role::READONLY, auth::Permission::CREATE_COLLECTION));
  CHECK_FALSE(auth::HasPermission(auth::Role::READONLY, auth::Permission::DROP_COLLECTION));
}

TEST_CASE("HasPermission - collection_admin like readwrite") {
  CHECK(auth::HasPermission(auth::Role::COLLECTION_ADMIN, auth::Permission::INSERT));
  CHECK(auth::HasPermission(auth::Role::COLLECTION_ADMIN, auth::Permission::SEARCH));
  CHECK(auth::HasPermission(auth::Role::COLLECTION_ADMIN, auth::Permission::DELETE));
  CHECK_FALSE(auth::HasPermission(auth::Role::COLLECTION_ADMIN, auth::Permission::CREATE_COLLECTION));
  CHECK_FALSE(auth::HasPermission(auth::Role::COLLECTION_ADMIN, auth::Permission::DROP_COLLECTION));
}

// ============================================================================
// Collection access tests
// ============================================================================

TEST_CASE("HasCollectionAccess - admin always has access") {
  auth::ApiKeyRole role{"key", auth::Role::ADMIN, {}};
  CHECK(auth::HasCollectionAccess(role, "anything"));
}

TEST_CASE("HasCollectionAccess - wildcard grants all") {
  auth::ApiKeyRole role{"key", auth::Role::READWRITE, {"*"}};
  CHECK(auth::HasCollectionAccess(role, "products"));
  CHECK(auth::HasCollectionAccess(role, "embeddings"));
}

TEST_CASE("HasCollectionAccess - specific collection") {
  auth::ApiKeyRole role{"key", auth::Role::READWRITE, {"products"}};
  CHECK(auth::HasCollectionAccess(role, "products"));
  CHECK_FALSE(auth::HasCollectionAccess(role, "embeddings"));
}

TEST_CASE("HasCollectionAccess - multiple collections") {
  auth::ApiKeyRole role{"key", auth::Role::READWRITE, {"products", "embeddings"}};
  CHECK(auth::HasCollectionAccess(role, "products"));
  CHECK(auth::HasCollectionAccess(role, "embeddings"));
  CHECK_FALSE(auth::HasCollectionAccess(role, "other"));
}

TEST_CASE("HasCollectionAccess - empty collection name always passes") {
  auth::ApiKeyRole role{"key", auth::Role::READWRITE, {"products"}};
  CHECK(auth::HasCollectionAccess(role, ""));
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
// RbacStore::Create tests — valid configs
// ============================================================================

TEST_CASE("RbacStore - lookup found") {
  utils::AuthConfig config;
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
  utils::AuthConfig config;
  config.roles = {{"my-key", "admin", {}}};
  auto store = auth::RbacStore::Create(config);
  REQUIRE(store.ok());

  CHECK((*store)->Lookup("nonexistent") == nullptr);
}

TEST_CASE("RbacStore - legacy api_keys treated as admin") {
  utils::AuthConfig config;
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
  utils::AuthConfig config;
  config.api_keys = {"shared-key"};
  config.roles = {{"shared-key", "readonly", {"*"}}};
  auto store = auth::RbacStore::Create(config);
  REQUIRE(store.ok());

  auto* role = (*store)->Lookup("shared-key");
  REQUIRE(role != nullptr);
  CHECK_EQ(role->role, auth::Role::READONLY);
}

TEST_CASE("RbacStore - size") {
  utils::AuthConfig config;
  config.api_keys = {"key1"};
  config.roles = {{"key2", "readwrite", {"*"}}, {"key3", "readonly", {"*"}}};
  auto store = auth::RbacStore::Create(config);
  REQUIRE(store.ok());
  CHECK_EQ((*store)->Size(), 3);
}

// ============================================================================
// RbacStore::Create — invalid configs (validation)
// ============================================================================

TEST_CASE("RbacStore rejects unknown role string") {
  utils::AuthConfig config;
  config.roles = {{"key1", "readwritee", {"*"}}};
  auto result = auth::RbacStore::Create(config);
  CHECK_FALSE(result.ok());
  CHECK(result.status().message().find("Unknown role") != std::string::npos);
}

TEST_CASE("RbacStore rejects empty key") {
  utils::AuthConfig config;
  config.roles = {{"", "admin", {}}};
  auto result = auth::RbacStore::Create(config);
  CHECK_FALSE(result.ok());
  CHECK(result.status().message().find("empty key") != std::string::npos);
}

TEST_CASE("RbacStore rejects empty legacy key") {
  utils::AuthConfig config;
  config.api_keys = {""};
  auto result = auth::RbacStore::Create(config);
  CHECK_FALSE(result.ok());
  CHECK(result.status().message().find("empty key") != std::string::npos);
}

TEST_CASE("RbacStore rejects readwrite with empty collections") {
  utils::AuthConfig config;
  config.roles = {{"key1", "readwrite", {}}};
  auto result = auth::RbacStore::Create(config);
  CHECK_FALSE(result.ok());
  CHECK(result.status().message().find("must specify collections") != std::string::npos);
}

TEST_CASE("RbacStore rejects readonly with empty collections") {
  utils::AuthConfig config;
  config.roles = {{"key1", "readonly", {}}};
  auto result = auth::RbacStore::Create(config);
  CHECK_FALSE(result.ok());
  CHECK(result.status().message().find("must specify collections") != std::string::npos);
}

TEST_CASE("RbacStore rejects collection_admin with empty collections") {
  utils::AuthConfig config;
  config.roles = {{"key1", "collection_admin", {}}};
  auto result = auth::RbacStore::Create(config);
  CHECK_FALSE(result.ok());
  CHECK(result.status().message().find("must specify collections") != std::string::npos);
}

TEST_CASE("RbacStore accepts admin with empty collections") {
  utils::AuthConfig config;
  config.roles = {{"key1", "admin", {}}};
  auto result = auth::RbacStore::Create(config);
  CHECK(result.ok());
}

TEST_CASE("RbacStore rejects enabled auth with no keys") {
  utils::AuthConfig config;
  // enabled but no api_keys and no roles
  auto result = auth::RbacStore::Create(config);
  CHECK_FALSE(result.ok());
  CHECK(result.status().message().find("no API keys configured") != std::string::npos);
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
