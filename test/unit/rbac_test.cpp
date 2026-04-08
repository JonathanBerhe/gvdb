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

TEST_CASE("HasCollectionAccess - empty collections means all") {
  auth::ApiKeyRole role{"key", auth::Role::READWRITE, {}};
  CHECK(auth::HasCollectionAccess(role, "products"));
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

TEST_CASE("RoleFromString") {
  CHECK_EQ(auth::RoleFromString("admin"), auth::Role::ADMIN);
  CHECK_EQ(auth::RoleFromString("readwrite"), auth::Role::READWRITE);
  CHECK_EQ(auth::RoleFromString("readonly"), auth::Role::READONLY);
  CHECK_EQ(auth::RoleFromString("collection_admin"), auth::Role::COLLECTION_ADMIN);
  CHECK_EQ(auth::RoleFromString("unknown"), auth::Role::ADMIN);  // default
}

// ============================================================================
// RbacStore tests
// ============================================================================

TEST_CASE("RbacStore - lookup found") {
  utils::AuthConfig config;
  config.roles = {{"my-key", "readwrite", {"products"}}};
  auth::RbacStore store(config);

  auto* role = store.Lookup("my-key");
  REQUIRE(role != nullptr);
  CHECK_EQ(role->role, auth::Role::READWRITE);
  CHECK_EQ(role->collections.size(), 1);
  CHECK_EQ(role->collections[0], "products");
}

TEST_CASE("RbacStore - lookup not found") {
  utils::AuthConfig config;
  config.roles = {{"my-key", "admin", {}}};
  auth::RbacStore store(config);

  CHECK(store.Lookup("nonexistent") == nullptr);
}

TEST_CASE("RbacStore - legacy api_keys treated as admin") {
  utils::AuthConfig config;
  config.api_keys = {"old-key-1", "old-key-2"};
  auth::RbacStore store(config);

  auto* role1 = store.Lookup("old-key-1");
  REQUIRE(role1 != nullptr);
  CHECK_EQ(role1->role, auth::Role::ADMIN);

  auto* role2 = store.Lookup("old-key-2");
  REQUIRE(role2 != nullptr);
  CHECK_EQ(role2->role, auth::Role::ADMIN);
}

TEST_CASE("RbacStore - role-based takes precedence over legacy") {
  utils::AuthConfig config;
  config.api_keys = {"shared-key"};
  config.roles = {{"shared-key", "readonly", {"*"}}};
  auth::RbacStore store(config);

  auto* role = store.Lookup("shared-key");
  REQUIRE(role != nullptr);
  CHECK_EQ(role->role, auth::Role::READONLY);  // not admin
}

TEST_CASE("RbacStore - size") {
  utils::AuthConfig config;
  config.api_keys = {"key1"};
  config.roles = {{"key2", "readwrite", {}}, {"key3", "readonly", {}}};
  auth::RbacStore store(config);

  CHECK_EQ(store.Size(), 3);
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
