// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <fstream>
#include <string>

#include "absl/status/statusor.h"
#include "config/config.h"
#include "support/temp_dir.h"

namespace gvdb::config::test {
namespace {

// Loads YAML through Config::LoadFromFile from a file in a unique
// temporary folder, which is removed afterwards.
class ConfigFileTest {
 public:
  absl::StatusOr<config::GVDBConfig> Load(const std::string& yaml) {
    const auto path = dir_ / "config.yaml";
    {
      std::ofstream out(path);
      REQUIRE(out.is_open());
      out << yaml;
    }
    return config::Config::LoadFromFile(path.string());
  }

  gvdb::test::ScopedTempDir dir_{"gvdb_config_test"};
};

TEST_CASE_FIXTURE(ConfigFileTest, "ConfigTest - LoadsAuthRolesObjectStoreAndLocalBackupDir") {
  auto config = Load(R"(
server:
  auth:
    enabled: true
    api_keys:
      - legacy-key-1
      - legacy-key-2
    roles:
      - key: admin-key
        role: admin
      - key: rw-key
        role: readwrite
        collections: [products, embeddings]
      - key: ro-key
        role: readonly
        collections: ["*"]
      - key: ca-key
        role: collection_admin
        collections: [products]
storage:
  local_backup_dir: /var/lib/gvdb/backups
  object_store:
    type: minio
    endpoint: localhost:9000
    access_key: test-access-key
    secret_key: test-secret-key
    bucket: gvdb-test
    region: eu-west-1
    prefix: cluster-a
    project: test-project
    credentials_path: /etc/gvdb/gcs.json
    use_ssl: false
    local_cache_size_mb: 512
    upload_threads: 4
)");
  INFO(config.status().message());
  REQUIRE(config.ok());

  const auto& auth = config->server.auth;
  CHECK(auth.enabled);
  const std::vector<std::string> legacy_keys = {"legacy-key-1", "legacy-key-2"};
  CHECK_EQ(auth.api_keys, legacy_keys);

  REQUIRE_EQ(auth.roles.size(), 4u);
  CHECK_EQ(auth.roles[0].key, "admin-key");
  CHECK_EQ(auth.roles[0].role, "admin");
  CHECK(auth.roles[0].collections.empty());

  const std::vector<std::string> rw_collections = {"products", "embeddings"};
  CHECK_EQ(auth.roles[1].key, "rw-key");
  CHECK_EQ(auth.roles[1].role, "readwrite");
  CHECK_EQ(auth.roles[1].collections, rw_collections);

  const std::vector<std::string> all_collections = {"*"};
  CHECK_EQ(auth.roles[2].key, "ro-key");
  CHECK_EQ(auth.roles[2].role, "readonly");
  CHECK_EQ(auth.roles[2].collections, all_collections);

  const std::vector<std::string> ca_collections = {"products"};
  CHECK_EQ(auth.roles[3].key, "ca-key");
  CHECK_EQ(auth.roles[3].role, "collection_admin");
  CHECK_EQ(auth.roles[3].collections, ca_collections);

  const auto& storage = config->storage;
  CHECK_EQ(storage.local_backup_dir, "/var/lib/gvdb/backups");
  CHECK_EQ(storage.object_store.type, "minio");
  CHECK_EQ(storage.object_store.endpoint, "localhost:9000");
  CHECK_EQ(storage.object_store.access_key, "test-access-key");
  CHECK_EQ(storage.object_store.secret_key, "test-secret-key");
  CHECK_EQ(storage.object_store.bucket, "gvdb-test");
  CHECK_EQ(storage.object_store.region, "eu-west-1");
  CHECK_EQ(storage.object_store.prefix, "cluster-a");
  CHECK_EQ(storage.object_store.project, "test-project");
  CHECK_EQ(storage.object_store.credentials_path, "/etc/gvdb/gcs.json");
  CHECK_FALSE(storage.object_store.use_ssl);
  CHECK_EQ(storage.object_store_cache_size_mb, 512);
  CHECK_EQ(storage.object_store_upload_threads, 4);
}

TEST_CASE_FIXTURE(ConfigFileTest, "ConfigTest - ObjectStoreKeysLeftOutGetDefaults") {
  auto config = Load(R"(
storage:
  object_store:
    type: s3
    endpoint: s3.example.com
    bucket: gvdb-data
)");
  INFO(config.status().message());
  REQUIRE(config.ok());

  const auto& storage = config->storage;
  CHECK_EQ(storage.object_store.type, "s3");
  CHECK_EQ(storage.object_store.endpoint, "s3.example.com");
  CHECK_EQ(storage.object_store.bucket, "gvdb-data");
  CHECK(storage.object_store.access_key.empty());
  CHECK(storage.object_store.secret_key.empty());
  CHECK(storage.object_store.project.empty());
  CHECK(storage.object_store.credentials_path.empty());
  CHECK_EQ(storage.object_store.region, "us-east-1");
  CHECK_EQ(storage.object_store.prefix, "gvdb");
  CHECK(storage.object_store.use_ssl);
  CHECK_EQ(storage.object_store_cache_size_mb, 256);
  CHECK_EQ(storage.object_store_upload_threads, 2);

  // Without local_backup_dir, local backup targets stay disabled.
  CHECK(storage.local_backup_dir.empty());

  const auto& auth = config->server.auth;
  CHECK_FALSE(auth.enabled);
  CHECK(auth.api_keys.empty());
  CHECK(auth.roles.empty());
}

}  // namespace
}  // namespace gvdb::config::test
