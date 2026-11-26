// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Runs the object store contract (object_store_contract.h) against
// GcsObjectStore on a GCS emulator, for example fake-gcs-server from
// test/integration/docker-compose.fake-gcs.yml.
//
// Environment:
//   GVDB_TEST_GCS_ENDPOINT  required, for example http://localhost:4443
//   GVDB_TEST_GCS_BUCKET    default gvdb-test (the bucket must exist)
// Without GVDB_TEST_GCS_ENDPOINT the binary exits with code 77, which CTest
// reports as skipped.

#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

#include "object_store_contract.h"
#include "storage/gcs_object_store.h"

namespace gvdb {
namespace storage {
namespace {

constexpr int kSkipReturnCode = 77;

std::string EnvOr(const char* name, const char* fallback) {
  const char* value = std::getenv(name);
  return (value != nullptr && *value != '\0') ? value : fallback;
}

struct GcsObjectStoreFixture {
  GcsObjectStoreFixture()
      : tmp_dir(contract::MakeUniqueTempDir("gvdb_gcs_objstore")) {
    GcsConfig config;
    config.endpoint = EnvOr("GVDB_TEST_GCS_ENDPOINT", "");
    config.bucket = EnvOr("GVDB_TEST_GCS_BUCKET", "gvdb-test");
    auto created = GcsObjectStore::Create(config);
    REQUIRE(created.ok());
    store = std::make_unique<contract::PrefixedObjectStore>(
        std::move(*created), contract::PrefixedObjectStore::UniquePrefix());
  }

  ~GcsObjectStoreFixture() {
    std::error_code ec;
    std::filesystem::remove_all(tmp_dir, ec);
  }

  IObjectStore& Get() { return *store; }

  std::filesystem::path tmp_dir;
  std::unique_ptr<contract::PrefixedObjectStore> store;
};

}  // namespace

namespace contract {
GVDB_OBJECT_STORE_CONTRACT(GcsObjectStoreFixture);
}  // namespace contract

}  // namespace storage
}  // namespace gvdb

int main(int argc, char** argv) {
  const char* endpoint = std::getenv("GVDB_TEST_GCS_ENDPOINT");
  if (endpoint == nullptr || *endpoint == '\0') {
    std::fprintf(stderr,
                 "object_store_gcs_tests: GVDB_TEST_GCS_ENDPOINT is not set, "
                 "skipping the GCS contract tests\n");
    return gvdb::storage::kSkipReturnCode;
  }
  doctest::Context context(argc, argv);
  return context.run();
}
