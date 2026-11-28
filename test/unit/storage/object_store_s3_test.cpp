// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Runs the object store contract (object_store_contract.h) against
// S3ObjectStore on a real S3-compatible endpoint, for example adobe/s3mock
// from test/integration/docker-compose.s3.yml.
//
// Environment:
//   GVDB_TEST_S3_ENDPOINT    required, for example http://localhost:9000
//   GVDB_TEST_S3_BUCKET      default gvdb-test (the bucket must exist)
//   GVDB_TEST_S3_ACCESS_KEY  default minioadmin
//   GVDB_TEST_S3_SECRET_KEY  default minioadmin
//   GVDB_TEST_S3_REGION      default us-east-1
// Without GVDB_TEST_S3_ENDPOINT the binary exits with code 77, which CTest
// reports as skipped.

#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>

#include "object_store_contract.h"
#include "storage/objstore/s3_object_store.h"
#include "support/temp_dir.h"

namespace gvdb::storage {
namespace {

constexpr int kSkipReturnCode = 77;

std::string EnvOr(const char* name, const char* fallback) {
  const char* value = std::getenv(name);
  return (value != nullptr && *value != '\0') ? value : fallback;
}

// Builds the S3 configuration from the environment. The scheme of the
// endpoint URL decides whether the client uses TLS.
S3Config ConfigFromEnv() {
  S3Config config;
  std::string endpoint = EnvOr("GVDB_TEST_S3_ENDPOINT", "");
  constexpr std::string_view kHttps = "https://";
  constexpr std::string_view kHttp = "http://";
  config.use_ssl = !endpoint.starts_with(kHttp);
  if (endpoint.starts_with(kHttps)) {
    endpoint.erase(0, kHttps.size());
  } else if (endpoint.starts_with(kHttp)) {
    endpoint.erase(0, kHttp.size());
  }
  config.endpoint = endpoint;
  config.bucket = EnvOr("GVDB_TEST_S3_BUCKET", "gvdb-test");
  config.access_key = EnvOr("GVDB_TEST_S3_ACCESS_KEY", "minioadmin");
  config.secret_key = EnvOr("GVDB_TEST_S3_SECRET_KEY", "minioadmin");
  config.region = EnvOr("GVDB_TEST_S3_REGION", "us-east-1");
  config.path_style = true;
  return config;
}

struct S3ObjectStoreFixture {
  S3ObjectStoreFixture() : tmp_dir("gvdb_s3_objstore") {
    auto created = S3ObjectStore::Create(ConfigFromEnv());
    REQUIRE(created.ok());
    store = std::make_unique<contract::PrefixedObjectStore>(
        std::move(*created), contract::PrefixedObjectStore::UniquePrefix());
  }

  IObjectStore& Get() { return *store; }

  test::ScopedTempDir tmp_dir;
  std::unique_ptr<contract::PrefixedObjectStore> store;
};

}  // namespace
}  // namespace gvdb::storage

TYPE_TO_STRING_AS("S3ObjectStore", gvdb::storage::S3ObjectStoreFixture);

namespace gvdb::storage {

namespace contract {
GVDB_OBJECT_STORE_CONTRACT(S3ObjectStoreFixture);
}  // namespace contract

}  // namespace gvdb::storage

int main(int argc, char** argv) {
  const char* endpoint = std::getenv("GVDB_TEST_S3_ENDPOINT");
  if (endpoint == nullptr || *endpoint == '\0') {
    std::fprintf(stderr,
                 "object_store_s3_tests: GVDB_TEST_S3_ENDPOINT is not set, "
                 "skipping the S3 contract tests\n");
    return gvdb::storage::kSkipReturnCode;
  }
  doctest::Context context(argc, argv);
  return context.run();
}
