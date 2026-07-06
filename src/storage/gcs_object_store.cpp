// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifdef GVDB_HAS_GCS

#include "storage/gcs_object_store.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "google/cloud/credentials.h"
#include "google/cloud/options.h"
#include "google/cloud/status.h"
#include "google/cloud/storage/client.h"

namespace gvdb {
namespace storage {

namespace {

namespace gc = ::google::cloud;
namespace gcs = ::google::cloud::storage;

// google::cloud::StatusCode mirrors absl::StatusCode 1:1, so a numeric cast
// preserves the code (kNotFound stays kNotFound, which the callers and the
// IObjectStore contract tests rely on via absl::IsNotFound).
core::Status ToAbslStatus(const gc::Status& status) {
  if (status.ok()) return core::OkStatus();
  return absl::Status(
      static_cast<absl::StatusCode>(static_cast<int>(status.code())),
      status.message());
}

}  // namespace

// Holds the google-cloud-cpp client so the SDK stays out of the header.
struct GcsObjectStore::Impl {
  Impl(gcs::Client c, std::string b)
      : client(std::move(c)), bucket(std::move(b)) {}

  // A Client is NOT safe for concurrent use by multiple threads, but copies
  // are cheap and share one connection pool. Each public method takes a copy
  // (Get()) so concurrent calls each drive their own Client. This is the
  // pattern google-cloud-cpp documents for multi-threaded access.
  gcs::Client Get() const { return client; }

  gcs::Client client;
  std::string bucket;
};

GcsObjectStore::GcsObjectStore(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

GcsObjectStore::~GcsObjectStore() = default;

core::StatusOr<std::unique_ptr<GcsObjectStore>> GcsObjectStore::Create(
    const GcsConfig& config) {
  if (config.bucket.empty()) {
    return core::InvalidArgumentError("GcsConfig.bucket must be set");
  }

  gc::Options options;
  if (!config.endpoint.empty()) {
    // Emulator (fake-gcs-server): plain-HTTP REST endpoint + anonymous creds.
    options.set<gcs::RestEndpointOption>(config.endpoint)
        .set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials());
  } else {
    // Production: Application Default Credentials. On GKE this is Workload
    // Identity; locally GOOGLE_APPLICATION_CREDENTIALS points at a key file.
    // An explicit credentials_path is exported into that env var (without
    // clobbering one already set in the environment) so ADC honors it.
    if (!config.credentials_path.empty()) {
      ::setenv("GOOGLE_APPLICATION_CREDENTIALS", config.credentials_path.c_str(),
               /*overwrite=*/0);
    }
    options.set<gc::UnifiedCredentialsOption>(
        gc::MakeGoogleDefaultCredentials());
  }
  if (!config.project_id.empty()) {
    options.set<gcs::ProjectIdOption>(config.project_id);
  }

  auto impl = std::make_unique<Impl>(gcs::Client(std::move(options)),
                                     config.bucket);
  return std::unique_ptr<GcsObjectStore>(
      new GcsObjectStore(std::move(impl)));
}

core::Status GcsObjectStore::PutObject(
    const std::string& key, const std::string& data) {
  auto client = impl_->Get();
  auto metadata = client.InsertObject(impl_->bucket, key, data);
  if (!metadata) {
    return ToAbslStatus(metadata.status());
  }
  return core::OkStatus();
}

core::StatusOr<std::string> GcsObjectStore::GetObject(const std::string& key) {
  auto client = impl_->Get();
  auto reader = client.ReadObject(impl_->bucket, key);
  std::string body(std::istreambuf_iterator<char>{reader},
                   std::istreambuf_iterator<char>{});
  if (!reader.status().ok()) {
    return ToAbslStatus(reader.status());
  }
  return body;
}

core::Status GcsObjectStore::DeleteObject(const std::string& key) {
  auto client = impl_->Get();
  auto status = client.DeleteObject(impl_->bucket, key);
  // Idempotent: deleting a missing key is success (matches the interface
  // contract and S3's behavior).
  if (!status.ok() && status.code() != gc::StatusCode::kNotFound) {
    return ToAbslStatus(status);
  }
  return core::OkStatus();
}

core::StatusOr<std::vector<std::string>> GcsObjectStore::ListObjects(
    const std::string& prefix) {
  auto client = impl_->Get();
  std::vector<std::string> keys;
  for (auto&& item : client.ListObjects(impl_->bucket, gcs::Prefix(prefix))) {
    // Pagination errors surface mid-iteration, so each element is checked.
    if (!item) {
      return ToAbslStatus(item.status());
    }
    keys.push_back(item->name());
  }
  // Sort to match the IObjectStore contract (InMemory/Filesystem backends
  // return keys in ascending order).
  std::sort(keys.begin(), keys.end());
  return keys;
}

core::StatusOr<bool> GcsObjectStore::ObjectExists(const std::string& key) {
  auto client = impl_->Get();
  auto metadata = client.GetObjectMetadata(impl_->bucket, key);
  if (metadata) {
    return true;
  }
  if (metadata.status().code() == gc::StatusCode::kNotFound) {
    return false;
  }
  // A permission/network error is not "absent" — surface it rather than
  // masquerading as false.
  return ToAbslStatus(metadata.status());
}

core::Status GcsObjectStore::PutObjectFromFile(
    const std::string& key, const std::string& local_file_path) {
  std::ifstream input(local_file_path, std::ios::binary);
  if (!input.good()) {
    return core::NotFoundError("Local file not found: " + local_file_path);
  }

  auto client = impl_->Get();
  auto writer = client.WriteObject(impl_->bucket, key);
  // Streaming `input.rdbuf()` into an ostream sets failbit when the source is
  // empty (no characters inserted); guard so an empty file still produces a
  // valid empty object rather than a spuriously-failed write.
  if (input.peek() != std::char_traits<char>::eof()) {
    writer << input.rdbuf();
  }
  writer.Close();
  auto metadata = std::move(writer).metadata();
  if (!metadata) {
    return ToAbslStatus(metadata.status());
  }
  return core::OkStatus();
}

core::Status GcsObjectStore::GetObjectToFile(
    const std::string& key, const std::string& local_file_path) {
  auto client = impl_->Get();
  auto reader = client.ReadObject(impl_->bucket, key);
  // Force the request so a missing object is detected before a local file is
  // created; peek() does not consume the body of an object that does exist.
  reader.peek();
  if (!reader.status().ok()) {
    return ToAbslStatus(reader.status());
  }

  std::ofstream output(local_file_path, std::ios::binary);
  if (!output.is_open()) {
    return core::InternalError("Failed to open local file: " + local_file_path);
  }
  output << reader.rdbuf();
  if (!reader.status().ok()) {
    output.close();
    std::error_code ec;
    std::filesystem::remove(local_file_path, ec);
    return ToAbslStatus(reader.status());
  }
  return core::OkStatus();
}

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_HAS_GCS
