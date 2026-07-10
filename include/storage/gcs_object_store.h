// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_GCS_OBJECT_STORE_H_
#define GVDB_STORAGE_GCS_OBJECT_STORE_H_

#ifdef GVDB_HAS_GCS

#include <memory>
#include <string>

#include "core/status.h"
#include "storage/object_store.h"

namespace gvdb {
namespace storage {

// Configuration for creating a GcsObjectStore.
//
// Authentication is Application Default Credentials (ADC): on GKE this is
// Workload Identity (no key file); locally the GOOGLE_APPLICATION_CREDENTIALS
// env var points ADC at a service-account JSON. There are intentionally no
// static access/secret keys and no region — a GCS bucket's location is a
// property of the bucket, not the client.
struct GcsConfig {
  std::string bucket;
  // Optional GCP project id. ADC usually supplies it; set only when the
  // ambient project differs from the one owning the bucket.
  std::string project_id;
  // Optional path to a service-account JSON key. When set (and no emulator
  // endpoint is used), the key is loaded and used directly for this store's
  // client — no process-global env mutation. Empty on GKE Workload Identity
  // (no key file); when empty, auth falls back to Application Default
  // Credentials, which honor the GOOGLE_APPLICATION_CREDENTIALS env var.
  std::string credentials_path;
  // Optional REST endpoint override for a local emulator (fake-gcs-server).
  // When set, the client talks plain HTTP with anonymous credentials, so it
  // must only ever point at a trusted in-cluster/test emulator. Empty in
  // production, where ADC + the real GCS endpoint are used.
  std::string endpoint;
};

// ============================================================================
// GcsObjectStore - IObjectStore implementation using Google Cloud Storage
// ============================================================================
// Uses google-cloud-cpp (storage component only). The underlying
// google::cloud::storage::Client is held in a PIMPL so the SDK never leaks
// into includers, and — because a single Client instance is not safe for
// concurrent use — each call operates on a cheap copy of it (copies share
// one connection pool, which is the pattern the library documents).
// Thread-safe for concurrent calls.
class GcsObjectStore : public IObjectStore {
 public:
  // Factory method — constructs the GCS client from config. Returns an error
  // only for clearly-invalid config (e.g. empty bucket); connection/auth
  // problems surface on the first request, mirroring S3ObjectStore::Create.
  [[nodiscard]] static core::StatusOr<std::unique_ptr<GcsObjectStore>> Create(
      const GcsConfig& config);

  ~GcsObjectStore() override;

  core::Status PutObject(
      const std::string& key, const std::string& data) override;

  core::StatusOr<std::string> GetObject(const std::string& key) override;

  core::Status DeleteObject(const std::string& key) override;

  core::StatusOr<std::vector<std::string>> ListObjects(
      const std::string& prefix) override;

  core::StatusOr<bool> ObjectExists(const std::string& key) override;

  core::Status PutObjectFromFile(
      const std::string& key,
      const std::string& local_file_path) override;

  core::Status GetObjectToFile(
      const std::string& key,
      const std::string& local_file_path) override;

 private:
  // Holds the google::cloud::storage::Client + bucket; defined in the .cpp so
  // no google-cloud-cpp headers appear here.
  struct Impl;

  explicit GcsObjectStore(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_HAS_GCS
#endif  // GVDB_STORAGE_GCS_OBJECT_STORE_H_
