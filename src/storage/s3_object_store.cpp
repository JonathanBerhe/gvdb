// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifdef GVDB_HAS_S3

#include "storage/s3_object_store.h"

#include <fstream>
#include <sstream>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

namespace gvdb {
namespace storage {

namespace {

// RAII guard for AWS SDK initialization.
// ShutdownAPI must receive the same options instance as InitAPI.
struct AwsSdkGuard {
  Aws::SDKOptions options;
  AwsSdkGuard() { Aws::InitAPI(options); }
  ~AwsSdkGuard() { Aws::ShutdownAPI(options); }
};

// Singleton — initialized once, shutdown on program exit
AwsSdkGuard& GetSdkGuard() {
  static AwsSdkGuard guard;
  return guard;
}

}  // namespace

S3ObjectStore::S3ObjectStore(std::shared_ptr<Aws::S3::S3Client> client,
                             const std::string& bucket)
    : client_(std::move(client)), bucket_(bucket) {}

S3ObjectStore::~S3ObjectStore() = default;

core::StatusOr<std::unique_ptr<S3ObjectStore>> S3ObjectStore::Create(
    const S3Config& config) {
  // Ensure AWS SDK is initialized
  (void)GetSdkGuard();

  Aws::S3::S3ClientConfiguration aws_config;
  if (!config.region.empty()) {
    aws_config.region = config.region;
  }
  if (!config.endpoint.empty()) {
    aws_config.endpointOverride = config.endpoint;
  }
  aws_config.scheme = config.use_ssl ? Aws::Http::Scheme::HTTPS
                                     : Aws::Http::Scheme::HTTP;
  aws_config.useVirtualAddressing = !config.path_style;

  Aws::Auth::AWSCredentials credentials(config.access_key, config.secret_key);

  auto client = std::make_shared<Aws::S3::S3Client>(
      credentials, nullptr, aws_config);

  return std::unique_ptr<S3ObjectStore>(
      new S3ObjectStore(std::move(client), config.bucket));
}

core::Status S3ObjectStore::PutObject(
    const std::string& key, const std::string& data) {
  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(bucket_);
  request.SetKey(key);

  auto body = Aws::MakeShared<Aws::StringStream>("PutObject");
  body->write(data.data(), data.size());
  request.SetBody(body);
  request.SetContentLength(data.size());

  auto outcome = client_->PutObject(request);
  if (!outcome.IsSuccess()) {
    return core::InternalError(outcome.GetError().GetMessage());
  }
  return core::OkStatus();
}

core::StatusOr<std::string> S3ObjectStore::GetObject(
    const std::string& key) {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket_);
  request.SetKey(key);

  auto outcome = client_->GetObject(request);
  if (!outcome.IsSuccess()) {
    auto& error = outcome.GetError();
    if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY ||
        error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      return core::NotFoundError("Object not found: " + key);
    }
    return core::InternalError(error.GetMessage());
  }

  std::ostringstream ss;
  ss << outcome.GetResult().GetBody().rdbuf();
  return ss.str();
}

core::Status S3ObjectStore::DeleteObject(const std::string& key) {
  Aws::S3::Model::DeleteObjectRequest request;
  request.SetBucket(bucket_);
  request.SetKey(key);

  auto outcome = client_->DeleteObject(request);
  if (!outcome.IsSuccess()) {
    // Delete of non-existent key is OK (S3 returns success anyway)
    return core::InternalError(outcome.GetError().GetMessage());
  }
  return core::OkStatus();
}

core::StatusOr<std::vector<std::string>> S3ObjectStore::ListObjects(
    const std::string& prefix) {
  std::vector<std::string> keys;

  Aws::S3::Model::ListObjectsV2Request request;
  request.SetBucket(bucket_);
  request.SetPrefix(prefix);

  bool has_more = true;
  while (has_more) {
    auto outcome = client_->ListObjectsV2(request);
    if (!outcome.IsSuccess()) {
      return core::InternalError(outcome.GetError().GetMessage());
    }

    auto& result = outcome.GetResult();
    for (const auto& obj : result.GetContents()) {
      keys.push_back(obj.GetKey());
    }

    has_more = result.GetIsTruncated();
    if (has_more) {
      request.SetContinuationToken(result.GetNextContinuationToken());
    }
  }

  return keys;
}

core::StatusOr<bool> S3ObjectStore::ObjectExists(const std::string& key) {
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(bucket_);
  request.SetKey(key);

  auto outcome = client_->HeadObject(request);
  return outcome.IsSuccess();
}

core::Status S3ObjectStore::PutObjectFromFile(
    const std::string& key, const std::string& local_file_path) {
  auto input = Aws::MakeShared<Aws::FStream>(
      "PutObjectFromFile", local_file_path,
      std::ios_base::in | std::ios_base::binary);

  if (!input->good()) {
    return core::NotFoundError("Local file not found: " + local_file_path);
  }

  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(bucket_);
  request.SetKey(key);
  request.SetBody(input);

  auto outcome = client_->PutObject(request);
  if (!outcome.IsSuccess()) {
    return core::InternalError(outcome.GetError().GetMessage());
  }
  return core::OkStatus();
}

core::Status S3ObjectStore::GetObjectToFile(
    const std::string& key, const std::string& local_file_path) {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket_);
  request.SetKey(key);

  auto outcome = client_->GetObject(request);
  if (!outcome.IsSuccess()) {
    auto& error = outcome.GetError();
    if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY ||
        error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      return core::NotFoundError("Object not found: " + key);
    }
    return core::InternalError(error.GetMessage());
  }

  std::ofstream file(local_file_path, std::ios::binary);
  if (!file.is_open()) {
    return core::InternalError("Failed to open local file: " + local_file_path);
  }

  file << outcome.GetResult().GetBody().rdbuf();
  return core::OkStatus();
}

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_HAS_S3
