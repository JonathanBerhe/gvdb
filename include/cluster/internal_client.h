// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/types.h"
#include "internal.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

namespace gvdb {
namespace cluster {

// Abstract interface for calling InternalService RPCs
// This enables mocking in tests and dependency injection
class IInternalServiceClient {
 public:
  virtual ~IInternalServiceClient() = default;

  // RPC methods (add more as needed)
  virtual grpc::Status CreateSegment(
      grpc::ClientContext* context,
      const proto::internal::CreateSegmentRequest& request,
      proto::internal::CreateSegmentResponse* response) = 0;

  virtual grpc::Status DeleteSegment(
      grpc::ClientContext* context,
      const proto::internal::DeleteSegmentRequest& request,
      proto::internal::DeleteSegmentResponse* response) = 0;

  virtual grpc::Status ReplicateSegment(
      grpc::ClientContext* context,
      const proto::internal::ReplicateSegmentRequest& request,
      proto::internal::ReplicateSegmentResponse* response) = 0;

  virtual grpc::Status GetSegment(
      grpc::ClientContext* context,
      const proto::internal::GetSegmentRequest& request,
      proto::internal::GetSegmentResponse* response) = 0;

  virtual grpc::Status ListSegments(
      grpc::ClientContext* context,
      const proto::internal::ListSegmentsRequest& request,
      proto::internal::ListSegmentsResponse* response) = 0;

  // Two-phase primary swap RPCs. Called by the coordinator's
  // TransferPrimary orchestrator: PausePrimary on the outgoing primary
  // first, then PreparePromote on the incoming primary, then commit
  // via ShardManager. Both are idempotent on the data-node side so a
  // retried call after a transient failure is safe.
  virtual grpc::Status PausePrimary(
      grpc::ClientContext* context,
      const proto::internal::PausePrimaryRequest& request,
      proto::internal::PausePrimaryResponse* response) = 0;

  virtual grpc::Status PreparePromote(
      grpc::ClientContext* context,
      const proto::internal::PreparePromoteRequest& request,
      proto::internal::PreparePromoteResponse* response) = 0;

  // Backup / restore orchestration RPCs. Per-shard fan-out from the
  // coordinator: FreezeWrites brackets BackupShard; UnfreezeWrites
  // matches the freeze token. RestoreShard runs the inverse path on
  // the receiving data-node. All four are idempotent on the data-node
  // side so coordinator retries are safe.
  virtual grpc::Status BackupShard(
      grpc::ClientContext* context,
      const proto::internal::BackupShardRequest& request,
      proto::internal::BackupShardResponse* response) = 0;

  virtual grpc::Status RestoreShard(
      grpc::ClientContext* context,
      const proto::internal::RestoreShardRequest& request,
      proto::internal::RestoreShardResponse* response) = 0;

  virtual grpc::Status FreezeWrites(
      grpc::ClientContext* context,
      const proto::internal::FreezeWritesRequest& request,
      proto::internal::FreezeWritesResponse* response) = 0;

  virtual grpc::Status UnfreezeWrites(
      grpc::ClientContext* context,
      const proto::internal::UnfreezeWritesRequest& request,
      proto::internal::UnfreezeWritesResponse* response) = 0;
};

// Abstract factory for creating IInternalServiceClient instances
class IInternalServiceClientFactory {
 public:
  virtual ~IInternalServiceClientFactory() = default;

  // Create a client for the given node
  // Returns nullptr if client cannot be created
  virtual std::unique_ptr<IInternalServiceClient> CreateClient(
      core::NodeId node_id,
      const std::string& address) = 0;
};

// Production implementation using real gRPC
class GrpcInternalServiceClient : public IInternalServiceClient {
 public:
  explicit GrpcInternalServiceClient(std::unique_ptr<proto::internal::InternalService::Stub> stub);

  grpc::Status CreateSegment(
      grpc::ClientContext* context,
      const proto::internal::CreateSegmentRequest& request,
      proto::internal::CreateSegmentResponse* response) override;

  grpc::Status DeleteSegment(
      grpc::ClientContext* context,
      const proto::internal::DeleteSegmentRequest& request,
      proto::internal::DeleteSegmentResponse* response) override;

  grpc::Status ReplicateSegment(
      grpc::ClientContext* context,
      const proto::internal::ReplicateSegmentRequest& request,
      proto::internal::ReplicateSegmentResponse* response) override;

  grpc::Status GetSegment(
      grpc::ClientContext* context,
      const proto::internal::GetSegmentRequest& request,
      proto::internal::GetSegmentResponse* response) override;

  grpc::Status ListSegments(
      grpc::ClientContext* context,
      const proto::internal::ListSegmentsRequest& request,
      proto::internal::ListSegmentsResponse* response) override;

  grpc::Status PausePrimary(
      grpc::ClientContext* context,
      const proto::internal::PausePrimaryRequest& request,
      proto::internal::PausePrimaryResponse* response) override;

  grpc::Status PreparePromote(
      grpc::ClientContext* context,
      const proto::internal::PreparePromoteRequest& request,
      proto::internal::PreparePromoteResponse* response) override;

  grpc::Status BackupShard(
      grpc::ClientContext* context,
      const proto::internal::BackupShardRequest& request,
      proto::internal::BackupShardResponse* response) override;

  grpc::Status RestoreShard(
      grpc::ClientContext* context,
      const proto::internal::RestoreShardRequest& request,
      proto::internal::RestoreShardResponse* response) override;

  grpc::Status FreezeWrites(
      grpc::ClientContext* context,
      const proto::internal::FreezeWritesRequest& request,
      proto::internal::FreezeWritesResponse* response) override;

  grpc::Status UnfreezeWrites(
      grpc::ClientContext* context,
      const proto::internal::UnfreezeWritesRequest& request,
      proto::internal::UnfreezeWritesResponse* response) override;

 private:
  std::unique_ptr<proto::internal::InternalService::Stub> stub_;
};

// Production factory that creates real gRPC clients
class GrpcInternalServiceClientFactory : public IInternalServiceClientFactory {
 public:
  GrpcInternalServiceClientFactory() = default;

  std::unique_ptr<IInternalServiceClient> CreateClient(
      core::NodeId node_id,
      const std::string& address) override;
};

// Null factory for tests (returns nullptr - no actual RPC calls made)
class NullInternalServiceClientFactory : public IInternalServiceClientFactory {
 public:
  NullInternalServiceClientFactory() = default;

  std::unique_ptr<IInternalServiceClient> CreateClient(
      core::NodeId node_id,
      const std::string& address) override {
    return nullptr;  // No clients created in tests
  }
};

}  // namespace cluster
}  // namespace gvdb