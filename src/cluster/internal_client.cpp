// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/internal_client.h"
#include "utils/logger.h"

namespace gvdb {
namespace cluster {

// ============================================================================
// GrpcInternalServiceClient Implementation
// ============================================================================

GrpcInternalServiceClient::GrpcInternalServiceClient(
    std::unique_ptr<proto::internal::InternalService::Stub> stub)
    : stub_(std::move(stub)) {}

grpc::Status GrpcInternalServiceClient::CreateSegment(
    grpc::ClientContext* context,
    const proto::internal::CreateSegmentRequest& request,
    proto::internal::CreateSegmentResponse* response) {
  return stub_->CreateSegment(context, request, response);
}

grpc::Status GrpcInternalServiceClient::DeleteSegment(
    grpc::ClientContext* context,
    const proto::internal::DeleteSegmentRequest& request,
    proto::internal::DeleteSegmentResponse* response) {
  return stub_->DeleteSegment(context, request, response);
}

grpc::Status GrpcInternalServiceClient::ReplicateSegment(
    grpc::ClientContext* context,
    const proto::internal::ReplicateSegmentRequest& request,
    proto::internal::ReplicateSegmentResponse* response) {
  return stub_->ReplicateSegment(context, request, response);
}

grpc::Status GrpcInternalServiceClient::GetSegment(
    grpc::ClientContext* context,
    const proto::internal::GetSegmentRequest& request,
    proto::internal::GetSegmentResponse* response) {
  return stub_->GetSegment(context, request, response);
}

grpc::Status GrpcInternalServiceClient::ListSegments(
    grpc::ClientContext* context,
    const proto::internal::ListSegmentsRequest& request,
    proto::internal::ListSegmentsResponse* response) {
  return stub_->ListSegments(context, request, response);
}

grpc::Status GrpcInternalServiceClient::PausePrimary(
    grpc::ClientContext* context,
    const proto::internal::PausePrimaryRequest& request,
    proto::internal::PausePrimaryResponse* response) {
  return stub_->PausePrimary(context, request, response);
}

grpc::Status GrpcInternalServiceClient::PreparePromote(
    grpc::ClientContext* context,
    const proto::internal::PreparePromoteRequest& request,
    proto::internal::PreparePromoteResponse* response) {
  return stub_->PreparePromote(context, request, response);
}

grpc::Status GrpcInternalServiceClient::BackupShard(
    grpc::ClientContext* context,
    const proto::internal::BackupShardRequest& request,
    proto::internal::BackupShardResponse* response) {
  return stub_->BackupShard(context, request, response);
}

grpc::Status GrpcInternalServiceClient::RestoreShard(
    grpc::ClientContext* context,
    const proto::internal::RestoreShardRequest& request,
    proto::internal::RestoreShardResponse* response) {
  return stub_->RestoreShard(context, request, response);
}

grpc::Status GrpcInternalServiceClient::FreezeWrites(
    grpc::ClientContext* context,
    const proto::internal::FreezeWritesRequest& request,
    proto::internal::FreezeWritesResponse* response) {
  return stub_->FreezeWrites(context, request, response);
}

grpc::Status GrpcInternalServiceClient::UnfreezeWrites(
    grpc::ClientContext* context,
    const proto::internal::UnfreezeWritesRequest& request,
    proto::internal::UnfreezeWritesResponse* response) {
  return stub_->UnfreezeWrites(context, request, response);
}

// ============================================================================
// GrpcInternalServiceClientFactory Implementation
// ============================================================================

std::unique_ptr<IInternalServiceClient> GrpcInternalServiceClientFactory::CreateClient(
    core::NodeId node_id,
    const std::string& address) {
  // Create gRPC channel
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

  // Create stub
  auto stub = proto::internal::InternalService::NewStub(channel);

  utils::Logger::Instance().Info("Created gRPC client for node {} at {}",
                                  core::ToUInt32(node_id), address);

  // Wrap in our interface
  return std::make_unique<GrpcInternalServiceClient>(std::move(stub));
}

}  // namespace cluster
}  // namespace gvdb