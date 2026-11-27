// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// VectorDBService: construction, the permission and write-gate checks, and
// the segment lookups that the handlers in this folder share.

#include "service/vectordb/vectordb_service.h"
#include <grpcpp/grpcpp.h>
#include <optional>
#include <unordered_set>
#include <utility>
#include "absl/strings/str_cat.h"
#include "auth/audit_context.h"
#include "auth/auth_context.h"
#include "cluster/datanode/primary_term_tracker.h"
#include "cluster/datanode/shard_write_gate.h"
#include "common/logger.h"
#include "core/status.h"
#include "internal.grpc.pb.h"
#include "rpc/primary_term_header.h"
#include "service/vectordb/collection_resolver.h"
#include "storage/backup/backup_manager.h"
#include "storage/backup/restore_manager.h"

namespace gvdb::service {

VectorDBService::VectorDBService(std::shared_ptr<storage::ISegmentStore> segment_store,
                                 std::shared_ptr<compute::QueryExecutor> query_executor,
                                 std::unique_ptr<ICollectionResolver> resolver,
                                 VectorDBServiceOptions options)
    : segment_store_(std::move(segment_store)),
      query_executor_(std::move(query_executor)),
      resolver_(std::move(resolver)),
      rbac_store_(std::move(options.rbac_store)),
      bulk_importer_(std::move(options.bulk_importer)),
      backup_manager_(std::move(options.backup_manager)),
      restore_manager_(std::move(options.restore_manager)),
      coordinator_(std::move(options.coordinator)),
      primary_term_tracker_(options.primary_term_tracker),
      shard_write_gate_(options.shard_write_gate) {
  common::Logger::Instance().Info(
      "VectorDBService initialized (RBAC {}, BulkImport {}, Backup {}, Restore {})",
      rbac_store_ ? "enabled" : "disabled", bulk_importer_ ? "enabled" : "disabled",
      backup_manager_ ? "enabled" : "disabled", restore_manager_ ? "enabled" : "disabled");
}

VectorDBService::~VectorDBService() = default;

grpc::Status VectorDBService::CheckPermission(auth::Permission perm,
                                              const std::string& collection_name) const {
  if (!rbac_store_) return grpc::Status::OK;

  const auto& key = auth::AuthContext::GetCurrentKey();
  if (key.empty()) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "No API key in auth context");
  }

  auto* role = rbac_store_->Lookup(key);
  if (!role) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Unknown API key");
  }

  if (!auth::HasPermission(role->role, perm)) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                        "Role does not have permission for this operation");
  }

  if (!collection_name.empty() && !auth::HasCollectionAccess(*role, collection_name)) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                        "Access denied for collection: " + collection_name);
  }

  return grpc::Status::OK;
}

grpc::Status VectorDBService::AuthorizeCollection(auth::Permission perm,
                                                  const std::string& collection_name,
                                                  std::optional<size_t> item_count) const {
  RETURN_IF_ERROR(CheckPermission(perm, collection_name));
  auth::AuditContext::SetCollection(collection_name);
  if (item_count) auth::AuditContext::SetItemCount(static_cast<int64_t>(*item_count));
  return grpc::Status::OK;
}

std::optional<core::Metadata> VectorDBService::FindMetadata(
    core::VectorId id, const std::vector<core::SegmentId>& segment_ids) const {
  for (auto seg_id : segment_ids) {
    auto* segment = segment_store_->GetSegment(seg_id);
    if (!segment) continue;
    auto meta = segment->GetMetadata(id);
    if (meta.ok()) return std::move(*meta);
  }
  return std::nullopt;
}

// ============================================================================
// Helpers
// ============================================================================

grpc::Status VectorDBService::EvaluateWriteGate(grpc::ServerContext* context,
                                                const std::string& collection_name,
                                                uint64_t sample_vector_id) {
  // Single-node / tests / query-nodes: no tracker plumbed in, gate is
  // a no-op for the primary-term check. The shard-freeze fence is
  // consulted independently: a single-node BackupManager binary can
  // still freeze writes during a snapshot even without a tracker.
  const bool has_tracker = primary_term_tracker_ != nullptr;
  const bool has_freeze_gate = shard_write_gate_ != nullptr;
  if (!has_tracker && !has_freeze_gate) return grpc::Status::OK;

  // Read the per-write term from the gRPC client metadata header. A
  // client that doesn't send the header is either a pre-1.x proxy
  // (rolling upgrade) or a single-node tool talking directly to the
  // data-node. Accept silently with a debug log so a real misconfig
  // is still observable in operator dashboards via metrics, not by
  // breaking the request.
  auto hdr = rpc::ReadPrimaryTermHeader(context);

  // Map the request to a shard via the same hash the proxy uses
  // (core::ShardIndexForVector). num_shards comes from the resolver's segment
  // list: every shard owns one segment by the ShardSegmentId scheme.
  // If the resolver can't answer (collection unknown locally), fall
  // through and let the regular handler return its own error.
  auto seg_ids = resolver_->GetSegmentIds(collection_name);
  if (!seg_ids.ok() || seg_ids->empty()) {
    common::Logger::Instance().Debug(
        "Write gate: resolver could not produce segment list for '{}'; "
        "deferring to handler-level error",
        collection_name);
    return grpc::Status::OK;
  }
  const auto num_shards = static_cast<uint32_t>(seg_ids->size());
  const uint32_t shard_id = core::ShardIndexForVector(sample_vector_id, num_shards);

  // Per-shard backup freeze: a frozen shard rejects writes for the
  // duration of an in-flight backup, regardless of primary-term state.
  // Checked BEFORE the primary-term check because a frozen shard's
  // term is fine: we just need writes to back off briefly.
  if (has_freeze_gate &&
      shard_write_gate_->IsFrozen(core::MakeShardId(static_cast<uint16_t>(shard_id)))) {
    return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                        absl::StrCat("shard ", shard_id,
                                     " is frozen for backup; retry after the backup completes"));
  }

  if (!has_tracker) return grpc::Status::OK;
  if (!hdr.has_term) {
    common::Logger::Instance().Debug("Write to '{}' has no {} header; accepting (back-compat path)",
                                     collection_name, rpc::kPrimaryTermHeader);
    return grpc::Status::OK;
  }
  if (hdr.parse_error) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        std::string("malformed ") + rpc::kPrimaryTermHeader + " header (must be decimal uint64)");
  }

  using D = cluster::PrimaryTermTracker::AcceptDecision;
  const D decision = primary_term_tracker_->EvaluateWrite(shard_id, hdr.term);
  switch (decision) {
    case D::Accept:
      return grpc::Status::OK;
    case D::StaleTerm: {
      // The proxy's cached routing has the wrong term. ABORTED tells
      // the proxy to re-route via fresh RouteQuery and retry: the
      // replica-fallback helper classifies ABORTED as transient.
      auto snap = primary_term_tracker_->Get(shard_id);
      return grpc::Status(grpc::StatusCode::ABORTED,
                          absl::StrCat("stale shard term for shard ", shard_id, " (request term=",
                                       hdr.term, ", current term=", snap.term, ")"));
    }
    case D::NotPrimary: {
      auto snap = primary_term_tracker_->Get(shard_id);
      return grpc::Status(grpc::StatusCode::ABORTED,
                          absl::StrCat("not primary for shard ", shard_id,
                                       " (current term=", snap.term, "); re-route via RouteQuery"));
    }
    case D::UnknownShard:
      // Heartbeat hasn't synced yet, OR the proxy's routing genuinely
      // points at a node that doesn't own the shard. Distinct code so
      // observers can tell the two apart. Caller (proxy) should re-
      // route via fresh RouteQuery; we don't tag it ABORTED because
      // the cure is the same but the diagnosis is different.
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          absl::StrCat("no primary record for shard ", shard_id,
                                       " on this data-node; re-route via RouteQuery"));
  }
  return grpc::Status::OK;
}

storage::Segment* VectorDBService::GetOrReplicateSegment(core::SegmentId segment_id) {
  auto* segment = segment_store_->GetSegment(segment_id);
  if (segment) return segment;

  // Segment not found locally: try pulling from coordinator if available
  auto* stub = resolver_->GetCoordinatorStub();
  if (!stub) return nullptr;

  common::Logger::Instance().Info("Segment {} not found locally, pulling from coordinator",
                                  static_cast<uint64_t>(core::ToUInt32(segment_id)));

  grpc::ClientContext context;
  proto::internal::GetSegmentRequest request;
  request.set_segment_id(static_cast<uint64_t>(core::ToUInt32(segment_id)));
  proto::internal::GetSegmentResponse response;

  auto grpc_status = stub->GetSegment(&context, request, &response);
  if (!grpc_status.ok()) {
    common::Logger::Instance().Error("Failed to fetch segment {} from coordinator: {}",
                                     static_cast<uint64_t>(core::ToUInt32(segment_id)),
                                     grpc_status.error_message());
    return nullptr;
  }

  if (!response.has_segment_info() || response.segment_data().empty()) {
    common::Logger::Instance().Warn("Segment {} not found on coordinator",
                                    static_cast<uint64_t>(core::ToUInt32(segment_id)));
    return nullptr;
  }

  auto segment_result = storage::Segment::DeserializeFromBytes(response.segment_data());
  if (!segment_result.ok()) {
    common::Logger::Instance().Error("Failed to deserialize segment {}: {}",
                                     response.segment_info().segment_id(),
                                     segment_result.status().message());
    return nullptr;
  }

  auto add_status = segment_store_->AddReplicatedSegment(std::move(segment_result.value()));
  if (!add_status.ok()) {
    common::Logger::Instance().Error("Failed to add replicated segment {}: {}",
                                     response.segment_info().segment_id(), add_status.message());
    return nullptr;
  }

  common::Logger::Instance().Info("Replicated segment {} ({} vectors)",
                                  response.segment_info().segment_id(),
                                  response.segment_info().vector_count());

  return segment_store_->GetSegment(segment_id);
}

absl::StatusOr<std::vector<storage::Segment*>> VectorDBService::CollectCollectionSegments(
    const std::string& collection_name) {
  // The resolver knows the shard-primary segments but not the rotated ones
  // auto-seal created; those live in the store keyed by collection id. Start
  // from the primaries (so the replica-fallback pull still applies to them),
  // then add every segment the store tracks for the collection.
  auto segment_ids_result = resolver_->GetSegmentIds(collection_name);
  if (!segment_ids_result.ok()) {
    return segment_ids_result.status();
  }
  const auto& shard_segment_ids = *segment_ids_result;
  if (shard_segment_ids.empty()) {
    return absl::NotFoundError("Segment not found");
  }

  std::vector<storage::Segment*> segments;
  std::unordered_set<uint32_t> seen;
  bool have_collection = false;
  core::CollectionId collection_id{};

  for (const auto& seg_id : shard_segment_ids) {
    auto* segment = GetOrReplicateSegment(seg_id);
    if (!segment) continue;
    if (!have_collection) {
      collection_id = segment->GetCollectionId();
      have_collection = true;
    }
    if (seen.insert(core::ToUInt32(seg_id)).second) {
      segments.push_back(segment);
    }
  }

  if (!have_collection) {
    return absl::NotFoundError("Segment not found");
  }

  for (const auto& seg_id : segment_store_->GetCollectionSegments(collection_id)) {
    if (!seen.insert(core::ToUInt32(seg_id)).second) continue;
    auto* segment = segment_store_->GetSegment(seg_id);
    if (segment) segments.push_back(segment);
  }

  return segments;
}

}  // namespace gvdb::service
