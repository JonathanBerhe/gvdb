// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// The two-phase primary swap of a shard.

#include "absl/strings/str_cat.h"
#include "cluster/coordinator/coordinator.h"
#include "cluster/coordinator/internal_client.h"
#include "common/logger.h"
#include "core/status.h"

namespace gvdb::cluster {

// Two-phase primary swap. Bumps the per-shard term, pauses the old
// primary, prepares the new primary, then commits the assignment in
// ShardManager. Each step is idempotent on the data-node side so a
// reconcile retry after a transient failure is safe; we surface the
// first failing status and let the caller's reconcile loop pick up
// next pass.
//
// `old_node` may be kInvalidNodeId (cold-promote case, no current
// primary, e.g. during recovery from a multi-failure where the shard
// was unowned). PausePrimary is then skipped.
absl::Status Coordinator::TransferPrimary(core::ShardId shard_id, core::NodeId old_node,
                                          core::NodeId new_node) {
  if (new_node == core::kInvalidNodeId) {
    return absl::InvalidArgumentError(
        absl::StrCat("TransferPrimary: new_node must be a valid node id (shard ",
                     core::ToUInt16(shard_id), ")"));
  }
  if (new_node == old_node) {
    return absl::InvalidArgumentError(absl::StrCat("TransferPrimary: old_node == new_node (shard ",
                                                   core::ToUInt16(shard_id), ", node ",
                                                   core::ToUInt32(new_node), "): refusing no-op"));
  }

  // 1. Compute the next term. We must read the current term atomically
  //    with the primary so a concurrent rebalance can't pair us with a
  //    primary from a different swap. If the shard has no entry yet
  //    (NotFound), treat current_term as 0; the cold-promote bumps to 1.
  uint64_t current_term = 0;
  {
    auto view = shard_manager_->GetPrimaryNodeAndTerm(shard_id);
    if (view.ok()) {
      current_term = view->term;
    }
    // NotFound is acceptable: shard was never claimed (cold-promote).
  }
  const uint64_t new_term = current_term + 1;

  // 2. PausePrimary on the old node. Skipped when old_node is invalid
  //    (cold-promote: no one currently owns the shard).
  if (old_node != core::kInvalidNodeId) {
    auto* old_client = GetOrCreateDataNodeClient(old_node);
    if (!old_client) {
      // No client typically means single-node / non-distributed mode
      // (factory not wired): fall through to the commit step. A real
      // distributed cluster with a missing client is a transient
      // unreachability we surface as Unavailable.
      if (client_factory_) {
        return absl::UnavailableError(absl::StrCat("TransferPrimary: cannot dial old primary node ",
                                                   core::ToUInt32(old_node), " for shard ",
                                                   core::ToUInt16(shard_id)));
      }
    } else {
      proto::internal::PausePrimaryRequest req;
      req.set_shard_id(core::ToUInt16(shard_id));
      req.set_new_term(new_term);
      req.set_new_primary_node_id(core::ToUInt32(new_node));
      proto::internal::PausePrimaryResponse resp;
      grpc::ClientContext ctx;
      auto status = old_client->PausePrimary(&ctx, req, &resp);
      if (!status.ok()) {
        return absl::UnavailableError(absl::StrCat(
            "TransferPrimary: PausePrimary on node ", core::ToUInt32(old_node), " for shard ",
            core::ToUInt16(shard_id), " failed: ", status.error_message()));
      }
      if (!resp.success()) {
        return absl::FailedPreconditionError(
            absl::StrCat("TransferPrimary: PausePrimary on node ", core::ToUInt32(old_node),
                         " for shard ", core::ToUInt16(shard_id), " refused: ", resp.message()));
      }
    }
  }

  // 3. PreparePromote on the new node. Required: the new primary must
  //    accept writes at new_term before we commit, or proxies will
  //    route to a node that rejects them.
  auto* new_client = GetOrCreateDataNodeClient(new_node);
  if (!new_client) {
    if (client_factory_) {
      return absl::UnavailableError(absl::StrCat("TransferPrimary: cannot dial new primary node ",
                                                 core::ToUInt32(new_node), " for shard ",
                                                 core::ToUInt16(shard_id)));
    }
    // Single-node / tests: no prepare step needed; fall through to commit.
  } else {
    proto::internal::PreparePromoteRequest req;
    req.set_shard_id(core::ToUInt16(shard_id));
    req.set_new_term(new_term);
    proto::internal::PreparePromoteResponse resp;
    grpc::ClientContext ctx;
    auto status = new_client->PreparePromote(&ctx, req, &resp);
    if (!status.ok()) {
      return absl::UnavailableError(absl::StrCat(
          "TransferPrimary: PreparePromote on node ", core::ToUInt32(new_node), " for shard ",
          core::ToUInt16(shard_id), " failed: ", status.error_message()));
    }
    if (!resp.success()) {
      return absl::FailedPreconditionError(
          absl::StrCat("TransferPrimary: PreparePromote on node ", core::ToUInt32(new_node),
                       " for shard ", core::ToUInt16(shard_id), " refused: ", resp.message()));
    }
  }

  // 4. Commit the assignment with the explicit term. Term-aware overload
  //    rejects regression so a concurrent swap that already advanced
  //    the term gets surfaced rather than silently overwritten.
  RETURN_IF_ERROR(shard_manager_->SetPrimaryNode(shard_id, new_node, new_term));

  common::Logger::Instance().Info("TransferPrimary: shard={} old_node={} new_node={} new_term={}",
                                  core::ToUInt16(shard_id),
                                  old_node == core::kInvalidNodeId ? 0u : core::ToUInt32(old_node),
                                  core::ToUInt32(new_node), new_term);
  PersistRegistry();
  return absl::OkStatus();
}

}  // namespace gvdb::cluster
