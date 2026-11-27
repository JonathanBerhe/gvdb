// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// The collection registry and the shard map on disk, as JSON.

#include <mutex>
#include <sstream>
#include <utility>
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "cluster/coordinator/coordinator.h"
#include "common/file_util.h"
#include "common/json_util.h"
#include "common/logger.h"

namespace gvdb::cluster {

// =============================================================================
// Registry persistence
// =============================================================================

void Coordinator::PersistRegistry() const {
  std::shared_lock lock(collection_mutex_);
  PersistRegistryLocked();
}

void Coordinator::PersistRegistryLocked() const {
  if (registry_path_.empty()) return;

  std::ostringstream ss;
  ss << "{\"version\":1"
     << ",\"next_collection_id\":" << next_collection_id_.load(std::memory_order_relaxed)
     << ",\"collections\":[";
  bool first = true;
  for (const auto& [id, meta] : collections_) {
    if (!first) ss << ",";
    first = false;
    ss << "{\"collection_id\":" << core::ToUInt32(id) << ",\"name\":\""
       << common::json::EscapeJson(meta.collection_name) << "\""
       << ",\"dimension\":" << meta.dimension
       << ",\"metric\":" << std::to_underlying(meta.metric_type)
       << ",\"index_type\":" << std::to_underlying(meta.index_type)
       << ",\"num_shards\":" << meta.num_shards
       << ",\"replication_factor\":" << meta.replication_factor
       << ",\"created_at\":" << meta.created_at << ",\"updated_at\":" << meta.updated_at
       << ",\"shard_ids\":[";
    for (size_t i = 0; i < meta.shard_ids.size(); ++i) {
      if (i) ss << ",";
      ss << core::ToUInt16(meta.shard_ids[i]);
    }
    ss << "]}";
  }
  ss << "],\"shards\":[";
  first = true;
  for (const auto& shard : shard_manager_->GetAllShards()) {
    // Only assignments matter for recovery; untouched pool entries
    // (term 0, no primary) are recreated by the ShardManager constructor.
    if (shard.primary_term == 0 && shard.primary_node == core::kInvalidNodeId) {
      continue;
    }
    if (!first) ss << ",";
    first = false;
    ss << "{\"shard_id\":" << core::ToUInt16(shard.shard_id)
       << ",\"primary_node\":" << core::ToUInt32(shard.primary_node)
       << ",\"primary_term\":" << shard.primary_term << ",\"replicas\":[";
    for (size_t i = 0; i < shard.replica_nodes.size(); ++i) {
      if (i) ss << ",";
      ss << core::ToUInt32(shard.replica_nodes[i]);
    }
    ss << "]}";
  }
  ss << "]}";

  if (auto status = common::WriteFileAtomically(registry_path_, ss.str()); !status.ok()) {
    common::Logger::Instance().Error("Failed to persist the coordinator registry: {}",
                                     status.message());
  }
}

absl::Status Coordinator::LoadRegistry() {
  std::unique_lock lock(collection_mutex_);
  if (registry_path_.empty()) {
    return absl::FailedPreconditionError("LoadRegistry needs CoordinatorOptions::registry_dir");
  }

  auto read = common::ReadFileToString(registry_path_);
  if (absl::IsNotFound(read.status())) {
    common::Logger::Instance().Info("No coordinator registry at {} (first boot)", registry_path_);
    return absl::OkStatus();
  }
  if (!read.ok()) return read.status();
  const std::string doc = std::move(*read);

  uint32_t next_id = 0;
  if (common::json::ParseUint32(doc, "next_collection_id", next_id) && next_id > 0) {
    next_collection_id_.store(next_id, std::memory_order_relaxed);
  }

  auto collections_body = common::json::ExtractArrayBody(doc, "collections");
  size_t loaded = 0;
  for (const auto& obj : common::json::SplitObjects(collections_body)) {
    uint32_t cid = 0;
    std::string name;
    if (!common::json::ParseUint32(obj, "collection_id", cid) ||
        !common::json::ParseString(obj, "name", name)) {
      return absl::DataLossError(
          absl::StrCat("Malformed collection entry in ", registry_path_, ": ", obj));
    }

    meta::CollectionMetadata meta;
    meta.collection_id = core::MakeCollectionId(cid);
    meta.collection_name = name;
    uint32_t dim = 0;
    if (common::json::ParseUint32(obj, "dimension", dim)) {
      meta.dimension = static_cast<core::Dimension>(dim);
    }
    int32_t metric = 0;
    if (common::json::ParseInt32(obj, "metric", metric)) {
      meta.metric_type = static_cast<core::MetricType>(metric);
    }
    int32_t index_type = 0;
    if (common::json::ParseInt32(obj, "index_type", index_type)) {
      meta.index_type = static_cast<core::IndexType>(index_type);
    }
    uint64_t num_shards = 1;
    if (common::json::ParseUint64(obj, "num_shards", num_shards)) {
      meta.num_shards = static_cast<size_t>(num_shards);
    }
    uint64_t replication = 1;
    if (common::json::ParseUint64(obj, "replication_factor", replication)) {
      meta.replication_factor = static_cast<size_t>(replication);
    }
    (void)common::json::ParseUint64(obj, "created_at", meta.created_at);
    (void)common::json::ParseUint64(obj, "updated_at", meta.updated_at);

    auto shard_ids_body = common::json::ExtractArrayBody(obj, "shard_ids");
    if (shard_ids_body.size() >= 2) {
      std::string inner(shard_ids_body.substr(1, shard_ids_body.size() - 2));
      for (absl::string_view piece : absl::StrSplit(inner, ',', absl::SkipEmpty())) {
        uint32_t sid = 0;
        if (absl::SimpleAtoi(piece, &sid)) {
          meta.shard_ids.push_back(core::MakeShardId(static_cast<uint16_t>(sid)));
        }
      }
    }

    collections_[meta.collection_id] = meta;
    collection_name_to_id_[name] = meta.collection_id;
    ++loaded;
  }

  auto shards_body = common::json::ExtractArrayBody(doc, "shards");
  size_t restored_shards = 0;
  for (const auto& obj : common::json::SplitObjects(shards_body)) {
    uint32_t shard_raw = 0;
    uint32_t primary_raw = 0;
    uint64_t term = 0;
    if (!common::json::ParseUint32(obj, "shard_id", shard_raw) ||
        !common::json::ParseUint32(obj, "primary_node", primary_raw) ||
        !common::json::ParseUint64(obj, "primary_term", term)) {
      continue;
    }
    if (term == 0) continue;
    auto shard_id = core::MakeShardId(static_cast<uint16_t>(shard_raw));
    // Term-explicit setter rejects regressions; on a fresh ShardManager
    // (all terms 0) restoring the persisted term always applies.
    auto status = shard_manager_->SetPrimaryNode(shard_id, core::MakeNodeId(primary_raw), term);
    if (!status.ok()) {
      common::Logger::Instance().Warn("Registry: could not restore primary for shard {}: {}",
                                      shard_raw, status.message());
      continue;
    }
    auto replicas_body = common::json::ExtractArrayBody(obj, "replicas");
    if (replicas_body.size() >= 2) {
      std::string inner(replicas_body.substr(1, replicas_body.size() - 2));
      for (absl::string_view piece : absl::StrSplit(inner, ',', absl::SkipEmpty())) {
        uint32_t rid = 0;
        if (absl::SimpleAtoi(piece, &rid)) {
          (void)shard_manager_->AddReplica(shard_id, core::MakeNodeId(rid));
        }
      }
    }
    ++restored_shards;
  }

  common::Logger::Instance().Info(
      "Loaded coordinator registry from {}: {} collection(s), {} shard "
      "assignment(s), next_collection_id={}",
      registry_path_, loaded, restored_shards, next_collection_id_.load(std::memory_order_relaxed));
  return absl::OkStatus();
}

}  // namespace gvdb::cluster
