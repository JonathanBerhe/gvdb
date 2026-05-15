// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_BACKUP_MANIFEST_H_
#define GVDB_STORAGE_BACKUP_MANIFEST_H_

#include <cstdint>
#include <string>
#include <vector>

#include "core/status.h"

namespace gvdb {
namespace storage {

// ============================================================================
// Backup manifest schemas (v1)
// ============================================================================
//
// Two manifests describe a backup:
//
// (1) Top-level manifest at <prefix>/backups/<backup_id>/backup.manifest.json
//     — describes the collection and lists per-shard manifests.
// (2) Per-shard manifest at <prefix>/backups/<backup_id>/shards/<shard_id>/
//     shard.manifest.json — lists every segment uploaded for that shard.
//
// The top-level manifest is written LAST, after every shard has succeeded.
// A restore that cannot find the top-level manifest treats the backup as
// non-existent, so a partial backup is never observable.
//
// JSON is hand-rolled to match the rest of this module (segment_manifest
// avoids the nlohmann/json dependency on purpose). The schema is flat and
// the parser is simple field-by-field; future fields are tolerated as
// unknown by the parser.

// Subset of collection metadata that must round-trip to a restore.
struct BackupCollectionMeta {
  uint32_t collection_id = 0;
  std::string collection_name;
  int32_t dimension = 0;
  int32_t metric = 0;       // MetricType as int
  int32_t index_type = 0;   // IndexType as int
  uint32_t num_shards = 0;
  uint32_t replication_factor = 0;
};

// Reference from the top-level manifest to a per-shard manifest.
struct BackupShardRef {
  uint32_t shard_id = 0;
  std::string manifest_key;  // Object key of the per-shard manifest.
};

struct BackupManifestV1 {
  uint32_t manifest_version = 1;
  std::string backup_id;
  BackupCollectionMeta collection;
  int64_t created_at_unix_ms = 0;
  uint64_t vector_count = 0;
  uint64_t size_bytes = 0;
  std::vector<BackupShardRef> shards;
  // Empty for a full backup. The current implementation always produces
  // full backups; this field is reserved for a future incremental.
  std::string incremental_from;
  std::string gvdb_version;
};

// One file belonging to a segment, recorded in the per-shard manifest.
struct ShardBackupFile {
  // One of: "metadata.txt", "vectors.bin", "index.faiss", "growing.bin".
  std::string name;
  // Object key (or filesystem path under the LocalTarget root).
  std::string key;
  uint64_t size = 0;
};

struct ShardBackupSegment {
  uint64_t segment_id = 0;
  // "sealed" or "growing". A sealed segment was on disk and copied as-is;
  // a growing segment was serialized via Segment::SerializeToBytes() under
  // its shared_lock and emitted as growing.bin.
  std::string state;
  uint64_t vector_count = 0;
  uint64_t size_bytes = 0;
  std::vector<ShardBackupFile> files;
};

struct ShardManifestV1 {
  uint32_t manifest_version = 1;
  uint32_t shard_id = 0;
  uint32_t node_id = 0;
  uint64_t primary_term = 0;
  std::vector<ShardBackupSegment> segments;
};

// ============================================================================
// BackupManifest - JSON serialization for both manifest kinds
// ============================================================================
class BackupManifest {
 public:
  [[nodiscard]] static std::string SerializeTop(const BackupManifestV1& m);
  [[nodiscard]] static core::StatusOr<BackupManifestV1> DeserializeTop(
      const std::string& json);

  [[nodiscard]] static std::string SerializeShard(const ShardManifestV1& m);
  [[nodiscard]] static core::StatusOr<ShardManifestV1> DeserializeShard(
      const std::string& json);

  // Conventional object keys / paths. All keys are slash-delimited and
  // relative to the backup root (which is itself relative to the
  // IObjectStore's namespace, e.g. <S3 prefix>/ or <LocalTarget root>/).
  static std::string TopManifestKey(const std::string& backup_id);
  static std::string ShardManifestKey(const std::string& backup_id,
                                      uint32_t shard_id);
  static std::string SegmentFileKey(const std::string& backup_id,
                                    uint32_t shard_id,
                                    uint64_t segment_id,
                                    const std::string& filename);
  static std::string BackupRootPrefix(const std::string& backup_id);
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_BACKUP_MANIFEST_H_
