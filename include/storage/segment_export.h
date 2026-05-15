// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_SEGMENT_EXPORT_H_
#define GVDB_STORAGE_SEGMENT_EXPORT_H_

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "core/status.h"
#include "core/types.h"
#include "storage/segment.h"

namespace gvdb {
namespace storage {

// ============================================================================
// SegmentExporter - package and reconstruct one segment for backup/restore
// ============================================================================
//
// Backup direction (`Export`):
//   - SEALED segments are already on disk under <flushed_root>/segment_<id>/
//     with files {metadata.txt, vectors.bin, index.faiss}. Export returns
//     paths pointing into that directory without any copy. The caller
//     uploads them and does not need to clean anything up.
//   - GROWING segments are not on disk. Export takes Segment::mutex_ via
//     Segment::SerializeToBytes() (under the segment's shared_lock) and
//     writes a `growing.bin` file plus a `metadata.txt` describing the
//     segment to a fresh per-segment subdirectory under `tmp_root`. The
//     caller deletes `tmp_dir` after upload.
//
// Restore direction (`Import`):
//   - Given a local directory containing the files produced by Export,
//     reconstruct a Segment. SEALED layout uses Segment::Load; GROWING
//     layout uses Segment::DeserializeFromBytes. The returned Segment
//     can be installed via ISegmentStore::AddReplicatedSegment.
//
// The exporter does NOT remap collection_id or segment_id. Restoring
// into a collection with a different collection_id is the caller's
// responsibility (see RestoreManager).

struct SegmentExportFile {
  // Display name of the file as it appears in the backup manifest:
  // one of "metadata.txt", "vectors.bin", "index.faiss", "growing.bin".
  std::string name;
  // Absolute path on this host to read the file from for upload.
  std::filesystem::path source_path;
  uint64_t size = 0;
};

struct SegmentExport {
  uint64_t segment_id = 0;
  bool is_growing = false;
  uint64_t vector_count = 0;
  uint64_t total_size_bytes = 0;
  std::vector<SegmentExportFile> files;
  // Directory the caller should delete after upload. Empty for SEALED
  // segments (their files live in the durable flushed-segments tree
  // and must NOT be deleted by the caller).
  std::filesystem::path tmp_dir;
};

class SegmentExporter {
 public:
  // Materialize a snapshot of the segment suitable for upload.
  // `flushed_segments_root` is the base path the segment manager uses
  // for Segment::Flush — i.e. the value such that the flushed sealed
  // segment lives at <flushed_segments_root>/segment_<id>/.
  // `tmp_root` is a writable directory used for GROWING-segment
  // snapshots.
  [[nodiscard]] static core::StatusOr<SegmentExport> Export(
      const Segment& segment,
      const std::filesystem::path& flushed_segments_root,
      const std::filesystem::path& tmp_root);

  // Inverse of Export. The directory must contain the files Export
  // produced (either SEALED-layout or GROWING-layout). Returns a
  // Segment owned by the caller, ready to hand to
  // ISegmentStore::AddReplicatedSegment.
  [[nodiscard]] static core::StatusOr<std::unique_ptr<Segment>> Import(
      const std::filesystem::path& segment_dir,
      core::SegmentId segment_id);
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_SEGMENT_EXPORT_H_
