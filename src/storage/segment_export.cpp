// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/segment_export.h"

#include <fstream>
#include <sstream>
#include <system_error>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace storage {

namespace {

constexpr const char* kMetadataFile = "metadata.txt";
constexpr const char* kVectorsFile = "vectors.bin";
constexpr const char* kIndexFile = "index.faiss";
constexpr const char* kGrowingFile = "growing.bin";

uint64_t FileSize(const std::filesystem::path& p) {
  std::error_code ec;
  auto sz = std::filesystem::file_size(p, ec);
  return ec ? 0 : sz;
}

// Render a minimal metadata.txt for a GROWING-segment snapshot. This is
// a human-readable companion to growing.bin; the bytes in growing.bin
// already carry the full segment header. We write the same key=value
// shape Segment::Flush uses so the file looks familiar to operators.
std::string RenderGrowingMetadata(const Segment& segment) {
  std::ostringstream ss;
  ss << "segment_id=" << core::ToUInt32(segment.GetId()) << "\n";
  ss << "collection_id=" << core::ToUInt32(segment.GetCollectionId()) << "\n";
  ss << "dimension=" << segment.GetDimension() << "\n";
  ss << "metric=" << static_cast<int>(segment.GetMetric()) << "\n";
  ss << "vector_count=" << segment.GetVectorCount() << "\n";
  ss << "index_type=" << static_cast<int>(segment.GetIndexType()) << "\n";
  ss << "state=growing\n";
  return ss.str();
}

// Read the full contents of a file into a string. Used by Import for
// growing.bin (which is a flat binary blob produced by SerializeToBytes).
core::StatusOr<std::string> ReadFileBytes(const std::filesystem::path& p) {
  std::ifstream in(p, std::ios::binary);
  if (!in.is_open()) {
    return core::NotFoundError(absl::StrCat("Cannot open: ", p.string()));
  }
  std::ostringstream ss;
  ss << in.rdbuf();
  if (in.bad()) {
    return core::InternalError(absl::StrCat("Read failed: ", p.string()));
  }
  return ss.str();
}

}  // namespace

// ============================================================================
// Export
// ============================================================================

core::StatusOr<SegmentExport> SegmentExporter::Export(
    const Segment& segment,
    const std::filesystem::path& flushed_segments_root,
    const std::filesystem::path& tmp_root) {

  SegmentExport out;
  out.segment_id = core::ToUInt32(segment.GetId());
  out.vector_count = segment.GetVectorCount();

  const auto state = segment.GetState();
  // FLUSHED is the post-Load shape — already a sealed-shape directory
  // on disk, so we treat it identically to SEALED.
  const bool is_sealed_on_disk =
      state == core::SegmentState::SEALED ||
      state == core::SegmentState::FLUSHED;

  if (is_sealed_on_disk) {
    out.is_growing = false;
    auto seg_dir = flushed_segments_root /
                   absl::StrCat("segment_", out.segment_id);
    std::error_code ec;
    if (!std::filesystem::exists(seg_dir, ec)) {
      return core::FailedPreconditionError(absl::StrCat(
          "Sealed segment ", out.segment_id,
          " has no flushed directory at ", seg_dir.string(),
          " — Flush() must run before backup"));
    }
    // metadata.txt and vectors.bin are mandatory; index.faiss is
    // expected for sealed segments but tolerated as missing if the
    // index hadn't been built (defensive).
    for (const char* name : {kMetadataFile, kVectorsFile, kIndexFile}) {
      auto p = seg_dir / name;
      if (!std::filesystem::exists(p, ec)) {
        if (name == std::string(kIndexFile)) continue;  // tolerate
        return core::FailedPreconditionError(absl::StrCat(
            "Sealed segment ", out.segment_id, " missing required file ",
            name));
      }
      auto sz = FileSize(p);
      out.total_size_bytes += sz;
      out.files.push_back(
          SegmentExportFile{name, p, sz});
    }
    return out;
  }

  // GROWING (or unsealed): snapshot via SerializeToBytes.
  out.is_growing = true;
  auto bytes_or = segment.SerializeToBytes();
  if (!bytes_or.ok()) {
    return bytes_or.status();
  }

  std::error_code ec;
  std::filesystem::create_directories(tmp_root, ec);
  // Use a per-segment subdir so concurrent exports on the same shard
  // can't collide.
  auto tmp_dir = tmp_root / absl::StrCat("segment_", out.segment_id);
  std::filesystem::remove_all(tmp_dir, ec);
  std::filesystem::create_directories(tmp_dir, ec);
  if (ec) {
    return core::InternalError(absl::StrCat(
        "Failed to create export tmp dir: ", tmp_dir.string(),
        ": ", ec.message()));
  }
  out.tmp_dir = tmp_dir;

  // Write growing.bin.
  auto growing_path = tmp_dir / kGrowingFile;
  {
    std::ofstream f(growing_path, std::ios::binary);
    if (!f.is_open()) {
      return core::InternalError(absl::StrCat(
          "Failed to open ", growing_path.string()));
    }
    f.write(bytes_or->data(),
            static_cast<std::streamsize>(bytes_or->size()));
    if (!f.good()) {
      return core::InternalError(absl::StrCat(
          "Failed to write ", growing_path.string()));
    }
  }
  auto growing_size = FileSize(growing_path);
  out.total_size_bytes += growing_size;
  out.files.push_back(SegmentExportFile{kGrowingFile, growing_path,
                                        growing_size});

  // Write metadata.txt companion.
  auto meta_path = tmp_dir / kMetadataFile;
  {
    std::ofstream f(meta_path);
    if (!f.is_open()) {
      return core::InternalError(absl::StrCat(
          "Failed to open ", meta_path.string()));
    }
    f << RenderGrowingMetadata(segment);
    if (!f.good()) {
      return core::InternalError(absl::StrCat(
          "Failed to write ", meta_path.string()));
    }
  }
  auto meta_size = FileSize(meta_path);
  out.total_size_bytes += meta_size;
  out.files.push_back(SegmentExportFile{kMetadataFile, meta_path, meta_size});

  return out;
}

// ============================================================================
// Import
// ============================================================================

core::StatusOr<std::unique_ptr<Segment>> SegmentExporter::Import(
    const std::filesystem::path& segment_dir,
    core::SegmentId segment_id) {

  std::error_code ec;
  if (!std::filesystem::is_directory(segment_dir, ec)) {
    return core::NotFoundError(absl::StrCat(
        "Segment import directory not found: ", segment_dir.string()));
  }

  auto growing_path = segment_dir / kGrowingFile;
  auto index_path = segment_dir / kIndexFile;
  auto vectors_path = segment_dir / kVectorsFile;

  const bool has_growing = std::filesystem::exists(growing_path, ec);
  const bool has_vectors = std::filesystem::exists(vectors_path, ec);

  if (has_growing) {
    auto bytes_or = ReadFileBytes(growing_path);
    if (!bytes_or.ok()) return bytes_or.status();
    auto seg_or = Segment::DeserializeFromBytes(*bytes_or);
    if (!seg_or.ok()) return seg_or.status();
    return std::move(*seg_or);
  }

  if (!has_vectors) {
    return core::InvalidArgumentError(absl::StrCat(
        "Segment import directory ", segment_dir.string(),
        " has neither growing.bin nor vectors.bin"));
  }

  // Sealed-layout: Segment::Load expects <base>/segment_<id>/<files>.
  // The caller is responsible for staging the files under that name
  // (typically the RestoreManager places them at
  //   <staging>/shard_<shard_id>/segment_<segment_id>/...).
  // Recover the base path = parent of segment_dir; Load wants the
  // *parent* of segment_<id>/.
  auto base_path = segment_dir.parent_path();
  return Segment::Load(base_path.string(), segment_id);
}

}  // namespace storage
}  // namespace gvdb
