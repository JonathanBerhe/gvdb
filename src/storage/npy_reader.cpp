// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/npy_reader.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <regex>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace storage {

// .npy magic: \x93NUMPY
static constexpr uint8_t kNpyMagic[] = {0x93, 'N', 'U', 'M', 'P', 'Y'};
static constexpr size_t kMagicLen = 6;

core::StatusOr<NpyHeader> NpyReader::ReadHeader(const std::string& file_path) {
  std::ifstream file(file_path, std::ios::binary);
  if (!file.is_open()) {
    return core::NotFoundError(
        absl::StrCat("Cannot open .npy file: ", file_path));
  }

  // Read magic bytes
  uint8_t magic[kMagicLen];
  file.read(reinterpret_cast<char*>(magic), kMagicLen);
  if (!file || std::memcmp(magic, kNpyMagic, kMagicLen) != 0) {
    return core::InvalidArgumentError(
        absl::StrCat("Not a valid .npy file (bad magic): ", file_path));
  }

  // Read version
  uint8_t major_version = 0;
  uint8_t minor_version = 0;
  file.read(reinterpret_cast<char*>(&major_version), 1);
  file.read(reinterpret_cast<char*>(&minor_version), 1);
  if (!file) {
    return core::InvalidArgumentError("Failed to read .npy version");
  }

  // Read header length (v1: 2 bytes LE, v2: 4 bytes LE)
  uint32_t header_len = 0;
  if (major_version == 1) {
    uint16_t len16 = 0;
    file.read(reinterpret_cast<char*>(&len16), 2);
    header_len = len16;
  } else if (major_version == 2 || major_version == 3) {
    file.read(reinterpret_cast<char*>(&header_len), 4);
  } else {
    return core::InvalidArgumentError(
        absl::StrCat("Unsupported .npy version: ", major_version, ".",
                      minor_version));
  }
  if (!file || header_len == 0 || header_len > 1024 * 1024) {
    return core::InvalidArgumentError("Invalid .npy header length");
  }

  // Read header string (Python dict)
  std::string header_str(header_len, '\0');
  file.read(header_str.data(), header_len);
  if (!file) {
    return core::InvalidArgumentError("Failed to read .npy header dict");
  }

  // Compute data offset
  size_t preamble_size = kMagicLen + 2 + (major_version == 1 ? 2 : 4);
  size_t data_offset = preamble_size + header_len;

  // Parse 'descr' — must be '<f4' (little-endian float32)
  std::regex descr_re(R"('descr'\s*:\s*'([^']+)')");
  std::smatch descr_match;
  if (!std::regex_search(header_str, descr_match, descr_re)) {
    return core::InvalidArgumentError(
        "Missing 'descr' in .npy header");
  }
  std::string descr = descr_match[1].str();
  if (descr != "<f4" && descr != "float32") {
    return core::InvalidArgumentError(
        absl::StrCat("Unsupported .npy dtype: '", descr,
                      "' (only '<f4' / float32 supported)"));
  }

  // Parse 'fortran_order' — must be False
  std::regex fortran_re(R"('fortran_order'\s*:\s*(True|False))");
  std::smatch fortran_match;
  if (std::regex_search(header_str, fortran_match, fortran_re)) {
    if (fortran_match[1].str() == "True") {
      return core::InvalidArgumentError(
          "Fortran-order .npy arrays are not supported (must be C-order)");
    }
  }

  // Parse 'shape' — expect (N,) or (N, D)
  std::regex shape_re(R"('shape'\s*:\s*\((\d+),\s*(\d+)\s*\))");
  std::smatch shape_match;
  if (!std::regex_search(header_str, shape_match, shape_re)) {
    // Try 1D shape (N,) — not supported for vectors
    std::regex shape_1d_re(R"('shape'\s*:\s*\((\d+),?\s*\))");
    std::smatch shape_1d_match;
    if (std::regex_search(header_str, shape_1d_match, shape_1d_re)) {
      return core::InvalidArgumentError(
          "1D .npy arrays are not supported (expected 2D shape (N, D))");
    }
    return core::InvalidArgumentError(
        "Failed to parse 'shape' from .npy header");
  }

  size_t num_vectors = std::stoull(shape_match[1].str());
  size_t dimension = std::stoull(shape_match[2].str());

  if (num_vectors == 0) {
    return core::InvalidArgumentError("Empty .npy file (0 vectors)");
  }
  if (dimension == 0) {
    return core::InvalidArgumentError("Zero-dimension vectors in .npy file");
  }

  return NpyHeader{num_vectors, dimension, data_offset};
}

core::StatusOr<std::vector<std::pair<core::VectorId, core::Vector>>>
NpyReader::ReadChunk(const std::string& file_path, const NpyHeader& header,
                     size_t start_row, size_t count, uint64_t start_id) {
  if (start_row >= header.num_vectors) {
    return std::vector<std::pair<core::VectorId, core::Vector>>{};
  }

  // Clamp count to available rows
  size_t actual_count = std::min(count, header.num_vectors - start_row);

  std::ifstream file(file_path, std::ios::binary);
  if (!file.is_open()) {
    return core::NotFoundError(
        absl::StrCat("Cannot open .npy file: ", file_path));
  }

  // Seek to the start of the chunk
  size_t row_bytes = header.dimension * sizeof(float);
  size_t offset = header.data_offset + start_row * row_bytes;
  file.seekg(static_cast<std::streamoff>(offset));
  if (!file) {
    return core::InternalError("Failed to seek in .npy file");
  }

  // Read all floats for this chunk at once
  size_t total_floats = actual_count * header.dimension;
  std::vector<float> buffer(total_floats);
  file.read(reinterpret_cast<char*>(buffer.data()),
            static_cast<std::streamsize>(total_floats * sizeof(float)));
  if (!file) {
    return core::InternalError(
        absl::StrCat("Failed to read ", actual_count,
                      " vectors from .npy file"));
  }

  // Build result
  std::vector<std::pair<core::VectorId, core::Vector>> result;
  result.reserve(actual_count);

  for (size_t i = 0; i < actual_count; ++i) {
    auto id = core::VectorId(start_id + i);
    std::vector<float> values(
        buffer.begin() + static_cast<ptrdiff_t>(i * header.dimension),
        buffer.begin() + static_cast<ptrdiff_t>((i + 1) * header.dimension));
    core::Vector vec(std::move(values));
    result.emplace_back(id, std::move(vec));
  }

  return result;
}

}  // namespace storage
}  // namespace gvdb
