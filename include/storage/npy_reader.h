// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_NPY_READER_H_
#define GVDB_STORAGE_NPY_READER_H_

#include <string>
#include <vector>

#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"

namespace gvdb {
namespace storage {

// Parsed header from a NumPy .npy file.
struct NpyHeader {
  size_t num_vectors;   // Number of rows (first dimension of shape)
  size_t dimension;     // Vector dimension (second dimension of shape)
  size_t data_offset;   // Byte offset where float32 data begins
};

// Reads NumPy .npy files containing 2D float32 arrays.
//
// The .npy format (v1/v2):
//   - 6-byte magic: \x93NUMPY
//   - 1 byte major version, 1 byte minor version
//   - 2-byte (v1) or 4-byte (v2) header length
//   - Python dict: {'descr': '<f4', 'fortran_order': False, 'shape': (N, D)}
//   - N * D * 4 bytes of contiguous float32 data
//
// Only little-endian float32 ('<f4') with C order (fortran_order=False)
// and 2D shape are supported.
class NpyReader {
 public:
  // Parse the header of a .npy file. Returns vector count and dimension.
  [[nodiscard]] static core::StatusOr<NpyHeader> ReadHeader(
      const std::string& file_path);

  // Read a chunk of vectors from [start_row, start_row + count).
  // Generates sequential VectorIds starting from start_id.
  // count is clamped to available rows.
  [[nodiscard]] static core::StatusOr<
      std::vector<std::pair<core::VectorId, core::Vector>>>
  ReadChunk(const std::string& file_path, const NpyHeader& header,
            size_t start_row, size_t count, uint64_t start_id);
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_NPY_READER_H_
