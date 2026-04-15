// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_PARQUET_READER_H_
#define GVDB_STORAGE_PARQUET_READER_H_

#include <memory>
#include <string>
#include <vector>

#include "core/metadata.h"
#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"

namespace gvdb {
namespace storage {

// Reads Parquet files following the GVDB schema convention:
//   - id column (int64)
//   - vector column (list<float32>)
//   - remaining columns → metadata (int64, double, string, bool)
//
// Iterates row-group-by-row-group to bound memory usage.
class ParquetReader {
 public:
  ~ParquetReader();

  // Open a Parquet file and validate schema.
  static core::StatusOr<std::unique_ptr<ParquetReader>> Open(
      const std::string& file_path,
      const std::string& vector_column = "vector",
      const std::string& id_column = "id");

  // Total number of rows across all row groups.
  [[nodiscard]] int64_t NumRows() const;

  // Vector dimension (inferred from first row group).
  [[nodiscard]] core::Dimension Dimension() const;

  // Number of row groups in the file.
  [[nodiscard]] int NumRowGroups() const;

  // Names of metadata columns (all columns except id and vector).
  [[nodiscard]] std::vector<std::string> MetadataColumns() const;

  // A chunk of vectors read from one row group.
  struct Chunk {
    std::vector<core::VectorId> ids;
    std::vector<core::Vector> vectors;
    std::vector<core::Metadata> metadata;
  };

  // Read the next row group. Returns empty chunk when exhausted.
  [[nodiscard]] core::StatusOr<Chunk> ReadNextChunk();

 private:
  ParquetReader() = default;

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_PARQUET_READER_H_
