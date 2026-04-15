// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/parquet_reader.h"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>

#include "absl/strings/str_cat.h"
#include "utils/logger.h"

namespace gvdb {
namespace storage {

// ============================================================================
// Impl — holds Arrow file reader state
// ============================================================================

struct ParquetReader::Impl {
  std::unique_ptr<parquet::arrow::FileReader> reader;
  std::string vector_column;
  std::string id_column;
  int64_t num_rows = 0;
  core::Dimension dimension = 0;
  int num_row_groups = 0;
  int next_row_group = 0;

  // Column indices in the Arrow schema (not Parquet schema)
  int id_col_idx = -1;
  int vec_col_idx = -1;
  std::vector<std::string> metadata_columns;
  std::vector<int> metadata_col_indices;
};

ParquetReader::~ParquetReader() = default;

// ============================================================================
// Open
// ============================================================================

core::StatusOr<std::unique_ptr<ParquetReader>> ParquetReader::Open(
    const std::string& file_path,
    const std::string& vector_column,
    const std::string& id_column) {

  // Open file
  auto file_result = arrow::io::ReadableFile::Open(file_path);
  if (!file_result.ok()) {
    return core::NotFoundError(
        absl::StrCat("Cannot open Parquet file: ", file_path,
                      " (", file_result.status().ToString(), ")"));
  }

  // Create Parquet reader via the new API (Arrow v23+)
  auto reader_result = parquet::arrow::OpenFile(
      *file_result, arrow::default_memory_pool());
  if (!reader_result.ok()) {
    return core::InvalidArgumentError(
        absl::StrCat("Failed to open Parquet file: ",
                      reader_result.status().ToString()));
  }
  auto arrow_reader = std::move(*reader_result);

  // Get Arrow schema
  std::shared_ptr<arrow::Schema> arrow_schema;
  auto schema_status = arrow_reader->GetSchema(&arrow_schema);
  if (!schema_status.ok()) {
    return core::InternalError(
        absl::StrCat("Failed to read Arrow schema: ",
                      schema_status.ToString()));
  }

  // Find id and vector columns in the Arrow schema
  int id_col_idx = arrow_schema->GetFieldIndex(id_column);
  int vec_col_idx = arrow_schema->GetFieldIndex(vector_column);

  if (id_col_idx < 0) {
    return core::InvalidArgumentError(
        absl::StrCat("ID column '", id_column,
                      "' not found in Parquet schema"));
  }
  if (vec_col_idx < 0) {
    return core::InvalidArgumentError(
        absl::StrCat("Vector column '", vector_column,
                      "' not found in Parquet schema"));
  }

  // Discover metadata columns (everything except id and vector)
  std::vector<std::string> metadata_cols;
  std::vector<int> metadata_indices;
  for (int i = 0; i < arrow_schema->num_fields(); ++i) {
    if (i != id_col_idx && i != vec_col_idx) {
      metadata_cols.push_back(arrow_schema->field(i)->name());
      metadata_indices.push_back(i);
    }
  }

  // Infer dimension from first row group
  core::Dimension dimension = 0;
  int64_t total_rows = arrow_reader->parquet_reader()->metadata()->num_rows();
  int num_row_groups = arrow_reader->parquet_reader()->metadata()->num_row_groups();

  if (total_rows > 0) {
    std::shared_ptr<arrow::Table> first_table;
    auto read_status = arrow_reader->ReadRowGroup(0, {vec_col_idx}, &first_table);
    if (!read_status.ok()) {
      return core::InternalError(
          absl::StrCat("Failed to read first row group: ",
                        read_status.ToString()));
    }
    auto vec_array = first_table->column(0)->chunk(0);
    auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(vec_array);
    if (!list_array || list_array->length() == 0) {
      return core::InvalidArgumentError(
          absl::StrCat("Vector column '", vector_column,
                        "' is not a list type or is empty"));
    }
    dimension = static_cast<core::Dimension>(list_array->value_length(0));
    if (dimension <= 0) {
      return core::InvalidArgumentError("Vector dimension is 0");
    }
  }

  // Re-open for fresh row group iteration
  auto file_result2 = arrow::io::ReadableFile::Open(file_path);
  if (!file_result2.ok()) {
    return core::InternalError("Failed to re-open Parquet file");
  }
  auto fresh_result = parquet::arrow::OpenFile(
      *file_result2, arrow::default_memory_pool());
  if (!fresh_result.ok()) {
    return core::InternalError("Failed to re-open Parquet reader");
  }

  auto reader = std::unique_ptr<ParquetReader>(new ParquetReader());
  reader->impl_ = std::make_unique<Impl>();
  reader->impl_->reader = std::move(*fresh_result);
  reader->impl_->vector_column = vector_column;
  reader->impl_->id_column = id_column;
  reader->impl_->num_rows = total_rows;
  reader->impl_->dimension = dimension;
  reader->impl_->num_row_groups = num_row_groups;
  reader->impl_->next_row_group = 0;
  reader->impl_->id_col_idx = id_col_idx;
  reader->impl_->vec_col_idx = vec_col_idx;
  reader->impl_->metadata_columns = std::move(metadata_cols);
  reader->impl_->metadata_col_indices = std::move(metadata_indices);

  utils::Logger::Instance().Info(
      "Opened Parquet file: {} ({} rows, {} row groups, dim={}, {} metadata cols)",
      file_path, total_rows, num_row_groups, dimension,
      reader->impl_->metadata_columns.size());

  return reader;
}

// ============================================================================
// Accessors
// ============================================================================

int64_t ParquetReader::NumRows() const { return impl_->num_rows; }
core::Dimension ParquetReader::Dimension() const { return impl_->dimension; }
int ParquetReader::NumRowGroups() const { return impl_->num_row_groups; }
std::vector<std::string> ParquetReader::MetadataColumns() const {
  return impl_->metadata_columns;
}

// ============================================================================
// ReadNextChunk
// ============================================================================

core::StatusOr<ParquetReader::Chunk> ParquetReader::ReadNextChunk() {
  if (impl_->next_row_group >= impl_->num_row_groups) {
    return Chunk{};  // exhausted
  }

  int group_idx = impl_->next_row_group++;

  // Build column index list: id + vector + metadata columns
  std::vector<int> col_indices;
  col_indices.push_back(impl_->id_col_idx);
  col_indices.push_back(impl_->vec_col_idx);
  for (int idx : impl_->metadata_col_indices) {
    col_indices.push_back(idx);
  }

  // Read row group
  std::shared_ptr<arrow::Table> table;
  auto status = impl_->reader->ReadRowGroup(group_idx, col_indices, &table);
  if (!status.ok()) {
    return core::InternalError(
        absl::StrCat("Failed to read row group ", group_idx, ": ",
                      status.ToString()));
  }

  int64_t num_rows = table->num_rows();
  if (num_rows == 0) return Chunk{};

  Chunk chunk;
  chunk.ids.reserve(static_cast<size_t>(num_rows));
  chunk.vectors.reserve(static_cast<size_t>(num_rows));
  chunk.metadata.resize(static_cast<size_t>(num_rows));

  // The table columns are in the order we requested:
  // [0] = id, [1] = vector, [2..] = metadata

  // Extract IDs (column 0)
  auto id_chunked = table->column(0);
  for (int c = 0; c < id_chunked->num_chunks(); ++c) {
    auto id_array = std::dynamic_pointer_cast<arrow::Int64Array>(
        id_chunked->chunk(c));
    if (!id_array) {
      return core::InvalidArgumentError(
          absl::StrCat("ID column '", impl_->id_column, "' is not int64"));
    }
    for (int64_t i = 0; i < id_array->length(); ++i) {
      chunk.ids.push_back(core::VectorId(
          static_cast<uint64_t>(id_array->Value(i))));
    }
  }

  // Extract vectors (column 1)
  auto vec_chunked = table->column(1);
  for (int c = 0; c < vec_chunked->num_chunks(); ++c) {
    auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(
        vec_chunked->chunk(c));
    if (!list_array) {
      return core::InvalidArgumentError(
          absl::StrCat("Vector column '", impl_->vector_column,
                        "' is not a list type"));
    }
    auto values = std::dynamic_pointer_cast<arrow::FloatArray>(
        list_array->values());
    if (!values) {
      return core::InvalidArgumentError(
          absl::StrCat("Vector column '", impl_->vector_column,
                        "' inner type is not float32"));
    }

    for (int64_t i = 0; i < list_array->length(); ++i) {
      int32_t start = list_array->value_offset(i);
      int32_t len = list_array->value_length(i);
      std::vector<float> vec_data(static_cast<size_t>(len));
      for (int32_t j = 0; j < len; ++j) {
        vec_data[static_cast<size_t>(j)] = values->Value(start + j);
      }
      chunk.vectors.emplace_back(std::move(vec_data));
    }
  }

  // Extract metadata columns (columns 2+)
  for (size_t m = 0; m < impl_->metadata_columns.size(); ++m) {
    const auto& col_name = impl_->metadata_columns[m];
    auto col_chunked = table->column(static_cast<int>(m + 2));

    size_t row_offset = 0;
    for (int c = 0; c < col_chunked->num_chunks(); ++c) {
      auto arr = col_chunked->chunk(c);

      for (int64_t i = 0; i < arr->length(); ++i) {
        if (arr->IsNull(i)) {
          row_offset++;
          continue;
        }

        core::MetadataValue val;
        switch (arr->type_id()) {
          case arrow::Type::INT64: {
            auto typed = std::static_pointer_cast<arrow::Int64Array>(arr);
            val = typed->Value(i);
            break;
          }
          case arrow::Type::DOUBLE: {
            auto typed = std::static_pointer_cast<arrow::DoubleArray>(arr);
            val = typed->Value(i);
            break;
          }
          case arrow::Type::FLOAT: {
            auto typed = std::static_pointer_cast<arrow::FloatArray>(arr);
            val = static_cast<double>(typed->Value(i));
            break;
          }
          case arrow::Type::STRING: {
            auto typed = std::static_pointer_cast<arrow::StringArray>(arr);
            val = typed->GetString(i);
            break;
          }
          case arrow::Type::LARGE_STRING: {
            auto typed = std::static_pointer_cast<arrow::LargeStringArray>(arr);
            val = typed->GetString(i);
            break;
          }
          case arrow::Type::BOOL: {
            auto typed = std::static_pointer_cast<arrow::BooleanArray>(arr);
            val = typed->Value(i);
            break;
          }
          case arrow::Type::INT32: {
            auto typed = std::static_pointer_cast<arrow::Int32Array>(arr);
            val = static_cast<int64_t>(typed->Value(i));
            break;
          }
          default:
            row_offset++;
            continue;
        }
        chunk.metadata[row_offset][col_name] = val;
        row_offset++;
      }
    }
  }

  return chunk;
}

}  // namespace storage
}  // namespace gvdb
