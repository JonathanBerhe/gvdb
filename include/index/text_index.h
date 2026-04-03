#pragma once

#include <memory>
#include <string>

#include "core/status.h"
#include "core/types.h"

namespace gvdb {
namespace index {

// Interface for text-based search indexes (BM25, inverted index, etc.)
class ITextIndex {
 public:
  virtual ~ITextIndex() = default;

  // Add a document to the index
  virtual core::Status AddDocument(core::VectorId id, const std::string& text) = 0;

  // Search for documents matching the query. Returns scored results.
  [[nodiscard]] virtual core::StatusOr<core::SearchResult> Search(
      const std::string& query, int k) = 0;

  // Number of indexed documents
  [[nodiscard]] virtual size_t GetDocumentCount() const = 0;

  // Approximate memory usage in bytes
  [[nodiscard]] virtual size_t GetMemoryUsage() const = 0;
};

}  // namespace index
}  // namespace gvdb
