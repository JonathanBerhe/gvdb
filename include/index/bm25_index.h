#pragma once

#include <cmath>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "index/text_index.h"

namespace gvdb {
namespace index {

// BM25 (Okapi BM25) full-text search index.
// Implements the standard BM25 scoring formula with inverted index.
class BM25Index : public ITextIndex {
 public:
  explicit BM25Index(float k1 = 1.5f, float b = 0.75f);
  ~BM25Index() override = default;

  core::Status AddDocument(core::VectorId id, const std::string& text) override;

  [[nodiscard]] core::StatusOr<core::SearchResult> Search(
      const std::string& query, int k) override;

  [[nodiscard]] size_t GetDocumentCount() const override;
  [[nodiscard]] size_t GetMemoryUsage() const override;

 private:
  struct Posting {
    core::VectorId id;
    uint32_t term_freq;
  };

  // Tokenize text into lowercase terms
  static std::vector<std::string> Tokenize(const std::string& text);

  // Compute IDF for a term
  float ComputeIDF(uint32_t doc_freq) const;

  // Compute BM25 score for a single term in a document
  float ComputeTermScore(uint32_t term_freq, uint32_t doc_len, float idf) const;

  // Inverted index: term → posting list
  std::unordered_map<std::string, std::vector<Posting>> inverted_index_;

  // Document lengths (in tokens)
  std::unordered_map<uint64_t, uint32_t> doc_lengths_;

  // Corpus statistics
  uint32_t total_docs_ = 0;
  uint64_t total_doc_length_ = 0;

  // BM25 parameters
  float k1_;
  float b_;
};

}  // namespace index
}  // namespace gvdb
