#include "index/bm25_index.h"

#include <algorithm>
#include <cctype>
#include <queue>
#include <sstream>
#include <unordered_set>

namespace gvdb {
namespace index {

BM25Index::BM25Index(float k1, float b) : k1_(k1), b_(b) {}

std::vector<std::string> BM25Index::Tokenize(const std::string& text) {
  std::vector<std::string> tokens;
  std::string current;
  for (char c : text) {
    if (std::isalnum(static_cast<unsigned char>(c))) {
      current += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    } else {
      if (!current.empty()) {
        tokens.push_back(std::move(current));
        current.clear();
      }
    }
  }
  if (!current.empty()) {
    tokens.push_back(std::move(current));
  }
  return tokens;
}

core::Status BM25Index::AddDocument(core::VectorId id, const std::string& text) {
  auto tokens = Tokenize(text);
  if (tokens.empty()) {
    return absl::OkStatus();
  }

  uint64_t id_val = core::ToUInt64(id);
  doc_lengths_[id_val] = static_cast<uint32_t>(tokens.size());
  total_doc_length_ += tokens.size();
  total_docs_++;

  // Count term frequencies
  std::unordered_map<std::string, uint32_t> term_freqs;
  for (const auto& token : tokens) {
    term_freqs[token]++;
  }

  // Add to inverted index
  for (const auto& [term, freq] : term_freqs) {
    inverted_index_[term].push_back(Posting{id, freq});
  }

  return absl::OkStatus();
}

float BM25Index::ComputeIDF(uint32_t doc_freq) const {
  float N = static_cast<float>(total_docs_);
  float df = static_cast<float>(doc_freq);
  // Lucene variant: always positive, never zero
  return std::log(1.0f + (N - df + 0.5f) / (df + 0.5f));
}

float BM25Index::ComputeTermScore(uint32_t term_freq, uint32_t doc_len,
                                   float idf) const {
  float tf = static_cast<float>(term_freq);
  float dl = static_cast<float>(doc_len);
  float avgdl = (total_docs_ > 0)
                    ? static_cast<float>(total_doc_length_) / total_docs_
                    : 1.0f;

  float numerator = tf * (k1_ + 1.0f);
  float denominator = tf + k1_ * (1.0f - b_ + b_ * dl / avgdl);
  return idf * numerator / denominator;
}

core::StatusOr<core::SearchResult> BM25Index::Search(
    const std::string& query, int k) {
  if (k <= 0) {
    return core::InvalidArgumentError("k must be positive");
  }
  if (total_docs_ == 0) {
    return core::SearchResult{};
  }

  auto query_tokens = Tokenize(query);
  if (query_tokens.empty()) {
    return core::SearchResult{};
  }

  // Deduplicate query terms
  std::unordered_set<std::string> unique_terms(query_tokens.begin(),
                                                query_tokens.end());

  // Accumulate BM25 scores per document
  std::unordered_map<uint64_t, float> scores;

  for (const auto& term : unique_terms) {
    auto it = inverted_index_.find(term);
    if (it == inverted_index_.end()) continue;

    const auto& postings = it->second;
    float idf = ComputeIDF(static_cast<uint32_t>(postings.size()));

    // Skip negative IDF (safety net — Lucene variant is always positive)
    if (idf < 0.0f) continue;

    for (const auto& posting : postings) {
      uint64_t id_val = core::ToUInt64(posting.id);
      auto len_it = doc_lengths_.find(id_val);
      uint32_t doc_len = (len_it != doc_lengths_.end()) ? len_it->second : 1;
      scores[id_val] += ComputeTermScore(posting.term_freq, doc_len, idf);
    }
  }

  // Extract top-k using min-heap
  using ScorePair = std::pair<float, uint64_t>;
  std::priority_queue<ScorePair, std::vector<ScorePair>, std::greater<>> heap;

  for (const auto& [id_val, score] : scores) {
    if (static_cast<int>(heap.size()) < k) {
      heap.push({score, id_val});
    } else if (score > heap.top().first) {
      heap.pop();
      heap.push({score, id_val});
    }
  }

  // Build result (highest score first)
  core::SearchResult result;
  while (!heap.empty()) {
    auto [score, id_val] = heap.top();
    heap.pop();
    // Use score as "distance" (higher = better, inverted from vector distance)
    result.entries.emplace_back(core::MakeVectorId(id_val), score);
  }
  std::reverse(result.entries.begin(), result.entries.end());

  return result;
}

size_t BM25Index::GetDocumentCount() const {
  return total_docs_;
}

size_t BM25Index::GetMemoryUsage() const {
  size_t usage = 0;
  for (const auto& [term, postings] : inverted_index_) {
    usage += term.capacity() + postings.capacity() * sizeof(Posting);
  }
  usage += doc_lengths_.size() * (sizeof(uint64_t) + sizeof(uint32_t));
  return usage;
}

}  // namespace index
}  // namespace gvdb
