// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_UTILS_CHECKSUM_H_
#define GVDB_UTILS_CHECKSUM_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "core/status.h"

namespace gvdb {
namespace utils {

// ============================================================================
// xxHash64 - Fast non-cryptographic 64-bit checksum
// ============================================================================
//
// Used for integrity verification of backup archives and any other content
// where collision-resistance but not cryptographic strength is required.
// Values produced here are persisted to manifests, so the algorithm (xxHash
// XXH64 with seed = 0) is part of the on-disk contract. If we ever change
// algorithm, we must bump the manifest schema version.

// One-shot hash of a contiguous byte buffer. A null pointer with size 0 is
// valid and hashes to the empty-buffer digest.
[[nodiscard]] uint64_t XXH64Hash(const void* data, std::size_t size) noexcept;
[[nodiscard]] uint64_t XXH64Hash(std::string_view sv) noexcept;

// Formats a hash as "0x" + 16 lowercase hex chars (18 chars total). Always
// fully zero-padded so lexicographic ordering matches numeric ordering.
[[nodiscard]] std::string XXH64ToHex(uint64_t hash);

// Parses the output of XXH64ToHex. Accepts a leading "0x" prefix (case-
// insensitive) and requires exactly 16 hex digits after it. Returns
// InvalidArgumentError on malformed input.
[[nodiscard]] core::StatusOr<uint64_t> XXH64FromHex(std::string_view hex);

// RAII wrapper around the xxHash streaming state (XXH64_state_t). The hash
// algorithm itself is provided by the upstream xxHash library; this class
// exists only to (a) own and free the state pointer via Pimpl, and (b) keep
// xxhash.h out of this public header so consumers don't transitively inherit
// it.
//
// Usage:
//   XXH64Hasher h;
//   h.Update(chunk1);
//   h.Update(chunk2);
//   uint64_t digest = h.Finalize();
//
// Finalize() resets internal state so the hasher is reusable.
class XXH64Hasher {
 public:
  XXH64Hasher();
  ~XXH64Hasher();

  XXH64Hasher(const XXH64Hasher&) = delete;
  XXH64Hasher& operator=(const XXH64Hasher&) = delete;
  XXH64Hasher(XXH64Hasher&&) noexcept;
  XXH64Hasher& operator=(XXH64Hasher&&) noexcept;

  // Feed bytes into the hasher. Safe to call with size == 0.
  void Update(const void* data, std::size_t size);
  void Update(std::string_view sv);

  // Produce the final digest and reset the hasher so it can be reused.
  [[nodiscard]] uint64_t Finalize();

  // Abandon any pending state without producing a digest.
  void Reset();

 private:
  // Pimpl so that xxhash.h never leaks through this header.
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// Streams a file through XXH64Hasher in chunks and returns the digest.
// Returns NotFoundError if the path does not exist and InternalError on
// read failure. Uses a small internal buffer (64 KiB) — safe for files of
// any size.
[[nodiscard]] core::StatusOr<uint64_t> XXH64HashFile(std::string_view path);

}  // namespace utils
}  // namespace gvdb

#endif  // GVDB_UTILS_CHECKSUM_H_
