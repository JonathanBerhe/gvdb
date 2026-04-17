// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "utils/checksum.h"

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <system_error>

#include "absl/strings/str_cat.h"

// Keep xxHash's definitions out of the public header. XXH_INLINE_ALL pulls the
// implementation into this translation unit only.
#define XXH_INLINE_ALL
#include "xxhash.h"

namespace gvdb {
namespace utils {

namespace {

// Seed used for all XXH64 digests in GVDB. Fixed at 0 so values are
// reproducible across runs and machines; persisted in backup manifests.
constexpr XXH64_hash_t kHashSeed = 0;

// Chunk size for streaming file reads. 64 KiB balances syscall overhead
// against stack/heap usage; small enough to stay on the heap safely.
constexpr std::size_t kFileReadChunkBytes = 64 * 1024;

}  // namespace

// ============================================================================
// One-shot API
// ============================================================================

uint64_t XXH64Hash(const void* data, std::size_t size) noexcept {
  return XXH64(data, size, kHashSeed);
}

uint64_t XXH64Hash(std::string_view sv) noexcept {
  return XXH64(sv.data(), sv.size(), kHashSeed);
}

// ============================================================================
// Hex formatting
// ============================================================================

std::string XXH64ToHex(uint64_t hash) {
  // 18 chars: "0x" + 16 hex digits + nul. absl::StrCat doesn't zero-pad
  // hex widths by default, so format manually to guarantee 16 digits.
  std::array<char, 19> buf{};
  const int written = std::snprintf(buf.data(), buf.size(), "0x%016llx",
                                    static_cast<unsigned long long>(hash));
  // snprintf guarantees exactly 18 chars on success; defensive check only.
  if (written != 18) {
    return "0x0000000000000000";
  }
  return std::string(buf.data(), 18);
}

core::StatusOr<uint64_t> XXH64FromHex(std::string_view hex) {
  std::string_view payload = hex;
  if (payload.size() >= 2 && payload[0] == '0' &&
      (payload[1] == 'x' || payload[1] == 'X')) {
    payload.remove_prefix(2);
  }
  if (payload.size() != 16) {
    return core::InvalidArgumentError(absl::StrCat(
        "XXH64FromHex: expected 16 hex digits after optional 0x prefix, got ",
        payload.size(), " (input=\"", hex, "\")"));
  }

  uint64_t value = 0;
  for (char c : payload) {
    uint64_t nibble = 0;
    if (c >= '0' && c <= '9') {
      nibble = static_cast<uint64_t>(c - '0');
    } else if (c >= 'a' && c <= 'f') {
      nibble = static_cast<uint64_t>(c - 'a' + 10);
    } else if (c >= 'A' && c <= 'F') {
      nibble = static_cast<uint64_t>(c - 'A' + 10);
    } else {
      return core::InvalidArgumentError(
          absl::StrCat("XXH64FromHex: non-hex character in input \"", hex, "\""));
    }
    value = (value << 4) | nibble;
  }
  return value;
}

// ============================================================================
// Streaming hasher
// ============================================================================

struct XXH64Hasher::Impl {
  XXH64_state_t* state = nullptr;

  Impl() : state(XXH64_createState()) {
    if (state != nullptr) {
      XXH64_reset(state, kHashSeed);
    }
  }

  ~Impl() {
    if (state != nullptr) {
      XXH64_freeState(state);
    }
  }

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl(Impl&&) = delete;
  Impl& operator=(Impl&&) = delete;
};

XXH64Hasher::XXH64Hasher() : impl_(std::make_unique<Impl>()) {}

XXH64Hasher::~XXH64Hasher() = default;

XXH64Hasher::XXH64Hasher(XXH64Hasher&&) noexcept = default;

XXH64Hasher& XXH64Hasher::operator=(XXH64Hasher&&) noexcept = default;

void XXH64Hasher::Update(const void* data, std::size_t size) {
  if (size == 0 || impl_->state == nullptr) {
    return;
  }
  XXH64_update(impl_->state, data, size);
}

void XXH64Hasher::Update(std::string_view sv) {
  Update(sv.data(), sv.size());
}

uint64_t XXH64Hasher::Finalize() {
  if (impl_->state == nullptr) {
    return 0;
  }
  const uint64_t digest = XXH64_digest(impl_->state);
  XXH64_reset(impl_->state, kHashSeed);
  return digest;
}

void XXH64Hasher::Reset() {
  if (impl_->state != nullptr) {
    XXH64_reset(impl_->state, kHashSeed);
  }
}

// ============================================================================
// File helper
// ============================================================================

core::StatusOr<uint64_t> XXH64HashFile(std::string_view path) {
  const std::string path_str(path);

  std::error_code ec;
  if (!std::filesystem::exists(path_str, ec) || ec) {
    return core::NotFoundError(
        absl::StrCat("XXH64HashFile: file not found: ", path_str));
  }
  if (std::filesystem::is_directory(path_str, ec)) {
    return core::InvalidArgumentError(
        absl::StrCat("XXH64HashFile: path is a directory: ", path_str));
  }

  std::ifstream in(path_str, std::ios::binary);
  if (!in.is_open()) {
    return core::InternalError(
        absl::StrCat("XXH64HashFile: failed to open: ", path_str));
  }

  XXH64Hasher hasher;
  auto buffer = std::make_unique<std::array<char, kFileReadChunkBytes>>();

  while (in) {
    in.read(buffer->data(), static_cast<std::streamsize>(buffer->size()));
    const std::streamsize n = in.gcount();
    if (n > 0) {
      hasher.Update(buffer->data(), static_cast<std::size_t>(n));
    }
  }

  // eof() is the expected terminator; anything else is a real read failure.
  if (in.bad() || (in.fail() && !in.eof())) {
    return core::InternalError(
        absl::StrCat("XXH64HashFile: read error: ", path_str));
  }

  return hasher.Finalize();
}

}  // namespace utils
}  // namespace gvdb
