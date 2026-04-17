// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "utils/checksum.h"

namespace gvdb {
namespace utils {
namespace test {

// ============================================================================
// Known-answer vectors
// ============================================================================
//
// These values are published in the xxHash specification for seed = 0. They
// act as a contract test: if a future xxHash upgrade changes the output, we
// would silently corrupt every backup manifest already on disk. Treat any
// failure here as a data-integrity regression, not a flaky test.
// Source: https://github.com/Cyan4973/xxHash (README, test vectors).

TEST_CASE("XXH64Hash produces the specification digest for the empty input") {
  constexpr uint64_t kEmptyDigest = 0xEF46DB3751D8E999ULL;

  CHECK_EQ(XXH64Hash(nullptr, 0), kEmptyDigest);
  CHECK_EQ(XXH64Hash(std::string_view{}), kEmptyDigest);
}

TEST_CASE("XXH64Hash produces the specification digest for \"abc\"") {
  constexpr uint64_t kAbcDigest = 0x44BC2CF5AD770999ULL;

  CHECK_EQ(XXH64Hash(std::string_view("abc")), kAbcDigest);
}

TEST_CASE("XXH64Hash is deterministic across invocations") {
  const std::string_view input = "the quick brown fox jumps over the lazy dog";

  CHECK_EQ(XXH64Hash(input), XXH64Hash(input));
  CHECK_EQ(XXH64Hash(input), XXH64Hash(input.data(), input.size()));
}

// ============================================================================
// Hex formatting
// ============================================================================
//
// The on-disk form of a digest is "0x" + 16 lowercase hex chars (18 chars
// total). The format is part of the manifest contract: it must be exactly
// 18 chars, fully zero-padded, and use lowercase digits so that string
// comparisons and alphabetical sort are deterministic across platforms
// (snprintf's %016llx + unsigned long long is portable C++17 and produces
// the same bytes on every compiler we support).

TEST_CASE("XXH64ToHex output format is fixed-width and zero-padded") {
  // Zero digests still produce a full 18-char string so manifests never
  // contain variable-width values.
  CHECK_EQ(XXH64ToHex(0ULL), "0x0000000000000000");

  // A small value pads on the left with zeroes, not spaces.
  CHECK_EQ(XXH64ToHex(0x1ULL), "0x0000000000000001");

  // Real-world digest (the empty-input xxHash vector). Shows that we lower-
  // case the output regardless of how the literal is written in source.
  CHECK_EQ(XXH64ToHex(0xEF46DB3751D8E999ULL), "0xef46db3751d8e999");

  // Upper bound: all 16 digits are 'f'.
  CHECK_EQ(XXH64ToHex(UINT64_MAX), "0xffffffffffffffff");
}

TEST_CASE("XXH64ToHex and XXH64FromHex are mutual inverses") {
  // For any digest we can format, re-parsing the formatted string must
  // return the original digest. Covers the lower/upper bounds and a few
  // representative values.
  const std::vector<uint64_t> sample_digests = {
      0ULL,                   // lower bound
      1ULL,                   // smallest non-zero (tests zero-padding)
      42ULL,                  // arbitrary small value
      0xDEADBEEFULL,          // classic 32-bit test pattern (tests high-half zeroes)
      0xEF46DB3751D8E999ULL,  // real xxHash digest (the empty-input vector)
      UINT64_MAX,             // upper bound
  };

  for (uint64_t original : sample_digests) {
    const std::string formatted = XXH64ToHex(original);
    auto parsed = XXH64FromHex(formatted);

    REQUIRE_MESSAGE(parsed.ok(),
                    "XXH64FromHex failed for ", formatted,
                    ": ", parsed.status().message());
    CHECK_EQ(*parsed, original);
  }
}

TEST_CASE("XXH64FromHex accepts uppercase and mixed case and both prefixes") {
  CHECK_EQ(*XXH64FromHex("0xef46db3751d8e999"), 0xEF46DB3751D8E999ULL);
  CHECK_EQ(*XXH64FromHex("0XEF46DB3751D8E999"), 0xEF46DB3751D8E999ULL);
  CHECK_EQ(*XXH64FromHex("Ef46Db3751d8E999"), 0xEF46DB3751D8E999ULL);
}

TEST_CASE("XXH64FromHex rejects malformed input") {
  // Too short
  CHECK_FALSE(XXH64FromHex("0x1234").ok());
  // Too long
  CHECK_FALSE(XXH64FromHex("0x00000000000000001").ok());
  // No digits
  CHECK_FALSE(XXH64FromHex("").ok());
  CHECK_FALSE(XXH64FromHex("0x").ok());
  // Non-hex digit
  CHECK_FALSE(XXH64FromHex("0xZZZZZZZZZZZZZZZZ").ok());
  CHECK_FALSE(XXH64FromHex("0x0000000000000g00").ok());
}

// ============================================================================
// Streaming hasher
// ============================================================================

TEST_CASE("XXH64Hasher single Update matches one-shot") {
  const std::string input = "streaming sanity check";
  XXH64Hasher hasher;
  hasher.Update(input);
  CHECK_EQ(hasher.Finalize(), XXH64Hash(input));
}

TEST_CASE("XXH64Hasher chunked updates match one-shot") {
  // 10 KiB of varied bytes so we cross xxHash's 32-byte stripe boundary
  // many times.
  std::vector<unsigned char> data(10 * 1024);
  std::mt19937 rng(0xC0FFEE);
  for (auto& b : data) {
    b = static_cast<unsigned char>(rng() & 0xFF);
  }

  const uint64_t expected = XXH64Hash(data.data(), data.size());

  SUBCASE("single chunk") {
    XXH64Hasher h;
    h.Update(data.data(), data.size());
    CHECK_EQ(h.Finalize(), expected);
  }

  SUBCASE("irregular chunk sizes") {
    XXH64Hasher h;
    const std::size_t chunks[] = {1, 7, 32, 33, 100, 500, 1024, 2048};
    std::size_t offset = 0;
    std::size_t idx = 0;
    while (offset < data.size()) {
      const std::size_t chunk = chunks[idx++ % (sizeof(chunks) / sizeof(*chunks))];
      const std::size_t n = std::min(chunk, data.size() - offset);
      h.Update(data.data() + offset, n);
      offset += n;
    }
    CHECK_EQ(h.Finalize(), expected);
  }

  SUBCASE("zero-size updates are no-ops") {
    XXH64Hasher h;
    h.Update(nullptr, 0);
    h.Update(data.data(), data.size());
    h.Update(std::string_view{});
    CHECK_EQ(h.Finalize(), expected);
  }
}

TEST_CASE("XXH64Hasher Finalize resets state") {
  XXH64Hasher h;
  h.Update("first");
  const uint64_t first = h.Finalize();

  h.Update("first");
  const uint64_t second = h.Finalize();

  CHECK_EQ(first, second);
  CHECK_EQ(first, XXH64Hash("first"));
}

TEST_CASE("XXH64Hasher Reset discards pending state") {
  XXH64Hasher h;
  h.Update("discarded");
  h.Reset();
  h.Update("kept");
  CHECK_EQ(h.Finalize(), XXH64Hash("kept"));
}

TEST_CASE("XXH64Hasher is movable") {
  XXH64Hasher a;
  a.Update("hello ");

  XXH64Hasher b(std::move(a));
  b.Update("world");
  CHECK_EQ(b.Finalize(), XXH64Hash("hello world"));

  XXH64Hasher c;
  c.Update("x");
  c = std::move(b);
  c.Update("y");
  CHECK_EQ(c.Finalize(), XXH64Hash("y"));
}

// ============================================================================
// File helper
// ============================================================================

// Fixture that creates a unique temp directory per test and cleans it up.
class TempDirFixture {
 public:
  TempDirFixture() {
    dir_ = std::filesystem::temp_directory_path() /
           ("gvdb_checksum_test_" + std::to_string(std::random_device{}()));
    std::filesystem::create_directories(dir_);
  }
  ~TempDirFixture() {
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);  // best-effort, don't throw in dtor
  }

  [[nodiscard]] std::filesystem::path write_file(std::string_view name,
                                    std::string_view contents) const {
    const std::filesystem::path p = dir_ / name;
    std::ofstream out(p, std::ios::binary);
    out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    return p;
  }

 protected:
  std::filesystem::path dir_;
};

TEST_CASE_FIXTURE(TempDirFixture, "XXH64HashFile matches one-shot on small file") {
  const std::string payload = "small file payload";
  const std::filesystem::path p = write_file("small.bin", payload);

  auto digest = XXH64HashFile(p.string());
  REQUIRE(digest.ok());
  CHECK_EQ(*digest, XXH64Hash(payload));
}

TEST_CASE_FIXTURE(TempDirFixture, "XXH64HashFile handles empty file") {
  const std::filesystem::path p = write_file("empty.bin", std::string_view{});

  auto digest = XXH64HashFile(p.string());
  REQUIRE(digest.ok());
  CHECK_EQ(*digest, XXH64Hash(""));
}

TEST_CASE_FIXTURE(TempDirFixture,
                  "XXH64HashFile matches one-shot on file larger than read chunk") {
  // Slightly over 3 x 64 KiB so we exercise multiple fread iterations and
  // a partial final chunk.
  std::string payload(64 * 1024 * 3 + 123, '\0');
  std::mt19937 rng(0xFEEDFACE);
  for (auto& c : payload) {
    c = static_cast<char>(rng() & 0xFF);
  }
  const std::filesystem::path p = write_file("large.bin", payload);

  auto digest = XXH64HashFile(p.string());
  REQUIRE(digest.ok());
  CHECK_EQ(*digest, XXH64Hash(payload));
}

TEST_CASE_FIXTURE(TempDirFixture, "XXH64HashFile returns NotFound for missing path") {
  auto digest = XXH64HashFile((dir_ / "does-not-exist").string());
  REQUIRE_FALSE(digest.ok());
  CHECK_EQ(digest.status().code(), absl::StatusCode::kNotFound);
}

TEST_CASE_FIXTURE(TempDirFixture, "XXH64HashFile rejects a directory path") {
  auto digest = XXH64HashFile(dir_.string());
  REQUIRE_FALSE(digest.ok());
  CHECK_EQ(digest.status().code(), absl::StatusCode::kInvalidArgument);
}

}  // namespace test
}  // namespace utils
}  // namespace gvdb
