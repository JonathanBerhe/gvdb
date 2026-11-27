// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include <pthread.h>

#include <climits>
#include <functional>
#include <string>

#include "core/config.h"
#include "core/filter.h"
#include "core/interfaces.h"
#include "core/metadata.h"
#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"

namespace gvdb {
namespace core {
namespace {

// ============================================================================
// Types Tests
// ============================================================================

TEST_CASE("TypesTest - VectorIdCreation") {
  VectorId id1 = MakeVectorId(42);
  VectorId id2 = MakeVectorId(42);
  VectorId id3 = MakeVectorId(43);

  CHECK_EQ(ToUInt64(id1), 42);
  CHECK_EQ(ToUInt64(id1), ToUInt64(id2));
  CHECK_NE(ToUInt64(id1), ToUInt64(id3));
}

TEST_CASE("TypesTest - CollectionIdCreation") {
  CollectionId id = MakeCollectionId(100);
  CHECK_EQ(ToUInt32(id), 100);
}

TEST_CASE("TypesTest - InvalidIds") {
  CHECK_EQ(ToUInt32(kInvalidCollectionId), 0);
  CHECK_EQ(ToUInt16(kInvalidShardId), 0);
}

TEST_CASE("TypesTest - SearchResultEntry") {
  SearchResultEntry entry(MakeVectorId(123), 0.5f);
  CHECK_EQ(ToUInt64(entry.id), 123);
  CHECK(entry.distance == doctest::Approx(0.5f));
}

TEST_CASE("TypesTest - SearchResult") {
  SearchResult result(10);
  CHECK_EQ(result.Size(), 0);

  result.AddEntry(MakeVectorId(1), 0.1f);
  result.AddEntry(MakeVectorId(2), 0.2f);

  CHECK_EQ(result.Size(), 2);
  CHECK_EQ(ToUInt64(result.entries[0].id), 1);
  CHECK(result.entries[0].distance == doctest::Approx(0.1f));
}

// ============================================================================
// absl::Status Tests
// ============================================================================

TEST_CASE("StatusTest - OkStatus") {
  absl::Status status = absl::OkStatus();
  CHECK(status.ok());
}

TEST_CASE("StatusTest - ErrorStatuses") {
  absl::Status invalid = absl::InvalidArgumentError("invalid arg");
  CHECK_FALSE(invalid.ok());
  CHECK(absl::IsInvalidArgument(invalid));

  absl::Status internal = absl::InternalError("internal error");
  CHECK_FALSE(internal.ok());
  CHECK(absl::IsInternal(internal));

  absl::Status not_found = absl::NotFoundError("not found");
  CHECK_FALSE(not_found.ok());
  CHECK(absl::IsNotFound(not_found));
}

TEST_CASE("StatusTest - StatusOrSuccess") {
  absl::StatusOr<int> result = 42;
  CHECK(result.ok());
  CHECK_EQ(result.value(), 42);
}

TEST_CASE("StatusTest - StatusOrError") {
  absl::StatusOr<int> result = absl::InvalidArgumentError("error");
  CHECK_FALSE(result.ok());
  CHECK(absl::IsInvalidArgument(result.status()));
}

// ============================================================================
// Vector Tests
// ============================================================================

TEST_CASE("VectorTest - Construction") {
  Vector vec(128);
  CHECK_EQ(vec.dimension(), 128);
  CHECK_EQ(vec.size(), 128);
  CHECK_NE(vec.data(), nullptr);
}

TEST_CASE("VectorTest - ConstructionZeroDimension") {
  CHECK_THROWS_AS(Vector(0), std::invalid_argument);
}

TEST_CASE("VectorTest - ConstructionNegativeDimension") {
  // std::vector throws std::length_error for negative size
  CHECK_THROWS_AS(Vector(-1), std::exception);
}

TEST_CASE("VectorTest - ConstructionFromData") {
  float data[4] = {1.0f, 2.0f, 3.0f, 4.0f};
  Vector vec(4, data);

  CHECK_EQ(vec.dimension(), 4);
  CHECK(vec[0] == doctest::Approx(1.0f));
  CHECK(vec[1] == doctest::Approx(2.0f));
  CHECK(vec[2] == doctest::Approx(3.0f));
  CHECK(vec[3] == doctest::Approx(4.0f));
}

TEST_CASE("VectorTest - ConstructionFromStdVector") {
  std::vector<float> data = {1.0f, 2.0f, 3.0f};
  Vector vec(data);

  CHECK_EQ(vec.dimension(), 3);
  CHECK(vec[0] == doctest::Approx(1.0f));
  CHECK(vec[1] == doctest::Approx(2.0f));
  CHECK(vec[2] == doctest::Approx(3.0f));
}

TEST_CASE("VectorTest - AlignmentVerification") {
  Vector vec(128);
  auto addr = reinterpret_cast<uintptr_t>(vec.data());
  CHECK_MESSAGE(addr % 32 == 0, "Vector data must be 32-byte aligned for AVX");
}

TEST_CASE("VectorTest - MoveSemantics") {
  Vector vec1(4);
  vec1[0] = 1.0f;
  vec1[1] = 2.0f;

  Vector vec2(std::move(vec1));
  CHECK_EQ(vec2.dimension(), 4);
  CHECK(vec2[0] == doctest::Approx(1.0f));
  CHECK(vec2[1] == doctest::Approx(2.0f));
}

TEST_CASE("VectorTest - CopySemantics") {
  Vector vec1(4);
  vec1[0] = 1.0f;
  vec1[1] = 2.0f;

  Vector vec2 = vec1;
  CHECK_EQ(vec2.dimension(), 4);
  CHECK(vec2[0] == doctest::Approx(1.0f));
  CHECK(vec2[1] == doctest::Approx(2.0f));

  // Modify vec2 shouldn't affect vec1
  vec2[0] = 5.0f;
  CHECK(vec1[0] == doctest::Approx(1.0f));
}

TEST_CASE("VectorTest - Norm") {
  std::vector<float> data = {3.0f, 4.0f};
  Vector vec(data);
  CHECK(vec.Norm() == doctest::Approx(5.0f));
}

TEST_CASE("VectorTest - L2Distance") {
  std::vector<float> data1 = {1.0f, 0.0f};
  std::vector<float> data2 = {4.0f, 0.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  CHECK(vec1.L2Distance(vec2) == doctest::Approx(3.0f));
}

TEST_CASE("VectorTest - L2DistanceDimensionMismatch") {
  Vector vec1(3);
  Vector vec2(4);
  CHECK_LT(vec1.L2Distance(vec2), 0.0f);  // Returns -1 for invalid
}

TEST_CASE("VectorTest - InnerProduct") {
  std::vector<float> data1 = {1.0f, 2.0f, 3.0f};
  std::vector<float> data2 = {4.0f, 5.0f, 6.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
  CHECK(vec1.InnerProduct(vec2) == doctest::Approx(32.0f));
}

TEST_CASE("VectorTest - CosineDistance") {
  std::vector<float> data1 = {1.0f, 0.0f};
  std::vector<float> data2 = {1.0f, 0.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  // Same vector: cosine similarity = 1, distance = 0
  CHECK(vec1.CosineDistance(vec2) == doctest::Approx(0.0f).epsilon(1e-6f));
}

TEST_CASE("VectorTest - CosineDistanceOrthogonal") {
  std::vector<float> data1 = {1.0f, 0.0f};
  std::vector<float> data2 = {0.0f, 1.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  // Orthogonal: cosine similarity = 0, distance = 1
  CHECK(vec1.CosineDistance(vec2) == doctest::Approx(1.0f).epsilon(1e-6f));
}

TEST_CASE("VectorTest - LargeDimension") {
  // Test with a large dimension to ensure no overflow
  Vector vec(1048576);  // 2^20 elements
  CHECK_EQ(vec.dimension(), 1048576);
}

// ============================================================================
// Vector Utility Tests
// ============================================================================

TEST_CASE("VectorUtilTest - ZeroVector") {
  Vector vec = ZeroVector(8);
  CHECK_EQ(vec.dimension(), 8);
  for (int i = 0; i < 8; ++i) {
    CHECK(vec[i] == doctest::Approx(0.0f));
  }
}

TEST_CASE("VectorUtilTest - RandomVector") {
  Vector vec = RandomVector(128);
  CHECK_EQ(vec.dimension(), 128);

  // Check that not all values are zero (very unlikely for random)
  bool has_non_zero = false;
  for (int i = 0; i < 128; ++i) {
    if (vec[i] != 0.0f) {
      has_non_zero = true;
      break;
    }
  }
  CHECK(has_non_zero);
}

// ============================================================================
// Metadata Tests
// ============================================================================

TEST_CASE("MetadataTest - MetadataValueTypes") {
  MetadataValue int_val = int64_t(42);
  MetadataValue double_val = 3.14;
  MetadataValue string_val = std::string("hello");
  MetadataValue bool_val = true;

  CHECK_EQ(std::get<int64_t>(int_val), 42);
  CHECK(std::get<double>(double_val) == doctest::Approx(3.14));
  CHECK_EQ(std::get<std::string>(string_val), "hello");
  CHECK_EQ(std::get<bool>(bool_val), true);
}

TEST_CASE("MetadataTest - CompareEqual") {
  CHECK(CompareMetadataValues(MetadataValue(int64_t(42)), ComparisonOp::EQUAL,
                              MetadataValue(int64_t(42))));

  CHECK_FALSE(CompareMetadataValues(MetadataValue(int64_t(42)), ComparisonOp::EQUAL,
                                    MetadataValue(int64_t(43))));

  CHECK(CompareMetadataValues(MetadataValue(std::string("hello")), ComparisonOp::EQUAL,
                              MetadataValue(std::string("hello"))));
}

TEST_CASE("MetadataTest - CompareNotEqual") {
  CHECK(CompareMetadataValues(MetadataValue(int64_t(42)), ComparisonOp::NOT_EQUAL,
                              MetadataValue(int64_t(43))));

  CHECK_FALSE(CompareMetadataValues(MetadataValue(int64_t(42)), ComparisonOp::NOT_EQUAL,
                                    MetadataValue(int64_t(42))));

  // NOT_EQUAL works across types
  CHECK(CompareMetadataValues(MetadataValue(int64_t(42)), ComparisonOp::NOT_EQUAL,
                              MetadataValue(std::string("42"))));
}

TEST_CASE("MetadataTest - CompareLessThan") {
  CHECK(CompareMetadataValues(MetadataValue(int64_t(10)), ComparisonOp::LESS_THAN,
                              MetadataValue(int64_t(20))));

  CHECK_FALSE(CompareMetadataValues(MetadataValue(int64_t(20)), ComparisonOp::LESS_THAN,
                                    MetadataValue(int64_t(10))));

  CHECK(CompareMetadataValues(MetadataValue(1.5), ComparisonOp::LESS_THAN, MetadataValue(2.5)));

  CHECK(CompareMetadataValues(MetadataValue(std::string("apple")), ComparisonOp::LESS_THAN,
                              MetadataValue(std::string("banana"))));
}

TEST_CASE("MetadataTest - CompareLessEqual") {
  CHECK(CompareMetadataValues(MetadataValue(int64_t(10)), ComparisonOp::LESS_EQUAL,
                              MetadataValue(int64_t(10))));

  CHECK(CompareMetadataValues(MetadataValue(int64_t(10)), ComparisonOp::LESS_EQUAL,
                              MetadataValue(int64_t(20))));

  CHECK_FALSE(CompareMetadataValues(MetadataValue(int64_t(20)), ComparisonOp::LESS_EQUAL,
                                    MetadataValue(int64_t(10))));
}

TEST_CASE("MetadataTest - CompareGreaterThan") {
  CHECK(CompareMetadataValues(MetadataValue(int64_t(100)), ComparisonOp::GREATER_THAN,
                              MetadataValue(int64_t(50))));

  CHECK_FALSE(CompareMetadataValues(MetadataValue(int64_t(50)), ComparisonOp::GREATER_THAN,
                                    MetadataValue(int64_t(100))));
}

TEST_CASE("MetadataTest - CompareGreaterEqual") {
  CHECK(CompareMetadataValues(MetadataValue(int64_t(100)), ComparisonOp::GREATER_EQUAL,
                              MetadataValue(int64_t(100))));

  CHECK(CompareMetadataValues(MetadataValue(int64_t(100)), ComparisonOp::GREATER_EQUAL,
                              MetadataValue(int64_t(50))));

  CHECK_FALSE(CompareMetadataValues(MetadataValue(int64_t(50)), ComparisonOp::GREATER_EQUAL,
                                    MetadataValue(int64_t(100))));
}

TEST_CASE("MetadataTest - LikePatternExactMatch") {
  CHECK(MatchLikePattern("hello", "hello"));
  CHECK_FALSE(MatchLikePattern("hello", "world"));
}

TEST_CASE("MetadataTest - LikePatternWildcard") {
  // % matches any sequence
  CHECK(MatchLikePattern("Nike Air Max", "Nike%"));
  CHECK(MatchLikePattern("Nike Pegasus", "Nike%"));
  CHECK(MatchLikePattern("best running shoes", "%running%"));
  CHECK(MatchLikePattern("running gear", "%running%"));
  CHECK_FALSE(MatchLikePattern("walking shoes", "%running%"));
}

TEST_CASE("MetadataTest - LikePatternSingleChar") {
  // _ matches single character
  CHECK(MatchLikePattern("Air Max", "Air_Max"));
  CHECK(MatchLikePattern("Air-Max", "Air_Max"));
  CHECK_FALSE(MatchLikePattern("AirMax", "Air_Max"));
}

TEST_CASE("MetadataTest - LikePatternEscape") {
  // Escaped % should match literal %
  CHECK(MatchLikePattern("50%", "50\\%"));
  CHECK_FALSE(MatchLikePattern("50off", "50\\%"));
}

TEST_CASE("MetadataTest - LikePatternEscapesAnyByte") {
  CHECK(MatchLikePattern("a_b", "a\\_b"));
  CHECK_FALSE(MatchLikePattern("axb", "a\\_b"));
  // A backslash before an ordinary byte makes it literal; it is not a regex
  // class, so "\d" matches the letter d and not a digit.
  CHECK(MatchLikePattern("ad", "a\\d"));
  CHECK_FALSE(MatchLikePattern("a5", "a\\d"));
  // A trailing lone backslash matches itself.
  CHECK(MatchLikePattern("50\\", "50\\"));
}

TEST_CASE("MetadataTest - LikePatternRegexMetacharactersAreLiteral") {
  CHECK(MatchLikePattern("a.b", "a.b"));
  CHECK_FALSE(MatchLikePattern("axb", "a.b"));
  CHECK(MatchLikePattern("(x)+[y]*{z}|^$?", "(x)+[y]*{z}|^$?"));
  CHECK_FALSE(MatchLikePattern("xx", "x+"));
}

TEST_CASE("MetadataTest - LikePatternBacktracking") {
  CHECK(MatchLikePattern("abcabcabd", "%abd"));
  CHECK(MatchLikePattern("mississippi", "m%ss%pi"));
  CHECK(MatchLikePattern("mississippi", "%s_s%"));
  CHECK_FALSE(MatchLikePattern("mississippi", "m%ss%px"));
  CHECK(MatchLikePattern("", "%"));
  CHECK(MatchLikePattern("", "%%"));
  CHECK_FALSE(MatchLikePattern("", "_"));
  CHECK_FALSE(MatchLikePattern("abc", ""));
  CHECK(MatchLikePattern("a\nb", "a%b"));
  CHECK(MatchLikePattern("a\nb", "a_b"));
}

TEST_CASE("MetadataTest - LikePatternLongInputs") {
  const std::string long_a(100000, 'a');
  CHECK(MatchLikePattern(long_a, long_a));
  CHECK(MatchLikePattern(long_a, std::string(100000, '%')));
  CHECK(MatchLikePattern(long_a, std::string(100000, '_')));
  CHECK_FALSE(MatchLikePattern(long_a, std::string(100001, '_')));

  const std::string value = std::string(65536, 'x') + "a";
  CHECK(MatchLikePattern(value, "%a"));
  CHECK(MatchLikePattern(value, "%a%"));
  CHECK_FALSE(MatchLikePattern(value, "a%"));
  CHECK_FALSE(MatchLikePattern(value, "%b%"));
}

TEST_CASE("MetadataTest - CompareLike") {
  CHECK(CompareMetadataValues(MetadataValue(std::string("Nike Air Max")), ComparisonOp::LIKE,
                              MetadataValue(std::string("Nike%"))));

  CHECK_FALSE(CompareMetadataValues(MetadataValue(std::string("Adidas Ultra")), ComparisonOp::LIKE,
                                    MetadataValue(std::string("Nike%"))));
}

TEST_CASE("MetadataTest - CompareNotLike") {
  CHECK_FALSE(CompareMetadataValues(MetadataValue(std::string("Nike Air Max")),
                                    ComparisonOp::NOT_LIKE, MetadataValue(std::string("Nike%"))));

  CHECK(CompareMetadataValues(MetadataValue(std::string("Adidas Ultra")), ComparisonOp::NOT_LIKE,
                              MetadataValue(std::string("Nike%"))));
}

TEST_CASE("MetadataTest - CompareInList") {
  std::vector<MetadataValue> brands = {MetadataValue(std::string("Nike")),
                                       MetadataValue(std::string("Adidas")),
                                       MetadataValue(std::string("Puma"))};

  CHECK(CompareMetadataValueInList(MetadataValue(std::string("Nike")),
                                   false,  // not negated
                                   brands));

  CHECK_FALSE(CompareMetadataValueInList(MetadataValue(std::string("Reebok")), false, brands));
}

TEST_CASE("MetadataTest - CompareNotInList") {
  std::vector<MetadataValue> numbers = {MetadataValue(int64_t(1)), MetadataValue(int64_t(2)),
                                        MetadataValue(int64_t(3))};

  CHECK_FALSE(CompareMetadataValueInList(MetadataValue(int64_t(2)),
                                         true,  // negated (NOT IN)
                                         numbers));

  CHECK(CompareMetadataValueInList(MetadataValue(int64_t(5)),
                                   true,  // negated (NOT IN)
                                   numbers));
}

TEST_CASE("MetadataTest - ValidateKeyValid") {
  CHECK(ValidateMetadataKey("price").ok());
  CHECK(ValidateMetadataKey("brand_name").ok());
  CHECK(ValidateMetadataKey("_internal").ok());
  CHECK(ValidateMetadataKey("field123").ok());
}

TEST_CASE("MetadataTest - ValidateKeyEmpty") {
  auto status = ValidateMetadataKey("");
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateKeyStartsWithDigit") {
  auto status = ValidateMetadataKey("123field");
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateKeyInvalidCharacters") {
  CHECK_FALSE(ValidateMetadataKey("field-name").ok());
  CHECK_FALSE(ValidateMetadataKey("field.name").ok());
  CHECK_FALSE(ValidateMetadataKey("field name").ok());
  CHECK_FALSE(ValidateMetadataKey("field@name").ok());
}

TEST_CASE("MetadataTest - ValidateKeyTooLong") {
  std::string long_key(256, 'a');
  auto status = ValidateMetadataKey(long_key);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateValueInt64") {
  CHECK(ValidateMetadataValue(MetadataValue(int64_t(42))).ok());
  CHECK(ValidateMetadataValue(MetadataValue(int64_t(-100))).ok());
}

TEST_CASE("MetadataTest - ValidateValueDouble") {
  CHECK(ValidateMetadataValue(MetadataValue(3.14)).ok());
  CHECK_FALSE(ValidateMetadataValue(MetadataValue(std::numeric_limits<double>::quiet_NaN())).ok());
  CHECK_FALSE(ValidateMetadataValue(MetadataValue(std::numeric_limits<double>::infinity())).ok());
}

TEST_CASE("MetadataTest - ValidateValueString") {
  CHECK(ValidateMetadataValue(MetadataValue(std::string("hello"))).ok());

  // String too long (>64KB)
  std::string long_string(65537, 'x');
  auto status = ValidateMetadataValue(MetadataValue(long_string));
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateValueBool") {
  CHECK(ValidateMetadataValue(MetadataValue(true)).ok());
  CHECK(ValidateMetadataValue(MetadataValue(false)).ok());
}

TEST_CASE("MetadataTest - ValidateMetadataValid") {
  Metadata metadata = {{"price", MetadataValue(int64_t(100))},
                       {"brand", MetadataValue(std::string("Nike"))},
                       {"in_stock", MetadataValue(true)},
                       {"rating", MetadataValue(4.5)}};

  CHECK(ValidateMetadata(metadata).ok());
}

TEST_CASE("MetadataTest - ValidateMetadataTooManyFields") {
  Metadata metadata;
  for (int i = 0; i < 101; ++i) {
    metadata["field_" + std::to_string(i)] = MetadataValue(int64_t(i));
  }

  auto status = ValidateMetadata(metadata);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateMetadataInvalidKey") {
  Metadata metadata = {{"invalid-key", MetadataValue(int64_t(100))}};

  auto status = ValidateMetadata(metadata);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateMetadataInvalidValue") {
  Metadata metadata = {{"rating", MetadataValue(std::numeric_limits<double>::quiet_NaN())}};

  auto status = ValidateMetadata(metadata);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

// ============================================================================
// Filter Parser Tests
// ============================================================================

TEST_CASE("FilterTest - ParseSimpleComparison") {
  auto filter = FilterParser::parse("price < 100");
  REQUIRE(filter.ok());

  Metadata metadata = {{"price", MetadataValue(int64_t(50))}};
  CHECK((*filter)->Evaluate(metadata));

  Metadata metadata2 = {{"price", MetadataValue(int64_t(150))}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseEqualityComparison") {
  auto filter = FilterParser::parse("brand = 'Nike'");
  REQUIRE(filter.ok());

  Metadata metadata = {{"brand", MetadataValue(std::string("Nike"))}};
  CHECK((*filter)->Evaluate(metadata));

  Metadata metadata2 = {{"brand", MetadataValue(std::string("Adidas"))}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseBooleanComparison") {
  auto filter = FilterParser::parse("in_stock = true");
  REQUIRE(filter.ok());

  Metadata metadata = {{"in_stock", MetadataValue(true)}};
  CHECK((*filter)->Evaluate(metadata));

  Metadata metadata2 = {{"in_stock", MetadataValue(false)}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseDoubleComparison") {
  auto filter = FilterParser::parse("rating >= 4.5");
  REQUIRE(filter.ok());

  Metadata metadata = {{"rating", MetadataValue(4.8)}};
  CHECK((*filter)->Evaluate(metadata));

  Metadata metadata2 = {{"rating", MetadataValue(3.2)}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseAndExpression") {
  auto filter = FilterParser::parse("price < 100 AND brand = 'Nike'");
  REQUIRE(filter.ok());

  Metadata metadata = {{"price", MetadataValue(int64_t(50))},
                       {"brand", MetadataValue(std::string("Nike"))}};
  CHECK((*filter)->Evaluate(metadata));

  Metadata metadata2 = {{"price", MetadataValue(int64_t(150))},
                        {"brand", MetadataValue(std::string("Nike"))}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));

  Metadata metadata3 = {{"price", MetadataValue(int64_t(50))},
                        {"brand", MetadataValue(std::string("Adidas"))}};
  CHECK_FALSE((*filter)->Evaluate(metadata3));
}

TEST_CASE("FilterTest - ParseOrExpression") {
  auto filter = FilterParser::parse("price < 50 OR discount > 20");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"price", MetadataValue(int64_t(30))}};
  CHECK((*filter)->Evaluate(metadata1));

  Metadata metadata2 = {{"discount", MetadataValue(int64_t(25))}};
  CHECK((*filter)->Evaluate(metadata2));

  Metadata metadata3 = {{"price", MetadataValue(int64_t(80))},
                        {"discount", MetadataValue(int64_t(10))}};
  CHECK_FALSE((*filter)->Evaluate(metadata3));
}

TEST_CASE("FilterTest - ParseNotExpression") {
  auto filter = FilterParser::parse("NOT (price > 100)");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"price", MetadataValue(int64_t(50))}};
  CHECK((*filter)->Evaluate(metadata1));

  Metadata metadata2 = {{"price", MetadataValue(int64_t(150))}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseParenthesizedExpression") {
  auto filter = FilterParser::parse("(price < 100 OR discount > 20) AND in_stock = true");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"price", MetadataValue(int64_t(50))}, {"in_stock", MetadataValue(true)}};
  CHECK((*filter)->Evaluate(metadata1));

  Metadata metadata2 = {{"discount", MetadataValue(int64_t(25))},
                        {"in_stock", MetadataValue(true)}};
  CHECK((*filter)->Evaluate(metadata2));

  Metadata metadata3 = {{"price", MetadataValue(int64_t(50))}, {"in_stock", MetadataValue(false)}};
  CHECK_FALSE((*filter)->Evaluate(metadata3));
}

TEST_CASE("FilterTest - ParseLikeExpression") {
  auto filter = FilterParser::parse("brand LIKE 'Nike%'");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"brand", MetadataValue(std::string("Nike Air Max"))}};
  CHECK((*filter)->Evaluate(metadata1));

  Metadata metadata2 = {{"brand", MetadataValue(std::string("Adidas"))}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseNotLikeExpression") {
  auto filter = FilterParser::parse("brand NOT LIKE '%test%'");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"brand", MetadataValue(std::string("Nike"))}};
  CHECK((*filter)->Evaluate(metadata1));

  Metadata metadata2 = {{"brand", MetadataValue(std::string("test product"))}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseInExpression") {
  auto filter = FilterParser::parse("category IN ('shoes', 'apparel', 'accessories')");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"category", MetadataValue(std::string("shoes"))}};
  CHECK((*filter)->Evaluate(metadata1));

  Metadata metadata2 = {{"category", MetadataValue(std::string("electronics"))}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseNotInExpression") {
  auto filter = FilterParser::parse("status NOT IN (0, 1, 2)");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"status", MetadataValue(int64_t(5))}};
  CHECK((*filter)->Evaluate(metadata1));

  Metadata metadata2 = {{"status", MetadataValue(int64_t(1))}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseInWithMixedTypes") {
  auto filter = FilterParser::parse("value IN (1, 2.5, 'text')");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"value", MetadataValue(int64_t(1))}};
  CHECK((*filter)->Evaluate(metadata1));

  Metadata metadata2 = {{"value", MetadataValue(2.5)}};
  CHECK((*filter)->Evaluate(metadata2));

  Metadata metadata3 = {{"value", MetadataValue(std::string("text"))}};
  CHECK((*filter)->Evaluate(metadata3));

  Metadata metadata4 = {{"value", MetadataValue(int64_t(99))}};
  CHECK_FALSE((*filter)->Evaluate(metadata4));
}

TEST_CASE("FilterTest - ComplexNestedExpression") {
  auto filter = FilterParser::parse(
      "(price < 100 AND brand = 'Nike') OR (discount > 50 AND in_stock = true)");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"price", MetadataValue(int64_t(80))},
                        {"brand", MetadataValue(std::string("Nike"))}};
  CHECK((*filter)->Evaluate(metadata1));

  Metadata metadata2 = {{"discount", MetadataValue(int64_t(60))},
                        {"in_stock", MetadataValue(true)}};
  CHECK((*filter)->Evaluate(metadata2));

  Metadata metadata3 = {{"price", MetadataValue(int64_t(120))},
                        {"brand", MetadataValue(std::string("Adidas"))},
                        {"discount", MetadataValue(int64_t(20))}};
  CHECK_FALSE((*filter)->Evaluate(metadata3));
}

TEST_CASE("FilterTest - ParseNegativeNumber") {
  auto filter = FilterParser::parse("temperature > -10");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"temperature", MetadataValue(int64_t(-5))}};
  CHECK((*filter)->Evaluate(metadata1));

  Metadata metadata2 = {{"temperature", MetadataValue(int64_t(-15))}};
  CHECK_FALSE((*filter)->Evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseStringWithEscapes") {
  auto filter = FilterParser::parse("name = 'O\\'Brien'");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"name", MetadataValue(std::string("O'Brien"))}};
  CHECK((*filter)->Evaluate(metadata1));
}

TEST_CASE("FilterTest - MissingField") {
  auto filter = FilterParser::parse("price < 100");
  REQUIRE(filter.ok());

  Metadata metadata = {{"brand", MetadataValue(std::string("Nike"))}};
  // Field not present -> false
  CHECK_FALSE((*filter)->Evaluate(metadata));
}

TEST_CASE("FilterTest - ParseErrorInvalidSyntax") {
  auto result = FilterParser::parse("price <");
  CHECK_FALSE(result.ok());
  CHECK(absl::IsInvalidArgument(result.status()));
}

TEST_CASE("FilterTest - ParseErrorUnclosedParen") {
  auto result = FilterParser::parse("(price < 100");
  CHECK_FALSE(result.ok());
  CHECK(absl::IsInvalidArgument(result.status()));
}

TEST_CASE("FilterTest - ParseErrorUnclosedString") {
  auto result = FilterParser::parse("brand = 'Nike");
  CHECK_FALSE(result.ok());
  CHECK(absl::IsInvalidArgument(result.status()));
}

TEST_CASE("FilterTest - ParseErrorEmptyInList") {
  auto result = FilterParser::parse("category IN ()");
  CHECK_FALSE(result.ok());
  CHECK(absl::IsInvalidArgument(result.status()));
}

TEST_CASE("FilterTest - RejectsTrailingInput") {
  for (const char* expr : {
           "price < 100)",
           "a = 1 b = 2",
           "k = 'a' ; DROP",
           "i < 10 OR",
           "i < 10 AND",
           "i < 5e3",
           "i = 5abc",
           "flag = true1",
           "i <= 3 OR i = 40) AND k = 'zzz'",
       }) {
    INFO(expr);
    auto result = FilterParser::parse(expr);
    CHECK(absl::IsInvalidArgument(result.status()));
  }

  CHECK(FilterParser::parse("  price < 100  ").ok());
}

TEST_CASE("FilterTest - IntegerLiteralBounds") {
  auto max = FilterParser::parse("i = 9223372036854775807");
  REQUIRE(max.ok());
  CHECK((*max)->Evaluate({{"i", MetadataValue(int64_t{INT64_MAX})}}));

  auto min = FilterParser::parse("i = -9223372036854775808");
  REQUIRE(min.ok());
  CHECK((*min)->Evaluate({{"i", MetadataValue(int64_t{INT64_MIN})}}));

  for (const char* expr : {
           "i < 9223372036854775808",
           "i > -9223372036854775809",
           "i < 99999999999999999999",
           "i IN (1, 99999999999999999999)",
       }) {
    INFO(expr);
    CHECK(absl::IsInvalidArgument(FilterParser::parse(expr).status()));
  }
}

TEST_CASE("FilterTest - DoubleLiteralBounds") {
  const std::string overflow = "x < 1" + std::string(400, '0') + ".0";
  CHECK(absl::IsInvalidArgument(FilterParser::parse(overflow).status()));

  // Underflow rounds toward zero rather than failing.
  const std::string tiny = "x > 0." + std::string(400, '0') + "1";
  auto filter = FilterParser::parse(tiny);
  REQUIRE(filter.ok());
  CHECK((*filter)->Evaluate({{"x", MetadataValue(0.5)}}));
}

TEST_CASE("FilterTest - NumericLiteralForms") {
  auto plus = FilterParser::parse("i = +5");
  REQUIRE(plus.ok());
  CHECK((*plus)->Evaluate({{"i", MetadataValue(int64_t{5})}}));

  auto trailing_dot = FilterParser::parse("x = 1.");
  REQUIRE(trailing_dot.ok());
  CHECK((*trailing_dot)->Evaluate({{"x", MetadataValue(1.0)}}));

  auto negative_fraction = FilterParser::parse("x = -.5");
  REQUIRE(negative_fraction.ok());
  CHECK((*negative_fraction)->Evaluate({{"x", MetadataValue(-0.5)}}));

  auto negative_zero = FilterParser::parse("i = -0");
  REQUIRE(negative_zero.ok());
  CHECK((*negative_zero)->Evaluate({{"i", MetadataValue(int64_t{0})}}));

  for (const char* expr : {"x = .5", "x = -", "x = 1.2.3", "x = +"}) {
    INFO(expr);
    CHECK(absl::IsInvalidArgument(FilterParser::parse(expr).status()));
  }
}

TEST_CASE("FilterTest - KeywordEdgeCases") {
  auto lower = FilterParser::parse("a = 1 and not b = 2 or c = 3");
  REQUIRE(lower.ok());
  CHECK((*lower)->Evaluate({{"a", MetadataValue(int64_t{1})}, {"b", MetadataValue(int64_t{5})}}));

  // Boolean literals are lowercase only.
  CHECK(absl::IsInvalidArgument(FilterParser::parse("f = TRUE").status()));

  // A keyword followed by an operator is still usable as a field name.
  auto field_named_and = FilterParser::parse("and = 1");
  REQUIRE(field_named_and.ok());
  CHECK((*field_named_and)->Evaluate({{"and", MetadataValue(int64_t{1})}}));

  CHECK(absl::IsInvalidArgument(FilterParser::parse("not = 1").status()));
}

TEST_CASE("FilterTest - NonAsciiInput") {
  auto quoted = FilterParser::parse("name = 'caf\xC3\xA9'");
  REQUIRE(quoted.ok());
  CHECK((*quoted)->Evaluate({{"name", MetadataValue(std::string("caf\xC3\xA9"))}}));

  CHECK(absl::IsInvalidArgument(FilterParser::parse("pr\xC3\xAF"
                                                    "ce = 1")
                                    .status()));
  CHECK(absl::IsInvalidArgument(FilterParser::parse("\xFF").status()));
}

TEST_CASE("FilterTest - LikeEscapesThroughParser") {
  // The string-literal lexer consumes one backslash, so the filter text
  // '50\\%' reaches LIKE as the pattern 50\% (a literal percent sign) ...
  auto literal = FilterParser::parse("k LIKE '50\\\\%'");
  REQUIRE(literal.ok());
  CHECK((*literal)->Evaluate({{"k", MetadataValue(std::string("50%"))}}));
  CHECK_FALSE((*literal)->Evaluate({{"k", MetadataValue(std::string("50x"))}}));

  // ... while '50\%' reaches it as 50% (a wildcard).
  auto wildcard = FilterParser::parse("k LIKE '50\\%'");
  REQUIRE(wildcard.ok());
  CHECK((*wildcard)->Evaluate({{"k", MetadataValue(std::string("50x"))}}));
}

namespace {

// Runs fn on a thread with a 512 KiB stack (the default for secondary threads
// on macOS, where gRPC handlers run), so unbounded recursion fails here rather
// than only in the server.
void RunOnSmallStack(std::function<void()> fn) {
  pthread_attr_t attr;
  REQUIRE(pthread_attr_init(&attr) == 0);
  REQUIRE(pthread_attr_setstacksize(&attr, 512 * 1024) == 0);
  pthread_t thread;
  REQUIRE(pthread_create(
              &thread, &attr,
              [](void* arg) -> void* {
                (*static_cast<std::function<void()>*>(arg))();
                return nullptr;
              },
              &fn) == 0);
  pthread_join(thread, nullptr);
  pthread_attr_destroy(&attr);
}

std::string Repeat(const std::string& piece, int times) {
  std::string out;
  out.reserve(piece.size() * times);
  for (int i = 0; i < times; ++i) out += piece;
  return out;
}

}  // namespace

TEST_CASE("FilterTest - NestingDepthLimit") {
  const int max = FilterParser::kMaxNestingDepth;
  auto parens = [](int depth) { return Repeat("(", depth) + "a = 1" + Repeat(")", depth); };

  bool at_limit_ok = false;
  bool at_limit_matches = false;
  bool not_at_limit_ok = false;
  RunOnSmallStack([&] {
    auto nested = FilterParser::parse(parens(max));
    at_limit_ok = nested.ok();
    at_limit_matches = nested.ok() && (*nested)->Evaluate({{"a", MetadataValue(int64_t{1})}});
    // max NOTs is an even count, so it evaluates like the bare comparison.
    not_at_limit_ok = FilterParser::parse(Repeat("NOT ", max) + "a = 1").ok();
  });
  CHECK(at_limit_ok);
  CHECK(at_limit_matches);
  CHECK(not_at_limit_ok);

  CHECK(absl::IsInvalidArgument(FilterParser::parse(parens(max + 1)).status()));
  CHECK(absl::IsInvalidArgument(FilterParser::parse(Repeat("NOT ", max + 1) + "a = 1").status()));
  // Deep enough to overflow any thread stack if the parser recursed on it.
  CHECK(absl::IsInvalidArgument(FilterParser::parse(parens(1000000)).status()));
  CHECK(absl::IsInvalidArgument(FilterParser::parse(Repeat("NOT ", 1000000) + "a = 1").status()));
}

TEST_CASE("FilterTest - LongChainsStayShallow") {
  const int terms = 100000;
  std::string or_chain = "id = 0";
  std::string and_chain = "a0 = 1";
  for (int i = 1; i < terms; ++i) {
    or_chain += " OR id = " + std::to_string(i);
    and_chain += " AND a" + std::to_string(i % 100) + " = 1";
  }

  Metadata all_ones;
  for (int i = 0; i < 100; ++i) {
    all_ones["a" + std::to_string(i)] = MetadataValue(int64_t{1});
  }

  bool or_ok = false, or_first = false, or_last = false, or_miss = true;
  bool and_ok = false, and_all = false, and_one_off = true;
  RunOnSmallStack([&] {
    auto ors = FilterParser::parse(or_chain);
    or_ok = ors.ok();
    if (or_ok) {
      or_first = (*ors)->Evaluate({{"id", MetadataValue(int64_t{0})}});
      or_last = (*ors)->Evaluate({{"id", MetadataValue(int64_t{terms - 1})}});
      or_miss = (*ors)->Evaluate({{"id", MetadataValue(int64_t{terms})}});
    }
    auto ands = FilterParser::parse(and_chain);
    and_ok = ands.ok();
    if (and_ok) {
      and_all = (*ands)->Evaluate(all_ones);
      Metadata one_off = all_ones;
      one_off["a57"] = MetadataValue(int64_t{0});
      and_one_off = (*ands)->Evaluate(one_off);
    }
    // Both ASTs are destroyed on this thread as well.
  });
  CHECK(or_ok);
  CHECK(or_first);
  CHECK(or_last);
  CHECK_FALSE(or_miss);
  CHECK(and_ok);
  CHECK(and_all);
  CHECK_FALSE(and_one_off);
}

TEST_CASE("FilterTest - LargeInListAccepted") {
  std::string expr = "id IN (0";
  for (int i = 1; i < 100000; ++i) expr += ", " + std::to_string(i);
  expr += ")";

  auto filter = FilterParser::parse(expr);
  REQUIRE(filter.ok());
  CHECK((*filter)->Evaluate({{"id", MetadataValue(int64_t{99999})}}));
  CHECK_FALSE((*filter)->Evaluate({{"id", MetadataValue(int64_t{100000})}}));
}

// ============================================================================
// Config Tests
// ============================================================================

TEST_CASE("ConfigTest - IndexConfigValidation") {
  IndexConfig config;
  CHECK_FALSE(config.IsValid());  // dimension is 0

  config.dimension = 128;
  CHECK(config.IsValid());
}

// ============================================================================
// MetadataSerializer Tests
// ============================================================================

TEST_CASE("MetadataSerializerTest - SerializeDeserializeInt64") {
  Metadata metadata;
  metadata["count"] = static_cast<int64_t>(12345);
  metadata["price"] = static_cast<int64_t>(-999);

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  REQUIRE_MESSAGE(result.ok(), result.status().message());

  const auto& deserialized = result.value();
  REQUIRE_EQ(deserialized.size(), 2);
  CHECK_EQ(std::get<int64_t>(deserialized.at("count")), 12345);
  CHECK_EQ(std::get<int64_t>(deserialized.at("price")), -999);
}

TEST_CASE("MetadataSerializerTest - SerializeDeserializeDouble") {
  Metadata metadata;
  metadata["rating"] = 4.5;
  metadata["score"] = -1.234567;

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  REQUIRE(result.ok());

  const auto& deserialized = result.value();
  REQUIRE_EQ(deserialized.size(), 2);
  CHECK(std::get<double>(deserialized.at("rating")) == doctest::Approx(4.5));
  CHECK(std::get<double>(deserialized.at("score")) == doctest::Approx(-1.234567));
}

TEST_CASE("MetadataSerializerTest - SerializeDeserializeString") {
  Metadata metadata;
  metadata["brand"] = std::string("Nike");
  metadata["description"] = std::string("High-performance running shoes");
  metadata["empty"] = std::string("");

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  REQUIRE(result.ok());

  const auto& deserialized = result.value();
  REQUIRE_EQ(deserialized.size(), 3);
  CHECK_EQ(std::get<std::string>(deserialized.at("brand")), "Nike");
  CHECK_EQ(std::get<std::string>(deserialized.at("description")), "High-performance running shoes");
  CHECK_EQ(std::get<std::string>(deserialized.at("empty")), "");
}

TEST_CASE("MetadataSerializerTest - SerializeDeserializeBool") {
  Metadata metadata;
  metadata["in_stock"] = true;
  metadata["featured"] = false;

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  REQUIRE(result.ok());

  const auto& deserialized = result.value();
  REQUIRE_EQ(deserialized.size(), 2);
  CHECK(std::get<bool>(deserialized.at("in_stock")));
  CHECK_FALSE(std::get<bool>(deserialized.at("featured")));
}

TEST_CASE("MetadataSerializerTest - SerializeDeserializeMixedTypes") {
  Metadata metadata;
  metadata["price"] = 120.50;
  metadata["brand"] = std::string("Adidas");
  metadata["stock_count"] = static_cast<int64_t>(42);
  metadata["on_sale"] = true;
  metadata["category"] = std::string("sportswear");

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  REQUIRE(result.ok());

  const auto& deserialized = result.value();
  REQUIRE_EQ(deserialized.size(), 5);
  CHECK(std::get<double>(deserialized.at("price")) == doctest::Approx(120.50));
  CHECK_EQ(std::get<std::string>(deserialized.at("brand")), "Adidas");
  CHECK_EQ(std::get<int64_t>(deserialized.at("stock_count")), 42);
  CHECK(std::get<bool>(deserialized.at("on_sale")));
  CHECK_EQ(std::get<std::string>(deserialized.at("category")), "sportswear");
}

TEST_CASE("MetadataSerializerTest - SerializeDeserializeEmpty") {
  Metadata metadata;  // Empty

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  REQUIRE(result.ok());

  const auto& deserialized = result.value();
  CHECK_EQ(deserialized.size(), 0);
}

TEST_CASE("MetadataSerializerTest - DeserializeInvalidData") {
  std::stringstream ss;

  // Write invalid count (> 100)
  uint64_t invalid_count = 101;
  ss.write(reinterpret_cast<const char*>(&invalid_count), sizeof(invalid_count));

  auto result = MetadataSerializer::Deserialize(ss);
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_CASE("MetadataSerializerTest - LargeMetadata") {
  Metadata metadata;

  // Add 50 fields (within limit of 100)
  for (int i = 0; i < 50; ++i) {
    metadata["field_" + std::to_string(i)] = static_cast<int64_t>(i * 10);
  }

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  REQUIRE(result.ok());

  const auto& deserialized = result.value();
  REQUIRE_EQ(deserialized.size(), 50);

  for (int i = 0; i < 50; ++i) {
    std::string key = "field_" + std::to_string(i);
    CHECK_EQ(std::get<int64_t>(deserialized.at(key)), i * 10);
  }
}

}  // namespace
}  // namespace core
}  // namespace gvdb
