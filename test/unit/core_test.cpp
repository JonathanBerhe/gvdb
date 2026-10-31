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
  CHECK_EQ(ToUInt64(kInvalidVectorId), 0);
  CHECK_EQ(ToUInt32(kInvalidCollectionId), 0);
  CHECK_EQ(ToUInt32(kInvalidSegmentId), 0);
  CHECK_EQ(ToUInt16(kInvalidShardId), 0);
}

TEST_CASE("TypesTest - SearchResultEntry") {
  SearchResultEntry entry(MakeVectorId(123), 0.5f);
  CHECK_EQ(ToUInt64(entry.id), 123);
  CHECK(entry.distance == doctest::Approx(0.5f));
}

TEST_CASE("TypesTest - SearchResult") {
  SearchResult result(10);
  CHECK(result.Empty());
  CHECK_EQ(result.Size(), 0);

  result.AddEntry(MakeVectorId(1), 0.1f);
  result.AddEntry(MakeVectorId(2), 0.2f);

  CHECK_FALSE(result.Empty());
  CHECK_EQ(result.Size(), 2);
  CHECK_EQ(ToUInt64(result.entries[0].id), 1);
  CHECK(result.entries[0].distance == doctest::Approx(0.1f));
}

// ============================================================================
// Status Tests
// ============================================================================

TEST_CASE("StatusTest - OkStatus") {
  Status status = OkStatus();
  CHECK(status.ok());
}

TEST_CASE("StatusTest - ErrorStatuses") {
  Status invalid = InvalidArgumentError("invalid arg");
  CHECK_FALSE(invalid.ok());
  CHECK(absl::IsInvalidArgument(invalid));

  Status internal = InternalError("internal error");
  CHECK_FALSE(internal.ok());
  CHECK(absl::IsInternal(internal));

  Status not_found = NotFoundError("not found");
  CHECK_FALSE(not_found.ok());
  CHECK(absl::IsNotFound(not_found));
}

TEST_CASE("StatusTest - StatusOrSuccess") {
  StatusOr<int> result = 42;
  CHECK(result.ok());
  CHECK_EQ(result.value(), 42);
}

TEST_CASE("StatusTest - StatusOrError") {
  StatusOr<int> result = InvalidArgumentError("error");
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

TEST_CASE("VectorTest - NormalizeValidVector") {
  std::vector<float> data = {3.0f, 4.0f};
  Vector vec(data);

  auto result = vec.Normalize();
  REQUIRE(result.ok());

  Vector normalized = std::move(result.value());
  CHECK(normalized.Norm() == doctest::Approx(1.0f).epsilon(1e-6f));
  CHECK(normalized[0] == doctest::Approx(0.6f).epsilon(1e-6f));
  CHECK(normalized[1] == doctest::Approx(0.8f).epsilon(1e-6f));
}

TEST_CASE("VectorTest - NormalizeZeroVector") {
  Vector vec(4);  // All zeros
  auto result = vec.Normalize();
  CHECK_FALSE(result.ok());
  CHECK(absl::IsInvalidArgument(result.status()));
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

TEST_CASE("VectorTest - Validation") {
  Vector vec(4);
  CHECK(vec.IsValid());

  Status status = vec.Validate();
  CHECK(status.ok());
}

TEST_CASE("VectorTest - LargeDimension") {
  // Test with a large dimension to ensure no overflow
  Vector vec(1048576);  // 2^20 elements
  CHECK_EQ(vec.dimension(), 1048576);
  CHECK(vec.IsValid());
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

TEST_CASE("VectorUtilTest - ValidateDimensionMatch") {
  Vector vec1(4);
  Vector vec2(4);
  Vector vec3(8);

  Status status1 = ValidateDimensionMatch(vec1, vec2);
  CHECK(status1.ok());

  Status status2 = ValidateDimensionMatch(vec1, vec3);
  CHECK_FALSE(status2.ok());
  CHECK(absl::IsInvalidArgument(status2));
}

TEST_CASE("VectorUtilTest - ComputeDistanceL2") {
  std::vector<float> data1 = {1.0f, 0.0f};
  std::vector<float> data2 = {4.0f, 0.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  float dist = ComputeDistance(vec1, vec2, MetricType::L2);
  CHECK(dist == doctest::Approx(3.0f));
}

TEST_CASE("VectorUtilTest - ComputeDistanceInnerProduct") {
  std::vector<float> data1 = {1.0f, 2.0f};
  std::vector<float> data2 = {3.0f, 4.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  // Inner product: 1*3 + 2*4 = 11, negated = -11
  float dist = ComputeDistance(vec1, vec2, MetricType::INNER_PRODUCT);
  CHECK(dist == doctest::Approx(-11.0f));
}

TEST_CASE("VectorUtilTest - ComputeDistanceCosine") {
  std::vector<float> data1 = {1.0f, 0.0f};
  std::vector<float> data2 = {1.0f, 0.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  float dist = ComputeDistance(vec1, vec2, MetricType::COSINE);
  CHECK(dist == doctest::Approx(0.0f).epsilon(1e-6f));
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

TEST_CASE("MetadataTest - GetMetadataType") {
  CHECK_EQ(get_metadata_type(MetadataValue(int64_t(42))), MetadataType::INT64);
  CHECK_EQ(get_metadata_type(MetadataValue(3.14)), MetadataType::DOUBLE);
  CHECK_EQ(get_metadata_type(MetadataValue(std::string("test"))), MetadataType::STRING);
  CHECK_EQ(get_metadata_type(MetadataValue(true)), MetadataType::BOOL);
}

TEST_CASE("MetadataTest - CompareEqual") {
  CHECK(compare_metadata_values(
      MetadataValue(int64_t(42)),
      ComparisonOp::EQUAL,
      MetadataValue(int64_t(42))));

  CHECK_FALSE(compare_metadata_values(
      MetadataValue(int64_t(42)),
      ComparisonOp::EQUAL,
      MetadataValue(int64_t(43))));

  CHECK(compare_metadata_values(
      MetadataValue(std::string("hello")),
      ComparisonOp::EQUAL,
      MetadataValue(std::string("hello"))));
}

TEST_CASE("MetadataTest - CompareNotEqual") {
  CHECK(compare_metadata_values(
      MetadataValue(int64_t(42)),
      ComparisonOp::NOT_EQUAL,
      MetadataValue(int64_t(43))));

  CHECK_FALSE(compare_metadata_values(
      MetadataValue(int64_t(42)),
      ComparisonOp::NOT_EQUAL,
      MetadataValue(int64_t(42))));

  // NOT_EQUAL works across types
  CHECK(compare_metadata_values(
      MetadataValue(int64_t(42)),
      ComparisonOp::NOT_EQUAL,
      MetadataValue(std::string("42"))));
}

TEST_CASE("MetadataTest - CompareLessThan") {
  CHECK(compare_metadata_values(
      MetadataValue(int64_t(10)),
      ComparisonOp::LESS_THAN,
      MetadataValue(int64_t(20))));

  CHECK_FALSE(compare_metadata_values(
      MetadataValue(int64_t(20)),
      ComparisonOp::LESS_THAN,
      MetadataValue(int64_t(10))));

  CHECK(compare_metadata_values(
      MetadataValue(1.5),
      ComparisonOp::LESS_THAN,
      MetadataValue(2.5)));

  CHECK(compare_metadata_values(
      MetadataValue(std::string("apple")),
      ComparisonOp::LESS_THAN,
      MetadataValue(std::string("banana"))));
}

TEST_CASE("MetadataTest - CompareLessEqual") {
  CHECK(compare_metadata_values(
      MetadataValue(int64_t(10)),
      ComparisonOp::LESS_EQUAL,
      MetadataValue(int64_t(10))));

  CHECK(compare_metadata_values(
      MetadataValue(int64_t(10)),
      ComparisonOp::LESS_EQUAL,
      MetadataValue(int64_t(20))));

  CHECK_FALSE(compare_metadata_values(
      MetadataValue(int64_t(20)),
      ComparisonOp::LESS_EQUAL,
      MetadataValue(int64_t(10))));
}

TEST_CASE("MetadataTest - CompareGreaterThan") {
  CHECK(compare_metadata_values(
      MetadataValue(int64_t(100)),
      ComparisonOp::GREATER_THAN,
      MetadataValue(int64_t(50))));

  CHECK_FALSE(compare_metadata_values(
      MetadataValue(int64_t(50)),
      ComparisonOp::GREATER_THAN,
      MetadataValue(int64_t(100))));
}

TEST_CASE("MetadataTest - CompareGreaterEqual") {
  CHECK(compare_metadata_values(
      MetadataValue(int64_t(100)),
      ComparisonOp::GREATER_EQUAL,
      MetadataValue(int64_t(100))));

  CHECK(compare_metadata_values(
      MetadataValue(int64_t(100)),
      ComparisonOp::GREATER_EQUAL,
      MetadataValue(int64_t(50))));

  CHECK_FALSE(compare_metadata_values(
      MetadataValue(int64_t(50)),
      ComparisonOp::GREATER_EQUAL,
      MetadataValue(int64_t(100))));
}

TEST_CASE("MetadataTest - LikePatternExactMatch") {
  CHECK(match_like_pattern("hello", "hello"));
  CHECK_FALSE(match_like_pattern("hello", "world"));
}

TEST_CASE("MetadataTest - LikePatternWildcard") {
  // % matches any sequence
  CHECK(match_like_pattern("Nike Air Max", "Nike%"));
  CHECK(match_like_pattern("Nike Pegasus", "Nike%"));
  CHECK(match_like_pattern("best running shoes", "%running%"));
  CHECK(match_like_pattern("running gear", "%running%"));
  CHECK_FALSE(match_like_pattern("walking shoes", "%running%"));
}

TEST_CASE("MetadataTest - LikePatternSingleChar") {
  // _ matches single character
  CHECK(match_like_pattern("Air Max", "Air_Max"));
  CHECK(match_like_pattern("Air-Max", "Air_Max"));
  CHECK_FALSE(match_like_pattern("AirMax", "Air_Max"));
}

TEST_CASE("MetadataTest - LikePatternEscape") {
  // Escaped % should match literal %
  CHECK(match_like_pattern("50%", "50\\%"));
  CHECK_FALSE(match_like_pattern("50off", "50\\%"));
}

TEST_CASE("MetadataTest - LikePatternEscapesAnyByte") {
  CHECK(match_like_pattern("a_b", "a\\_b"));
  CHECK_FALSE(match_like_pattern("axb", "a\\_b"));
  // A backslash before an ordinary byte makes it literal; it is not a regex
  // class, so "\d" matches the letter d and not a digit.
  CHECK(match_like_pattern("ad", "a\\d"));
  CHECK_FALSE(match_like_pattern("a5", "a\\d"));
  // A trailing lone backslash matches itself.
  CHECK(match_like_pattern("50\\", "50\\"));
}

TEST_CASE("MetadataTest - LikePatternRegexMetacharactersAreLiteral") {
  CHECK(match_like_pattern("a.b", "a.b"));
  CHECK_FALSE(match_like_pattern("axb", "a.b"));
  CHECK(match_like_pattern("(x)+[y]*{z}|^$?", "(x)+[y]*{z}|^$?"));
  CHECK_FALSE(match_like_pattern("xx", "x+"));
}

TEST_CASE("MetadataTest - LikePatternBacktracking") {
  CHECK(match_like_pattern("abcabcabd", "%abd"));
  CHECK(match_like_pattern("mississippi", "m%ss%pi"));
  CHECK(match_like_pattern("mississippi", "%s_s%"));
  CHECK_FALSE(match_like_pattern("mississippi", "m%ss%px"));
  CHECK(match_like_pattern("", "%"));
  CHECK(match_like_pattern("", "%%"));
  CHECK_FALSE(match_like_pattern("", "_"));
  CHECK_FALSE(match_like_pattern("abc", ""));
  CHECK(match_like_pattern("a\nb", "a%b"));
  CHECK(match_like_pattern("a\nb", "a_b"));
}

TEST_CASE("MetadataTest - LikePatternLongInputs") {
  const std::string long_a(100000, 'a');
  CHECK(match_like_pattern(long_a, long_a));
  CHECK(match_like_pattern(long_a, std::string(100000, '%')));
  CHECK(match_like_pattern(long_a, std::string(100000, '_')));
  CHECK_FALSE(match_like_pattern(long_a, std::string(100001, '_')));

  const std::string value = std::string(65536, 'x') + "a";
  CHECK(match_like_pattern(value, "%a"));
  CHECK(match_like_pattern(value, "%a%"));
  CHECK_FALSE(match_like_pattern(value, "a%"));
  CHECK_FALSE(match_like_pattern(value, "%b%"));
}

TEST_CASE("MetadataTest - CompareLike") {
  CHECK(compare_metadata_values(
      MetadataValue(std::string("Nike Air Max")),
      ComparisonOp::LIKE,
      MetadataValue(std::string("Nike%"))));

  CHECK_FALSE(compare_metadata_values(
      MetadataValue(std::string("Adidas Ultra")),
      ComparisonOp::LIKE,
      MetadataValue(std::string("Nike%"))));
}

TEST_CASE("MetadataTest - CompareNotLike") {
  CHECK_FALSE(compare_metadata_values(
      MetadataValue(std::string("Nike Air Max")),
      ComparisonOp::NOT_LIKE,
      MetadataValue(std::string("Nike%"))));

  CHECK(compare_metadata_values(
      MetadataValue(std::string("Adidas Ultra")),
      ComparisonOp::NOT_LIKE,
      MetadataValue(std::string("Nike%"))));
}

TEST_CASE("MetadataTest - CompareInList") {
  std::vector<MetadataValue> brands = {
      MetadataValue(std::string("Nike")),
      MetadataValue(std::string("Adidas")),
      MetadataValue(std::string("Puma"))
  };

  CHECK(compare_metadata_value_in_list(
      MetadataValue(std::string("Nike")),
      false,  // not negated
      brands));

  CHECK_FALSE(compare_metadata_value_in_list(
      MetadataValue(std::string("Reebok")),
      false,
      brands));
}

TEST_CASE("MetadataTest - CompareNotInList") {
  std::vector<MetadataValue> numbers = {
      MetadataValue(int64_t(1)),
      MetadataValue(int64_t(2)),
      MetadataValue(int64_t(3))
  };

  CHECK_FALSE(compare_metadata_value_in_list(
      MetadataValue(int64_t(2)),
      true,  // negated (NOT IN)
      numbers));

  CHECK(compare_metadata_value_in_list(
      MetadataValue(int64_t(5)),
      true,  // negated (NOT IN)
      numbers));
}

TEST_CASE("MetadataTest - ValidateKeyValid") {
  CHECK(validate_metadata_key("price").ok());
  CHECK(validate_metadata_key("brand_name").ok());
  CHECK(validate_metadata_key("_internal").ok());
  CHECK(validate_metadata_key("field123").ok());
}

TEST_CASE("MetadataTest - ValidateKeyEmpty") {
  auto status = validate_metadata_key("");
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateKeyStartsWithDigit") {
  auto status = validate_metadata_key("123field");
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateKeyInvalidCharacters") {
  CHECK_FALSE(validate_metadata_key("field-name").ok());
  CHECK_FALSE(validate_metadata_key("field.name").ok());
  CHECK_FALSE(validate_metadata_key("field name").ok());
  CHECK_FALSE(validate_metadata_key("field@name").ok());
}

TEST_CASE("MetadataTest - ValidateKeyTooLong") {
  std::string long_key(256, 'a');
  auto status = validate_metadata_key(long_key);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateValueInt64") {
  CHECK(validate_metadata_value(MetadataValue(int64_t(42))).ok());
  CHECK(validate_metadata_value(MetadataValue(int64_t(-100))).ok());
}

TEST_CASE("MetadataTest - ValidateValueDouble") {
  CHECK(validate_metadata_value(MetadataValue(3.14)).ok());
  CHECK_FALSE(validate_metadata_value(
      MetadataValue(std::numeric_limits<double>::quiet_NaN())).ok());
  CHECK_FALSE(validate_metadata_value(
      MetadataValue(std::numeric_limits<double>::infinity())).ok());
}

TEST_CASE("MetadataTest - ValidateValueString") {
  CHECK(validate_metadata_value(MetadataValue(std::string("hello"))).ok());

  // String too long (>64KB)
  std::string long_string(65537, 'x');
  auto status = validate_metadata_value(MetadataValue(long_string));
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateValueBool") {
  CHECK(validate_metadata_value(MetadataValue(true)).ok());
  CHECK(validate_metadata_value(MetadataValue(false)).ok());
}

TEST_CASE("MetadataTest - ValidateMetadataValid") {
  Metadata metadata = {
      {"price", MetadataValue(int64_t(100))},
      {"brand", MetadataValue(std::string("Nike"))},
      {"in_stock", MetadataValue(true)},
      {"rating", MetadataValue(4.5)}
  };

  CHECK(validate_metadata(metadata).ok());
}

TEST_CASE("MetadataTest - ValidateMetadataTooManyFields") {
  Metadata metadata;
  for (int i = 0; i < 101; ++i) {
    metadata["field_" + std::to_string(i)] = MetadataValue(int64_t(i));
  }

  auto status = validate_metadata(metadata);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateMetadataInvalidKey") {
  Metadata metadata = {
      {"invalid-key", MetadataValue(int64_t(100))}
  };

  auto status = validate_metadata(metadata);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE("MetadataTest - ValidateMetadataInvalidValue") {
  Metadata metadata = {
      {"rating", MetadataValue(std::numeric_limits<double>::quiet_NaN())}
  };

  auto status = validate_metadata(metadata);
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
  CHECK((*filter)->evaluate(metadata));

  Metadata metadata2 = {{"price", MetadataValue(int64_t(150))}};
  CHECK_FALSE((*filter)->evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseEqualityComparison") {
  auto filter = FilterParser::parse("brand = 'Nike'");
  REQUIRE(filter.ok());

  Metadata metadata = {{"brand", MetadataValue(std::string("Nike"))}};
  CHECK((*filter)->evaluate(metadata));

  Metadata metadata2 = {{"brand", MetadataValue(std::string("Adidas"))}};
  CHECK_FALSE((*filter)->evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseBooleanComparison") {
  auto filter = FilterParser::parse("in_stock = true");
  REQUIRE(filter.ok());

  Metadata metadata = {{"in_stock", MetadataValue(true)}};
  CHECK((*filter)->evaluate(metadata));

  Metadata metadata2 = {{"in_stock", MetadataValue(false)}};
  CHECK_FALSE((*filter)->evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseDoubleComparison") {
  auto filter = FilterParser::parse("rating >= 4.5");
  REQUIRE(filter.ok());

  Metadata metadata = {{"rating", MetadataValue(4.8)}};
  CHECK((*filter)->evaluate(metadata));

  Metadata metadata2 = {{"rating", MetadataValue(3.2)}};
  CHECK_FALSE((*filter)->evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseAndExpression") {
  auto filter = FilterParser::parse("price < 100 AND brand = 'Nike'");
  REQUIRE(filter.ok());

  Metadata metadata = {
      {"price", MetadataValue(int64_t(50))},
      {"brand", MetadataValue(std::string("Nike"))}};
  CHECK((*filter)->evaluate(metadata));

  Metadata metadata2 = {
      {"price", MetadataValue(int64_t(150))},
      {"brand", MetadataValue(std::string("Nike"))}};
  CHECK_FALSE((*filter)->evaluate(metadata2));

  Metadata metadata3 = {
      {"price", MetadataValue(int64_t(50))},
      {"brand", MetadataValue(std::string("Adidas"))}};
  CHECK_FALSE((*filter)->evaluate(metadata3));
}

TEST_CASE("FilterTest - ParseOrExpression") {
  auto filter = FilterParser::parse("price < 50 OR discount > 20");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"price", MetadataValue(int64_t(30))}};
  CHECK((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"discount", MetadataValue(int64_t(25))}};
  CHECK((*filter)->evaluate(metadata2));

  Metadata metadata3 = {
      {"price", MetadataValue(int64_t(80))},
      {"discount", MetadataValue(int64_t(10))}};
  CHECK_FALSE((*filter)->evaluate(metadata3));
}

TEST_CASE("FilterTest - ParseNotExpression") {
  auto filter = FilterParser::parse("NOT (price > 100)");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"price", MetadataValue(int64_t(50))}};
  CHECK((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"price", MetadataValue(int64_t(150))}};
  CHECK_FALSE((*filter)->evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseParenthesizedExpression") {
  auto filter = FilterParser::parse("(price < 100 OR discount > 20) AND in_stock = true");
  REQUIRE(filter.ok());

  Metadata metadata1 = {
      {"price", MetadataValue(int64_t(50))},
      {"in_stock", MetadataValue(true)}};
  CHECK((*filter)->evaluate(metadata1));

  Metadata metadata2 = {
      {"discount", MetadataValue(int64_t(25))},
      {"in_stock", MetadataValue(true)}};
  CHECK((*filter)->evaluate(metadata2));

  Metadata metadata3 = {
      {"price", MetadataValue(int64_t(50))},
      {"in_stock", MetadataValue(false)}};
  CHECK_FALSE((*filter)->evaluate(metadata3));
}

TEST_CASE("FilterTest - ParseLikeExpression") {
  auto filter = FilterParser::parse("brand LIKE 'Nike%'");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"brand", MetadataValue(std::string("Nike Air Max"))}};
  CHECK((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"brand", MetadataValue(std::string("Adidas"))}};
  CHECK_FALSE((*filter)->evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseNotLikeExpression") {
  auto filter = FilterParser::parse("brand NOT LIKE '%test%'");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"brand", MetadataValue(std::string("Nike"))}};
  CHECK((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"brand", MetadataValue(std::string("test product"))}};
  CHECK_FALSE((*filter)->evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseInExpression") {
  auto filter = FilterParser::parse("category IN ('shoes', 'apparel', 'accessories')");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"category", MetadataValue(std::string("shoes"))}};
  CHECK((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"category", MetadataValue(std::string("electronics"))}};
  CHECK_FALSE((*filter)->evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseNotInExpression") {
  auto filter = FilterParser::parse("status NOT IN (0, 1, 2)");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"status", MetadataValue(int64_t(5))}};
  CHECK((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"status", MetadataValue(int64_t(1))}};
  CHECK_FALSE((*filter)->evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseInWithMixedTypes") {
  auto filter = FilterParser::parse("value IN (1, 2.5, 'text')");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"value", MetadataValue(int64_t(1))}};
  CHECK((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"value", MetadataValue(2.5)}};
  CHECK((*filter)->evaluate(metadata2));

  Metadata metadata3 = {{"value", MetadataValue(std::string("text"))}};
  CHECK((*filter)->evaluate(metadata3));

  Metadata metadata4 = {{"value", MetadataValue(int64_t(99))}};
  CHECK_FALSE((*filter)->evaluate(metadata4));
}

TEST_CASE("FilterTest - ComplexNestedExpression") {
  auto filter = FilterParser::parse(
      "(price < 100 AND brand = 'Nike') OR (discount > 50 AND in_stock = true)");
  REQUIRE(filter.ok());

  Metadata metadata1 = {
      {"price", MetadataValue(int64_t(80))},
      {"brand", MetadataValue(std::string("Nike"))}};
  CHECK((*filter)->evaluate(metadata1));

  Metadata metadata2 = {
      {"discount", MetadataValue(int64_t(60))},
      {"in_stock", MetadataValue(true)}};
  CHECK((*filter)->evaluate(metadata2));

  Metadata metadata3 = {
      {"price", MetadataValue(int64_t(120))},
      {"brand", MetadataValue(std::string("Adidas"))},
      {"discount", MetadataValue(int64_t(20))}};
  CHECK_FALSE((*filter)->evaluate(metadata3));
}

TEST_CASE("FilterTest - ParseNegativeNumber") {
  auto filter = FilterParser::parse("temperature > -10");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"temperature", MetadataValue(int64_t(-5))}};
  CHECK((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"temperature", MetadataValue(int64_t(-15))}};
  CHECK_FALSE((*filter)->evaluate(metadata2));
}

TEST_CASE("FilterTest - ParseStringWithEscapes") {
  auto filter = FilterParser::parse("name = 'O\\'Brien'");
  REQUIRE(filter.ok());

  Metadata metadata1 = {{"name", MetadataValue(std::string("O'Brien"))}};
  CHECK((*filter)->evaluate(metadata1));
}

TEST_CASE("FilterTest - MissingField") {
  auto filter = FilterParser::parse("price < 100");
  REQUIRE(filter.ok());

  Metadata metadata = {{"brand", MetadataValue(std::string("Nike"))}};
  // Field not present -> false
  CHECK_FALSE((*filter)->evaluate(metadata));
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

TEST_CASE("FilterTest - EvaluateFilterUtility") {
  Metadata metadata = {
      {"price", MetadataValue(int64_t(50))},
      {"brand", MetadataValue(std::string("Nike"))}};

  auto result = evaluate_filter("price < 100 AND brand = 'Nike'", metadata);
  REQUIRE(result.ok());
  CHECK(*result);

  auto result2 = evaluate_filter("price > 100", metadata);
  REQUIRE(result2.ok());
  CHECK_FALSE(*result2);
}

TEST_CASE("FilterTest - ValidateFilterUtility") {
  CHECK(validate_filter("price < 100").ok());
  CHECK(validate_filter("brand = 'Nike' AND in_stock = true").ok());
  CHECK_FALSE(validate_filter("price <").ok());
  CHECK_FALSE(validate_filter("invalid syntax @#$").ok());
}

TEST_CASE("FilterTest - FilterToString") {
  auto filter = FilterParser::parse("price < 100");
  REQUIRE(filter.ok());

  std::string str = (*filter)->to_string();
  CHECK_FALSE(str.empty());
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
  CHECK((*max)->evaluate({{"i", MetadataValue(int64_t{INT64_MAX})}}));

  auto min = FilterParser::parse("i = -9223372036854775808");
  REQUIRE(min.ok());
  CHECK((*min)->evaluate({{"i", MetadataValue(int64_t{INT64_MIN})}}));

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
  CHECK((*filter)->evaluate({{"x", MetadataValue(0.5)}}));
}

TEST_CASE("FilterTest - NumericLiteralForms") {
  auto plus = FilterParser::parse("i = +5");
  REQUIRE(plus.ok());
  CHECK((*plus)->evaluate({{"i", MetadataValue(int64_t{5})}}));

  auto trailing_dot = FilterParser::parse("x = 1.");
  REQUIRE(trailing_dot.ok());
  CHECK((*trailing_dot)->evaluate({{"x", MetadataValue(1.0)}}));

  auto negative_fraction = FilterParser::parse("x = -.5");
  REQUIRE(negative_fraction.ok());
  CHECK((*negative_fraction)->evaluate({{"x", MetadataValue(-0.5)}}));

  auto negative_zero = FilterParser::parse("i = -0");
  REQUIRE(negative_zero.ok());
  CHECK((*negative_zero)->evaluate({{"i", MetadataValue(int64_t{0})}}));

  for (const char* expr : {"x = .5", "x = -", "x = 1.2.3", "x = +"}) {
    INFO(expr);
    CHECK(absl::IsInvalidArgument(FilterParser::parse(expr).status()));
  }
}

TEST_CASE("FilterTest - KeywordEdgeCases") {
  auto lower = FilterParser::parse("a = 1 and not b = 2 or c = 3");
  REQUIRE(lower.ok());
  CHECK((*lower)->evaluate({{"a", MetadataValue(int64_t{1})},
                            {"b", MetadataValue(int64_t{5})}}));

  // Boolean literals are lowercase only.
  CHECK(absl::IsInvalidArgument(FilterParser::parse("f = TRUE").status()));

  // A keyword followed by an operator is still usable as a field name.
  auto field_named_and = FilterParser::parse("and = 1");
  REQUIRE(field_named_and.ok());
  CHECK((*field_named_and)->evaluate({{"and", MetadataValue(int64_t{1})}}));

  CHECK(absl::IsInvalidArgument(FilterParser::parse("not = 1").status()));
}

TEST_CASE("FilterTest - NonAsciiInput") {
  auto quoted = FilterParser::parse("name = 'caf\xC3\xA9'");
  REQUIRE(quoted.ok());
  CHECK((*quoted)->evaluate({{"name", MetadataValue(std::string("caf\xC3\xA9"))}}));

  CHECK(absl::IsInvalidArgument(
      FilterParser::parse("pr\xC3\xAF""ce = 1").status()));
  CHECK(absl::IsInvalidArgument(FilterParser::parse("\xFF").status()));
}

TEST_CASE("FilterTest - LikeEscapesThroughParser") {
  // The string-literal lexer consumes one backslash, so the filter text
  // '50\\%' reaches LIKE as the pattern 50\% (a literal percent sign) ...
  auto literal = FilterParser::parse("k LIKE '50\\\\%'");
  REQUIRE(literal.ok());
  CHECK((*literal)->evaluate({{"k", MetadataValue(std::string("50%"))}}));
  CHECK_FALSE((*literal)->evaluate({{"k", MetadataValue(std::string("50x"))}}));

  // ... while '50\%' reaches it as 50% (a wildcard).
  auto wildcard = FilterParser::parse("k LIKE '50\\%'");
  REQUIRE(wildcard.ok());
  CHECK((*wildcard)->evaluate({{"k", MetadataValue(std::string("50x"))}}));
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
  auto parens = [](int depth) {
    return Repeat("(", depth) + "a = 1" + Repeat(")", depth);
  };

  bool at_limit_ok = false;
  bool at_limit_matches = false;
  bool not_at_limit_ok = false;
  RunOnSmallStack([&] {
    auto nested = FilterParser::parse(parens(max));
    at_limit_ok = nested.ok();
    at_limit_matches =
        nested.ok() && (*nested)->evaluate({{"a", MetadataValue(int64_t{1})}});
    // max NOTs is an even count, so it evaluates like the bare comparison.
    not_at_limit_ok = FilterParser::parse(Repeat("NOT ", max) + "a = 1").ok();
  });
  CHECK(at_limit_ok);
  CHECK(at_limit_matches);
  CHECK(not_at_limit_ok);

  CHECK(absl::IsInvalidArgument(FilterParser::parse(parens(max + 1)).status()));
  CHECK(absl::IsInvalidArgument(
      FilterParser::parse(Repeat("NOT ", max + 1) + "a = 1").status()));
  // Deep enough to overflow any thread stack if the parser recursed on it.
  CHECK(absl::IsInvalidArgument(FilterParser::parse(parens(1000000)).status()));
  CHECK(absl::IsInvalidArgument(
      FilterParser::parse(Repeat("NOT ", 1000000) + "a = 1").status()));
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
      or_first = (*ors)->evaluate({{"id", MetadataValue(int64_t{0})}});
      or_last = (*ors)->evaluate({{"id", MetadataValue(int64_t{terms - 1})}});
      or_miss = (*ors)->evaluate({{"id", MetadataValue(int64_t{terms})}});
    }
    auto ands = FilterParser::parse(and_chain);
    and_ok = ands.ok();
    if (and_ok) {
      and_all = (*ands)->evaluate(all_ones);
      Metadata one_off = all_ones;
      one_off["a57"] = MetadataValue(int64_t{0});
      and_one_off = (*ands)->evaluate(one_off);
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
  CHECK((*filter)->evaluate({{"id", MetadataValue(int64_t{99999})}}));
  CHECK_FALSE((*filter)->evaluate({{"id", MetadataValue(int64_t{100000})}}));
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

TEST_CASE("ConfigTest - StorageConfigValidation") {
  StorageConfig config;
  CHECK(config.IsValid());  // Has default values

  config.base_path = "";
  CHECK_FALSE(config.IsValid());

  config.base_path = "/tmp/data";
  config.max_segment_size = 0;
  CHECK_FALSE(config.IsValid());
}

TEST_CASE("ConfigTest - ClusterConfigValidation") {
  ClusterConfig config;
  CHECK_FALSE(config.IsValid());  // node_id is invalid

  config.node_id = MakeNodeId(1);
  CHECK_FALSE(config.IsValid());  // node_address is empty

  config.node_address = "localhost";
  CHECK(config.IsValid());
}

TEST_CASE("ConfigTest - QueryConfigValidation") {
  QueryConfig config;
  CHECK(config.IsValid());

  config.default_topk = 0;
  CHECK_FALSE(config.IsValid());

  config.default_topk = 10;
  config.max_topk = 5;  // Less than default
  CHECK_FALSE(config.IsValid());
}

TEST_CASE("ConfigTest - SystemConfigValidation") {
  SystemConfig config;
  config.cluster_config.node_id = MakeNodeId(1);
  config.cluster_config.node_address = "localhost";

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
  CHECK_EQ(std::get<std::string>(deserialized.at("description")),
            "High-performance running shoes");
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
