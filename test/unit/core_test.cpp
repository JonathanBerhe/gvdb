#include <gtest/gtest.h>

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

TEST(TypesTest, VectorIdCreation) {
  VectorId id1 = MakeVectorId(42);
  VectorId id2 = MakeVectorId(42);
  VectorId id3 = MakeVectorId(43);

  EXPECT_EQ(ToUInt64(id1), 42);
  EXPECT_EQ(ToUInt64(id1), ToUInt64(id2));
  EXPECT_NE(ToUInt64(id1), ToUInt64(id3));
}

TEST(TypesTest, CollectionIdCreation) {
  CollectionId id = MakeCollectionId(100);
  EXPECT_EQ(ToUInt32(id), 100);
}

TEST(TypesTest, InvalidIds) {
  EXPECT_EQ(ToUInt64(kInvalidVectorId), 0);
  EXPECT_EQ(ToUInt32(kInvalidCollectionId), 0);
  EXPECT_EQ(ToUInt32(kInvalidSegmentId), 0);
  EXPECT_EQ(ToUInt16(kInvalidShardId), 0);
}

TEST(TypesTest, SearchResultEntry) {
  SearchResultEntry entry(MakeVectorId(123), 0.5f);
  EXPECT_EQ(ToUInt64(entry.id), 123);
  EXPECT_FLOAT_EQ(entry.distance, 0.5f);
}

TEST(TypesTest, SearchResult) {
  SearchResult result(10);
  EXPECT_TRUE(result.Empty());
  EXPECT_EQ(result.Size(), 0);

  result.AddEntry(MakeVectorId(1), 0.1f);
  result.AddEntry(MakeVectorId(2), 0.2f);

  EXPECT_FALSE(result.Empty());
  EXPECT_EQ(result.Size(), 2);
  EXPECT_EQ(ToUInt64(result.entries[0].id), 1);
  EXPECT_FLOAT_EQ(result.entries[0].distance, 0.1f);
}

// ============================================================================
// Status Tests
// ============================================================================

TEST(StatusTest, OkStatus) {
  Status status = OkStatus();
  EXPECT_TRUE(status.ok());
}

TEST(StatusTest, ErrorStatuses) {
  Status invalid = InvalidArgumentError("invalid arg");
  EXPECT_FALSE(invalid.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(invalid));

  Status internal = InternalError("internal error");
  EXPECT_FALSE(internal.ok());
  EXPECT_TRUE(absl::IsInternal(internal));

  Status not_found = NotFoundError("not found");
  EXPECT_FALSE(not_found.ok());
  EXPECT_TRUE(absl::IsNotFound(not_found));
}

TEST(StatusTest, StatusOrSuccess) {
  StatusOr<int> result = 42;
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value(), 42);
}

TEST(StatusTest, StatusOrError) {
  StatusOr<int> result = InvalidArgumentError("error");
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(result.status()));
}

// ============================================================================
// Vector Tests
// ============================================================================

TEST(VectorTest, Construction) {
  Vector vec(128);
  EXPECT_EQ(vec.dimension(), 128);
  EXPECT_EQ(vec.size(), 128);
  EXPECT_NE(vec.data(), nullptr);
}

TEST(VectorTest, ConstructionZeroDimension) {
  EXPECT_THROW(Vector(0), std::invalid_argument);
}

TEST(VectorTest, ConstructionNegativeDimension) {
  // std::vector throws std::length_error for negative size
  EXPECT_THROW(Vector(-1), std::exception);
}

TEST(VectorTest, ConstructionFromData) {
  float data[4] = {1.0f, 2.0f, 3.0f, 4.0f};
  Vector vec(4, data);

  EXPECT_EQ(vec.dimension(), 4);
  EXPECT_FLOAT_EQ(vec[0], 1.0f);
  EXPECT_FLOAT_EQ(vec[1], 2.0f);
  EXPECT_FLOAT_EQ(vec[2], 3.0f);
  EXPECT_FLOAT_EQ(vec[3], 4.0f);
}

TEST(VectorTest, ConstructionFromStdVector) {
  std::vector<float> data = {1.0f, 2.0f, 3.0f};
  Vector vec(data);

  EXPECT_EQ(vec.dimension(), 3);
  EXPECT_FLOAT_EQ(vec[0], 1.0f);
  EXPECT_FLOAT_EQ(vec[1], 2.0f);
  EXPECT_FLOAT_EQ(vec[2], 3.0f);
}

TEST(VectorTest, AlignmentVerification) {
  Vector vec(128);
  auto addr = reinterpret_cast<uintptr_t>(vec.data());
  EXPECT_EQ(addr % 32, 0) << "Vector data must be 32-byte aligned for AVX";
}

TEST(VectorTest, MoveSemantics) {
  Vector vec1(4);
  vec1[0] = 1.0f;
  vec1[1] = 2.0f;

  Vector vec2(std::move(vec1));
  EXPECT_EQ(vec2.dimension(), 4);
  EXPECT_FLOAT_EQ(vec2[0], 1.0f);
  EXPECT_FLOAT_EQ(vec2[1], 2.0f);
}

TEST(VectorTest, CopySemantics) {
  Vector vec1(4);
  vec1[0] = 1.0f;
  vec1[1] = 2.0f;

  Vector vec2 = vec1;
  EXPECT_EQ(vec2.dimension(), 4);
  EXPECT_FLOAT_EQ(vec2[0], 1.0f);
  EXPECT_FLOAT_EQ(vec2[1], 2.0f);

  // Modify vec2 shouldn't affect vec1
  vec2[0] = 5.0f;
  EXPECT_FLOAT_EQ(vec1[0], 1.0f);
}

TEST(VectorTest, Norm) {
  std::vector<float> data = {3.0f, 4.0f};
  Vector vec(data);
  EXPECT_FLOAT_EQ(vec.Norm(), 5.0f);
}

TEST(VectorTest, NormalizeValidVector) {
  std::vector<float> data = {3.0f, 4.0f};
  Vector vec(data);

  auto result = vec.Normalize();
  ASSERT_TRUE(result.ok());

  Vector normalized = std::move(result.value());
  EXPECT_NEAR(normalized.Norm(), 1.0f, 1e-6f);
  EXPECT_NEAR(normalized[0], 0.6f, 1e-6f);
  EXPECT_NEAR(normalized[1], 0.8f, 1e-6f);
}

TEST(VectorTest, NormalizeZeroVector) {
  Vector vec(4);  // All zeros
  auto result = vec.Normalize();
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(result.status()));
}

TEST(VectorTest, L2Distance) {
  std::vector<float> data1 = {1.0f, 0.0f};
  std::vector<float> data2 = {4.0f, 0.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  EXPECT_FLOAT_EQ(vec1.L2Distance(vec2), 3.0f);
}

TEST(VectorTest, L2DistanceDimensionMismatch) {
  Vector vec1(3);
  Vector vec2(4);
  EXPECT_LT(vec1.L2Distance(vec2), 0.0f);  // Returns -1 for invalid
}

TEST(VectorTest, InnerProduct) {
  std::vector<float> data1 = {1.0f, 2.0f, 3.0f};
  std::vector<float> data2 = {4.0f, 5.0f, 6.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
  EXPECT_FLOAT_EQ(vec1.InnerProduct(vec2), 32.0f);
}

TEST(VectorTest, CosineDistance) {
  std::vector<float> data1 = {1.0f, 0.0f};
  std::vector<float> data2 = {1.0f, 0.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  // Same vector: cosine similarity = 1, distance = 0
  EXPECT_NEAR(vec1.CosineDistance(vec2), 0.0f, 1e-6f);
}

TEST(VectorTest, CosineDistanceOrthogonal) {
  std::vector<float> data1 = {1.0f, 0.0f};
  std::vector<float> data2 = {0.0f, 1.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  // Orthogonal: cosine similarity = 0, distance = 1
  EXPECT_NEAR(vec1.CosineDistance(vec2), 1.0f, 1e-6f);
}

TEST(VectorTest, Validation) {
  Vector vec(4);
  EXPECT_TRUE(vec.IsValid());

  Status status = vec.Validate();
  EXPECT_TRUE(status.ok());
}

TEST(VectorTest, LargeDimension) {
  // Test with a large dimension to ensure no overflow
  Vector vec(1048576);  // 2^20 elements
  EXPECT_EQ(vec.dimension(), 1048576);
  EXPECT_TRUE(vec.IsValid());
}

// ============================================================================
// Vector Utility Tests
// ============================================================================

TEST(VectorUtilTest, ZeroVector) {
  Vector vec = ZeroVector(8);
  EXPECT_EQ(vec.dimension(), 8);
  for (int i = 0; i < 8; ++i) {
    EXPECT_FLOAT_EQ(vec[i], 0.0f);
  }
}

TEST(VectorUtilTest, RandomVector) {
  Vector vec = RandomVector(128);
  EXPECT_EQ(vec.dimension(), 128);

  // Check that not all values are zero (very unlikely for random)
  bool has_non_zero = false;
  for (int i = 0; i < 128; ++i) {
    if (vec[i] != 0.0f) {
      has_non_zero = true;
      break;
    }
  }
  EXPECT_TRUE(has_non_zero);
}

TEST(VectorUtilTest, ValidateDimensionMatch) {
  Vector vec1(4);
  Vector vec2(4);
  Vector vec3(8);

  Status status1 = ValidateDimensionMatch(vec1, vec2);
  EXPECT_TRUE(status1.ok());

  Status status2 = ValidateDimensionMatch(vec1, vec3);
  EXPECT_FALSE(status2.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status2));
}

TEST(VectorUtilTest, ComputeDistanceL2) {
  std::vector<float> data1 = {1.0f, 0.0f};
  std::vector<float> data2 = {4.0f, 0.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  float dist = ComputeDistance(vec1, vec2, MetricType::L2);
  EXPECT_FLOAT_EQ(dist, 3.0f);
}

TEST(VectorUtilTest, ComputeDistanceInnerProduct) {
  std::vector<float> data1 = {1.0f, 2.0f};
  std::vector<float> data2 = {3.0f, 4.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  // Inner product: 1*3 + 2*4 = 11, negated = -11
  float dist = ComputeDistance(vec1, vec2, MetricType::INNER_PRODUCT);
  EXPECT_FLOAT_EQ(dist, -11.0f);
}

TEST(VectorUtilTest, ComputeDistanceCosine) {
  std::vector<float> data1 = {1.0f, 0.0f};
  std::vector<float> data2 = {1.0f, 0.0f};
  Vector vec1(data1);
  Vector vec2(data2);

  float dist = ComputeDistance(vec1, vec2, MetricType::COSINE);
  EXPECT_NEAR(dist, 0.0f, 1e-6f);
}

// ============================================================================
// Metadata Tests
// ============================================================================

TEST(MetadataTest, MetadataValueTypes) {
  MetadataValue int_val = int64_t(42);
  MetadataValue double_val = 3.14;
  MetadataValue string_val = std::string("hello");
  MetadataValue bool_val = true;

  EXPECT_EQ(std::get<int64_t>(int_val), 42);
  EXPECT_DOUBLE_EQ(std::get<double>(double_val), 3.14);
  EXPECT_EQ(std::get<std::string>(string_val), "hello");
  EXPECT_EQ(std::get<bool>(bool_val), true);
}

TEST(MetadataTest, GetMetadataType) {
  EXPECT_EQ(get_metadata_type(MetadataValue(int64_t(42))), MetadataType::INT64);
  EXPECT_EQ(get_metadata_type(MetadataValue(3.14)), MetadataType::DOUBLE);
  EXPECT_EQ(get_metadata_type(MetadataValue(std::string("test"))), MetadataType::STRING);
  EXPECT_EQ(get_metadata_type(MetadataValue(true)), MetadataType::BOOL);
}

TEST(MetadataTest, CompareEqual) {
  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(int64_t(42)),
      ComparisonOp::EQUAL,
      MetadataValue(int64_t(42))));

  EXPECT_FALSE(compare_metadata_values(
      MetadataValue(int64_t(42)),
      ComparisonOp::EQUAL,
      MetadataValue(int64_t(43))));

  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(std::string("hello")),
      ComparisonOp::EQUAL,
      MetadataValue(std::string("hello"))));
}

TEST(MetadataTest, CompareNotEqual) {
  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(int64_t(42)),
      ComparisonOp::NOT_EQUAL,
      MetadataValue(int64_t(43))));

  EXPECT_FALSE(compare_metadata_values(
      MetadataValue(int64_t(42)),
      ComparisonOp::NOT_EQUAL,
      MetadataValue(int64_t(42))));

  // NOT_EQUAL works across types
  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(int64_t(42)),
      ComparisonOp::NOT_EQUAL,
      MetadataValue(std::string("42"))));
}

TEST(MetadataTest, CompareLessThan) {
  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(int64_t(10)),
      ComparisonOp::LESS_THAN,
      MetadataValue(int64_t(20))));

  EXPECT_FALSE(compare_metadata_values(
      MetadataValue(int64_t(20)),
      ComparisonOp::LESS_THAN,
      MetadataValue(int64_t(10))));

  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(1.5),
      ComparisonOp::LESS_THAN,
      MetadataValue(2.5)));

  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(std::string("apple")),
      ComparisonOp::LESS_THAN,
      MetadataValue(std::string("banana"))));
}

TEST(MetadataTest, CompareLessEqual) {
  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(int64_t(10)),
      ComparisonOp::LESS_EQUAL,
      MetadataValue(int64_t(10))));

  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(int64_t(10)),
      ComparisonOp::LESS_EQUAL,
      MetadataValue(int64_t(20))));

  EXPECT_FALSE(compare_metadata_values(
      MetadataValue(int64_t(20)),
      ComparisonOp::LESS_EQUAL,
      MetadataValue(int64_t(10))));
}

TEST(MetadataTest, CompareGreaterThan) {
  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(int64_t(100)),
      ComparisonOp::GREATER_THAN,
      MetadataValue(int64_t(50))));

  EXPECT_FALSE(compare_metadata_values(
      MetadataValue(int64_t(50)),
      ComparisonOp::GREATER_THAN,
      MetadataValue(int64_t(100))));
}

TEST(MetadataTest, CompareGreaterEqual) {
  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(int64_t(100)),
      ComparisonOp::GREATER_EQUAL,
      MetadataValue(int64_t(100))));

  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(int64_t(100)),
      ComparisonOp::GREATER_EQUAL,
      MetadataValue(int64_t(50))));

  EXPECT_FALSE(compare_metadata_values(
      MetadataValue(int64_t(50)),
      ComparisonOp::GREATER_EQUAL,
      MetadataValue(int64_t(100))));
}

TEST(MetadataTest, LikePatternExactMatch) {
  EXPECT_TRUE(match_like_pattern("hello", "hello"));
  EXPECT_FALSE(match_like_pattern("hello", "world"));
}

TEST(MetadataTest, LikePatternWildcard) {
  // % matches any sequence
  EXPECT_TRUE(match_like_pattern("Nike Air Max", "Nike%"));
  EXPECT_TRUE(match_like_pattern("Nike Pegasus", "Nike%"));
  EXPECT_TRUE(match_like_pattern("best running shoes", "%running%"));
  EXPECT_TRUE(match_like_pattern("running gear", "%running%"));
  EXPECT_FALSE(match_like_pattern("walking shoes", "%running%"));
}

TEST(MetadataTest, LikePatternSingleChar) {
  // _ matches single character
  EXPECT_TRUE(match_like_pattern("Air Max", "Air_Max"));
  EXPECT_TRUE(match_like_pattern("Air-Max", "Air_Max"));
  EXPECT_FALSE(match_like_pattern("AirMax", "Air_Max"));
}

TEST(MetadataTest, LikePatternEscape) {
  // Escaped % should match literal %
  EXPECT_TRUE(match_like_pattern("50%", "50\\%"));
  EXPECT_FALSE(match_like_pattern("50off", "50\\%"));
}

TEST(MetadataTest, CompareLike) {
  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(std::string("Nike Air Max")),
      ComparisonOp::LIKE,
      MetadataValue(std::string("Nike%"))));

  EXPECT_FALSE(compare_metadata_values(
      MetadataValue(std::string("Adidas Ultra")),
      ComparisonOp::LIKE,
      MetadataValue(std::string("Nike%"))));
}

TEST(MetadataTest, CompareNotLike) {
  EXPECT_FALSE(compare_metadata_values(
      MetadataValue(std::string("Nike Air Max")),
      ComparisonOp::NOT_LIKE,
      MetadataValue(std::string("Nike%"))));

  EXPECT_TRUE(compare_metadata_values(
      MetadataValue(std::string("Adidas Ultra")),
      ComparisonOp::NOT_LIKE,
      MetadataValue(std::string("Nike%"))));
}

TEST(MetadataTest, CompareInList) {
  std::vector<MetadataValue> brands = {
      MetadataValue(std::string("Nike")),
      MetadataValue(std::string("Adidas")),
      MetadataValue(std::string("Puma"))
  };

  EXPECT_TRUE(compare_metadata_value_in_list(
      MetadataValue(std::string("Nike")),
      false,  // not negated
      brands));

  EXPECT_FALSE(compare_metadata_value_in_list(
      MetadataValue(std::string("Reebok")),
      false,
      brands));
}

TEST(MetadataTest, CompareNotInList) {
  std::vector<MetadataValue> numbers = {
      MetadataValue(int64_t(1)),
      MetadataValue(int64_t(2)),
      MetadataValue(int64_t(3))
  };

  EXPECT_FALSE(compare_metadata_value_in_list(
      MetadataValue(int64_t(2)),
      true,  // negated (NOT IN)
      numbers));

  EXPECT_TRUE(compare_metadata_value_in_list(
      MetadataValue(int64_t(5)),
      true,  // negated (NOT IN)
      numbers));
}

TEST(MetadataTest, ValidateKeyValid) {
  EXPECT_TRUE(validate_metadata_key("price").ok());
  EXPECT_TRUE(validate_metadata_key("brand_name").ok());
  EXPECT_TRUE(validate_metadata_key("_internal").ok());
  EXPECT_TRUE(validate_metadata_key("field123").ok());
}

TEST(MetadataTest, ValidateKeyEmpty) {
  auto status = validate_metadata_key("");
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST(MetadataTest, ValidateKeyStartsWithDigit) {
  auto status = validate_metadata_key("123field");
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST(MetadataTest, ValidateKeyInvalidCharacters) {
  EXPECT_FALSE(validate_metadata_key("field-name").ok());
  EXPECT_FALSE(validate_metadata_key("field.name").ok());
  EXPECT_FALSE(validate_metadata_key("field name").ok());
  EXPECT_FALSE(validate_metadata_key("field@name").ok());
}

TEST(MetadataTest, ValidateKeyTooLong) {
  std::string long_key(256, 'a');
  auto status = validate_metadata_key(long_key);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST(MetadataTest, ValidateValueInt64) {
  EXPECT_TRUE(validate_metadata_value(MetadataValue(int64_t(42))).ok());
  EXPECT_TRUE(validate_metadata_value(MetadataValue(int64_t(-100))).ok());
}

TEST(MetadataTest, ValidateValueDouble) {
  EXPECT_TRUE(validate_metadata_value(MetadataValue(3.14)).ok());
  EXPECT_FALSE(validate_metadata_value(
      MetadataValue(std::numeric_limits<double>::quiet_NaN())).ok());
  EXPECT_FALSE(validate_metadata_value(
      MetadataValue(std::numeric_limits<double>::infinity())).ok());
}

TEST(MetadataTest, ValidateValueString) {
  EXPECT_TRUE(validate_metadata_value(MetadataValue(std::string("hello"))).ok());

  // String too long (>64KB)
  std::string long_string(65537, 'x');
  auto status = validate_metadata_value(MetadataValue(long_string));
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST(MetadataTest, ValidateValueBool) {
  EXPECT_TRUE(validate_metadata_value(MetadataValue(true)).ok());
  EXPECT_TRUE(validate_metadata_value(MetadataValue(false)).ok());
}

TEST(MetadataTest, ValidateMetadataValid) {
  Metadata metadata = {
      {"price", MetadataValue(int64_t(100))},
      {"brand", MetadataValue(std::string("Nike"))},
      {"in_stock", MetadataValue(true)},
      {"rating", MetadataValue(4.5)}
  };

  EXPECT_TRUE(validate_metadata(metadata).ok());
}

TEST(MetadataTest, ValidateMetadataTooManyFields) {
  Metadata metadata;
  for (int i = 0; i < 101; ++i) {
    metadata["field_" + std::to_string(i)] = MetadataValue(int64_t(i));
  }

  auto status = validate_metadata(metadata);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST(MetadataTest, ValidateMetadataInvalidKey) {
  Metadata metadata = {
      {"invalid-key", MetadataValue(int64_t(100))}
  };

  auto status = validate_metadata(metadata);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST(MetadataTest, ValidateMetadataInvalidValue) {
  Metadata metadata = {
      {"rating", MetadataValue(std::numeric_limits<double>::quiet_NaN())}
  };

  auto status = validate_metadata(metadata);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

// ============================================================================
// Filter Parser Tests
// ============================================================================

TEST(FilterTest, ParseSimpleComparison) {
  auto filter = FilterParser::parse("price < 100");
  ASSERT_TRUE(filter.ok());

  Metadata metadata = {{"price", MetadataValue(int64_t(50))}};
  EXPECT_TRUE((*filter)->evaluate(metadata));

  Metadata metadata2 = {{"price", MetadataValue(int64_t(150))}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));
}

TEST(FilterTest, ParseEqualityComparison) {
  auto filter = FilterParser::parse("brand = 'Nike'");
  ASSERT_TRUE(filter.ok());

  Metadata metadata = {{"brand", MetadataValue(std::string("Nike"))}};
  EXPECT_TRUE((*filter)->evaluate(metadata));

  Metadata metadata2 = {{"brand", MetadataValue(std::string("Adidas"))}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));
}

TEST(FilterTest, ParseBooleanComparison) {
  auto filter = FilterParser::parse("in_stock = true");
  ASSERT_TRUE(filter.ok());

  Metadata metadata = {{"in_stock", MetadataValue(true)}};
  EXPECT_TRUE((*filter)->evaluate(metadata));

  Metadata metadata2 = {{"in_stock", MetadataValue(false)}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));
}

TEST(FilterTest, ParseDoubleComparison) {
  auto filter = FilterParser::parse("rating >= 4.5");
  ASSERT_TRUE(filter.ok());

  Metadata metadata = {{"rating", MetadataValue(4.8)}};
  EXPECT_TRUE((*filter)->evaluate(metadata));

  Metadata metadata2 = {{"rating", MetadataValue(3.2)}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));
}

TEST(FilterTest, ParseAndExpression) {
  auto filter = FilterParser::parse("price < 100 AND brand = 'Nike'");
  ASSERT_TRUE(filter.ok());

  Metadata metadata = {
      {"price", MetadataValue(int64_t(50))},
      {"brand", MetadataValue(std::string("Nike"))}};
  EXPECT_TRUE((*filter)->evaluate(metadata));

  Metadata metadata2 = {
      {"price", MetadataValue(int64_t(150))},
      {"brand", MetadataValue(std::string("Nike"))}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));

  Metadata metadata3 = {
      {"price", MetadataValue(int64_t(50))},
      {"brand", MetadataValue(std::string("Adidas"))}};
  EXPECT_FALSE((*filter)->evaluate(metadata3));
}

TEST(FilterTest, ParseOrExpression) {
  auto filter = FilterParser::parse("price < 50 OR discount > 20");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {{"price", MetadataValue(int64_t(30))}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"discount", MetadataValue(int64_t(25))}};
  EXPECT_TRUE((*filter)->evaluate(metadata2));

  Metadata metadata3 = {
      {"price", MetadataValue(int64_t(80))},
      {"discount", MetadataValue(int64_t(10))}};
  EXPECT_FALSE((*filter)->evaluate(metadata3));
}

TEST(FilterTest, ParseNotExpression) {
  auto filter = FilterParser::parse("NOT (price > 100)");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {{"price", MetadataValue(int64_t(50))}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"price", MetadataValue(int64_t(150))}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));
}

TEST(FilterTest, ParseParenthesizedExpression) {
  auto filter = FilterParser::parse("(price < 100 OR discount > 20) AND in_stock = true");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {
      {"price", MetadataValue(int64_t(50))},
      {"in_stock", MetadataValue(true)}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));

  Metadata metadata2 = {
      {"discount", MetadataValue(int64_t(25))},
      {"in_stock", MetadataValue(true)}};
  EXPECT_TRUE((*filter)->evaluate(metadata2));

  Metadata metadata3 = {
      {"price", MetadataValue(int64_t(50))},
      {"in_stock", MetadataValue(false)}};
  EXPECT_FALSE((*filter)->evaluate(metadata3));
}

TEST(FilterTest, ParseLikeExpression) {
  auto filter = FilterParser::parse("brand LIKE 'Nike%'");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {{"brand", MetadataValue(std::string("Nike Air Max"))}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"brand", MetadataValue(std::string("Adidas"))}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));
}

TEST(FilterTest, ParseNotLikeExpression) {
  auto filter = FilterParser::parse("brand NOT LIKE '%test%'");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {{"brand", MetadataValue(std::string("Nike"))}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"brand", MetadataValue(std::string("test product"))}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));
}

TEST(FilterTest, ParseInExpression) {
  auto filter = FilterParser::parse("category IN ('shoes', 'apparel', 'accessories')");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {{"category", MetadataValue(std::string("shoes"))}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"category", MetadataValue(std::string("electronics"))}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));
}

TEST(FilterTest, ParseNotInExpression) {
  auto filter = FilterParser::parse("status NOT IN (0, 1, 2)");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {{"status", MetadataValue(int64_t(5))}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"status", MetadataValue(int64_t(1))}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));
}

TEST(FilterTest, ParseInWithMixedTypes) {
  auto filter = FilterParser::parse("value IN (1, 2.5, 'text')");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {{"value", MetadataValue(int64_t(1))}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"value", MetadataValue(2.5)}};
  EXPECT_TRUE((*filter)->evaluate(metadata2));

  Metadata metadata3 = {{"value", MetadataValue(std::string("text"))}};
  EXPECT_TRUE((*filter)->evaluate(metadata3));

  Metadata metadata4 = {{"value", MetadataValue(int64_t(99))}};
  EXPECT_FALSE((*filter)->evaluate(metadata4));
}

TEST(FilterTest, ComplexNestedExpression) {
  auto filter = FilterParser::parse(
      "(price < 100 AND brand = 'Nike') OR (discount > 50 AND in_stock = true)");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {
      {"price", MetadataValue(int64_t(80))},
      {"brand", MetadataValue(std::string("Nike"))}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));

  Metadata metadata2 = {
      {"discount", MetadataValue(int64_t(60))},
      {"in_stock", MetadataValue(true)}};
  EXPECT_TRUE((*filter)->evaluate(metadata2));

  Metadata metadata3 = {
      {"price", MetadataValue(int64_t(120))},
      {"brand", MetadataValue(std::string("Adidas"))},
      {"discount", MetadataValue(int64_t(20))}};
  EXPECT_FALSE((*filter)->evaluate(metadata3));
}

TEST(FilterTest, ParseNegativeNumber) {
  auto filter = FilterParser::parse("temperature > -10");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {{"temperature", MetadataValue(int64_t(-5))}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));

  Metadata metadata2 = {{"temperature", MetadataValue(int64_t(-15))}};
  EXPECT_FALSE((*filter)->evaluate(metadata2));
}

TEST(FilterTest, ParseStringWithEscapes) {
  auto filter = FilterParser::parse("name = 'O\\'Brien'");
  ASSERT_TRUE(filter.ok());

  Metadata metadata1 = {{"name", MetadataValue(std::string("O'Brien"))}};
  EXPECT_TRUE((*filter)->evaluate(metadata1));
}

TEST(FilterTest, MissingField) {
  auto filter = FilterParser::parse("price < 100");
  ASSERT_TRUE(filter.ok());

  Metadata metadata = {{"brand", MetadataValue(std::string("Nike"))}};
  // Field not present -> false
  EXPECT_FALSE((*filter)->evaluate(metadata));
}

TEST(FilterTest, ParseErrorInvalidSyntax) {
  auto result = FilterParser::parse("price <");
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(result.status()));
}

TEST(FilterTest, ParseErrorUnclosedParen) {
  auto result = FilterParser::parse("(price < 100");
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(result.status()));
}

TEST(FilterTest, ParseErrorUnclosedString) {
  auto result = FilterParser::parse("brand = 'Nike");
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(result.status()));
}

TEST(FilterTest, ParseErrorEmptyInList) {
  auto result = FilterParser::parse("category IN ()");
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(result.status()));
}

TEST(FilterTest, EvaluateFilterUtility) {
  Metadata metadata = {
      {"price", MetadataValue(int64_t(50))},
      {"brand", MetadataValue(std::string("Nike"))}};

  auto result = evaluate_filter("price < 100 AND brand = 'Nike'", metadata);
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(*result);

  auto result2 = evaluate_filter("price > 100", metadata);
  ASSERT_TRUE(result2.ok());
  EXPECT_FALSE(*result2);
}

TEST(FilterTest, ValidateFilterUtility) {
  EXPECT_TRUE(validate_filter("price < 100").ok());
  EXPECT_TRUE(validate_filter("brand = 'Nike' AND in_stock = true").ok());
  EXPECT_FALSE(validate_filter("price <").ok());
  EXPECT_FALSE(validate_filter("invalid syntax @#$").ok());
}

TEST(FilterTest, FilterToString) {
  auto filter = FilterParser::parse("price < 100");
  ASSERT_TRUE(filter.ok());

  std::string str = (*filter)->to_string();
  EXPECT_FALSE(str.empty());
}

// ============================================================================
// Config Tests
// ============================================================================

TEST(ConfigTest, IndexConfigValidation) {
  IndexConfig config;
  EXPECT_FALSE(config.IsValid());  // dimension is 0

  config.dimension = 128;
  EXPECT_TRUE(config.IsValid());
}

TEST(ConfigTest, StorageConfigValidation) {
  StorageConfig config;
  EXPECT_TRUE(config.IsValid());  // Has default values

  config.base_path = "";
  EXPECT_FALSE(config.IsValid());

  config.base_path = "/tmp/data";
  config.max_segment_size = 0;
  EXPECT_FALSE(config.IsValid());
}

TEST(ConfigTest, ClusterConfigValidation) {
  ClusterConfig config;
  EXPECT_FALSE(config.IsValid());  // node_id is invalid

  config.node_id = MakeNodeId(1);
  EXPECT_FALSE(config.IsValid());  // node_address is empty

  config.node_address = "localhost";
  EXPECT_TRUE(config.IsValid());
}

TEST(ConfigTest, QueryConfigValidation) {
  QueryConfig config;
  EXPECT_TRUE(config.IsValid());

  config.default_topk = 0;
  EXPECT_FALSE(config.IsValid());

  config.default_topk = 10;
  config.max_topk = 5;  // Less than default
  EXPECT_FALSE(config.IsValid());
}

TEST(ConfigTest, SystemConfigValidation) {
  SystemConfig config;
  config.cluster_config.node_id = MakeNodeId(1);
  config.cluster_config.node_address = "localhost";

  EXPECT_TRUE(config.IsValid());
}

// ============================================================================
// MetadataSerializer Tests
// ============================================================================

TEST(MetadataSerializerTest, SerializeDeserializeInt64) {
  Metadata metadata;
  metadata["count"] = static_cast<int64_t>(12345);
  metadata["price"] = static_cast<int64_t>(-999);

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  ASSERT_TRUE(result.ok()) << result.status().message();

  const auto& deserialized = result.value();
  ASSERT_EQ(deserialized.size(), 2);
  EXPECT_EQ(std::get<int64_t>(deserialized.at("count")), 12345);
  EXPECT_EQ(std::get<int64_t>(deserialized.at("price")), -999);
}

TEST(MetadataSerializerTest, SerializeDeserializeDouble) {
  Metadata metadata;
  metadata["rating"] = 4.5;
  metadata["score"] = -1.234567;

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  ASSERT_TRUE(result.ok());

  const auto& deserialized = result.value();
  ASSERT_EQ(deserialized.size(), 2);
  EXPECT_DOUBLE_EQ(std::get<double>(deserialized.at("rating")), 4.5);
  EXPECT_DOUBLE_EQ(std::get<double>(deserialized.at("score")), -1.234567);
}

TEST(MetadataSerializerTest, SerializeDeserializeString) {
  Metadata metadata;
  metadata["brand"] = std::string("Nike");
  metadata["description"] = std::string("High-performance running shoes");
  metadata["empty"] = std::string("");

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  ASSERT_TRUE(result.ok());

  const auto& deserialized = result.value();
  ASSERT_EQ(deserialized.size(), 3);
  EXPECT_EQ(std::get<std::string>(deserialized.at("brand")), "Nike");
  EXPECT_EQ(std::get<std::string>(deserialized.at("description")),
            "High-performance running shoes");
  EXPECT_EQ(std::get<std::string>(deserialized.at("empty")), "");
}

TEST(MetadataSerializerTest, SerializeDeserializeBool) {
  Metadata metadata;
  metadata["in_stock"] = true;
  metadata["featured"] = false;

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  ASSERT_TRUE(result.ok());

  const auto& deserialized = result.value();
  ASSERT_EQ(deserialized.size(), 2);
  EXPECT_TRUE(std::get<bool>(deserialized.at("in_stock")));
  EXPECT_FALSE(std::get<bool>(deserialized.at("featured")));
}

TEST(MetadataSerializerTest, SerializeDeserializeMixedTypes) {
  Metadata metadata;
  metadata["price"] = 120.50;
  metadata["brand"] = std::string("Adidas");
  metadata["stock_count"] = static_cast<int64_t>(42);
  metadata["on_sale"] = true;
  metadata["category"] = std::string("sportswear");

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  ASSERT_TRUE(result.ok());

  const auto& deserialized = result.value();
  ASSERT_EQ(deserialized.size(), 5);
  EXPECT_DOUBLE_EQ(std::get<double>(deserialized.at("price")), 120.50);
  EXPECT_EQ(std::get<std::string>(deserialized.at("brand")), "Adidas");
  EXPECT_EQ(std::get<int64_t>(deserialized.at("stock_count")), 42);
  EXPECT_TRUE(std::get<bool>(deserialized.at("on_sale")));
  EXPECT_EQ(std::get<std::string>(deserialized.at("category")), "sportswear");
}

TEST(MetadataSerializerTest, SerializeDeserializeEmpty) {
  Metadata metadata;  // Empty

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  ASSERT_TRUE(result.ok());

  const auto& deserialized = result.value();
  EXPECT_EQ(deserialized.size(), 0);
}

TEST(MetadataSerializerTest, DeserializeInvalidData) {
  std::stringstream ss;

  // Write invalid count (> 100)
  uint64_t invalid_count = 101;
  ss.write(reinterpret_cast<const char*>(&invalid_count), sizeof(invalid_count));

  auto result = MetadataSerializer::Deserialize(ss);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST(MetadataSerializerTest, LargeMetadata) {
  Metadata metadata;

  // Add 50 fields (within limit of 100)
  for (int i = 0; i < 50; ++i) {
    metadata["field_" + std::to_string(i)] = static_cast<int64_t>(i * 10);
  }

  std::stringstream ss;
  MetadataSerializer::Serialize(metadata, ss);

  auto result = MetadataSerializer::Deserialize(ss);
  ASSERT_TRUE(result.ok());

  const auto& deserialized = result.value();
  ASSERT_EQ(deserialized.size(), 50);

  for (int i = 0; i < 50; ++i) {
    std::string key = "field_" + std::to_string(i);
    EXPECT_EQ(std::get<int64_t>(deserialized.at(key)), i * 10);
  }
}

}  // namespace
}  // namespace core
}  // namespace gvdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
