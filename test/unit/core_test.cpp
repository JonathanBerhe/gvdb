#include <gtest/gtest.h>

#include "core/config.h"
#include "core/interfaces.h"
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

}  // namespace
}  // namespace core
}  // namespace gvdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
