// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/segment_manifest.h"

#include <doctest/doctest.h>

namespace gvdb {
namespace storage {
namespace {

ManifestEntry MakeEntry(uint32_t seg_id, uint32_t col_id, uint64_t count) {
  return ManifestEntry{
      .segment_id = seg_id,
      .collection_id = col_id,
      .dimension = 128,
      .metric = 2,
      .index_type = 1,
      .vector_count = count,
      .size_bytes = count * 128 * 4,
      .uploaded_at = "2026-04-10T12:00:00Z"};
}

TEST_CASE("SerializeEmpty") {
  auto json = SegmentManifest::Serialize({});
  CHECK_NE(json.find("\"segments\":[]"), std::string::npos);
}

TEST_CASE("SerializeAndDeserialize") {
  std::vector<ManifestEntry> entries = {
      MakeEntry(1, 100, 1000),
      MakeEntry(2, 100, 2000),
      MakeEntry(3, 200, 500),
  };

  auto json = SegmentManifest::Serialize(entries);
  auto result = SegmentManifest::Deserialize(json);
  REQUIRE(result.ok());
  REQUIRE_EQ(result->size(), 3);

  CHECK_EQ((*result)[0].segment_id, 1);
  CHECK_EQ((*result)[0].collection_id, 100);
  CHECK_EQ((*result)[0].dimension, 128);
  CHECK_EQ((*result)[0].vector_count, 1000);
  CHECK_EQ((*result)[0].uploaded_at, "2026-04-10T12:00:00Z");

  CHECK_EQ((*result)[1].segment_id, 2);
  CHECK_EQ((*result)[1].vector_count, 2000);

  CHECK_EQ((*result)[2].segment_id, 3);
  CHECK_EQ((*result)[2].collection_id, 200);
}

TEST_CASE("DeserializeEmpty") {
  auto result = SegmentManifest::Deserialize("");
  REQUIRE(result.ok());
  CHECK(result->empty());
}

TEST_CASE("DeserializeInvalid") {
  auto result = SegmentManifest::Deserialize("not json");
  CHECK_FALSE(result.ok());
}

TEST_CASE("AddEntry") {
  auto json = SegmentManifest::Serialize({MakeEntry(1, 100, 1000)});
  json = SegmentManifest::AddEntry(json, MakeEntry(2, 100, 2000));

  auto result = SegmentManifest::Deserialize(json);
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), 2);
  CHECK_EQ((*result)[0].segment_id, 1);
  CHECK_EQ((*result)[1].segment_id, 2);
}

TEST_CASE("AddEntryReplaceExisting") {
  auto json = SegmentManifest::Serialize({MakeEntry(1, 100, 1000)});
  auto updated = MakeEntry(1, 100, 5000);  // same segment_id, different count
  json = SegmentManifest::AddEntry(json, updated);

  auto result = SegmentManifest::Deserialize(json);
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), 1);
  CHECK_EQ((*result)[0].vector_count, 5000);
}

TEST_CASE("AddEntryToEmpty") {
  auto json = SegmentManifest::AddEntry("", MakeEntry(1, 100, 1000));
  auto result = SegmentManifest::Deserialize(json);
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), 1);
}

TEST_CASE("RemoveEntry") {
  std::vector<ManifestEntry> entries = {
      MakeEntry(1, 100, 1000),
      MakeEntry(2, 100, 2000),
      MakeEntry(3, 200, 500),
  };
  auto json = SegmentManifest::Serialize(entries);
  json = SegmentManifest::RemoveEntry(json, 2);

  auto result = SegmentManifest::Deserialize(json);
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), 2);
  CHECK_EQ((*result)[0].segment_id, 1);
  CHECK_EQ((*result)[1].segment_id, 3);
}

TEST_CASE("RemoveNonexistent") {
  auto json = SegmentManifest::Serialize({MakeEntry(1, 100, 1000)});
  auto json2 = SegmentManifest::RemoveEntry(json, 99);
  auto result = SegmentManifest::Deserialize(json2);
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), 1);  // unchanged
}

}  // namespace
}  // namespace storage
}  // namespace gvdb
