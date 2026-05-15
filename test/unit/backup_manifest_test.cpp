// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include "storage/backup_manifest.h"

using namespace gvdb;
using namespace gvdb::storage;

TEST_CASE("BackupManifest: top-level round-trip preserves every field") {
  BackupManifestV1 m;
  m.manifest_version = 1;
  m.backup_id = "bk-2026-05-15-abc";
  m.collection.collection_id = 7;
  m.collection.collection_name = "products";
  m.collection.dimension = 768;
  m.collection.metric = 1;
  m.collection.index_type = 2;
  m.collection.num_shards = 4;
  m.collection.replication_factor = 2;
  m.created_at_unix_ms = 1747318921000;
  m.vector_count = 1500000;
  m.size_bytes = 2147483648ULL;
  m.shards.push_back(
      BackupShardRef{0, "test/backups/bk/shards/0/shard.manifest.json"});
  m.shards.push_back(
      BackupShardRef{1, "test/backups/bk/shards/1/shard.manifest.json"});
  m.incremental_from = "";
  m.gvdb_version = "1.3.0-rc.1";

  auto json = BackupManifest::SerializeTop(m);
  auto round_or = BackupManifest::DeserializeTop(json);
  REQUIRE(round_or.ok());
  const auto& r = *round_or;

  CHECK_EQ(r.manifest_version, 1u);
  CHECK_EQ(r.backup_id, "bk-2026-05-15-abc");
  CHECK_EQ(r.collection.collection_id, 7u);
  CHECK_EQ(r.collection.collection_name, "products");
  CHECK_EQ(r.collection.dimension, 768);
  CHECK_EQ(r.collection.metric, 1);
  CHECK_EQ(r.collection.index_type, 2);
  CHECK_EQ(r.collection.num_shards, 4u);
  CHECK_EQ(r.collection.replication_factor, 2u);
  CHECK_EQ(r.created_at_unix_ms, 1747318921000LL);
  CHECK_EQ(r.vector_count, 1500000u);
  CHECK_EQ(r.size_bytes, 2147483648ULL);
  REQUIRE_EQ(r.shards.size(), 2u);
  CHECK_EQ(r.shards[0].shard_id, 0u);
  CHECK_EQ(r.shards[0].manifest_key,
           "test/backups/bk/shards/0/shard.manifest.json");
  CHECK_EQ(r.shards[1].shard_id, 1u);
  CHECK_EQ(r.gvdb_version, "1.3.0-rc.1");
}

TEST_CASE("BackupManifest: shard round-trip with nested files") {
  ShardManifestV1 m;
  m.manifest_version = 1;
  m.shard_id = 3;
  m.node_id = 5;
  m.primary_term = 42;

  ShardBackupSegment s1;
  s1.segment_id = 1042;
  s1.state = "sealed";
  s1.vector_count = 500'000;
  s1.size_bytes = 716'800'000;
  s1.files = {
      {"metadata.txt", "backups/bk/shards/3/segments/1042/metadata.txt", 124},
      {"vectors.bin", "backups/bk/shards/3/segments/1042/vectors.bin",
       716'800'000},
      {"index.faiss", "backups/bk/shards/3/segments/1042/index.faiss",
       134'217'728},
  };
  m.segments.push_back(s1);

  ShardBackupSegment s2;
  s2.segment_id = 1043;
  s2.state = "growing";
  s2.vector_count = 12'000;
  s2.size_bytes = 36'864'000;
  s2.files = {
      {"metadata.txt", "backups/bk/shards/3/segments/1043/metadata.txt", 89},
      {"growing.bin", "backups/bk/shards/3/segments/1043/growing.bin",
       36'864'000},
  };
  m.segments.push_back(s2);

  auto json = BackupManifest::SerializeShard(m);
  auto round_or = BackupManifest::DeserializeShard(json);
  REQUIRE(round_or.ok());
  const auto& r = *round_or;

  CHECK_EQ(r.manifest_version, 1u);
  CHECK_EQ(r.shard_id, 3u);
  CHECK_EQ(r.node_id, 5u);
  CHECK_EQ(r.primary_term, 42u);
  REQUIRE_EQ(r.segments.size(), 2u);

  // Segment 1 (sealed).
  CHECK_EQ(r.segments[0].segment_id, 1042u);
  CHECK_EQ(r.segments[0].state, "sealed");
  CHECK_EQ(r.segments[0].vector_count, 500'000u);
  REQUIRE_EQ(r.segments[0].files.size(), 3u);
  CHECK_EQ(r.segments[0].files[0].name, "metadata.txt");
  CHECK_EQ(r.segments[0].files[0].size, 124u);
  CHECK_EQ(r.segments[0].files[1].name, "vectors.bin");
  CHECK_EQ(r.segments[0].files[2].name, "index.faiss");

  // Segment 2 (growing).
  CHECK_EQ(r.segments[1].segment_id, 1043u);
  CHECK_EQ(r.segments[1].state, "growing");
  REQUIRE_EQ(r.segments[1].files.size(), 2u);
  CHECK_EQ(r.segments[1].files[1].name, "growing.bin");
}

TEST_CASE("BackupManifest: rejects unsupported version") {
  std::string bad =
      R"({"manifest_version":2,"backup_id":"x","collection":{)"
      R"("collection_id":0,"collection_name":"","dimension":0,"metric":0,)"
      R"("index_type":0,"num_shards":0,"replication_factor":0},)"
      R"("created_at_unix_ms":0,"vector_count":0,"size_bytes":0,"shards":[],)"
      R"("incremental_from":"","gvdb_version":""})";
  auto r = BackupManifest::DeserializeTop(bad);
  CHECK_FALSE(r.ok());
  CHECK(std::string(r.status().message()).find("Unsupported") !=
        std::string::npos);
}

TEST_CASE("BackupManifest: rejects empty input") {
  CHECK_FALSE(BackupManifest::DeserializeTop("").ok());
  CHECK_FALSE(BackupManifest::DeserializeShard("").ok());
}

TEST_CASE("BackupManifest: handles strings with backslashes and quotes") {
  ShardManifestV1 m;
  m.manifest_version = 1;
  m.shard_id = 0;
  ShardBackupSegment s;
  s.segment_id = 1;
  s.state = "sealed";
  s.files = {{"weird\nname", R"(prefix/path/with\backslash"and\quotes)", 0}};
  m.segments.push_back(s);
  auto json = BackupManifest::SerializeShard(m);
  auto r = BackupManifest::DeserializeShard(json);
  REQUIRE(r.ok());
  REQUIRE_EQ(r->segments.size(), 1u);
  REQUIRE_EQ(r->segments[0].files.size(), 1u);
  CHECK_EQ(r->segments[0].files[0].name, "weird\nname");
  CHECK_EQ(r->segments[0].files[0].key,
           R"(prefix/path/with\backslash"and\quotes)");
}

TEST_CASE("BackupManifest: key conventions are stable") {
  CHECK_EQ(BackupManifest::BackupRootPrefix("bk-1"), "backups/bk-1");
  CHECK_EQ(BackupManifest::TopManifestKey("bk-1"),
           "backups/bk-1/backup.manifest.json");
  CHECK_EQ(BackupManifest::ShardManifestKey("bk-1", 7),
           "backups/bk-1/shards/7/shard.manifest.json");
  CHECK_EQ(BackupManifest::SegmentFileKey("bk-1", 7, 42, "vectors.bin"),
           "backups/bk-1/shards/7/segments/42/vectors.bin");
}
