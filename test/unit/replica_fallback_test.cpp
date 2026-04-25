// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/replica_fallback.h"

#include <chrono>
#include <string>
#include <vector>

#include <doctest/doctest.h>

using namespace gvdb::network;
using gvdb::proto::internal::RouteQueryNodeOption;
using google::protobuf::RepeatedPtrField;

namespace {

// Build a 1-shard options list with the specified (node_id, address,
// is_primary) entries. First entry is treated as the primary by default
// unless the caller overrides via the third argument.
RepeatedPtrField<RouteQueryNodeOption> MakeOptions(
    std::vector<std::tuple<uint32_t, std::string, bool>> spec) {
  RepeatedPtrField<RouteQueryNodeOption> out;
  for (auto& [id, addr, is_primary] : spec) {
    auto* o = out.Add();
    o->set_node_id(id);
    o->set_node_address(addr);
    o->set_is_primary(is_primary);
  }
  return out;
}

// Returns a CallFn that produces a sequence of canned statuses, one per
// invocation. Caller drives it through the helper and the recorder
// captures the addresses the helper actually dialed.
struct StatusSequence {
  std::vector<grpc::Status> sequence;
  std::vector<std::string> dialed;
  size_t index = 0;

  grpc::Status operator()(grpc::ClientContext* /*ctx*/,
                           const std::string& addr) {
    dialed.push_back(addr);
    if (index >= sequence.size()) {
      // Default: succeed so a too-short sequence doesn't loop forever.
      return grpc::Status::OK;
    }
    return sequence[index++];
  }
};

}  // namespace

TEST_CASE("CallWithReplicaFallback succeeds on the first option when it returns OK") {
  auto options = MakeOptions({{101, "addr-101", true}, {102, "addr-102", false}});
  StatusSequence seq{{grpc::Status::OK}, {}, 0};

  auto result = CallWithReplicaFallback(options, std::chrono::milliseconds(500),
                                         "test_op", std::ref(seq));

  CHECK(result.final_status.ok());
  CHECK(result.attempts == 1);
  CHECK(result.target_node_id_used == 101);
  CHECK_FALSE(result.used_replica_fallback);
  REQUIRE(seq.dialed.size() == 1);
  CHECK(seq.dialed[0] == "addr-101");
}

TEST_CASE("CallWithReplicaFallback falls over to a replica on UNAVAILABLE") {
  auto options = MakeOptions({{101, "addr-101", true}, {102, "addr-102", false}});
  StatusSequence seq{
      {grpc::Status(grpc::StatusCode::UNAVAILABLE, "primary down"),
       grpc::Status::OK},
      {},
      0};

  auto result = CallWithReplicaFallback(options, std::chrono::milliseconds(500),
                                         "test_op", std::ref(seq));

  CHECK(result.final_status.ok());
  CHECK(result.attempts == 2);
  CHECK(result.target_node_id_used == 102);
  CHECK(result.used_replica_fallback);
  REQUIRE(seq.dialed.size() == 2);
  CHECK(seq.dialed[0] == "addr-101");
  CHECK(seq.dialed[1] == "addr-102");
}

TEST_CASE("CallWithReplicaFallback walks past multiple unhealthy candidates") {
  auto options = MakeOptions({
      {101, "addr-101", true},
      {102, "addr-102", false},
      {103, "addr-103", false},
  });
  StatusSequence seq{
      {grpc::Status(grpc::StatusCode::UNAVAILABLE, "p down"),
       grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "r1 slow"),
       grpc::Status::OK},
      {},
      0};

  auto result = CallWithReplicaFallback(options, std::chrono::milliseconds(500),
                                         "test_op", std::ref(seq));

  CHECK(result.final_status.ok());
  CHECK(result.attempts == 3);
  CHECK(result.target_node_id_used == 103);
  CHECK(result.used_replica_fallback);
}

TEST_CASE("CallWithReplicaFallback returns immediately on a non-transient error") {
  // INTERNAL is a server-side problem — every replica would return the same
  // error, so don't waste round-trips. The helper short-circuits.
  auto options = MakeOptions({
      {101, "addr-101", true},
      {102, "addr-102", false},
      {103, "addr-103", false},
  });
  StatusSequence seq{
      {grpc::Status(grpc::StatusCode::INTERNAL, "bad payload"),
       grpc::Status::OK},
      {},
      0};

  auto result = CallWithReplicaFallback(options, std::chrono::milliseconds(500),
                                         "test_op", std::ref(seq));

  CHECK_FALSE(result.final_status.ok());
  CHECK(result.final_status.error_code() == grpc::StatusCode::INTERNAL);
  CHECK(result.attempts == 1);
  CHECK(seq.dialed.size() == 1);  // never dialed addr-102 / addr-103
}

TEST_CASE("CallWithReplicaFallback exhausts every option and surfaces last error") {
  auto options = MakeOptions({
      {101, "addr-101", true},
      {102, "addr-102", false},
  });
  StatusSequence seq{
      {grpc::Status(grpc::StatusCode::UNAVAILABLE, "p down"),
       grpc::Status(grpc::StatusCode::UNAVAILABLE, "r down")},
      {},
      0};

  auto result = CallWithReplicaFallback(options, std::chrono::milliseconds(500),
                                         "test_op", std::ref(seq));

  CHECK_FALSE(result.final_status.ok());
  CHECK(result.final_status.error_code() == grpc::StatusCode::UNAVAILABLE);
  CHECK(result.attempts == 2);
}

TEST_CASE("CallWithReplicaFallback returns FAILED_PRECONDITION on empty options") {
  RepeatedPtrField<RouteQueryNodeOption> options;  // empty
  StatusSequence seq{{}, {}, 0};

  auto result = CallWithReplicaFallback(options, std::chrono::milliseconds(500),
                                         "test_op", std::ref(seq));

  CHECK_FALSE(result.final_status.ok());
  CHECK(result.final_status.error_code() ==
        grpc::StatusCode::FAILED_PRECONDITION);
  CHECK(result.attempts == 0);
  CHECK(seq.dialed.empty());
}

TEST_CASE("IsTransientReplicaError flags only routing-relevant codes") {
  CHECK(IsTransientReplicaError(grpc::StatusCode::UNAVAILABLE));
  CHECK(IsTransientReplicaError(grpc::StatusCode::DEADLINE_EXCEEDED));
  CHECK_FALSE(IsTransientReplicaError(grpc::StatusCode::OK));
  CHECK_FALSE(IsTransientReplicaError(grpc::StatusCode::INTERNAL));
  CHECK_FALSE(IsTransientReplicaError(grpc::StatusCode::NOT_FOUND));
  CHECK_FALSE(IsTransientReplicaError(grpc::StatusCode::INVALID_ARGUMENT));
  CHECK_FALSE(IsTransientReplicaError(grpc::StatusCode::PERMISSION_DENIED));
}
