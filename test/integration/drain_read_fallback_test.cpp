// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Wire-level integration test for the read-path replica-fallback helper.
//
// Spins up:
//   - A coordinator that answers RouteQuery with a fixed two-option list
//     (mock1 first, mock2 second).
//   - Two mock data-node servers (mock1, mock2) on independent ports.
//   - A proxy in front, dialing the coordinator + the mock data-nodes.
//
// Each test toggles the mocks' fail modes and asserts the proxy's read
// RPC either falls over or surfaces UNAVAILABLE as appropriate. Together
// these cover:
//   - Happy path (first option succeeds → no fallback).
//   - Single-step fallback (first option UNAVAILABLE, second succeeds).
//   - Exhausted candidates (both options fail → UNAVAILABLE surfaced).
// The exhausted case fails on builds without the helper because the old
// proxy returned the first option's UNAVAILABLE without ever trying the
// second; we now try both before surfacing.

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <initializer_list>
#include <memory>
#include <string>

#include "internal.grpc.pb.h"
#include "service/proxy_service.h"
#include "support/grpc_server.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace test {

// Mock InternalService that answers RouteQuery with a fixed two-option
// list. Other RPCs are unimplemented (and unused by this test).
class DrainFallbackCoordinatorService
    : public proto::internal::InternalService::Service {
 public:
  std::atomic<int> route_query_calls{0};
  std::string mock1_address;
  std::string mock2_address;

  grpc::Status RouteQuery(
      grpc::ServerContext* /*context*/,
      const proto::internal::RouteQueryRequest* /*request*/,
      proto::internal::RouteQueryResponse* response) override {
    route_query_calls++;

    response->set_collection_id(1);
    response->add_target_shard_ids(0);
    // Legacy fields point at the first option for back-compat.
    response->add_target_node_ids(101);
    response->add_target_node_addresses(mock1_address);
    // Per-shard options: primary first, replica second.
    auto* shard_opts = response->add_per_shard_options();
    shard_opts->set_shard_id(0);
    auto* primary = shard_opts->add_options();
    primary->set_node_id(101);
    primary->set_node_address(mock1_address);
    primary->set_is_primary(true);
    auto* replica = shard_opts->add_options();
    replica->set_node_id(102);
    replica->set_node_address(mock2_address);
    replica->set_is_primary(false);
    return grpc::Status::OK;
  }
};

// Mock VectorDBService data-node. Get returns a stable response when
// fail_mode_=false, returns UNAVAILABLE when true. The counter lets the
// test verify which mock the proxy actually dialed.
class DrainFallbackDataNodeService : public proto::VectorDBService::Service {
 public:
  std::atomic<int> get_calls{0};
  std::atomic<bool> fail_mode{false};
  std::string label;

  explicit DrainFallbackDataNodeService(std::string l) : label(std::move(l)) {}

  grpc::Status Get(grpc::ServerContext* /*context*/,
                    const proto::GetRequest* request,
                    proto::GetResponse* response) override {
    get_calls++;
    if (fail_mode.load()) {
      // UNAVAILABLE is the transient code the helper retries on; if the
      // helper short-circuited on this it would fail the test.
      return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                           "drain-fallback test: " + label + " in fail mode");
    }
    // Echo the requested ids so the test can confirm the response came
    // from a real data-node call.
    for (auto id : request->ids()) {
      auto* v = response->add_vectors();
      v->set_id(id);
    }
    return grpc::Status::OK;
  }
};

namespace {

// Starts a server on a free local port with the given services.
std::unique_ptr<TestGrpcServer> StartServer(
    std::initializer_list<grpc::Service*> services) {
  auto server = std::make_unique<TestGrpcServer>(services);
  REQUIRE(server->ok());
  return server;
}

}  // namespace

class DrainReadFallbackTest {
 public:
  DrainReadFallbackTest() {
    mock1_ = std::make_unique<DrainFallbackDataNodeService>("mock1");
    mock2_ = std::make_unique<DrainFallbackDataNodeService>("mock2");
    mock1_server_ = StartServer({mock1_.get()});
    mock2_server_ = StartServer({mock2_.get()});

    // Coordinator (RouteQuery only).
    coord_ = std::make_unique<DrainFallbackCoordinatorService>();
    coord_->mock1_address = mock1_server_->address();
    coord_->mock2_address = mock2_server_->address();
    coord_server_ = StartServer({coord_.get()});

    // Proxy (the system under test).
    proxy_ = std::make_unique<service::ProxyService>(
        std::vector<std::string>{coord_server_->address()},
        // The proxy's query-node URI is irrelevant for Get (proxy routes
        // Get via RouteQuery) but the constructor requires a value.
        mock1_server_->address());
    proxy_server_ = StartServer({proxy_.get()});

    client_ = proto::VectorDBService::NewStub(proxy_server_->NewChannel());
  }

  ~DrainReadFallbackTest() {
    // Close every client channel before the server that it dials. A
    // graceful server shutdown waits until each open client connection
    // answers the server's GOAWAY, and on macOS an idle in-process channel
    // answers only at gRPC's next client backup poll (every 5 s).
    client_.reset();
    proxy_server_.reset();
    proxy_.reset();  // Its channels dial the coordinator and both mocks.
    coord_server_.reset();
    mock1_server_.reset();
    mock2_server_.reset();
  }

  proto::GetResponse IssueGet() {
    proto::GetRequest req;
    req.set_collection_name("drain_fallback_test");
    req.add_ids(42);
    proto::GetResponse resp;
    grpc::ClientContext ctx;
    auto status = client_->Get(&ctx, req, &resp);
    last_status_ = status;
    return resp;
  }

  std::unique_ptr<DrainFallbackDataNodeService> mock1_;
  std::unique_ptr<DrainFallbackDataNodeService> mock2_;
  std::unique_ptr<TestGrpcServer> mock1_server_, mock2_server_;

  std::unique_ptr<DrainFallbackCoordinatorService> coord_;
  std::unique_ptr<TestGrpcServer> coord_server_;

  std::unique_ptr<service::ProxyService> proxy_;
  std::unique_ptr<TestGrpcServer> proxy_server_;

  std::unique_ptr<proto::VectorDBService::Stub> client_;
  grpc::Status last_status_;
};

TEST_CASE_FIXTURE(DrainReadFallbackTest,
                  "GetSucceedsOnPrimaryWhenHealthy") {
  // Both mocks healthy → proxy hits mock1 (primary) and never mock2.
  auto resp = IssueGet();

  CHECK(last_status_.ok());
  CHECK(mock1_->get_calls.load() == 1);
  CHECK(mock2_->get_calls.load() == 0);
  REQUIRE(resp.vectors_size() == 1);
  CHECK(resp.vectors(0).id() == 42);
}

TEST_CASE_FIXTURE(DrainReadFallbackTest,
                  "GetFallsOverToReplicaWhenPrimaryUnavailable") {
  // mock1 (primary) fails UNAVAILABLE; mock2 (replica) healthy.
  // Proxy must dial mock1, see UNAVAILABLE, dial mock2, succeed.
  mock1_->fail_mode.store(true);

  auto resp = IssueGet();

  CHECK(last_status_.ok());
  CHECK(mock1_->get_calls.load() == 1);
  CHECK(mock2_->get_calls.load() == 1);
  REQUIRE(resp.vectors_size() == 1);
  CHECK(resp.vectors(0).id() == 42);
}

TEST_CASE_FIXTURE(DrainReadFallbackTest,
                  "GetSurfacesUnavailableWhenAllCandidatesFail") {
  // Both mocks return UNAVAILABLE. Proxy must walk both candidates and
  // surface UNAVAILABLE (not silently truncate).
  mock1_->fail_mode.store(true);
  mock2_->fail_mode.store(true);

  auto resp = IssueGet();

  CHECK_FALSE(last_status_.ok());
  CHECK(last_status_.error_code() == grpc::StatusCode::UNAVAILABLE);
  // Both mocks were dialed before the proxy gave up.
  CHECK(mock1_->get_calls.load() == 1);
  CHECK(mock2_->get_calls.load() == 1);
}

}  // namespace test
}  // namespace gvdb
