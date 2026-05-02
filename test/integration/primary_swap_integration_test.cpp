// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Wire-level integration test for the write-path primary-swap protocol.
//
// Spins up:
//   - A coordinator that answers RouteQuery from a tunable stack of
//     (primary_address, primary_term) views — the test pre-loads a
//     "stale-view-then-fresh-view" sequence to deterministically simulate
//     a primary swap landing during a write.
//   - Two mock data-nodes (mock_a, mock_b). Each one knows its own
//     accepted_term; an Insert/Upsert with a different stamped term is
//     rejected with ABORTED, mirroring the real PrimaryTermTracker
//     write gate.
//   - A real ProxyService in front, dialing the mock coordinator + data-
//     nodes. The proxy's write-side fallback helper is the system under
//     test.
//
// Each test asserts the proxy's externally-observable behavior across a
// swap: an upstream client sees OK after a single retry hop, even though
// the data-node bounced the first attempt with ABORTED.

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include "internal.grpc.pb.h"
#include "network/primary_term_header.h"
#include "network/proxy_service.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace test {

// Coordinator stand-in. Holds a queue of (address, term) views that
// RouteQuery returns in FIFO order; the last entry "sticks" once
// drained. This lets a test pre-load "stale-then-fresh" without any
// timing-based synchronization.
class PrimarySwapCoordinator
    : public proto::internal::InternalService::Service {
 public:
  std::atomic<int> route_query_calls{0};

  void Push(std::string addr, uint64_t term) {
    std::lock_guard<std::mutex> lock(mu_);
    queue_.push_back({std::move(addr), term});
  }

  grpc::Status RouteQuery(
      grpc::ServerContext* /*context*/,
      const proto::internal::RouteQueryRequest* /*request*/,
      proto::internal::RouteQueryResponse* response) override {
    route_query_calls++;
    View v;
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (queue_.empty()) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                             "test coordinator: no view queued");
      }
      v = queue_.front();
      // Pop until exactly one entry remains so the last view "sticks".
      if (queue_.size() > 1) queue_.pop_front();
    }
    response->set_collection_id(1);
    response->add_target_shard_ids(0);
    response->add_target_node_ids(101);
    response->add_target_node_addresses(v.address);
    auto* shard_opts = response->add_per_shard_options();
    shard_opts->set_shard_id(0);
    auto* primary = shard_opts->add_options();
    primary->set_node_id(101);
    primary->set_node_address(v.address);
    primary->set_is_primary(true);
    primary->set_primary_term(v.term);
    return grpc::Status::OK;
  }

 private:
  struct View {
    std::string address;
    uint64_t term = 0;
  };
  std::mutex mu_;
  std::deque<View> queue_;
};

// Mock data-node. Reads the gvdb-shard-term header on Upsert and bounces
// the call with ABORTED when the incoming term doesn't match
// accepted_term_. A header-absent call is accepted (matches the
// back-compat path on a real data-node when the proxy hasn't been
// upgraded). Upsert is the simplest write path that goes through
// RouteWriteAndCallWithFallback, so we exercise it for the retry test.
class PrimarySwapDataNode : public proto::VectorDBService::Service {
 public:
  std::atomic<int> upsert_calls{0};
  std::atomic<int> aborted_calls{0};
  std::string label;

  PrimarySwapDataNode(std::string l, uint64_t accepted_term)
      : label(std::move(l)), accepted_term_(accepted_term) {}

  void SetAcceptedTerm(uint64_t term) { accepted_term_.store(term); }

  grpc::Status Upsert(grpc::ServerContext* context,
                      const proto::UpsertRequest* request,
                      proto::UpsertResponse* response) override {
    upsert_calls++;
    auto header = network::ReadPrimaryTermHeader(context);
    if (header.parse_error) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                           "primary-swap test: bad term header");
    }
    if (header.has_term) {
      const uint64_t accepted = accepted_term_.load();
      if (header.term != accepted) {
        aborted_calls++;
        return grpc::Status(
            grpc::StatusCode::ABORTED,
            "primary-swap test: " + label + " accepted_term=" +
                std::to_string(accepted) +
                " got_term=" + std::to_string(header.term));
      }
    }
    response->set_upserted_count(request->vectors_size());
    return grpc::Status::OK;
  }

 private:
  std::atomic<uint64_t> accepted_term_;
};

class PrimarySwapTest {
 public:
  PrimarySwapTest() {
    mock_a_ = std::make_unique<PrimarySwapDataNode>("mock_a", /*term=*/1);
    mock_b_ = std::make_unique<PrimarySwapDataNode>("mock_b", /*term=*/2);
    mock_a_server_ = StartServer(mock_a_.get(), &mock_a_address_);
    mock_b_server_ = StartServer(mock_b_.get(), &mock_b_address_);

    coord_ = std::make_unique<PrimarySwapCoordinator>();
    coord_server_ = StartServer(coord_.get(), &coord_address_);

    proxy_ = std::make_unique<network::ProxyService>(
        std::vector<std::string>{coord_address_}, mock_a_address_);
    proxy_server_ = StartServer(proxy_.get(), &proxy_address_);

    auto channel = grpc::CreateChannel(proxy_address_,
                                        grpc::InsecureChannelCredentials());
    client_ = proto::VectorDBService::NewStub(channel);
  }

  ~PrimarySwapTest() {
    client_.reset();
    if (proxy_server_) { proxy_server_->Shutdown(); proxy_server_->Wait(); }
    if (coord_server_) { coord_server_->Shutdown(); coord_server_->Wait(); }
    if (mock_a_server_) { mock_a_server_->Shutdown(); mock_a_server_->Wait(); }
    if (mock_b_server_) { mock_b_server_->Shutdown(); mock_b_server_->Wait(); }
  }

  // Single-vector Upsert to exercise RouteWriteAndCallWithFallback.
  grpc::Status IssueUpsert(uint64_t vector_id) {
    proto::UpsertRequest req;
    req.set_collection_name("primary_swap_test");
    auto* v = req.add_vectors();
    v->set_id(vector_id);
    proto::UpsertResponse resp;
    grpc::ClientContext ctx;
    return client_->Upsert(&ctx, req, &resp);
  }

  std::unique_ptr<PrimarySwapDataNode> mock_a_;
  std::unique_ptr<PrimarySwapDataNode> mock_b_;
  std::unique_ptr<grpc::Server> mock_a_server_, mock_b_server_;
  std::string mock_a_address_, mock_b_address_;

  std::unique_ptr<PrimarySwapCoordinator> coord_;
  std::unique_ptr<grpc::Server> coord_server_;
  std::string coord_address_;

  std::unique_ptr<network::ProxyService> proxy_;
  std::unique_ptr<grpc::Server> proxy_server_;
  std::string proxy_address_;

  std::unique_ptr<proto::VectorDBService::Stub> client_;

 private:
  template <typename ServiceT>
  std::unique_ptr<grpc::Server> StartServer(ServiceT* svc,
                                              std::string* out_address) {
    grpc::ServerBuilder b;
    int port = 0;
    b.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
    b.RegisterService(svc);
    auto server = b.BuildAndStart();
    REQUIRE_NE(server, nullptr);
    *out_address = "localhost:" + std::to_string(port);
    return server;
  }
};

// Happy path: coordinator and primary agree on (mock_a, term=1). One
// RouteQuery, one Upsert call, no retries.
TEST_CASE_FIXTURE(PrimarySwapTest, "UpsertSucceedsWithMatchingTerm") {
  coord_->Push(mock_a_address_, /*term=*/1);

  auto status = IssueUpsert(42);

  CHECK(status.ok());
  CHECK_EQ(coord_->route_query_calls.load(), 1);
  CHECK_EQ(mock_a_->upsert_calls.load(), 1);
  CHECK_EQ(mock_a_->aborted_calls.load(), 0);
  CHECK_EQ(mock_b_->upsert_calls.load(), 0);
}

// Retry across a real swap. Pre-load TWO views in the coordinator queue:
//   1. (mock_a, term=1) — stale; the proxy's first RouteQuery picks
//      this. mock_a's accepted_term is 2 (it was paused), so the call
//      bounces with ABORTED.
//   2. (mock_b, term=2) — fresh; the proxy's helper sees ABORTED,
//      re-issues RouteQuery, gets this, dispatches to mock_b which
//      accepts. The user-visible result is OK.
//
// This is the load-bearing invariant: a write that races a primary
// swap is invisible to the writer except for one extra RTT.
TEST_CASE_FIXTURE(PrimarySwapTest, "UpsertRetriesAcrossSwapMidFlight") {
  // Stale view first, fresh view second (and sticky thereafter).
  coord_->Push(mock_a_address_, /*term=*/1);
  coord_->Push(mock_b_address_, /*term=*/2);

  // mock_a was paused: it now rejects term-1 writes (its accepted
  // term advanced to 2 via PausePrimary's last_known_term).
  mock_a_->SetAcceptedTerm(2);
  // mock_b was promoted to primary at term 2.
  mock_b_->SetAcceptedTerm(2);

  auto status = IssueUpsert(123);

  CHECK(status.ok());
  // First RouteQuery + at least one re-route after ABORTED.
  CHECK(coord_->route_query_calls.load() >= 2);
  // mock_a saw the stale-term Upsert and rejected it.
  CHECK_EQ(mock_a_->aborted_calls.load(), 1);
  // mock_b accepted the retry.
  CHECK_EQ(mock_b_->upsert_calls.load(), 1);
  CHECK_EQ(mock_b_->aborted_calls.load(), 0);
}

// Exhausted re-routes: coordinator never advances past the stale view,
// every attempt bounces ABORTED. The proxy must give up after
// kRouteWriteMaxReroutes and surface the ABORTED upstream.
TEST_CASE_FIXTURE(PrimarySwapTest, "UpsertSurfacesAbortedAfterExhaustingReroutes") {
  // Coordinator stays stuck on (mock_a, term=1) forever (single-entry
  // queue is sticky on the last view).
  coord_->Push(mock_a_address_, /*term=*/1);
  // mock_a expects a different term and bounces every attempt.
  mock_a_->SetAcceptedTerm(2);

  auto status = IssueUpsert(7);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::ABORTED);
  // Three attempts: 1 first-try + kRouteWriteMaxReroutes (=2) re-routes.
  CHECK_EQ(mock_a_->aborted_calls.load(), 3);
}

}  // namespace test
}  // namespace gvdb
