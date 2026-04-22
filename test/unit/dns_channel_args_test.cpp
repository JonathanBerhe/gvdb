// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/dns_channel_args.h"

#include <doctest/doctest.h>

using namespace gvdb::network;

TEST_CASE("IsDnsUri recognizes the dns:/// scheme") {
  CHECK(IsDnsUri("dns:///svc.ns.svc.cluster.local:50070"));
  CHECK(IsDnsUri("dns:///localhost:50070"));
  CHECK_FALSE(IsDnsUri("localhost:50070"));
  CHECK_FALSE(IsDnsUri(""));
  CHECK_FALSE(IsDnsUri("dns://host:port"));  // wrong scheme (two slashes)
  CHECK_FALSE(IsDnsUri("grpc:///svc:50070"));
}

TEST_CASE("BuildDnsChannelArgs sets aggressive resolution/reconnect tuning") {
  // The returned grpc::ChannelArguments is an opaque-ish bag, but we can
  // round-trip it through its native arg list to verify each arg we own.
  auto args = BuildDnsChannelArgs();

  grpc_channel_args c_args;
  args.SetChannelArgs(&c_args);

  auto find_int = [&](const char* key) -> int {
    for (size_t i = 0; i < c_args.num_args; ++i) {
      if (std::string(c_args.args[i].key) == key &&
          c_args.args[i].type == GRPC_ARG_INTEGER) {
        return c_args.args[i].value.integer;
      }
    }
    return -1;
  };
  auto find_string = [&](const char* key) -> std::string {
    for (size_t i = 0; i < c_args.num_args; ++i) {
      if (std::string(c_args.args[i].key) == key &&
          c_args.args[i].type == GRPC_ARG_STRING) {
        return c_args.args[i].value.string;
      }
    }
    return "";
  };

  CHECK_EQ(find_int(GRPC_ARG_DNS_MIN_TIME_BETWEEN_RESOLUTIONS_MS),
           kDnsMinTimeBetweenResolutionsMs);
  CHECK_EQ(find_int(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS),
           kInitialReconnectBackoffMs);
  CHECK_EQ(find_int(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS),
           kMaxReconnectBackoffMs);
  CHECK_EQ(find_string(GRPC_ARG_LB_POLICY_NAME), "round_robin");
}

TEST_CASE("BuildDnsChannelArgs constants match production budgets") {
  // These constants are the contract with operators deploying the proxy in
  // K8s: recovery time budget after a pod restart is bounded by
  // kDnsMinTimeBetweenResolutionsMs (when to re-resolve) + backoff. Drifting
  // them silently would regress our scale-up SLO, so lock them here.
  CHECK_EQ(kDnsMinTimeBetweenResolutionsMs, 5000);
  CHECK_EQ(kInitialReconnectBackoffMs, 200);
  CHECK_EQ(kMaxReconnectBackoffMs, 2000);
}
