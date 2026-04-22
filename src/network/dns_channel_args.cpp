// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/dns_channel_args.h"

namespace gvdb {
namespace network {

grpc::ChannelArguments BuildDnsChannelArgs() {
  grpc::ChannelArguments args;
  // round_robin across all A records returned by DNS, so queries fan out
  // across all live query-node pods without application-level wiring.
  args.SetLoadBalancingPolicyName("round_robin");
  args.SetInt(GRPC_ARG_DNS_MIN_TIME_BETWEEN_RESOLUTIONS_MS,
              kDnsMinTimeBetweenResolutionsMs);
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS,
              kInitialReconnectBackoffMs);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, kMaxReconnectBackoffMs);
  return args;
}

bool IsDnsUri(const std::string& target) {
  return target.rfind("dns:///", 0) == 0;
}

}  // namespace network
}  // namespace gvdb
