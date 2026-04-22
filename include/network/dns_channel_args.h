// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>
#include <string>

namespace gvdb {
namespace network {

// Minimum time between DNS re-resolutions for dns:/// channels. gRPC's default
// is 30s; for our K8s scale-up / pod-restart recovery target we want faster
// pickup of new backend endpoints (and forgetting of dead ones).
constexpr int kDnsMinTimeBetweenResolutionsMs = 5000;

// Initial reconnect backoff. Default is ~1s; we want the first failed dial to
// retry quickly so a single bad pod doesn't stall a request.
constexpr int kInitialReconnectBackoffMs = 200;

// Cap on reconnect backoff. Default is ~120s; we cap at 2s so a long-dead
// pod that comes back is picked up in the next DNS re-resolution window, not
// minutes later.
constexpr int kMaxReconnectBackoffMs = 2000;

// BuildDnsChannelArgs returns grpc::ChannelArguments configured for a
// dns:///<headless-service>:<port> channel with aggressive re-resolution and
// reconnect tuning. Callers pair this with a round_robin load-balancing
// policy so the channel fans out across all resolved A records.
//
// Rationale for each arg is documented inline at the constants above.
grpc::ChannelArguments BuildDnsChannelArgs();

// IsDnsUri returns true if the given target string uses the dns:/// scheme
// (the format gRPC's built-in DNS resolver expects). Used by the proxy to
// decide whether to apply BuildDnsChannelArgs — legacy unit tests or local
// runs may still pass a bare host:port.
bool IsDnsUri(const std::string& target);

}  // namespace network
}  // namespace gvdb
