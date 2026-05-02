// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/primary_term_header.h"

#include <cerrno>
#include <cstdlib>
#include <string>

namespace gvdb {
namespace network {

PrimaryTermFromHeader ReadPrimaryTermHeader(grpc::ServerContext* context) {
  PrimaryTermFromHeader out;
  if (context == nullptr) return out;
  const auto& md = context->client_metadata();
  auto it = md.find(kPrimaryTermHeader);
  if (it == md.end()) {
    return out;  // pre-1.x client, no header
  }
  out.has_term = true;

  // grpc::string_ref is not null-terminated; copy into a std::string
  // before strtoull. The header value is short (decimal uint64 max
  // 20 chars) so the copy is cheap.
  const std::string raw(it->second.data(), it->second.size());
  if (raw.empty()) {
    out.parse_error = true;
    return out;
  }
  errno = 0;
  char* end = nullptr;
  unsigned long long parsed =
      std::strtoull(raw.c_str(), &end, 10);
  // Reject empty parses, trailing junk, and overflow. uint64_t fits
  // strtoull's return on every supported platform; ULLONG_MAX returned
  // with errno==ERANGE means overflow.
  if (end == raw.c_str() || end == nullptr || *end != '\0' || errno == ERANGE) {
    out.parse_error = true;
    out.term = 0;
    return out;
  }
  out.term = static_cast<uint64_t>(parsed);
  return out;
}

void StampPrimaryTermHeader(grpc::ClientContext* context, uint64_t term) {
  if (context == nullptr) return;
  context->AddMetadata(kPrimaryTermHeader, std::to_string(term));
}

}  // namespace network
}  // namespace gvdb
