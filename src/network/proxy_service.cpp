// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/proxy_service.h"

#include <thread>

#include "network/audit_context.h"
#include "network/dns_channel_args.h"
#include "network/primary_term_header.h"
#include "network/replica_fallback.h"
#include "utils/logger.h"

namespace gvdb {
namespace network {

ProxyService::ProxyService(
    const std::vector<std::string>& coordinator_addrs,
    const std::string& query_node_uri)
    : coordinator_addrs_(coordinator_addrs),
      query_node_uri_(query_node_uri) {}

proto::VectorDBService::Stub* ProxyService::GetCoordinatorClient() {
  // Hot-path: lock-free load. clients_mutex_ is only taken for first-time
  // init; afterwards the stub is immutable and gRPC guarantees concurrent-
  // invocation safety on it.
  if (coordinator_client_ready_.load(std::memory_order_acquire)) {
    return coordinator_client_.get();
  }
  std::lock_guard<std::mutex> lock(clients_mutex_);
  if (coordinator_client_ready_.load(std::memory_order_relaxed)) {
    return coordinator_client_.get();
  }
  if (coordinator_addrs_.empty()) {
    return nullptr;
  }
  auto channel = grpc::CreateChannel(coordinator_addrs_[0],
                                    grpc::InsecureChannelCredentials());
  coordinator_client_ = proto::VectorDBService::NewStub(channel);
  coordinator_client_ready_.store(true, std::memory_order_release);
  return coordinator_client_.get();
}

proto::VectorDBService::Stub* ProxyService::GetQueryNodeClient() {
  if (query_node_client_ready_.load(std::memory_order_acquire)) {
    return query_node_client_.get();
  }
  std::lock_guard<std::mutex> lock(clients_mutex_);
  if (query_node_client_ready_.load(std::memory_order_relaxed)) {
    return query_node_client_.get();
  }
  if (query_node_uri_.empty()) {
    utils::Logger::Instance().Error("No query-node URI configured");
    return nullptr;
  }
  // Single channel, DNS-resolved headless service, round_robin LB across all
  // live pods. gRPC re-resolves DNS every ~5s (see BuildDnsChannelArgs) so
  // `kubectl scale query-node` is picked up without proxy restart.
  // Non-dns targets (single-node / legacy) still work via a bare insecure
  // channel without the extra args.
  std::shared_ptr<grpc::Channel> channel;
  if (IsDnsUri(query_node_uri_)) {
    channel = grpc::CreateCustomChannel(
        query_node_uri_, grpc::InsecureChannelCredentials(),
        BuildDnsChannelArgs());
  } else {
    channel = grpc::CreateChannel(
        query_node_uri_, grpc::InsecureChannelCredentials());
  }
  query_node_client_ = proto::VectorDBService::NewStub(channel);
  query_node_client_ready_.store(true, std::memory_order_release);
  return query_node_client_.get();
}

proto::internal::InternalService::Stub* ProxyService::GetCoordinatorInternalClient() {
  if (coordinator_internal_client_ready_.load(std::memory_order_acquire)) {
    return coordinator_internal_client_.get();
  }
  std::lock_guard<std::mutex> lock(clients_mutex_);
  if (coordinator_internal_client_ready_.load(std::memory_order_relaxed)) {
    return coordinator_internal_client_.get();
  }
  if (coordinator_addrs_.empty()) {
    return nullptr;
  }
  auto channel = grpc::CreateChannel(coordinator_addrs_[0],
                                    grpc::InsecureChannelCredentials());
  coordinator_internal_client_ = proto::internal::InternalService::NewStub(channel);
  coordinator_internal_client_ready_.store(true, std::memory_order_release);
  return coordinator_internal_client_.get();
}

proto::VectorDBService::Stub* ProxyService::GetDataNodeClientForCollection(
    const std::string& collection_name, bool read_only,
    uint64_t* out_primary_term) {
  // Ask the coordinator which data node owns this collection's shard
  auto* internal_client = GetCoordinatorInternalClient();
  if (!internal_client) {
    return nullptr;
  }

  // RouteQuery returns shard→node mappings; top_k is unused for routing
  proto::internal::RouteQueryRequest route_req;
  route_req.set_collection_name(collection_name);
  route_req.set_top_k(0);
  // Reads can safely land on a routable replica if the primary is draining.
  // Writes must always go to the primary.
  route_req.set_prefer_routable_replica(read_only);

  proto::internal::RouteQueryResponse route_resp;
  grpc::ClientContext ctx;
  auto status = internal_client->RouteQuery(&ctx, route_req, &route_resp);

  if (!status.ok() || route_resp.target_node_addresses_size() == 0) {
    return nullptr;
  }

  // Pull the routed shard's primary_term from the new per_shard_options
  // list when present; falls back to 0 when talking to an older
  // coordinator that doesn't emit the field. A zero term is a signal
  // to the caller "don't stamp the header" (back-compat path).
  if (out_primary_term != nullptr) {
    *out_primary_term = 0;
    if (route_resp.per_shard_options_size() > 0 &&
        route_resp.per_shard_options(0).options_size() > 0) {
      *out_primary_term =
          route_resp.per_shard_options(0).options(0).primary_term();
    }
  }

  const std::string& addr = route_resp.target_node_addresses(0);
  return GetOrCreateDataClient(addr);
}

proto::VectorDBService::Stub* ProxyService::GetOrCreateDataClient(
    const std::string& address) {
  std::lock_guard<std::mutex> lock(clients_mutex_);
  auto it = data_client_by_addr_.find(address);
  if (it == data_client_by_addr_.end()) {
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    data_client_by_addr_[address] = proto::VectorDBService::NewStub(channel);
    return data_client_by_addr_[address].get();
  }
  return it->second.get();
}

grpc::Status ProxyService::HealthCheck(
    grpc::ServerContext* context,
    const proto::HealthCheckRequest* request,
    proto::HealthCheckResponse* response) {

  response->set_status(proto::HealthCheckResponse::SERVING);
  response->set_message("Proxy is healthy");
  return grpc::Status::OK;
}

grpc::Status ProxyService::CreateCollection(
    grpc::ServerContext* context,
    const proto::CreateCollectionRequest* request,
    proto::CreateCollectionResponse* response) {
  AuditContext::SetCollection(request->collection_name());

  auto* client = GetCoordinatorClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No coordinator available");
  }

  // Forward to coordinator's InternalService
  proto::CreateCollectionRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  auto status = client->CreateCollection(&client_ctx, internal_req, response);

  if (!status.ok()) {
    utils::Logger::Instance().Error("CreateCollection failed: {}", status.error_message());
  }
  return status;
}

grpc::Status ProxyService::DropCollection(
    grpc::ServerContext* context,
    const proto::DropCollectionRequest* request,
    proto::DropCollectionResponse* response) {
  AuditContext::SetCollection(request->collection_name());

  auto* client = GetCoordinatorClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No coordinator available");
  }

  proto::DropCollectionRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->DropCollection(&client_ctx, internal_req, response);
}

grpc::Status ProxyService::ListCollections(
    grpc::ServerContext* context,
    const proto::ListCollectionsRequest* request,
    proto::ListCollectionsResponse* response) {

  auto* client = GetCoordinatorClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No coordinator available");
  }

  proto::ListCollectionsRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->ListCollections(&client_ctx, internal_req, response);
}

grpc::Status ProxyService::Insert(
    grpc::ServerContext* context,
    const proto::InsertRequest* request,
    proto::InsertResponse* response) {
  AuditContext::SetCollection(request->collection_name());
  AuditContext::SetItemCount(request->vectors().size());

  // Get shard->node mapping from coordinator
  auto* internal_client = GetCoordinatorInternalClient();
  if (!internal_client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No coordinator available");
  }

  proto::internal::RouteQueryRequest route_req;
  route_req.set_collection_name(request->collection_name());
  proto::internal::RouteQueryResponse route_resp;
  grpc::ClientContext route_ctx;
  auto route_status = internal_client->RouteQuery(&route_ctx, route_req, &route_resp);

  if (!route_status.ok() || route_resp.target_node_addresses_size() == 0) {
    // Fallback: ask RouteQuery per-collection (different code path — the
    // coordinator may answer for single-shard collections even when the
    // general RouteQuery call above failed). If still no route, the proxy
    // returns UNAVAILABLE; there is no "random data-node" fan-out because
    // the --data-nodes static list was removed when the proxy switched to
    // DNS-based discovery.
    uint64_t primary_term = 0;
    auto* client = GetDataNodeClientForCollection(
        request->collection_name(), /*read_only=*/false, &primary_term);
    if (!client) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
    }
    proto::InsertRequest internal_req = *request;
    grpc::ClientContext client_ctx;
    if (primary_term > 0) StampPrimaryTermHeader(&client_ctx, primary_term);
    return client->Insert(&client_ctx, internal_req, response);
  }

  // Helper: pull the term for shard `shard_idx` from per_shard_options.
  // Falls back to 0 ("don't stamp the header") when the coordinator
  // didn't emit per_shard_options (older binary, mixed-version cluster).
  auto shard_term_at = [&route_resp](int shard_idx) -> uint64_t {
    if (shard_idx < route_resp.per_shard_options_size()) {
      const auto& opts = route_resp.per_shard_options(shard_idx);
      if (opts.options_size() > 0) {
        return opts.options(0).primary_term();
      }
    }
    return 0;
  };

  int num_shards = route_resp.target_node_addresses_size();

  if (num_shards == 1) {
    // Single shard — use the write-side fallback helper which handles
    // re-routing on ABORTED (term-mismatch during a primary swap).
    proto::InsertRequest internal_req = *request;
    return network::RouteWriteAndCallWithFallback(
        GetCoordinatorInternalClient(), request->collection_name(), "insert",
        std::chrono::milliseconds(5000),
        [this](const std::string& addr) { return GetOrCreateDataClient(addr); },
        [&](grpc::ClientContext* ctx, proto::VectorDBService::Stub* stub) {
          return stub->Insert(ctx, internal_req, response);
        });
  }

  // Multi-shard: split vectors by shard and route each batch to correct node.
  // Per-shard ABORTED retries are inline (one re-route hop per shard) because
  // the helper assumes a single primary; multi-shard fan-out is bespoke to
  // the proxy-side hash-by-vector-id split.
  std::vector<proto::InsertRequest> shard_reqs(num_shards);
  for (int i = 0; i < num_shards; ++i) {
    shard_reqs[i].set_collection_name(request->collection_name());
  }

  for (const auto& vec : request->vectors()) {
    uint32_t shard_idx = static_cast<uint32_t>(vec.id() % num_shards);
    *shard_reqs[shard_idx].add_vectors() = vec;
  }

  uint64_t total_inserted = 0;
  std::string first_error;
  int failed_shards = 0;

  for (int i = 0; i < num_shards; ++i) {
    if (shard_reqs[i].vectors_size() == 0) continue;

    // Send to the routed primary; on ABORTED (primary swap raced our
    // route lookup), re-issue RouteQuery, pick the fresh primary for
    // shard `i`, and try once more before declaring this shard failed.
    std::string addr = route_resp.target_node_addresses(i);
    uint64_t shard_term = shard_term_at(i);

    grpc::Status status;
    proto::InsertResponse shard_resp;
    auto attempt_call = [&](const std::string& a, uint64_t t) -> grpc::Status {
      auto* client = GetOrCreateDataClient(a);
      if (!client) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                             "Data node unavailable");
      }
      grpc::ClientContext ctx;
      if (t > 0) StampPrimaryTermHeader(&ctx, t);
      shard_resp.Clear();
      return client->Insert(&ctx, shard_reqs[i], &shard_resp);
    };

    status = attempt_call(addr, shard_term);

    if (!status.ok() &&
        (status.error_code() == grpc::StatusCode::ABORTED ||
         status.error_code() == grpc::StatusCode::FAILED_PRECONDITION)) {
      // Re-route once. ABORTED: a concurrent primary swap moved this
      // shard's primary and the data-node bounced our stale-term write.
      // FAILED_PRECONDITION: the node has no primary record for the
      // shard yet (fresh creation racing the coordinator's record
      // push); the routing is usually already right, so give the record
      // a short beat to land before the retry.
      if (status.error_code() == grpc::StatusCode::FAILED_PRECONDITION) {
        std::this_thread::sleep_for(network::kWriteUnknownShardBackoff);
      }
      utils::Logger::Instance().Info(
          "Insert: shard {} got {} (term={}, msg=\"{}\"); re-routing",
          i,
          status.error_code() == grpc::StatusCode::ABORTED
              ? "ABORTED" : "FAILED_PRECONDITION",
          shard_term, status.error_message());
      proto::internal::RouteQueryRequest reroute_req;
      reroute_req.set_collection_name(request->collection_name());
      proto::internal::RouteQueryResponse reroute_resp;
      grpc::ClientContext reroute_ctx;
      auto reroute_status = internal_client->RouteQuery(
          &reroute_ctx, reroute_req, &reroute_resp);
      if (reroute_status.ok() &&
          i < reroute_resp.target_node_addresses_size()) {
        addr = reroute_resp.target_node_addresses(i);
        if (i < reroute_resp.per_shard_options_size() &&
            reroute_resp.per_shard_options(i).options_size() > 0) {
          shard_term = reroute_resp.per_shard_options(i).options(0).primary_term();
        }
        status = attempt_call(addr, shard_term);
      }
    }

    if (status.ok()) {
      total_inserted += shard_resp.inserted_count();
    } else {
      utils::Logger::Instance().Warn("Insert: shard {} failed on {}: {}", i, addr,
                                      status.error_message());
      if (first_error.empty()) {
        first_error = status.error_message();
      }
      ++failed_shards;
    }
  }

  if (failed_shards > 0) {
    std::string msg = "Insert partially failed: " + std::to_string(failed_shards) +
        " shard(s) failed, " + std::to_string(total_inserted) +
        " vectors inserted: " + first_error;
    return grpc::Status(grpc::StatusCode::INTERNAL, msg);
  }

  response->set_inserted_count(total_inserted);
  response->set_message("Inserted across " + std::to_string(num_shards) + " shards");
  return grpc::Status::OK;
}

grpc::Status ProxyService::StreamInsert(
    grpc::ServerContext* context,
    grpc::ServerReader<proto::InsertRequest>* reader,
    proto::InsertResponse* response) {

  // Read first chunk to determine collection for routing
  proto::InsertRequest first_chunk;
  if (!reader->Read(&first_chunk)) {
    response->set_inserted_count(0);
    response->set_message("Empty stream");
    return grpc::Status::OK;
  }

  uint64_t primary_term = 0;
  auto* client = GetDataNodeClientForCollection(
      first_chunk.collection_name(), /*read_only=*/false, &primary_term);
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
  }

  // Open client-side stream to data node
  grpc::ClientContext client_ctx;
  if (primary_term > 0) StampPrimaryTermHeader(&client_ctx, primary_term);
  auto writer = client->StreamInsert(&client_ctx, response);

  // Forward first chunk
  if (!writer->Write(first_chunk)) {
    writer->WritesDone();
    return writer->Finish();
  }

  // Forward remaining chunks
  proto::InsertRequest chunk;
  while (reader->Read(&chunk)) {
    if (!writer->Write(chunk)) {
      writer->WritesDone();
      return writer->Finish();
    }
  }

  writer->WritesDone();
  return writer->Finish();
}

grpc::Status ProxyService::Get(
    grpc::ServerContext* context,
    const proto::GetRequest* request,
    proto::GetResponse* response) {
  AuditContext::SetCollection(request->collection_name());
  AuditContext::SetItemCount(request->ids().size());

  // Read with replica fallback: if the first candidate is unreachable
  // (transient drain-window failure), try the next routable replica before
  // surfacing UNAVAILABLE to the caller.
  proto::GetRequest internal_req = *request;
  return network::RouteReadAndCallWithFallback(
      GetCoordinatorInternalClient(), request->collection_name(), "get",
      std::chrono::milliseconds(5000),
      [this](const std::string& addr) { return GetOrCreateDataClient(addr); },
      [&](grpc::ClientContext* ctx, proto::VectorDBService::Stub* stub) {
        return stub->Get(ctx, internal_req, response);
      });
}

grpc::Status ProxyService::Delete(
    grpc::ServerContext* context,
    const proto::DeleteRequest* request,
    proto::DeleteResponse* response) {
  AuditContext::SetCollection(request->collection_name());
  AuditContext::SetItemCount(request->ids().size());

  // Route via the write-side fallback helper: re-issues RouteQuery on
  // ABORTED so a primary-swap race resolves with one extra RTT instead
  // of bouncing the error to the user.
  proto::DeleteRequest internal_req = *request;
  return network::RouteWriteAndCallWithFallback(
      GetCoordinatorInternalClient(), request->collection_name(), "delete",
      std::chrono::milliseconds(5000),
      [this](const std::string& addr) { return GetOrCreateDataClient(addr); },
      [&](grpc::ClientContext* ctx, proto::VectorDBService::Stub* stub) {
        return stub->Delete(ctx, internal_req, response);
      });
}

grpc::Status ProxyService::UpdateMetadata(
    grpc::ServerContext* context,
    const proto::UpdateMetadataRequest* request,
    proto::UpdateMetadataResponse* response) {
  AuditContext::SetCollection(request->collection_name());

  proto::UpdateMetadataRequest internal_req = *request;
  return network::RouteWriteAndCallWithFallback(
      GetCoordinatorInternalClient(), request->collection_name(),
      "update_metadata", std::chrono::milliseconds(5000),
      [this](const std::string& addr) { return GetOrCreateDataClient(addr); },
      [&](grpc::ClientContext* ctx, proto::VectorDBService::Stub* stub) {
        return stub->UpdateMetadata(ctx, internal_req, response);
      });
}

grpc::Status ProxyService::Upsert(
    grpc::ServerContext* context,
    const proto::UpsertRequest* request,
    proto::UpsertResponse* response) {
  AuditContext::SetCollection(request->collection_name());
  AuditContext::SetItemCount(request->vectors().size());

  proto::UpsertRequest internal_req = *request;
  return network::RouteWriteAndCallWithFallback(
      GetCoordinatorInternalClient(), request->collection_name(), "upsert",
      std::chrono::milliseconds(5000),
      [this](const std::string& addr) { return GetOrCreateDataClient(addr); },
      [&](grpc::ClientContext* ctx, proto::VectorDBService::Stub* stub) {
        return stub->Upsert(ctx, internal_req, response);
      });
}

grpc::Status ProxyService::ListVectors(
    grpc::ServerContext* context,
    const proto::ListVectorsRequest* request,
    proto::ListVectorsResponse* response) {

  // Read with replica fallback (see ProxyService::Get for rationale).
  proto::ListVectorsRequest internal_req = *request;
  return network::RouteReadAndCallWithFallback(
      GetCoordinatorInternalClient(), request->collection_name(), "list_vectors",
      std::chrono::milliseconds(5000),
      [this](const std::string& addr) { return GetOrCreateDataClient(addr); },
      [&](grpc::ClientContext* ctx, proto::VectorDBService::Stub* stub) {
        return stub->ListVectors(ctx, internal_req, response);
      });
}

grpc::Status ProxyService::HybridSearch(
    grpc::ServerContext* context,
    const proto::HybridSearchRequest* request,
    proto::HybridSearchResponse* response) {
  AuditContext::SetCollection(request->collection_name());

  // Read with replica fallback (see ProxyService::Get for rationale).
  proto::HybridSearchRequest internal_req = *request;
  return network::RouteReadAndCallWithFallback(
      GetCoordinatorInternalClient(), request->collection_name(), "hybrid_search",
      std::chrono::milliseconds(5000),
      [this](const std::string& addr) { return GetOrCreateDataClient(addr); },
      [&](grpc::ClientContext* ctx, proto::VectorDBService::Stub* stub) {
        return stub->HybridSearch(ctx, internal_req, response);
      });
}

grpc::Status ProxyService::Search(
    grpc::ServerContext* context,
    const proto::SearchRequest* request,
    proto::SearchResponse* response) {

  // Prefer query nodes; fall back to data nodes if none configured
  proto::VectorDBService::Stub* client = GetQueryNodeClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
        "No query node or data node available");
  }

  proto::SearchRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->Search(&client_ctx, internal_req, response);
}

grpc::Status ProxyService::RangeSearch(
    grpc::ServerContext* context,
    const proto::RangeSearchRequest* request,
    proto::RangeSearchResponse* response) {

  proto::VectorDBService::Stub* client = GetQueryNodeClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
        "No query node or data node available");
  }

  proto::RangeSearchRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->RangeSearch(&client_ctx, internal_req, response);
}

grpc::Status ProxyService::GetStats(
    grpc::ServerContext* context,
    const proto::GetStatsRequest* request,
    proto::GetStatsResponse* response) {

  auto* client = GetCoordinatorClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No coordinator available");
  }

  proto::GetStatsRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->GetStats(&client_ctx, internal_req, response);
}

// =============================================================================
// Backup / Restore — forward to coordinator (admin-level, multi-shard)
// =============================================================================

grpc::Status ProxyService::BackupCollection(
    grpc::ServerContext* /*context*/,
    const proto::BackupCollectionRequest* request,
    proto::BackupCollectionResponse* response) {
  AuditContext::SetCollection(request->collection_name());
  auto* client = GetCoordinatorClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "No coordinator available");
  }
  proto::BackupCollectionRequest req = *request;
  grpc::ClientContext ctx;
  return client->BackupCollection(&ctx, req, response);
}

grpc::Status ProxyService::RestoreCollection(
    grpc::ServerContext* /*context*/,
    const proto::RestoreCollectionRequest* request,
    proto::RestoreCollectionResponse* response) {
  AuditContext::SetCollection(request->target_collection_name());
  auto* client = GetCoordinatorClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "No coordinator available");
  }
  proto::RestoreCollectionRequest req = *request;
  grpc::ClientContext ctx;
  return client->RestoreCollection(&ctx, req, response);
}

grpc::Status ProxyService::GetBackupStatus(
    grpc::ServerContext* /*context*/,
    const proto::GetBackupStatusRequest* request,
    proto::GetBackupStatusResponse* response) {
  auto* client = GetCoordinatorClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "No coordinator available");
  }
  proto::GetBackupStatusRequest req = *request;
  grpc::ClientContext ctx;
  return client->GetBackupStatus(&ctx, req, response);
}

grpc::Status ProxyService::GetRestoreStatus(
    grpc::ServerContext* /*context*/,
    const proto::GetRestoreStatusRequest* request,
    proto::GetRestoreStatusResponse* response) {
  auto* client = GetCoordinatorClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "No coordinator available");
  }
  proto::GetRestoreStatusRequest req = *request;
  grpc::ClientContext ctx;
  return client->GetRestoreStatus(&ctx, req, response);
}

grpc::Status ProxyService::ListBackups(
    grpc::ServerContext* /*context*/,
    const proto::ListBackupsRequest* request,
    proto::ListBackupsResponse* response) {
  auto* client = GetCoordinatorClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "No coordinator available");
  }
  proto::ListBackupsRequest req = *request;
  grpc::ClientContext ctx;
  return client->ListBackups(&ctx, req, response);
}

grpc::Status ProxyService::CancelBackup(
    grpc::ServerContext* /*context*/,
    const proto::CancelBackupRequest* request,
    proto::CancelBackupResponse* response) {
  auto* client = GetCoordinatorClient();
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "No coordinator available");
  }
  proto::CancelBackupRequest req = *request;
  grpc::ClientContext ctx;
  return client->CancelBackup(&ctx, req, response);
}

}  // namespace network
}  // namespace gvdb