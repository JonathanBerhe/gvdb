// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/proxy_service.h"
#include "network/audit_context.h"
#include "network/dns_channel_args.h"
#include "utils/logger.h"

namespace gvdb {
namespace network {

ProxyService::ProxyService(
    const std::vector<std::string>& coordinator_addrs,
    const std::string& query_node_uri)
    : coordinator_addrs_(coordinator_addrs),
      query_node_uri_(query_node_uri) {}

proto::VectorDBService::Stub* ProxyService::GetCoordinatorClient() {
  std::lock_guard<std::mutex> lock(clients_mutex_);
  if (!coordinator_client_ && !coordinator_addrs_.empty()) {
    auto channel = grpc::CreateChannel(coordinator_addrs_[0],
                                      grpc::InsecureChannelCredentials());
    coordinator_client_ = proto::VectorDBService::NewStub(channel);
  }
  return coordinator_client_.get();
}

proto::VectorDBService::Stub* ProxyService::GetQueryNodeClient() {
  std::lock_guard<std::mutex> lock(clients_mutex_);
  if (query_node_client_) {
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
  return query_node_client_.get();
}

proto::internal::InternalService::Stub* ProxyService::GetCoordinatorInternalClient() {
  std::lock_guard<std::mutex> lock(clients_mutex_);
  if (!coordinator_internal_client_ && !coordinator_addrs_.empty()) {
    auto channel = grpc::CreateChannel(coordinator_addrs_[0],
                                      grpc::InsecureChannelCredentials());
    coordinator_internal_client_ = proto::internal::InternalService::NewStub(channel);
  }
  return coordinator_internal_client_.get();
}

proto::VectorDBService::Stub* ProxyService::GetDataNodeClientForCollection(
    const std::string& collection_name, bool read_only) {
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
  // Writes must always go to the primary (roadmap 0b.1).
  route_req.set_prefer_routable_replica(read_only);

  proto::internal::RouteQueryResponse route_resp;
  grpc::ClientContext ctx;
  auto status = internal_client->RouteQuery(&ctx, route_req, &route_resp);

  if (!status.ok() || route_resp.target_node_addresses_size() == 0) {
    return nullptr;
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
    // DNS-based discovery (roadmap 1.7).
    auto* client = GetDataNodeClientForCollection(request->collection_name());
    if (!client) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
    }
    proto::InsertRequest internal_req = *request;
    grpc::ClientContext client_ctx;
    return client->Insert(&client_ctx, internal_req, response);
  }

  int num_shards = route_resp.target_node_addresses_size();

  if (num_shards == 1) {
    // Single shard — send everything to one node
    const std::string& addr = route_resp.target_node_addresses(0);
    auto* client = GetOrCreateDataClient(addr);
    if (!client) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Data node unavailable");
    }
    proto::InsertRequest internal_req = *request;
    grpc::ClientContext client_ctx;
    return client->Insert(&client_ctx, internal_req, response);
  }

  // Multi-shard: split vectors by shard and route each batch to correct node
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

    const std::string& addr = route_resp.target_node_addresses(i);
    auto* client = GetOrCreateDataClient(addr);
    if (!client) {
      utils::Logger::Instance().Warn("Insert: data node unavailable for shard {}: {}", i, addr);
      if (first_error.empty()) {
        first_error = "Data node unavailable for shard " + std::to_string(i);
      }
      ++failed_shards;
      continue;
    }

    proto::InsertResponse shard_resp;
    grpc::ClientContext client_ctx;
    auto status = client->Insert(&client_ctx, shard_reqs[i], &shard_resp);
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

  auto* client = GetDataNodeClientForCollection(first_chunk.collection_name());
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
  }

  // Open client-side stream to data node
  grpc::ClientContext client_ctx;
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

  // Read path — replica fallback OK when primary is draining (0b.1).
  auto* client = GetDataNodeClientForCollection(
      request->collection_name(), /*read_only=*/true);
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
  }

  proto::GetRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->Get(&client_ctx, internal_req, response);
}

grpc::Status ProxyService::Delete(
    grpc::ServerContext* context,
    const proto::DeleteRequest* request,
    proto::DeleteResponse* response) {
  AuditContext::SetCollection(request->collection_name());
  AuditContext::SetItemCount(request->ids().size());

  auto* client = GetDataNodeClientForCollection(request->collection_name());
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
  }

  proto::DeleteRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->Delete(&client_ctx, internal_req, response);
}

grpc::Status ProxyService::UpdateMetadata(
    grpc::ServerContext* context,
    const proto::UpdateMetadataRequest* request,
    proto::UpdateMetadataResponse* response) {
  AuditContext::SetCollection(request->collection_name());

  auto* client = GetDataNodeClientForCollection(request->collection_name());
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
  }

  proto::UpdateMetadataRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->UpdateMetadata(&client_ctx, internal_req, response);
}

grpc::Status ProxyService::Upsert(
    grpc::ServerContext* context,
    const proto::UpsertRequest* request,
    proto::UpsertResponse* response) {
  AuditContext::SetCollection(request->collection_name());
  AuditContext::SetItemCount(request->vectors().size());

  auto* client = GetDataNodeClientForCollection(request->collection_name());
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
  }

  proto::UpsertRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->Upsert(&client_ctx, internal_req, response);
}

grpc::Status ProxyService::ListVectors(
    grpc::ServerContext* context,
    const proto::ListVectorsRequest* request,
    proto::ListVectorsResponse* response) {

  // Read path — replica fallback OK when primary is draining (0b.1).
  auto* client = GetDataNodeClientForCollection(
      request->collection_name(), /*read_only=*/true);
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
  }

  proto::ListVectorsRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->ListVectors(&client_ctx, internal_req, response);
}

grpc::Status ProxyService::HybridSearch(
    grpc::ServerContext* context,
    const proto::HybridSearchRequest* request,
    proto::HybridSearchResponse* response) {
  AuditContext::SetCollection(request->collection_name());

  // Read path — replica fallback OK when primary is draining (0b.1).
  auto* client = GetDataNodeClientForCollection(
      request->collection_name(), /*read_only=*/true);
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
  }

  proto::HybridSearchRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->HybridSearch(&client_ctx, internal_req, response);
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

}  // namespace network
}  // namespace gvdb