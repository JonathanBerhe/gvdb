// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/proxy_service.h"
#include "utils/logger.h"

namespace gvdb {
namespace network {

ProxyService::ProxyService(
    const std::vector<std::string>& coordinator_addrs,
    const std::vector<std::string>& query_node_addrs,
    const std::vector<std::string>& data_node_addrs)
    : coordinator_addrs_(coordinator_addrs),
      query_node_addrs_(query_node_addrs),
      data_node_addrs_(data_node_addrs) {

  // Initialize load balancer and query nodes
  if (!query_node_addrs_.empty()) {
    load_balancer_ = std::make_unique<cluster::LoadBalancer>(
        cluster::LoadBalancingStrategy::ROUND_ROBIN);

    // Create QueryNode structs with incremental NodeIds
    uint32_t node_id_counter = 1;
    for (const auto& addr : query_node_addrs_) {
      QueryNode node;
      node.id = static_cast<core::NodeId>(node_id_counter++);
      node.address = addr;
      // Client will be lazily initialized
      query_nodes_.push_back(std::move(node));
    }
  }
}

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

  // Lazy initialize query node clients
  for (auto& node : query_nodes_) {
    if (!node.client) {
      auto channel = grpc::CreateChannel(node.address, grpc::InsecureChannelCredentials());
      node.client = proto::VectorDBService::NewStub(channel);
    }
  }

  if (query_nodes_.empty() || !load_balancer_) {
    utils::Logger::Instance().Error(
        "No query nodes available (nodes_empty={}, load_balancer_null={})",
        query_nodes_.empty(), !load_balancer_);
    return nullptr;
  }

  // Collect available NodeIds for LoadBalancer
  std::vector<core::NodeId> available_nodes;
  for (const auto& node : query_nodes_) {
    available_nodes.push_back(node.id);
  }

  // Use LoadBalancer to select a query node (round-robin)
  auto selected = load_balancer_->SelectNode(available_nodes);
  if (!selected.ok()) {
    utils::Logger::Instance().Error("LoadBalancer failed to select node: {}",
                                    selected.status().message());
    return nullptr;
  }

  // Find the QueryNode with the selected NodeId
  core::NodeId selected_node_id = selected.value();
  for (auto& node : query_nodes_) {
    if (node.id == selected_node_id) {
      return node.client.get();
    }
  }

  utils::Logger::Instance().Error("Selected node ID not found: {}",
                                  static_cast<uint32_t>(selected_node_id));
  return nullptr;
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
    const std::string& collection_name) {
  // Ask the coordinator which data node owns this collection's shard
  auto* internal_client = GetCoordinatorInternalClient();
  if (!internal_client) {
    return nullptr;
  }

  // RouteQuery returns shard→node mappings; top_k is unused for routing
  proto::internal::RouteQueryRequest route_req;
  route_req.set_collection_name(collection_name);
  route_req.set_top_k(0);

  proto::internal::RouteQueryResponse route_resp;
  grpc::ClientContext ctx;
  auto status = internal_client->RouteQuery(&ctx, route_req, &route_resp);

  if (!status.ok() || route_resp.target_node_addresses_size() == 0) {
    return nullptr;
  }

  const std::string& addr = route_resp.target_node_addresses(0);

  std::lock_guard<std::mutex> lock(clients_mutex_);
  auto it = data_client_by_addr_.find(addr);
  if (it == data_client_by_addr_.end()) {
    auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    data_client_by_addr_[addr] = proto::VectorDBService::NewStub(channel);
    return data_client_by_addr_[addr].get();
  }
  return it->second.get();
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

proto::VectorDBService::Stub* ProxyService::GetDataNodeClient(int shard_id) {
  std::lock_guard<std::mutex> lock(clients_mutex_);
  if (data_clients_.empty() && !data_node_addrs_.empty()) {
    for (const auto& addr : data_node_addrs_) {
      auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      data_clients_.push_back(proto::VectorDBService::NewStub(channel));
    }
  }
  // Simple round-robin for now
  int index = shard_id % std::max(1, static_cast<int>(data_clients_.size()));
  return data_clients_.empty() ? nullptr : data_clients_[index].get();
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
    // Fallback: single node
    auto* client = GetDataNodeClientForCollection(request->collection_name());
    if (!client) {
      int shard = data_node_counter_.fetch_add(1, std::memory_order_relaxed);
      client = GetDataNodeClient(shard);
    }
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
  for (int i = 0; i < num_shards; ++i) {
    if (shard_reqs[i].vectors_size() == 0) continue;

    const std::string& addr = route_resp.target_node_addresses(i);
    auto* client = GetOrCreateDataClient(addr);
    if (!client) continue;

    proto::InsertResponse shard_resp;
    grpc::ClientContext client_ctx;
    auto status = client->Insert(&client_ctx, shard_reqs[i], &shard_resp);
    if (status.ok()) {
      total_inserted += shard_resp.inserted_count();
    }
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
    int shard = data_node_counter_.fetch_add(1, std::memory_order_relaxed);
    client = GetDataNodeClient(shard);
  }
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

  auto* client = GetDataNodeClientForCollection(request->collection_name());
  if (!client) {
    int shard = data_node_counter_.fetch_add(1, std::memory_order_relaxed);
    client = GetDataNodeClient(shard);
  }
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

  auto* client = GetDataNodeClientForCollection(request->collection_name());
  if (!client) {
    int shard = data_node_counter_.fetch_add(1, std::memory_order_relaxed);
    client = GetDataNodeClient(shard);
  }
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

  auto* client = GetDataNodeClientForCollection(request->collection_name());
  if (!client) {
    int shard = data_node_counter_.fetch_add(1, std::memory_order_relaxed);
    client = GetDataNodeClient(shard);
  }
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No data node available");
  }

  proto::UpdateMetadataRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->UpdateMetadata(&client_ctx, internal_req, response);
}

grpc::Status ProxyService::HybridSearch(
    grpc::ServerContext* context,
    const proto::HybridSearchRequest* request,
    proto::HybridSearchResponse* response) {

  auto* client = GetDataNodeClientForCollection(request->collection_name());
  if (!client) {
    int shard = data_node_counter_.fetch_add(1, std::memory_order_relaxed);
    client = GetDataNodeClient(shard);
  }
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
    int shard = data_node_counter_.fetch_add(1, std::memory_order_relaxed);
    client = GetDataNodeClient(shard);
  }
  if (!client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
        "No query node or data node available");
  }

  proto::SearchRequest internal_req = *request;
  grpc::ClientContext client_ctx;
  return client->Search(&client_ctx, internal_req, response);
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