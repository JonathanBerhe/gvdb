// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Integration test: coordinator creates collection -> data node serves Insert/Search

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <initializer_list>
#include <memory>
#include <string>

#include "cluster/coordinator/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/coordinator/node_registry.h"
#include "cluster/coordinator/internal_client.h"
#include "cluster/datanode/primary_term_tracker.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "service/internal/internal_service.h"
#include "service/vectordb/vectordb_service.h"
#include "service/vectordb/collection_resolver.h"
#include "storage/segment/segment_manager.h"
#include "internal.grpc.pb.h"
#include "support/temp_dir.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace test {

namespace {

// Starts a server on a free local port with the given services and the
// 256 MB message limits of the production servers. Sets *address to
// "localhost:<port>".
std::unique_ptr<grpc::Server> StartServer(
    std::initializer_list<grpc::Service*> services, std::string* address) {
  grpc::ServerBuilder builder;
  int port = 0;
  builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
  for (auto* service : services) {
    builder.RegisterService(service);
  }
  builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
  builder.SetMaxSendMessageSize(256 * 1024 * 1024);
  auto server = builder.BuildAndStart();
  REQUIRE_NE(server, nullptr);
  REQUIRE_GT(port, 0);
  *address = "localhost:" + std::to_string(port);
  return server;
}

void ShutdownServer(std::unique_ptr<grpc::Server>& server) {
  if (server) {
    server->Shutdown();
    server->Wait();
    server.reset();
  }
}

}  // namespace

class DistributedDataNodeTest {
 public:
  DistributedDataNodeTest() {
    index_factory_ = std::make_unique<index::IndexFactory>();

    // --- Coordinator state ---
    shard_manager_ = std::make_shared<cluster::ShardManager>(8, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30));

    coord_segment_store_ = std::make_shared<storage::SegmentManager>(
        (dir_ / "coordinator").string(), index_factory_.get());
    coord_query_executor_ = std::make_shared<compute::QueryExecutor>(
        coord_segment_store_.get());

    // --- Data node setup (separate storage) ---
    // The data node starts first, so that the coordinator finds its address
    // in the NodeRegistry and sends it CreateSegment RPCs.
    dn_segment_store_ = std::make_shared<storage::SegmentManager>(
        (dir_ / "data_node").string(), index_factory_.get());
    dn_query_executor_ = std::make_shared<compute::QueryExecutor>(
        dn_segment_store_.get());
    auto dn_shard_manager = std::make_shared<cluster::ShardManager>(
        8, cluster::ShardingStrategy::HASH);
    dn_internal_service_ = std::make_unique<service::InternalService>(
        dn_shard_manager, dn_segment_store_, dn_query_executor_);
    dn_server_ = StartServer({dn_internal_service_.get()}, &dn_server_address_);

    // Register data node in NodeRegistry (simulates heartbeat)
    RegisterDataNode(dn_server_address_);

    // Create coordinator with client factory that can reach data node
    auto client_factory = std::make_shared<cluster::GrpcInternalServiceClientFactory>();
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_, client_factory);

    // Create coordinator's InternalService (serves GetCollectionMetadata)
    coord_internal_service_ = std::make_unique<service::InternalService>(
        shard_manager_, coord_segment_store_, coord_query_executor_,
        node_registry_, coordinator_);

    // Create coordinator's VectorDBService
    auto coord_resolver = service::MakeCoordinatorResolver(coordinator_);
    coord_vectordb_service_ = std::make_unique<service::VectorDBService>(
        coord_segment_store_, coord_query_executor_, std::move(coord_resolver));

    // Start coordinator gRPC server (InternalService + VectorDBService)
    coord_server_ = StartServer(
        {coord_internal_service_.get(), coord_vectordb_service_.get()},
        &coord_server_address_);

    // Create data node VectorDBService pointing at coordinator
    auto dn_resolver = service::MakeCachedCoordinatorResolver(coord_server_address_);
    dn_vectordb_service_ = std::make_unique<service::VectorDBService>(
        dn_segment_store_, dn_query_executor_, std::move(dn_resolver));
  }

  ~DistributedDataNodeTest() {
    // Close every client channel before the server that it dials. A
    // graceful server shutdown waits until each open client connection
    // answers the server's GOAWAY, and on macOS an idle in-process channel
    // answers only at gRPC's next client backup poll (every 5 s).
    dn_vectordb_service_.reset();  // Its resolver dials the coordinator.
    ShutdownServer(coord_server_);
    coord_vectordb_service_.reset();
    coord_internal_service_.reset();
    coordinator_.reset();  // Its clients dial the data nodes.
    ShutdownServer(dn_server_);
    ShutdownServer(dn2_server_);
  }

  // Starts a second data node with its own storage and returns its
  // address. The fixture owns it, so that the destructor can close the
  // coordinator's channel to it before it shuts the server down.
  std::string StartSecondDataNode(cluster::PrimaryTermTracker* tracker = nullptr) {
    dn2_segment_store_ = std::make_shared<storage::SegmentManager>(
        (dir_ / "data_node_2").string(), index_factory_.get());
    dn2_query_executor_ = std::make_shared<compute::QueryExecutor>(
        dn2_segment_store_.get());
    auto dn2_shard_manager = std::make_shared<cluster::ShardManager>(
        8, cluster::ShardingStrategy::HASH);
    dn2_internal_service_ = std::make_unique<service::InternalService>(
        dn2_shard_manager, dn2_segment_store_, dn2_query_executor_);
    if (tracker != nullptr) {
      dn2_internal_service_->SetPrimaryTermTracker(tracker);
    }
    std::string address;
    dn2_server_ = StartServer({dn2_internal_service_.get()}, &address);
    return address;
  }

 private:
  void RegisterDataNode(const std::string& address) {
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(100);
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node.set_grpc_address(address);
    proto_node.set_memory_total_bytes(8ULL * 1024 * 1024 * 1024);
    proto_node.set_disk_total_bytes(100ULL * 1024 * 1024 * 1024);
    node_registry_->UpdateNode(proto_node);
  }

 public:
  ScopedTempDir dir_{"gvdb_distributed_dn"};
  std::unique_ptr<index::IndexFactory> index_factory_;

  // Coordinator components
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
  std::shared_ptr<storage::ISegmentStore> coord_segment_store_;
  std::shared_ptr<compute::QueryExecutor> coord_query_executor_;
  std::unique_ptr<service::InternalService> coord_internal_service_;
  std::unique_ptr<service::VectorDBService> coord_vectordb_service_;
  std::unique_ptr<grpc::Server> coord_server_;
  std::string coord_server_address_;

  // Data node components
  std::shared_ptr<storage::ISegmentStore> dn_segment_store_;
  std::shared_ptr<compute::QueryExecutor> dn_query_executor_;
  std::unique_ptr<service::InternalService> dn_internal_service_;
  std::unique_ptr<service::VectorDBService> dn_vectordb_service_;
  std::unique_ptr<grpc::Server> dn_server_;
  std::string dn_server_address_;

  // Second data node, started by StartSecondDataNode()
  cluster::PrimaryTermTracker dn2_tracker_;
  std::shared_ptr<storage::ISegmentStore> dn2_segment_store_;
  std::shared_ptr<compute::QueryExecutor> dn2_query_executor_;
  std::unique_ptr<service::InternalService> dn2_internal_service_;
  std::unique_ptr<grpc::Server> dn2_server_;
};

// Test: Coordinator creates collection -> segment appears on data node
TEST_CASE_FIXTURE(DistributedDataNodeTest, "CoordinatorCreatesSegmentOnDataNode") {
  // Create collection via coordinator
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_distributed");
  create_req.set_dimension(128);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx;
  auto status = coord_vectordb_service_->CreateCollection(&ctx, &create_req, &create_resp);
  INFO("CreateCollection failed: " << status.error_message());
  REQUIRE(status.ok());
  CHECK_GT(create_resp.collection_id(), 0);

  uint32_t collection_id = create_resp.collection_id();

  // Verify segment was created on data node (shard 0)
  auto segment_id = core::ShardSegmentId(core::CollectionId(collection_id), 0);
  auto* segment = dn_segment_store_->GetSegment(segment_id);
  INFO("Segment not found on data node after CreateCollection");
  REQUIRE_NE(segment, nullptr);
  CHECK_EQ(segment->GetDimension(), 128);
}

// Test: Create and drop a FLAT collection, then create and drop an IVF_PQ
// collection, all through the coordinator. The coordinator sends the index
// type to the data node as a string, so the IVF_PQ segment on the data node
// shows that the type survives the CreateSegment RPC.
TEST_CASE_FIXTURE(DistributedDataNodeTest, "CoordinatorCreatesIvfPqAfterFlatDrop") {
  // Create a FLAT collection.
  proto::CreateCollectionRequest flat_req;
  flat_req.set_collection_name("test_flat");
  flat_req.set_dimension(384);
  flat_req.set_metric(proto::CreateCollectionRequest::L2);
  flat_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse flat_resp;
  grpc::ServerContext ctx1;
  auto flat_status = coord_vectordb_service_->CreateCollection(&ctx1, &flat_req, &flat_resp);
  INFO("FLAT CreateCollection failed: " << flat_status.error_message());
  REQUIRE(flat_status.ok());

  auto flat_segment_id = core::ShardSegmentId(
      core::CollectionId(flat_resp.collection_id()), 0);
  auto* flat_segment = dn_segment_store_->GetSegment(flat_segment_id);
  REQUIRE_NE(flat_segment, nullptr);
  CHECK_EQ(flat_segment->GetDimension(), 384);
  CHECK_EQ(flat_segment->GetIndexType(), core::IndexType::FLAT);

  // Drop it. The coordinator deletes the shard segment on the data node
  // before DropCollection returns.
  proto::DropCollectionRequest drop_flat_req;
  drop_flat_req.set_collection_name("test_flat");
  proto::DropCollectionResponse drop_flat_resp;
  grpc::ServerContext ctx2;
  auto drop_flat_status =
      coord_vectordb_service_->DropCollection(&ctx2, &drop_flat_req, &drop_flat_resp);
  INFO("FLAT DropCollection failed: " << drop_flat_status.error_message());
  REQUIRE(drop_flat_status.ok());
  CHECK_EQ(dn_segment_store_->GetSegment(flat_segment_id), nullptr);

  // Create an IVF_PQ collection with the same dimension.
  proto::CreateCollectionRequest ivf_req;
  ivf_req.set_collection_name("test_ivf_pq");
  ivf_req.set_dimension(384);
  ivf_req.set_metric(proto::CreateCollectionRequest::L2);
  ivf_req.set_index_type(proto::CreateCollectionRequest::IVF_PQ);

  proto::CreateCollectionResponse ivf_resp;
  grpc::ServerContext ctx3;
  auto ivf_status = coord_vectordb_service_->CreateCollection(&ctx3, &ivf_req, &ivf_resp);
  INFO("IVF_PQ CreateCollection failed: " << ivf_status.error_message());
  REQUIRE(ivf_status.ok());
  CHECK_NE(ivf_resp.collection_id(), flat_resp.collection_id());

  auto ivf_segment_id = core::ShardSegmentId(
      core::CollectionId(ivf_resp.collection_id()), 0);
  auto* ivf_segment = dn_segment_store_->GetSegment(ivf_segment_id);
  INFO("IVF_PQ segment not found on data node after CreateCollection");
  REQUIRE_NE(ivf_segment, nullptr);
  CHECK_EQ(ivf_segment->GetDimension(), 384);
  CHECK_EQ(ivf_segment->GetMetric(), core::MetricType::L2);
  CHECK_EQ(ivf_segment->GetIndexType(), core::IndexType::IVF_PQ);

  // Drop the IVF_PQ collection.
  proto::DropCollectionRequest drop_ivf_req;
  drop_ivf_req.set_collection_name("test_ivf_pq");
  proto::DropCollectionResponse drop_ivf_resp;
  grpc::ServerContext ctx4;
  auto drop_ivf_status =
      coord_vectordb_service_->DropCollection(&ctx4, &drop_ivf_req, &drop_ivf_resp);
  INFO("IVF_PQ DropCollection failed: " << drop_ivf_status.error_message());
  REQUIRE(drop_ivf_status.ok());
  CHECK_EQ(dn_segment_store_->GetSegment(ivf_segment_id), nullptr);
}

// Test: Insert vectors on data node via VectorDBService
TEST_CASE_FIXTURE(DistributedDataNodeTest, "InsertVectorsOnDataNode") {
  // Create collection via coordinator
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_insert");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx1;
  auto create_status = coord_vectordb_service_->CreateCollection(&ctx1, &create_req, &create_resp);
  INFO(create_status.error_message());
  REQUIRE(create_status.ok());

  // Insert vectors via data node's VectorDBService
  proto::InsertRequest insert_req;
  insert_req.set_collection_name("test_insert");

  for (int i = 1; i <= 5; ++i) {
    auto* vec = insert_req.add_vectors();
    vec->set_id(i);
    auto* v = vec->mutable_vector();
    v->set_dimension(4);
    v->add_values(static_cast<float>(i));
    v->add_values(static_cast<float>(i * 2));
    v->add_values(static_cast<float>(i * 3));
    v->add_values(static_cast<float>(i * 4));
  }

  proto::InsertResponse insert_resp;
  grpc::ServerContext ctx2;
  auto insert_status = dn_vectordb_service_->Insert(&ctx2, &insert_req, &insert_resp);
  INFO("Insert failed: " << insert_status.error_message());
  REQUIRE(insert_status.ok());
  CHECK_EQ(insert_resp.inserted_count(), 5);
}

// Test: Search on data node returns inserted vectors
TEST_CASE_FIXTURE(DistributedDataNodeTest, "SearchOnDataNode") {
  // Create collection
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_search");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx1;
  REQUIRE(coord_vectordb_service_->CreateCollection(&ctx1, &create_req, &create_resp).ok());

  // Insert vectors
  proto::InsertRequest insert_req;
  insert_req.set_collection_name("test_search");

  for (int i = 1; i <= 10; ++i) {
    auto* vec = insert_req.add_vectors();
    vec->set_id(i);
    auto* v = vec->mutable_vector();
    v->set_dimension(4);
    v->add_values(static_cast<float>(i));
    v->add_values(0.0f);
    v->add_values(0.0f);
    v->add_values(0.0f);
  }

  proto::InsertResponse insert_resp;
  grpc::ServerContext ctx2;
  REQUIRE(dn_vectordb_service_->Insert(&ctx2, &insert_req, &insert_resp).ok());

  // Search for vector closest to [1, 0, 0, 0]
  proto::SearchRequest search_req;
  search_req.set_collection_name("test_search");
  search_req.set_top_k(3);
  auto* query = search_req.mutable_query_vector();
  query->set_dimension(4);
  query->add_values(1.0f);
  query->add_values(0.0f);
  query->add_values(0.0f);
  query->add_values(0.0f);

  proto::SearchResponse search_resp;
  grpc::ServerContext ctx3;
  auto search_status = dn_vectordb_service_->Search(&ctx3, &search_req, &search_resp);
  INFO("Search failed: " << search_status.error_message());
  REQUIRE(search_status.ok());
  CHECK_EQ(search_resp.results_size(), 3);

  // First result should be vector 1 (exact match)
  CHECK_EQ(search_resp.results(0).id(), 1);
  CHECK(search_resp.results(0).distance() == doctest::Approx(0.0f).epsilon(0.001f));
}

// Test: Get vectors by ID on data node
TEST_CASE_FIXTURE(DistributedDataNodeTest, "GetVectorsOnDataNode") {
  // Create collection + insert
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_get");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx1;
  REQUIRE(coord_vectordb_service_->CreateCollection(&ctx1, &create_req, &create_resp).ok());

  proto::InsertRequest insert_req;
  insert_req.set_collection_name("test_get");
  auto* vec = insert_req.add_vectors();
  vec->set_id(42);
  auto* v = vec->mutable_vector();
  v->set_dimension(4);
  v->add_values(1.0f);
  v->add_values(2.0f);
  v->add_values(3.0f);
  v->add_values(4.0f);

  proto::InsertResponse insert_resp;
  grpc::ServerContext ctx2;
  REQUIRE(dn_vectordb_service_->Insert(&ctx2, &insert_req, &insert_resp).ok());

  // Get vector by ID
  proto::GetRequest get_req;
  get_req.set_collection_name("test_get");
  get_req.add_ids(42);

  proto::GetResponse get_resp;
  grpc::ServerContext ctx3;
  auto get_status = dn_vectordb_service_->Get(&ctx3, &get_req, &get_resp);
  INFO("Get failed: " << get_status.error_message());
  REQUIRE(get_status.ok());
  CHECK_EQ(get_resp.vectors_size(), 1);
  CHECK_EQ(get_resp.vectors(0).id(), 42);
}

// TODO: Rewrite for multi-shard -- needs separate fixture with 2 data nodes
TEST_CASE_FIXTURE(DistributedDataNodeTest, "DistributedSearchAcrossDataNodes" * doctest::skip(true)) {
  // Create collection via coordinator
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_distributed_search");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx1;
  REQUIRE(coord_vectordb_service_->CreateCollection(&ctx1, &create_req, &create_resp).ok());

  // Insert vectors on data node (directly via dn_vectordb_service_)
  proto::InsertRequest insert_req;
  insert_req.set_collection_name("test_distributed_search");

  // Vectors: [1,0,0,0], [2,0,0,0], ..., [5,0,0,0]
  for (int i = 1; i <= 5; ++i) {
    auto* vec = insert_req.add_vectors();
    vec->set_id(i);
    auto* v = vec->mutable_vector();
    v->set_dimension(4);
    v->add_values(static_cast<float>(i));
    v->add_values(0.0f);
    v->add_values(0.0f);
    v->add_values(0.0f);
  }

  proto::InsertResponse insert_resp;
  grpc::ServerContext ctx2;
  REQUIRE(dn_vectordb_service_->Insert(&ctx2, &insert_req, &insert_resp).ok());
  CHECK_EQ(insert_resp.inserted_count(), 5);

  // Now create a "query node" VectorDBService that has NO local data
  // It should fan out to data nodes via distributed search
  auto qn_segment_manager = std::make_shared<storage::SegmentManager>(
      (dir_ / "query_node").string(), index_factory_.get());
  auto qn_query_executor = std::make_shared<compute::QueryExecutor>(
      qn_segment_manager.get());
  auto qn_resolver = service::MakeCachedCoordinatorResolver(coord_server_address_);
  auto qn_service = std::make_unique<service::VectorDBService>(
      qn_segment_manager, qn_query_executor, std::move(qn_resolver));

  // Search from query node -- should trigger distributed fan-out to data node
  proto::SearchRequest search_req;
  search_req.set_collection_name("test_distributed_search");
  search_req.set_top_k(3);
  auto* query = search_req.mutable_query_vector();
  query->set_dimension(4);
  query->add_values(1.0f);
  query->add_values(0.0f);
  query->add_values(0.0f);
  query->add_values(0.0f);

  proto::SearchResponse search_resp;
  grpc::ServerContext ctx3;
  auto search_status = qn_service->Search(&ctx3, &search_req, &search_resp);
  INFO("Distributed search failed: " << search_status.error_message());
  REQUIRE(search_status.ok());

  // Should get results from the data node
  CHECK_GT(search_resp.results_size(), 0);
  CHECK_LE(search_resp.results_size(), 3);

  // First result should be vector 1 (closest to query [1,0,0,0])
  CHECK_EQ(search_resp.results(0).id(), 1);
  CHECK(search_resp.results(0).distance() == doctest::Approx(0.0f).epsilon(0.001f));
}

// Test: Coordinator replicates segment data between nodes
TEST_CASE_FIXTURE(DistributedDataNodeTest, "ReplicateSegmentBetweenNodes") {
  // Create collection (segment on data node)
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_replication");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx1;
  REQUIRE(coord_vectordb_service_->CreateCollection(&ctx1, &create_req, &create_resp).ok());
  uint32_t collection_id = create_resp.collection_id();

  // Insert vectors on data node
  proto::InsertRequest insert_req;
  insert_req.set_collection_name("test_replication");
  for (int i = 1; i <= 3; ++i) {
    auto* vec = insert_req.add_vectors();
    vec->set_id(i);
    auto* v = vec->mutable_vector();
    v->set_dimension(4);
    v->add_values(static_cast<float>(i));
    v->add_values(0.0f);
    v->add_values(0.0f);
    v->add_values(0.0f);
  }

  proto::InsertResponse insert_resp;
  grpc::ServerContext ctx2;
  REQUIRE(dn_vectordb_service_->Insert(&ctx2, &insert_req, &insert_resp).ok());

  // Start a second data node
  std::string dn2_address = StartSecondDataNode();

  // Register second data node
  proto::internal::NodeInfo proto_node2;
  proto_node2.set_node_id(200);
  proto_node2.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_node2.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_node2.set_grpc_address(dn2_address);
  node_registry_->UpdateNode(proto_node2);

  // Replicate segment from data node 1 to data node 2
  core::SegmentId seg_id = core::ShardSegmentId(core::CollectionId(collection_id), 0);
  core::NodeId source = core::MakeNodeId(100);
  core::NodeId target = core::MakeNodeId(200);

  auto status = coordinator_->ReplicateSegmentData(seg_id, source, target);
  INFO("Replication failed: " << status.message());
  REQUIRE(status.ok());

  // Verify data node 2 now has the segment with vectors
  auto* replicated_segment = dn2_segment_store_->GetSegment(seg_id);
  INFO("Replicated segment not found on node 2");
  REQUIRE_NE(replicated_segment, nullptr);
  CHECK_EQ(replicated_segment->GetVectorCount(), 3);
}

// Test: Coordinator handles failed node by promoting replica
TEST_CASE_FIXTURE(DistributedDataNodeTest, "FailoverPromotion") {
  // Stand up a real second data node so PreparePromote during failover
  // has a server to talk to. Mirrors ReplicateSegmentBetweenNodes.
  std::string dn2_address = StartSecondDataNode(&dn2_tracker_);

  proto::internal::NodeInfo proto_node2;
  proto_node2.set_node_id(200);
  proto_node2.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_node2.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_node2.set_grpc_address(dn2_address);
  node_registry_->UpdateNode(proto_node2);

  // Assign shard with node 100 as primary, node 200 as replica
  core::ShardId shard = core::MakeShardId(1);
  core::NodeId primary = core::MakeNodeId(100);
  core::NodeId replica = core::MakeNodeId(200);

  REQUIRE(shard_manager_->SetPrimaryNode(shard, primary).ok());
  REQUIRE(shard_manager_->AddReplica(shard, replica).ok());

  // Verify primary is node 100
  auto primary_before = shard_manager_->GetPrimaryNode(shard);
  REQUIRE(primary_before.ok());
  CHECK_EQ(*primary_before, primary);

  // Simulate node 100 failure -- remove from registry
  node_registry_->RemoveNode(100);

  // Trigger failover
  coordinator_->HandleFailedNode(primary);

  // Verify replica (node 200) was promoted to primary
  auto primary_after = shard_manager_->GetPrimaryNode(shard);
  REQUIRE(primary_after.ok());
  CHECK_EQ(*primary_after, replica);
}

}  // namespace test
}  // namespace gvdb
