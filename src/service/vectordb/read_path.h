// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <cstdint>
#include <memory>

#include "absl/status/statusor.h"
#include "compute/query_executor.h"
#include "core/vector.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/segment_locator.h"
#include "storage/segment/segment_store.h"
#include "vectordb.pb.h"

namespace gvdb::service {

// The canonical radius of a range search request: the largest distance of a
// result. L2 and COSINE take a positive `radius`. INNER_PRODUCT takes
// `similarity_threshold`, the smallest dot product, and no radius. Any other
// combination is an InvalidArgument error.
[[nodiscard]] absl::StatusOr<float> RangeSearchRadius(core::MetricType metric,
                                                      const proto::RangeSearchRequest& request);

// The read side of VectorDBService: search, range search, hybrid search, get
// and list. Search and range search run on the local segments, or fan out to
// the data nodes of the shards when this node has none of them. The handlers
// check the permission and the request shape first, then call these
// functions. The functions take proto requests but no gRPC context, so tests
// call them without a server.
//
// The resolver and the locator must outlive the read path.
class ReadPath {
 public:
  ReadPath(std::shared_ptr<storage::ISegmentStore> segment_store,
           std::shared_ptr<compute::QueryExecutor> query_executor, ICollectionResolver& resolver,
           SegmentLocator& locator);

  [[nodiscard]] grpc::Status Search(const proto::SearchRequest& request,
                                    proto::SearchResponse* response);
  [[nodiscard]] grpc::Status RangeSearch(const proto::RangeSearchRequest& request,
                                         proto::RangeSearchResponse* response);
  [[nodiscard]] grpc::Status HybridSearch(const proto::HybridSearchRequest& request,
                                          proto::HybridSearchResponse* response);
  [[nodiscard]] grpc::Status Get(const proto::GetRequest& request, proto::GetResponse* response);
  [[nodiscard]] grpc::Status ListVectors(const proto::ListVectorsRequest& request,
                                         proto::ListVectorsResponse* response);

  // The number of searches and range searches since start, and the summed
  // time of the searches. GetStats reports them.
  [[nodiscard]] uint64_t total_queries() const {
    return total_queries_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] uint64_t total_query_time_ms() const {
    return total_query_time_ms_.load(std::memory_order_relaxed);
  }

 private:
  // Fan out search to remote data nodes holding shards for the collection
  grpc::Status SearchDistributed(const proto::SearchRequest* request,
                                 proto::SearchResponse* response, const core::Vector& query);

  // Fan out range search to remote data nodes
  grpc::Status RangeSearchDistributed(const proto::RangeSearchRequest* request,
                                      proto::RangeSearchResponse* response);

  // InvalidArgument when the metric of the collection cannot search with the
  // query, for example a zero vector in a COSINE collection.
  [[nodiscard]] absl::Status CheckQueryVector(const std::string& collection_name,
                                              const core::Vector& query);

  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  ICollectionResolver& resolver_;
  SegmentLocator& locator_;

  std::atomic<uint64_t> total_queries_{0};
  std::atomic<uint64_t> total_query_time_ms_{0};
};

}  // namespace gvdb::service
