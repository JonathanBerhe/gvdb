// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "compute/query_executor.h"
#include "core/types.h"
#include "rpc/primary_term_header.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/segment_locator.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_store.h"
#include "vectordb.pb.h"

namespace gvdb {
namespace cluster {
class PrimaryTermTracker;
class ShardWriteGate;
}  // namespace cluster

namespace service {

// The write side of VectorDBService: the write gate, the writes into the
// segments of a collection, and the invalidation of the query cache. The
// handlers check the permission and the request shape first, then call
// these functions. The functions take proto requests but no gRPC context,
// so tests call them without a server.
//
// The resolver and the locator must outlive the write path. The tracker and
// the freeze gate are optional; VectorDBServiceOptions describes them.
class WritePath {
 public:
  WritePath(std::shared_ptr<storage::ISegmentStore> segment_store,
            std::shared_ptr<compute::QueryExecutor> query_executor, ICollectionResolver& resolver,
            SegmentLocator& locator, cluster::PrimaryTermTracker* primary_term_tracker,
            cluster::ShardWriteGate* shard_write_gate);

  // The per-write gate. `sample_vector_id` picks the shard with the hash
  // that the proxy uses (core::ShardIndexForVector).
  //
  //   - No tracker and no freeze gate (single node, query node, tests): OK.
  //   - The resolver has no segment for the collection: OK, so that the
  //     handler returns its own error.
  //   - The shard is frozen for a backup: RESOURCE_EXHAUSTED.
  //   - No tracker: OK.
  //   - No term header (an older client): OK.
  //   - A malformed term header: INVALID_ARGUMENT.
  //   - A stale term, or this node is not the primary: ABORTED.
  //   - The tracker does not know the shard: FAILED_PRECONDITION.
  [[nodiscard]] grpc::Status CheckGate(const rpc::PrimaryTermFromHeader& header,
                                       const std::string& collection_name,
                                       uint64_t sample_vector_id) const;

  // Groups the vectors of `request` by shard, splits each shard batch into
  // ranges that fit an empty segment, and writes each range to a writable
  // segment of the collection. Sets `inserted` to the number of written
  // vectors.
  [[nodiscard]] grpc::Status Insert(const proto::InsertRequest& request, size_t* inserted);

  // Writes one message of a StreamInsert to the shard-primary segments, with
  // metadata but without sparse vectors, TTL or the batch split. Adds the
  // written count to `inserted`. `metrics_collection` names the collection
  // in the failure metric.
  [[nodiscard]] grpc::Status StreamInsertChunk(const proto::InsertRequest& chunk,
                                               const std::string& metrics_collection,
                                               uint64_t* inserted);

  // Upserts the vectors of `request` into the first writable shard-primary
  // segment.
  [[nodiscard]] grpc::Status Upsert(const proto::UpsertRequest& request,
                                    storage::Segment::UpsertResult* result);

  struct DeleteOutcome {
    uint64_t deleted_count = 0;
    std::vector<core::VectorId> not_found_ids;
  };

  // Deletes each id from the segment of the collection that holds it.
  [[nodiscard]] grpc::Status Delete(const proto::DeleteRequest& request, DeleteOutcome* outcome);

  // Replaces or merges the metadata of one vector in the segment that holds
  // it. When no segment holds the id, or the segment rejects the update, it
  // sets `updated` false and the error text in `response`.
  [[nodiscard]] grpc::Status UpdateMetadata(const proto::UpdateMetadataRequest& request,
                                            proto::UpdateMetadataResponse* response);

  // Drops the cached query results of the collection.
  void InvalidateCache(const std::string& collection_name) const;

 private:
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  ICollectionResolver& resolver_;
  SegmentLocator& locator_;
  cluster::PrimaryTermTracker* primary_term_tracker_;
  cluster::ShardWriteGate* shard_write_gate_;
};

}  // namespace service
}  // namespace gvdb
