# Tests

```bash
# All tests
ctest --test-dir build --output-on-failure

# Single suite
ctest --test-dir build -R StorageTests
```

## Unit (`unit/`)

C++ / Google Test. One file per module.

| File | Covers |
|------|--------|
| `core_test.cpp` | Types, vectors, status, metadata, filters |
| `index_test.cpp` | FLAT, HNSW, IVF index operations |
| `storage_test.cpp` | Segment lifecycle, persistence round-trip |
| `compute_test.cpp` | Parallel query execution |
| `utils_test.cpp` | Logger, thread pool, timer |
| `network_test.cpp` | Proto conversions, VectorDBService CRUD |
| `cluster_simple_test.cpp` | ShardManager, Coordinator, LoadBalancer |
| `consensus_test.cpp` | Raft node, multi-node consensus |
| `consensus_persistence_test.cpp` | RocksDB log store |
| `proxy_service_test.cpp` | Proxy routing |
| `auth_processor_test.cpp` | API key authentication |
| `collection_metadata_cache_test.cpp` | Metadata cache |
| `internal_client_test.cpp` | gRPC client abstraction |
| `internal_service_metadata_test.cpp` | Metadata sync RPCs |
| `vectordb_service_distributed_test.cpp` | Distributed mode with mock coordinator |
| `vectordb_service_coordinator_test.cpp` | Coordinator mode |

## Integration (`integration/`)

C++ / Google Test. Spin up in-process gRPC servers to test multi-component flows.

| File | Covers |
|------|--------|
| `consensus_integration_test.cpp` | Multi-node Raft cluster |
| `metadata_sync_integration_test.cpp` | Coordinator to node metadata propagation |
| `segment_replication_integration_test.cpp` | Cross-node segment replication |
| `distributed_data_node_test.cpp` | Coordinator -> data node: create, insert, search, replicate, failover |
| `proxy_integration_test.cpp` | Full flow through proxy |

## E2E (`e2e/`)

Go tests against a running `gvdb-single-node` on `localhost:50051`.

| File | Covers |
|------|--------|
| `e2e.go` | Full CRUD workflow |
| `metadata.go` | Metadata filtering |
| `load.go` | 100K+ vectors, concurrent queries |
| `fuzzy.go` | Edge cases, error handling |
| `distributed_query.go` | Multi-node search |

```bash
./build/bin/gvdb-single-node --port 50051 --data-dir /tmp/gvdb-test &
cd test/e2e && go run e2e.go
```
