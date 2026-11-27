# Tests

```bash
# All tests of the dev build, eight test binaries at a time
ctest --preset dev --parallel 8

# One kind of test
ctest --preset dev -L unit
ctest --preset dev -L integration

# Single suite (CTest names equal the executable names)
ctest --preset dev -R storage_tests
```

The test binaries are isolated: each test writes to its own temp folder and starts its servers on free ports, so CTest can run them in parallel. `make test` does that.

## Shared helpers (`support/`)

Header-only helpers that every test links (`#include "support/<file>.h"`, namespace `gvdb::test`):

| File | Provides |
|------|----------|
| `temp_dir.h` | `MakeUniqueTempDir`, `ScopedTempDir`: a temp folder that no other test or process uses |
| `wait.h` | `WaitFor`: polls a condition with a timeout, instead of a fixed sleep |
| `grpc_server.h` | `TestGrpcServer`: an in-process gRPC server on a free port |
| `vectors.h` | `RandomVectors` (seeded) and `SequentialIds` |

## Unit (`unit/`)

C++ / doctest. One file per module.

| File | Covers |
|------|--------|
| `core_test.cpp` | Types, vectors, status, metadata, filters |
| `index_test.cpp` | FLAT, HNSW, IVF index operations |
| `storage_test.cpp` | Segment lifecycle, persistence round-trip |
| `compute_test.cpp` | Parallel query execution |
| `utils_test.cpp` | Logger, thread pool, timer, YAML configuration |
| `network_test.cpp` | Proto conversions, VectorDBService CRUD |
| `cluster_simple_test.cpp` | ShardManager, Coordinator |
| `consensus_test.cpp` | Timestamp oracle, Raft peer parsing, single-node Raft |
| `consensus_persistence_test.cpp` | RocksDB log store |
| `proxy_service_test.cpp` | Proxy routing |
| `auth_processor_test.cpp` | API key authentication, role by RPC by collection matrix |
| `collection_metadata_cache_test.cpp` | Metadata cache |
| `internal_client_test.cpp` | gRPC client abstraction |
| `internal_service_metadata_test.cpp` | Metadata sync RPCs, in process and over gRPC |
| `vectordb_service_distributed_test.cpp` | Distributed mode with mock coordinator |
| `vectordb_service_coordinator_test.cpp` | Coordinator mode |
| `vectordb_service_write_test.cpp` | Upsert, StreamInsert over gRPC, TTL expiry |
| `vectordb_service_search_test.cpp` | RangeSearch, HybridSearch, sparse search, filters, index types and metrics, concurrent search |
| `vectordb_service_import_test.cpp` | BulkImport, GetImportStatus, CancelImport |
| `vectordb_service_backup_test.cpp` | Backup and restore in single-node mode (S3 and local targets) |
| `object_store_test.cpp` | Object store contract (in-memory, filesystem), CreateObjectStore |
| `object_store_s3_test.cpp` | Object store contract against S3 (needs `GVDB_TEST_S3_ENDPOINT`, see `make test-s3`) |
| `object_store_gcs_test.cpp` | Object store contract against a GCS emulator (needs `GVDB_TEST_GCS_ENDPOINT`) |
| `server_flags_test.cpp` | Flag table of the server programs: values, lists, environment variables, usage text |

## Integration (`integration/`)

C++ / doctest. Most files start in-process gRPC servers to test multi-component flows. `server_programs_test.cpp` starts the five server programs as real processes.

| File | Covers |
|------|--------|
| `raft_auto_join_test.cpp` | Multi-node Raft: bootstrap, join, scale down, leadership transfer |
| `distributed_data_node_test.cpp` | Coordinator to data node: create, insert, search, replicate, failover |
| `segment_replication_integration_test.cpp` | Cross-node segment replication, read repair |
| `proxy_integration_test.cpp` | Full flow through proxy |
| `backup_restore_integration_test.cpp` | Backup and restore over gRPC |
| `drain_read_fallback_test.cpp` | Proxy reads while a data node drains |
| `primary_swap_integration_test.cpp` | Upsert retries across a primary swap |
| `server_programs_test.cpp` | The five programs: `--help` and bad flags, startup, a collection round trip through the single node and through a cluster behind the proxy, exit 0 on SIGTERM |

## Python SDK (`clients/python/tests`)

pytest. The tests start the `gvdb-single-node` binary of the active preset and call it through the SDK. CI runs them on SDK and server changes.

```bash
make test-sdk
```
