# Tests

```bash
# All tests of the dev build, eight at a time
ctest --preset dev --parallel 8

# One kind of test
ctest --preset dev -L unit
ctest --preset dev -L integration

# The unit tests of one module, or one test case
ctest --preset dev -R '^storage_tests:'
ctest --preset dev -R '^storage_tests:rejects unset target$'
```

CTest runs each unit test case as its own test, named `<module>_tests:<test case>`. An integration test binary is one CTest test, named after the binary. The tests are isolated: each test writes to its own temp folder and starts its servers on free ports, so CTest can run them in parallel. `make test` does that.

## Shared helpers (`support/`)

Header-only helpers that every test links (`#include "support/<file>.h"`, namespace `gvdb::test`):

| File | Provides |
|------|----------|
| `temp_dir.h` | `MakeUniqueTempDir`, `ScopedTempDir`: a temp folder that no other test or process uses |
| `wait.h` | `WaitFor`: polls a condition with a timeout, instead of a fixed sleep |
| `grpc_server.h` | `TestGrpcServer`: an in-process gRPC server on a free port |
| `vectors.h` | `RandomVectors` (seeded) and `SequentialIds` |

## Unit (`unit/`)

C++ / doctest. One folder per module of `src/`, and each folder builds one binary, `<module>_tests`. The test files of a folder share that binary, so each file keeps its helpers and fixtures in an anonymous namespace.

A contract is a set of cases that every implementation of an interface must pass. A contract file defines each case once with `TEST_CASE_TEMPLATE` and runs it against one fixture per implementation. The test name ends with the implementation, for example `SegmentStore contract: DropSegment removes the segment<TieredSegmentManager>`. Behavior that only one implementation has stays in a plain `TEST_CASE`.

| Folder | Files | Covers |
|--------|-------|--------|
| `core/` | `core_test.cpp` | Types, vectors, status, metadata, filters |
| `common/` | `common_test.cpp` | Logger, thread pool, timer, file helpers |
| `config/` | `config_test.cpp` | YAML configuration |
| `auth/` | `rbac_test.cpp` | API keys, roles, permissions |
| `index/` | `index_test.cpp`, `sparse_index_test.cpp`, `turboquant_test.cpp` | FLAT, HNSW, IVF, sparse and TurboQuant indexes |
| `storage/` | `storage_test.cpp`, `object_store_test.cpp`, `segment_cache_test.cpp`, `segment_manifest_test.cpp`, `tiered_segment_manager_test.cpp`, `segment_store_contract_test.cpp`, `bulk_import_test.cpp`, `backup_manifest_test.cpp`, `backup_manager_test.cpp` | Segment lifecycle and persistence, object store contract (in-memory, filesystem), `CreateObjectStore`, segment cache, tiered storage, segment store contract (`SegmentManager`, `TieredSegmentManager`), bulk import, backup and restore engines |
| `compute/` | `compute_test.cpp` | Parallel query execution |
| `cluster/` | `cluster_simple_test.cpp`, `node_registry_test.cpp`, `coordinator_backup_test.cpp`, `internal_client_test.cpp`, `data_node_test.cpp`, `query_node_test.cpp`, `primary_term_tracker_test.cpp`, `shard_write_gate_test.cpp` | ShardManager, Coordinator, node registry, backup fan-out, gRPC client, data and query nodes, primary terms, write fence |
| `consensus/` | `consensus_test.cpp`, `consensus_persistence_test.cpp` | Timestamp oracle, Raft peer parsing, single-node Raft, RocksDB log store |
| `rpc/` | `auth_processor_test.cpp`, `audit_interceptor_test.cpp`, `replica_fallback_test.cpp`, `dns_channel_args_test.cpp` | API key authentication by RPC and collection, audit log, replica fallback, DNS channel arguments |
| `service/` | `network_test.cpp`, `vectordb_service_*_test.cpp`, `collection_resolver_test.cpp`, `collection_metadata_cache_test.cpp`, `internal_service_*_test.cpp`, `proxy_service_test.cpp` | Proto conversions, VectorDBService in single-node, distributed and coordinator mode (collection operations shared by single-node and coordinator mode in `vectordb_service_modes_test.cpp`), resolver contract (local, coordinator), metadata cache, InternalService RPCs, proxy routing |
| `server/` | `server_flags_test.cpp` | Flag table of the server programs: values, lists, environment variables, usage text |

Four binaries stay outside this scheme, because they need special hardware or a service and run as one CTest test each:

| Binary | File | Runs when |
|--------|------|-----------|
| `cuda_smoke_tests` | `index/cuda_smoke_test.cpp` | The build has CUDA (label `gpu`) |
| `metal_distance_tests` | `index/metal_distance_test.cpp` | The build has Metal (label `gpu`) |
| `object_store_s3_tests` | `storage/object_store_s3_test.cpp` | `GVDB_TEST_S3_ENDPOINT` is set, see `make test-s3` |
| `object_store_gcs_tests` | `storage/object_store_gcs_test.cpp` | `GVDB_TEST_GCS_ENDPOINT` is set |

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
