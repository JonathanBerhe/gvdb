# Modules

The C++ codebase is organized by **architectural layer**. Each module exposes a public API via `include/<module>/` and keeps implementation details in `src/<module>/`.

## Dependency hierarchy

```
Layer 0 — Foundation
  core/    → (no dependencies)

Layer 1 — Infrastructure
  utils/    → core
  consensus/→ core
  auth/     → core, utils
  index/    → core
  network/  → core, auth

Layer 2 — Storage & Compute
  storage/  → core, index
  compute/  → core, index

Layer 3 — Orchestration
  cluster/  → core, consensus, storage, compute, network
```

Circular dependencies are forbidden. Each module is compiled and tested independently.

---

## core

**Foundation layer.** System-wide types, error handling, vector data structures.

- `core/types.h` — `VectorId`, `CollectionId`, `SegmentId`, `ShardId`
- `core/status.h` — `absl::Status` / `absl::StatusOr` aliases
- `core/vector.h` — SIMD-aligned vector storage
- `IVectorIndex` — abstract interface every index implements
- `IStorage` — abstract interface every storage backend implements

No external deps except abseil-cpp.

---

## index

Vector index implementations wrapping Faiss plus GVDB-native indexes.

- **Dense indexes**: FLAT, HNSW, IVF_FLAT, IVF_PQ, IVF_SQ, TurboQuant, IVF_TURBOQUANT
- `IndexFactory` — chooses the right impl per config; transparently returns `MetalFlatIndex` on macOS with `-DGVDB_WITH_METAL=ON`
- `IndexManager` — lifecycle + memory budget
- **SIMD**: runtime CPU detection (SSE, AVX2, AVX-512)
- **Metal** (macOS): `src/index/metal/` with MSL kernels dispatched via metal-cpp. Objective-C++ stays isolated to this directory.

See [vector search](../features/vector-search.md).

---

## storage

Segment management and persistence.

- `ISegmentStore` — 22-method abstract interface; all consumers use this
- `SegmentManager` — local-disk-only implementation
- `TieredSegmentManager` — composes `SegmentManager` + `IObjectStore` + `SegmentCache`; used when S3/MinIO is configured
- `IObjectStore` — abstract for S3, MinIO, GCS, Azure Blob
- `S3ObjectStore` — AWS SDK implementation (`-DGVDB_WITH_S3=ON`)
- `SegmentCache` — LRU disk cache for segments downloaded from S3
- `SegmentManifest` — JSON manifest of segments in object storage

See [tiered storage](../features/tiered-storage.md) and [architecture — storage](storage.md).

---

## compute

Query execution and result merging.

- Distributed top-k merging across shards
- BM25 / RRF fusion for hybrid search
- Metadata predicate evaluator
- LRU result cache

See [hybrid search](../features/hybrid-search.md).

---

## consensus

Raft consensus via **NuRaft** (eBay's Raft implementation).

- Leader election
- Metadata state machine (collections, shard assignments)
- Timestamp oracle (TSO) for total ordering
- Persistent log: RocksDB

See [architecture — consensus](consensus.md).

---

## cluster

Distributed coordination — sits on top of everything else.

- **Coordinator**: shard assignment, heartbeat, replication, rebalancing
- **Data node**: primary/replica lifecycle, segment replication
- **Query node**: fan-out + merge
- **Proxy**: client routing

Key files:

- `src/cluster/coordinator.cpp`
- `src/cluster/shard_manager.cpp`
- `src/cluster/replication.cpp`

---

## network

gRPC services and interceptors.

- **Service definitions**: `proto/vectordb.proto`
- **Auth interceptor**: sets thread-local `AuthContext`
- **Audit interceptor**: structured JSON audit trail, separate from auth
- **Rate limiting**, **metrics**, **tracing** hooks

See [RBAC](../features/rbac.md) and [security](../operations/security.md).

---

## auth

RBAC and authentication.

- `RbacStore` — thread-safe API key → role mapping
- `AuthContext` — thread-local for current request's API key
- Permission matrix (`HasPermission`, `HasCollectionAccess`)
- Roles: `admin`, `readwrite`, `readonly`, `collection_admin`

See [RBAC](../features/rbac.md).

---

## utils

Thread pool, structured logging (spdlog), config loader (yaml-cpp), audit logger, checksum helpers (xxHash64).

---

## main

Entry points for each binary:

- `main/coordinator_main.cpp`
- `main/data_node_main.cpp`
- `main/query_node_main.cpp`
- `main/proxy_main.cpp`
- `main/single_node_main.cpp`

## See also

- [Contributing](../contributing.md) — module-by-module contribution guide
- Full [`CLAUDE.md`](https://github.com/JonathanBerhe/gvdb/blob/main/CLAUDE.md) in the repo for the developer handbook
