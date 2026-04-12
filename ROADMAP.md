# GVDB Roadmap

**Last Updated**: 2026-04-12
**Current Version**: v0.10.0

---

## Completed

### Core Engine
- 7 dense index types: FLAT, HNSW, IVF_FLAT, IVF_PQ, IVF_SQ, TurboQuant, IVF_TURBOQUANT
- Sparse vector support: inverted posting-list index, three-way hybrid retrieval (dense + sparse + BM25)
- Distance metrics: L2, Inner Product, Cosine
- Upsert: atomic insert-or-update with inserted/updated counts
- Range search: radius-based filtering with max_results
- Hybrid search: BM25 (Lucene IDF) + dense vector + RRF fusion
- Metadata filtering: SQL-like (=, !=, <, >, <=, >=, IN, NOT IN, LIKE, AND/OR/NOT)
- Scalar metadata indexes: per-field inverted index (bitmap) + sorted numeric index for range queries
- Streaming inserts: gRPC client-streaming (1.9x faster than unary, 189K vec/s at 128D)
- LRU query result cache: FNV-1a hash, collection-version invalidation, 377x speedup on cache hits
- WAL (16MB buffer, 1s sync), segment lifecycle (GROWING → SEALED → FLUSHED)
- Data node index building: auto-seal on segment size threshold, background build with priority queue
- Segment compaction: merge small segments, skip deleted vectors, background task
- Per-vector TTL: atomic insert+expiry, background sweep, query-time filtering, serialization-safe
- Auto-index selection: AUTO resolves per-segment at seal time (<10K→FLAT, 10K-1M→HNSW, ≥1M→IVF_TURBOQUANT)
- RBAC: 4 roles (admin, readwrite, readonly, collection_admin), per-collection scoping, YAML config, legacy api_keys backward compat

### Distributed Architecture
- Coordinator (Raft/NuRaft), Data Node, Query Node, Proxy — all node types operational
- Shard-aware routing, ExecuteShardQuery, multi-shard per collection (hash-based)
- Consistent hashing: virtual node ring (150 vnodes per shard)
- Segment replication over gRPC, auto-replication of under-replicated shards
- Node failure detection via heartbeats, replica promotion to primary
- Proxy routing: shard-aware insert, search fan-out with fallback to data nodes
- Persistence: segment flush/load survives restarts, index rebuilt from loaded vectors

### Security & Observability
- TLS/SSL: mutual TLS support, YAML config, backward compatible (defaults to insecure)
- API key auth: Bearer token gRPC interceptor
- Prometheus metrics (:9090)
- Grafana dashboards: RED method (request rates, error rates, latency p50/p95/p99), auto-provisioned via docker-compose

### Clients & Tooling
- Python SDK: PyPI package, full CRUD + search + hybrid search + streaming insert + upsert + range search
- Web UI: React SPA (collection browser, search playground, metrics dashboard)
- CLI/TUI: Go (Bubble Tea + Cobra), 13 RPCs, 12MB binary

### Storage
- S3/MinIO tiered storage: `ISegmentStore` interface, `TieredSegmentManager` (local + S3 + LRU cache), manifest-based discovery, async upload, `-DGVDB_WITH_S3=ON`

### Infrastructure
- Docker: multi-stage build (Ubuntu 24.04 builder + minimal runtime, S3 support included)
- Helm chart: configurable replicas, resources, storage, OCI registry
- Kind: local K8s cluster for testing
- CI: paths-filter, `make build && make test`
- Release pipeline: conventional commits → release-please → Docker + Helm + PyPI auto-publish
- 28 C++ test suites, Go e2e tests

---

## Tier 0 — Complete the Foundation

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 0.1 | Data Node Index Building + Segment Compaction | Medium | **Done** |
| 0.2 | Read Repair | Medium | **Done** |
| 0.3 | Dynamic Shard Rebalancing Execution | High | **Done** |
| 0.4 | S3/MinIO Object Storage Backend | High | **Done** |

### 0.2 Read Repair
`ReplicationManager::ReadRepair()` returns UnimplementedError. After node failure/recovery, replicas diverge silently.
- Background sweep comparing vector counts/checksums across replicas
- Stream deltas via existing `ReplicateSegment` RPC
- Files: `src/cluster/replication.cpp`, `src/cluster/coordinator.cpp`

### 0.3 Dynamic Shard Rebalancing Execution
Greedy rebalancing algorithm with 20% imbalance threshold. Coordinator-driven execution: ACTIVE → MIGRATING → replicate → remap → cleanup → ACTIVE. Idempotent via shard state check, crash-recoverable via `RecoverMigratingShards()` in health check loop.
- `CalculateRebalancePlan()`: greedy algorithm sorting overloaded nodes by load DESC, shards by size DESC, targets by load ASC. Skips MIGRATING shards. Prefers replica moves over primary.
- `ExecuteRebalancePlan()` on Coordinator: reuses existing `ReplicateSegmentData()` + `DeleteSegment` RPC. Caps at 4 moves per cycle.
- `RebalanceShards` and `TransferData` RPCs wired to coordinator.
- `SetShardState()` / `GetShardState()` for state machine transitions.
- 7 unit tests (plan algorithm, state transitions, edge cases). Existing integration tests cover segment transfer mechanics.
- Files: `src/cluster/shard_manager.cpp`, `src/cluster/coordinator.cpp`, `src/network/internal_service.cpp`

### 0.4 S3/MinIO Object Storage Backend
Tiered storage: local disk (hot) + S3/MinIO (cold). Sealed segments uploaded asynchronously after local flush. LRU local cache for downloaded segments. Manifest-based discovery on startup.
- `ISegmentStore` interface extracted from `SegmentManager` (22 methods). All consumers refactored to use it.
- `TieredSegmentManager` composes `SegmentManager` + `IObjectStore` + `SegmentCache`. Async upload pool, lazy download, manifest tracking.
- `IObjectStore` interface with `S3ObjectStore` (AWS SDK) and `InMemoryObjectStore` (testing).
- `SegmentCache`: LRU disk cache with configurable max size and automatic eviction.
- `SegmentManifest`: JSON manifest in S3 for fast startup discovery without ListObjects.
- Build with `-DGVDB_WITH_S3=ON` (AWS SDK 1.11.789 via FetchContent, s3 component only).
- Config: `storage.object_store` YAML section (endpoint, bucket, region, prefix, cache size, upload threads).
- CI builds with S3 enabled. Docker image includes S3 runtime deps.
- Dead code removed: `LocalStorage` and `StorageFactory` replaced by `ISegmentStore`.
- Files: `include/storage/segment_store.h`, `include/storage/tiered_segment_manager.h`, `include/storage/object_store.h`, `include/storage/segment_cache.h`, `include/storage/segment_manifest.h`, `include/storage/s3_object_store.h`

---

## Tier 1 — Production Blockers

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 1.1 | RBAC | Medium | **Done** |
| 1.2 | Multi-Tenancy (Phase 1) | Medium | Pending |
| 1.3 | Backup and Restore | Medium | Pending |
| 1.4 | Audit Logging | Low | **Done** |
| 1.5 | Cloud-Native: Helm Hardening | Large | Pending |
| 1.6 | Cloud-Native: Cloud Storage (GCS, Azure Blob) | High | Pending |
| 1.7 | Cloud-Native: K8s-Native Service Discovery | Medium | Pending |
| 1.8 | Cloud-Native: Kubernetes Operator | X-Large | Pending |

### 1.1 RBAC (Role-Based Access Control)
Four roles with per-collection scoping, configured via YAML.
- `admin`: all operations, all collections
- `readwrite`: insert/search/get/delete/upsert/update on assigned collections
- `readonly`: search/get/range_search/hybrid_search/list on assigned collections
- `collection_admin`: all ops except create/drop on assigned collections
- HealthCheck/GetStats always allowed without auth
- Legacy `api_keys` backward compatible (treated as admin)
- `auth/` module: `RbacStore`, `AuthContext` (thread-local), permission matrix
- Wired into single-node, coordinator, and proxy via gRPC interceptor
- Future: Raft-replicated role management for runtime updates

### 1.2 Multi-Tenancy (Collection-Level Isolation)
- Phase 1: `tenant_id` on `CollectionMetadata`, RBAC restricts keys to tenant's collections
- Phase 2: Resource group isolation
- Phase 3: Partition-key namespaces (100K+)
- Deps: 1.1 (RBAC)
- Files: `include/cluster/coordinator.h`, `proto/vectordb.proto`

### 1.3 Backup and Restore
- Flush all segments + collection metadata → compress → upload to S3
- Add `BackupCollection`/`RestoreCollection` RPCs
- Incremental: only backup changed segments (creation timestamps)
- Deps: 0.4 (S3) for remote targets; local file backup independent
- Files: new `include/storage/backup.h`, proto changes

### 1.4 Audit Logging
Structured JSON audit trail for every non-public RPC. Opt-in via `logging.audit.enabled` in YAML config.
- Hybrid interceptor + thread-local context: gRPC `AuditInterceptor` captures timing and status at `PRE_SEND_STATUS`, service handlers enrich via `AuditContext::SetCollection()`/`SetItemCount()`
- Self-contained API key extraction: reads `authorization` header directly from gRPC metadata (avoids thread-local coupling with auth interceptor)
- Dedicated spdlog logger (`"audit"`) with rotating `.jsonl` file sink, sync flush (audit must not be dropped)
- JSON schema: `timestamp`, `api_key_id`, `operation`, `collection`, `status`, `grpc_code`, `latency_ms`, `item_count`
- Skips HealthCheck/GetStats (public endpoints)
- Wired into all 4 node types: single-node, proxy, coordinator, data-node
- 8 unit tests (real gRPC server with auth + audit interceptors)
- Files: `include/network/audit_interceptor.h`, `src/network/audit_interceptor.cpp`, `include/network/audit_context.h`, `src/network/audit_context.cpp`, `include/utils/audit_logger.h`, `src/utils/audit_logger.cpp`

### 1.5 Cloud-Native: Helm Hardening
Production-ready Helm chart for EKS, GKE, AKS — zero C++ changes.
- PodDisruptionBudgets, TopologySpreadConstraints, pod anti-affinity
- ServiceAccount with cloud IAM annotations (IRSA, Workload Identity, Azure WI)
- NetworkPolicy, ServiceMonitor/PodMonitor, cert-manager, External Secrets
- Cloud-specific value overlays (`values-eks.yaml`, `values-gke.yaml`, `values-aks.yaml`)
- Ingress/Gateway API, pod security hardening, pre-upgrade health check hook
- Files: `deploy/helm/gvdb/templates/`, `deploy/helm/gvdb/values-*.yaml`

### 1.6 Cloud-Native: Cloud Storage (GCS, Azure Blob)
Extends 0.4 (S3/MinIO) with GCS and Azure Blob backends.
- `IObjectStore` interface: `PutObject`, `GetObject`, `DeleteObject`, `ListObjects`
- Google Cloud C++ Client for GCS, Azure SDK for C++ for Azure Blob
- CMake compile-time opt-in: `-DGVDB_WITH_GCS=ON`, `-DGVDB_WITH_AZURE=ON`
- Tiered storage: local disk (hot) → object store (cold) with LRU local cache
- Deps: 0.4 (S3 establishes the pattern)
- Files: `include/storage/object_store.h`, new `src/storage/gcs_object_store.cpp`, `src/storage/azure_blob_object_store.cpp`

### 1.7 Cloud-Native: K8s-Native Service Discovery
Replace static node address lists with DNS-based discovery.
- Proxy resolves headless service DNS (gRPC `dns:///` resolver) instead of static `--data-nodes`
- Graceful shutdown: SIGTERM → deregistration heartbeat → coordinator marks node down immediately
- Raft peer auto-discovery from StatefulSet ordinal + headless service DNS
- `kubectl scale` works without `helm upgrade`
- Files: `src/main/proxy_main.cpp`, `src/main/data_node_main.cpp`, `src/main/coordinator_main.cpp`

### 1.8 Cloud-Native: Kubernetes Operator
Purpose-built Go operator for day-2 lifecycle automation.
- CRDs: `GVDBCluster`, `GVDBBackup`, `GVDBRestore` (gvdb.io/v1alpha1)
- Thin wrapper: renders Helm chart, adds reconciliation logic on top
- Raft-quorum-aware rolling upgrades, scale-up with auto-rebalancing, scale-down with pre-migration
- Automated backup scheduling to object storage
- Rich status: `kubectl get gvdbcluster` shows phase, leader, node counts, vector count
- Kubebuilder scaffolding, separate `operator/` directory
- Deps: 0.3 (rebalancing), 0.4 (S3), 1.3 (backup/restore)
- Full design: `CLOUD_NATIVE.md`

---

## Tier 2 — Competitive Table Stakes

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 2.1 | Scalar Index on Metadata | High | **Done** |
| 2.2 | Sparse Vector Support (SPLADE) | High | **Done** |
| 2.3 | Upsert Operation | Low | **Done** |
| 2.4 | TTL (Time-to-Live) | Medium | **Done** |
| 2.5 | OpenTelemetry | Medium | Pending |
| 2.6 | Range Search API | Low | **Done** |
| 2.7 | Spark Connector | High | Planned |
| 2.8 | Flink Connector | High | Planned |
| 2.9 | Bulk Data Import (Client-Side) | Medium | **Done** |
| 2.10 | Server-Side Bulk Import | High | Pending |

### 2.2 Sparse Vector Support (SPLADE)
All major competitors support sparse vectors. Prerequisite for best hybrid search.
- `SparseVector` type: `vector<pair<uint32_t, float>>`
- Per-dimension posting lists with early termination
- Hybrid: dense ANN + sparse retrieval + existing RRF
- Files: new `include/core/sparse_vector.h`, `include/index/sparse_index.h`, proto changes

### 2.4 TTL (Time-to-Live)
Per-vector TTL with background sweep and expiry filtering.
- `ttl_seconds` on `VectorWithId` proto, `expiry_map_` on Segment (O(1) lookup)
- Atomic insert+TTL (no race window), background sweeper every 30s on GROWING segments
- Search/Get filter expired vectors at query time; compaction skips expired vectors
- Over-fetch on sealed index search to compensate for TTL-filtered results
- Serialization backward-compatible (old segments without expiry map still load)
- Python SDK: `insert(ttl_seconds=[300, 600, ...])`
- Files: `src/storage/segment.cpp`, `src/network/vectordb_service.cpp`, `proto/vectordb.proto`

### 2.5 OpenTelemetry (Distributed Tracing)
Multi-hop paths with no tracing = blind to latency.
- OpenTelemetry C++ SDK, instrument gRPC interceptor
- Propagate trace context via metadata headers
- Export to Jaeger/OTLP

### 2.7 Spark Connector (DataSource V2)
Every major vector DB (Milvus, Qdrant, Pinecone, Weaviate) ships a Spark connector. Table stakes for enterprise ML pipeline adoption (Uber, LinkedIn, Spotify, Airbnb).
- **Language**: Java (Spark is JVM; PySpark delegates to JVM-side connectors)
- **API**: Spark DataSource V2 (`TableProvider` → `SupportsWrite` + `SupportsRead`)
- **Write path (gRPC mode, default)**: `df.write.format("io.gvdb.spark")` → batched `Upsert` gRPC (at-least-once, idempotent). For incremental updates and small-medium datasets.
- **Write path (bulk mode, after 2.10)**: `df.write.format("io.gvdb.spark").option("mode", "bulk")` → writes Parquet to S3 staging path → triggers `BulkImport` RPC → sealed segments directly (bypasses WAL, 3-5x faster). For initial loads, backfills, and large batch jobs. This is the Milvus BulkInsert pattern.
- **Read path**: `spark.read.format("io.gvdb.spark")` → paginated `ListVectors` RPC
- **Schema mapping**: Uses the shared GVDB Parquet schema convention (2.9): `id` (LongType) + `vector` (ArrayType\<FloatType\>) + remaining columns → metadata. Same convention across Python SDK, Spark connector, and BulkImport.
- **Build**: Gradle, shadow JAR, published to GitHub Packages (`io.gvdb:gvdb-spark-connector`)
- **Deps**: Shared `gvdb-java-client` module (gRPC client, connection pool, retry). Bulk mode additionally requires 0.4 (S3) and 2.10 (BulkImport RPC).
- Files: `connectors/gvdb-spark-connector/`, `connectors/gvdb-java-client/`

### 2.8 Flink Connector (Sink V2)
Real-time feature vector updates from Kafka → GVDB. Uber and LinkedIn use Flink for real-time ML feature pipelines.
- **Language**: Java (Flink is JVM; PyFlink delegates to JVM-side connectors)
- **API**: Flink Sink V2 (`Sink<GvdbSinkRecord>` → `SinkWriter` with buffered writes)
- **Write path**: `stream.sinkTo(GvdbSink.builder()...build())` → flushes via `Upsert` or `StreamInsert` gRPC
- **Checkpoint integration**: `flush()` on checkpoint barriers, at-least-once via upsert idempotency
- **Backpressure**: Synchronous flush blocks → Flink propagates backpressure upstream
- **Build**: Gradle, shadow JAR, published to GitHub Packages (`io.gvdb:gvdb-flink-connector`)
- **Deps**: Shared `gvdb-java-client` module
- Files: `connectors/gvdb-flink-connector/`, `connectors/gvdb-java-client/`

### Spark/Flink — Industry Context
| Vector DB | Spark Connector | Flink Connector | Bulk Path |
|-----------|----------------|-----------------|-----------|
| Milvus | Official (DSv2) | Community/CDC | Parquet → S3 → BulkInsert |
| Qdrant | Official (DSv2) | None | Batched gRPC |
| Pinecone | Official (DSv2) | None | Parquet → S3 → Import |
| Weaviate | Official (DSv2) | None | REST Batch |
| **GVDB** | **Planned** | **Planned** | **gRPC mode (2.7) + Parquet → S3 → BulkImport mode (2.10)** |

### 2.9 Bulk Data Import (Client-Side)
Five `import_*` methods on `GVDBClient` for common ML formats. Pure Python — uses batched `Upsert` gRPC (idempotent/resumable) by default, optional `stream_insert` mode for speed. No C++ changes.
- `client.import_parquet(path, collection, vector_column, id_column)` — auto-maps remaining columns to metadata
- `client.import_numpy(vectors, collection, ids, metadata)` — NumPy 2D array + optional metadata
- `client.import_dataframe(df, collection, vector_column, id_column)` — Pandas or Polars DataFrame
- `client.import_csv(path, collection, vector_column, id_column)` — JSON-encoded or dimension-prefixed vectors (auto-detected)
- `client.import_h5ad(path, collection, embedding_key, metadata_columns)` — AnnData .obsm embeddings for biology
- Auto-creates collection if missing (infers dimension, default metric=cosine, index_type=auto). Progress bar via tqdm (optional dep). Resume via upsert idempotency.
- Generator-based chunking: never materializes full dataset in memory. Retry with exponential backoff on gRPC failures.
- Returns `ImportResult` dataclass (total_count, batch_count, failed_count, elapsed_seconds, created_collection).
- **Establishes the GVDB Parquet schema convention**: `id` (int64/string) + `vector` (list\<float32\>) + remaining columns → metadata. Shared by Spark connector (2.7), BulkImport RPC (2.10), and biovector (3.5).
- Optional deps via extras: `pip install gvdb[parquet]`, `gvdb[numpy]`, `gvdb[pandas]`, `gvdb[h5ad]`, `gvdb[import-all]`
- Input validation: type guards at entry point users to the correct method on misuse
- 26 unit tests (mocked client) + 7 integration tests (real server)
- Files: `clients/python/gvdb/importers.py`, `clients/python/gvdb/client.py`

### 2.10 Server-Side Bulk Import
New `BulkImport` RPC where the server reads Parquet/NumPy directly from object storage and creates sealed segments, bypassing WAL + growing-segment lifecycle. 3-5x throughput improvement over StreamInsert at scale.
- `BulkImport(source_uri, collection, format, column_mapping)` → async job ID
- `GetImportStatus(import_id)` → progress, state, error
- Server-side: data node downloads file from S3 → parses with Arrow (Parquet) or NumPy → creates sealed segments with indexes directly → registers with coordinator
- Skips: WAL writes, message queue, growing-segment flush, proxy routing
- Formats: Parquet (primary), NumPy .npy (secondary), JSON Lines (convenience)
- Same schema convention as 2.9 (shared column mapping logic)
- Spark connector bulk mode (2.7) writes Parquet to S3 staging path, then calls this RPC
- Deps: 0.4 (S3/MinIO — server must read from object storage)
- Files: `proto/vectordb.proto`, new `src/storage/bulk_importer.h`, `src/storage/bulk_importer.cpp`, `src/network/vectordb_service.cpp`

### 2.9/2.10 — Competitive Context
| Capability | Milvus | Pinecone | Qdrant | Weaviate | **GVDB** |
|------------|--------|----------|--------|----------|----------|
| gRPC/REST insert | Yes | Yes | Yes | Yes | **Yes** |
| Streaming insert | Yes | No | No | No | **Yes (1.9x faster)** |
| Parquet bulk import (server-side) | Yes (S3) | Yes (S3) | No | No | **Yes (2.10, after 0.4)** |
| NumPy bulk import (server-side) | Yes (S3) | No | No | No | **Yes (2.10, after 0.4)** |
| SDK `from_parquet()` | No | No | No | No | **Yes (2.9)** |
| SDK `from_dataframe()` | No | No | No | No | **Yes (2.9)** |
| SDK `from_h5ad()` | No | No | No | No | **Yes (2.9, unique)** |
| Arrow Flight endpoint | No | No | No | No | **Future (4.5, unique)** |

---

## Tier 3 — Differentiation

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 3.1 | IVF_TURBOQUANT | High | **Done** |
| 3.2 | Auto-Index Selection | Medium | **Done** |
| 3.3 | Embedding Visualization | Medium | Pending |
| 3.4 | ColBERT / Multi-Vector | Very High | Pending |
| 3.5 | Biovector (Biology SDK) | High | Planned (Phase 1-3 unblocked) |

### 3.2 Auto-Index Selection
Zero-config index type selection — `AUTO` resolves per-segment at seal time:
- <10K → FLAT, 10K-1M → HNSW, ≥1M → IVF_TURBOQUANT
- `AUTO` enum in proto + core, default in Web UI, Python SDK `index_type="auto"`
- Explicit types still work unchanged — AUTO is opt-in
- Recall monitoring with probe set deferred to future enhancement

### 3.3 Embedding Visualization
2D/3D UMAP projection in Web UI. No competitor has this.
- Server-side UMAP (sample 10K), Plotly.js scatter plot
- Cache projections per collection

### 3.5 Biovector — Biology SDK
Python library bridging biological data with GVDB. No biology-aware vector database exists — biologists cobble together FAISS + scripts. $13-16B bioinformatics market, 13-17% CAGR. Validated by ERAST (Nature Biotech 2026, Tencent) proving billion-scale bio search demand, and Metagenomi + LanceDB (AWS 2025, 3.5B protein embeddings).
- Pluggable embedding models: ESM-C (proteins, recommended default), SCimilarity (cells, metric learning), DNABERT-S (DNA, search-aware), ChemBERTa/Morgan (molecules), PubMedBERT (literature RAG)
- Native data format loaders: FASTA, AnnData (.h5ad), SMILES, assembled genome FASTA (chunked windowing)
- Pre-indexed reference atlases: UniProt Swiss-Prot (570K proteins), ChEMBL (2M molecules), CELLxGENE subsets. Atlas IS the product — users search references, not just their own data.
- Biomedical RAG backend: chunked PubMed ingestion, hybrid search (BM25 + dense + sparse), LLM-friendly context output
- Pipeline API: `pipeline.index_proteins("proteins.fasta", model="esmc_300M")` or `pipeline.load_reference("uniprot_swissprot_esmc")`
- Wraps GVDB Python SDK. Published to PyPI as `biovector`. Lives in `clients/python/biovector/`
- Deps: 1.1 (RBAC — DONE), 0.4 (S3 — Phase 4+ only, not needed for MVP), 4.2 (GPU — nice-to-have for scale)
- Phase 1-3 have no hard GVDB blockers. Phase 4+ production needs 0.4.
- Full design plan: `BIOVECTOR.md`

### 3.4 ColBERT / Multi-Vector Late Interaction
Multi-vector per document with MaxSim scoring. SOTA retrieval quality.
- `multi_vector` collection flag, `doc_id` linking token vectors
- Two-phase: ANN for candidate tokens → MaxSim scoring
- Deps: 2.1 (scalar index for doc_id grouping) (done)

---

## Tier 4 — Forward-Looking

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 4.1 | DiskANN | Very High | Pending |
| 4.2 | GPU Acceleration (CUDA) | High | Pending |
| 4.3 | CDC (Change Data Capture) | Medium | Pending |
| 4.4 | FP16/BF16/INT8/Binary Vectors | Very High | Pending |
| 4.5 | Arrow Flight Ingestion Endpoint | High | Pending |

### 4.1 DiskANN
1B vectors with ~16GB RAM + NVMe. Consider integrating open-source DiskANN library.

### 4.2 GPU Acceleration
`faiss-gpu` for FLAT/IVF, custom CUDA kernels for TurboQuant WHT + quantized distance. Prioritize index building (10-100x speedup).

### 4.3 CDC (Change Data Capture)
Expose WAL as gRPC streaming endpoint `StreamChanges(from_position)`. Enables DR, ETL, cross-cluster replication.

### 4.4 FP16/BF16/INT8/Binary Vectors
Deep type system change. TurboQuant handles compression at index time, so this is lower priority. Do when multi-modal models demand native BF16 storage.

### 4.5 Arrow Flight Ingestion Endpoint
Zero-copy bulk ingestion via Apache Arrow Flight RPC protocol (benchmarked 6000 MB/s). No vector database offers this today — genuine differentiator.
- Accept Arrow RecordBatches over Flight, eliminating serialization overhead
- Enables direct ingestion from any Arrow-producing system: Spark, Polars, DuckDB, HuggingFace Datasets
- Data stays columnar end-to-end (no protobuf serialization round-trip)
- Spark connector (2.7) could use Flight as an alternative high-throughput transport alongside gRPC
- Deps: Arrow C++ library (already available via gRPC/protobuf build chain)
- Files: new `src/network/flight_service.h`, `src/network/flight_service.cpp`

---

## What GVDB Should NOT Build

1. OAuth2/OIDC identity provider — accept tokens, don't issue them
2. SQL query engine — filter parser is enough, no joins/aggregations
3. Custom consensus — NuRaft works
4. Synchronous index updates on every insert — GROWING → SEALED lifecycle is correct
5. Distributed caching layer — LRU query cache is enough
6. Embedding generation — store and search, not inference

---

## Implementation Order

```
NEXT: Foundation + ML Engineer Adoption
  0.2  Read Repair                                [Medium]   (done) Done
  1.4  Audit Logging                              [Low]      (done) Done
  2.9  Bulk Data Import — Client-Side             [Medium]   (done) Done

THEN: Production Readiness + Cloud Storage
  1.1  RBAC                                       [Medium]   (done) Done
  0.4  S3/MinIO Storage                           [High]     (done) Done
  1.6  GCS + Azure Blob Storage                   [High]
  0.3  Dynamic Rebalancing                        [High]     (done) Done
  1.2  Multi-Tenancy Phase 1                      [Medium]

THEN: Server-Side Bulk + K8s
  2.10 Server-Side Bulk Import (Parquet/NumPy)    [High]     ← needs 0.4
  1.8  Kubernetes Operator (CRDs, controllers)    [X-Large]
  1.3  Backup/Restore                             [Medium]   ← operator orchestrates

THEN: Competitive Parity
  2.4  TTL                                        [Medium]   (done) Done
  2.5  OpenTelemetry                              [Medium]

THEN: Ecosystem + Differentiation
  2.7  Spark Connector (gRPC mode → bulk mode after 2.10) [High]
  2.8  Flink Connector (Sink V2, StreamInsert)    [High]
  3.2  Auto-Index Selection                       [Medium]   (done) Done
  2.2  Sparse Vectors                             [High]
  3.3  Embedding Visualization                    [Medium]

THEN: Biology (Phase 1-3 parallel with S3 work, no hard blockers)
  3.5  Biovector Phase 1-3 (Foundation + Protein + Refs + DNA + Cell + RAG) [High]
  4.2  GPU Acceleration                           [High]  — benefits biovector Phase 6+ at scale

LATER: Advanced
  3.4  ColBERT / Multi-Vector                     [Very High]
  4.1  DiskANN                                    [Very High]
  4.3  CDC                                        [Medium]
  4.4  FP16/BF16/INT8/Binary Vectors              [Very High]
  4.5  Arrow Flight Ingestion Endpoint            [High]     ← no competitor has this
```

Critical path: **1.5 → 0.4 → 2.10 → 1.8 → 2.7**
Adoption path (parallel): **2.9 (done) → 2.7 gRPC mode → 2.10 → 2.7 bulk mode**
Biovector path (parallel): **3.5 Phase 1-3 (now) → 0.4 → 3.5 Phase 4+ (production)**

---

## Architecture Changes Required

1. **Pluggable Storage Backend** (for 0.4, 1.6): Refactor `SegmentManager` to use `IStorage` for all I/O. `IObjectStore` abstraction for S3/GCS/Azure. Tiered storage: local cache + async upload.
2. **Proto Schema Evolution** (for sparse, multi-vector): `oneof` for vector types, `SearchMode` enum, field numbers >100 for extensions.
3. **Vector Type Abstraction** (for 4.4): `VectorView` class providing float32 view of any underlying type.
4. **Java Proto Options** (for 2.7, 2.8): Add `option java_package` / `option java_multiple_files` to proto files. New `connectors/` directory with Gradle multi-module build (separate from CMake).
5. **Kubernetes Operator** (for 1.8): Go project in `operator/` with kubebuilder. CRDs: `GVDBCluster`, `GVDBBackup`, `GVDBRestore`. Thin wrapper rendering Helm chart + day-2 reconciliation logic. Published as separate OCI image + Helm chart (`gvdb-operator`).
6. **DNS-Based Discovery** (for 1.7): Proxy uses gRPC `dns:///` resolver for headless service DNS. Graceful pod shutdown sends deregistration heartbeat. Coordinator Raft peers derived from StatefulSet DNS.
7. **GVDB Parquet Schema Convention** (for 2.9, 2.10, 2.7): Shared column mapping across all ingestion paths — Python SDK `import_parquet()`, server-side `BulkImport`, and Spark connector. Convention: `id` (int64 or string) + `vector` (list\<float32\>) + remaining columns → metadata. Column names configurable but defaults standardized. PyArrow is the reference Parquet implementation (other writers have FixedSizeList interop issues).
8. **Server-Side Bulk Import Pipeline** (for 2.10): Data node reads files from S3 → parses with Arrow (Parquet) or NumPy → creates sealed segments with indexes directly → registers with coordinator. Bypasses WAL, proxy routing, and growing-segment lifecycle. Async job model with status polling. Idempotent (re-import same file is a no-op if segments exist).

---

## Key Files (Pending Work)

| File | What Needs Work |
|------|-----------------|
| `src/cluster/replication.cpp` | Read repair (0.2) |
| `src/cluster/shard_manager.cpp` | Rebalance execution (0.3) |
| `src/storage/storage_factory.cpp` | S3/MinIO/GCS/Azure backends (0.4, 1.6) |
| `src/network/auth_processor.cpp` | RBAC (1.1) |
| `src/network/vectordb_service.cpp` | ~~Audit logging (1.4)~~ **Done**, BulkImport RPC (2.10) |
| `deploy/helm/gvdb/templates/` | PDB, topology, SA, NetworkPolicy, ServiceMonitor (1.5) |
| `deploy/helm/gvdb/values-*.yaml` (new) | Cloud-specific overlays: EKS, GKE, AKS (1.5) |
| `include/storage/object_store.h` (new) | Abstract object store interface (1.6) |
| `src/main/proxy_main.cpp` | DNS-based discovery (1.7) |
| `src/main/coordinator_main.cpp` | Raft peer auto-discovery (1.7) |
| `operator/` (new) | Kubernetes Operator: CRDs, controllers (1.8) |
| `proto/vectordb.proto` | Java options, sparse vectors, BulkImport/GetImportStatus RPCs (2.10) |
| `connectors/` (new) | Spark + Flink connectors (2.7, 2.8) |
| `clients/python/gvdb/importers.py` | ~~Client-side import (2.9)~~ **Done** |
| `src/storage/bulk_importer.h` (new) | Server-side bulk import: parse files → sealed segments (2.10) |
| `src/network/flight_service.cpp` (new) | Arrow Flight ingestion endpoint (4.5) |
