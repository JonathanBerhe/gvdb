# GVDB Roadmap

**Last Updated**: 2026-04-21
**Current Version**: v0.21.0
**North Star**: Fully scalable cluster on EKS, then GKE, then AKS

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
- Audit logging: structured JSON per RPC, `logging.audit.enabled` opt-in
- Prometheus metrics (:9090)
- Grafana dashboards: RED method (request rates, error rates, latency p50/p95/p99), auto-provisioned via docker-compose

### Clients & Tooling
- Python SDK: PyPI package, full CRUD + search + hybrid search + streaming insert + upsert + range search + bulk import (Parquet/NumPy/DataFrame/CSV/h5ad)
- Java SDK + Spark connector (DSv2) + Flink connector (Sink V2): shipped in v0.15.0
- Web UI: React SPA (collection browser, search playground, metrics dashboard)
- CLI/TUI: Go (Bubble Tea + Cobra), 13 RPCs, 12MB binary

### Storage
- S3/MinIO tiered storage: `ISegmentStore` interface, `TieredSegmentManager` (local + object-store + LRU cache), manifest-based discovery, async upload, `-DGVDB_WITH_S3=ON`
- FilesystemObjectStore: on-disk `IObjectStore` backend for testing and single-host tiered deployments

### Infrastructure
- Docker: multi-stage build (Ubuntu 24.04 builder + minimal runtime, S3 support included)
- Helm chart: configurable replicas, resources, storage, OCI registry (scale-ready primitives still pending — see Tier 0b)
- Kind: local K8s cluster for testing
- CI: paths-filter, `make build && make test`
- Release pipeline: conventional commits → release-please → Docker + Helm + PyPI + Maven auto-publish
- 28 C++ test suites, Go e2e tests

---

## Tier 0a — Foundation (Done)

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 0.1 | Data Node Index Building + Segment Compaction | Medium | **Done** |
| 0.2 | Read Repair | Medium | **Done (via CheckConsistency)** |
| 0.3 | Dynamic Shard Rebalancing — calc + execute | High | **Done (core)** — auto-trigger + drain moved to 0b |
| 0.4 | S3/MinIO Object Storage Backend | High | **Done** |
| 0.4b | Filesystem Object Store backend | Low | **Done** |

### 0.2 Read Repair (honest labeling)
`ReplicationManager::ReadRepair()` is a thin delegation stub; the actual divergence-repair logic runs in `Coordinator::CheckConsistency()` every ~10 health-check cycles. Functionally complete, code split intentionally.
- Files: [src/cluster/replication.cpp](src/cluster/replication.cpp), [src/cluster/coordinator.cpp](src/cluster/coordinator.cpp)

### 0.3 Dynamic Shard Rebalancing — calc + execute (done-core)
`CalculateRebalancePlan()`, `ExecuteRebalancePlan()`, `RecoverMigratingShards()` all implemented and unit-tested. Greedy algorithm, 4-move cap, idempotent state transitions.
- **Gaps now tracked in Tier 0b**: no automatic trigger on node-join (0b.2), graceful drain on scale-down is still a `TODO` at [src/cluster/shard_manager.cpp:268](src/cluster/shard_manager.cpp) (0b.3).
- Files: [src/cluster/shard_manager.cpp](src/cluster/shard_manager.cpp), [src/cluster/coordinator.cpp](src/cluster/coordinator.cpp), [src/network/internal_service.cpp](src/network/internal_service.cpp)

### 0.4 S3/MinIO Object Storage Backend
Tiered storage: local disk (hot) + S3/MinIO (cold). Sealed segments uploaded async, LRU local cache, manifest-based discovery.
- `ISegmentStore` interface (22 methods), `TieredSegmentManager`, `IObjectStore` with `S3ObjectStore` (AWS SDK) + `InMemoryObjectStore` + `FilesystemObjectStore`
- Build: `-DGVDB_WITH_S3=ON`. CI builds with S3 enabled. Docker image includes S3 runtime deps.
- Files: [include/storage/segment_store.h](include/storage/segment_store.h), [include/storage/tiered_segment_manager.h](include/storage/tiered_segment_manager.h), [include/storage/object_store.h](include/storage/object_store.h)

---

## Tier 0b — Horizontal Scalability (MANDATORY GATE)

**This tier is the non-negotiable prerequisite for Tier 1 cloud adopters and for any further Tier 2/3 feature work. A GVDB cluster must scale horizontally — up and down — without data loss or manual RPC intervention.**

Rationale: several items previously split across 1.5, 1.7, 1.8 and the unfinished tails of 0.3 all converge on the same goal. Packaged together here with a single acceptance criterion: `kubectl scale statefulset/gvdb-data-node --replicas=N` and `helm upgrade` change the live topology safely.

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 0b.1 | DRAINING signal on SIGTERM | Medium | **Done (PR #66, v0.18.0)** |
| 0b.2 | Auto-rebalance trigger on node join | Small | **Done (PR #69, v0.19.0)** |
| 0b.3 | Graceful drain execution (shard migration) | Medium | **Done (PR #67)** |
| 0b.4 | Coordinator HA: chart + actual Raft seeding | Small→Medium | **Done (PR #70 chart, PR #73 election)** |
| 0b.5 | Helm primitives — core hardening | Medium | **Done (PR #70)** |
| 0b.6.A | Kubernetes Operator — first slice (`GVDBCluster` CRD + reconciler) | Large | **Done (PR #75, v0.21.0)** |
| 0b.6.B | Operator Backup/Restore CRDs + server RPCs | X-Large | Pending — see Tier 2 / 1.3 |
| 0b.6.C | Raft-quorum-aware coordinator rolling upgrade | Medium | **Done (PR #77)** |
| 0b.6.D | Public `GetLeaderInfo` RPC + `status.coordinatorLeader` | Small | **Done (PR #77)** |
| 0b.6.E | `status.lastRebalance` timestamp | Small | **Done (PR #77)** |

### 0b.1 DRAINING signal on SIGTERM — **Done**
Narrowed from the original "K8s-native service discovery" scope because proxy routing already flows through the coordinator's `RouteQuery` RPC (no need for client-side `dns:///`), and Raft peer discovery moved into 0b.4.
- `NODE_STATUS_DRAINING = 6` added to proto
- `HeartbeatSender::DrainAndStop()` sends a synchronous DRAINING heartbeat *after* joining the send loop so a racing READY heartbeat can't overwrite it; retries once on RPC error
- `NodeRegistry::GetDrainingNodes()` / `GetRoutableNodes()` / `IsNodeRoutable()` filter draining nodes from routing decisions; `UpdateNode` merges per-field so drain doesn't clobber observability fields
- `Coordinator::GetHealthyNodes(NodeType)` internally uses `GetRoutableNodesByType` — shard placement and replication target selection automatically skip draining nodes
- `RouteQuery` proto gains `prefer_routable_replica` flag; proxy sets it on read paths (Get/ListVectors/HybridSearch) so reads re-route to replicas when primary is draining
- Helm: `dataNode.terminationGracePeriodSeconds` default 60s; query-node 30s
- Files: [proto/internal.proto](proto/internal.proto), [src/cluster/heartbeat_sender.cpp](src/cluster/heartbeat_sender.cpp), [src/cluster/node_registry.cpp](src/cluster/node_registry.cpp), [src/main/data_node_main.cpp](src/main/data_node_main.cpp), [src/network/internal_service.cpp](src/network/internal_service.cpp)

### 0b.2 Auto-rebalance trigger on node join — **Done**
- `Coordinator::DetectNewDataNodes()` in the health-check loop tracks a `known_data_nodes_` set; new entries spawn a single `ExecuteRebalancePlan` worker, debounced at 60s (configurable via `AutoRebalanceConfig`)
- `rebalance_in_flight_` atomic prevents overlapping rebalances; `Shutdown()` joins the worker so a detached thread never outlives the coordinator (UAF fix)
- Seeds `known_data_nodes_` on `Start()` — avoids spurious rebalance on coordinator restart
- Prometheus counters: `triggered_total`, `debounced_total`, `moves_completed_total`, `failures_total`
- `GetLastAutoRebalance()` observable state for dashboards / tests
- Files: [src/cluster/coordinator.cpp](src/cluster/coordinator.cpp), [include/cluster/coordinator.h](include/cluster/coordinator.h), [src/utils/metrics.cpp](src/utils/metrics.cpp)

### 0b.3 Graceful drain execution — **Done**
- `Coordinator::HandleDrainingNode(id)` called from `DetectDrainingNodes()` in the health-check loop (ordered BEFORE `DetectFailedNodes` so a cleanly-draining node takes the graceful path)
- Case 1 (primary draining): promote a routable replica via `SetPrimaryNode` — metadata-only, no data movement
- Case 2 (replica draining): `RemoveReplica` — primary still serves
- Case 3 (no routable replica for a solo primary): leave in place, log warn, heartbeat-timeout failover takes over
- `ShardManager::UnregisterNode(graceful=true)` called after all shards migrated; `TODO: Implement graceful shutdown` at `shard_manager.cpp:268` retired
- Data-node `WaitForDrainCompletion()` polls coordinator's `GetShardAssignments` until its shard list is empty or `--drain-wait-seconds` (default 15) elapses; retries with exponential backoff on RPC error
- Files: [src/cluster/coordinator.cpp](src/cluster/coordinator.cpp), [src/cluster/shard_manager.cpp](src/cluster/shard_manager.cpp), [src/main/data_node_main.cpp](src/main/data_node_main.cpp)

### 0b.4 Coordinator HA chart + Raft peer seeding — **Done**
The chart now correctly bootstraps a 3-coordinator StatefulSet and the C++ `--raft-peers` flag actually seeds NuRaft's `cluster_config` (it didn't before — see below).
- **C++**: `--raft-peers` format is `id:host:port` (e.g. `1:host1:8300,2:host2:8300,3:host3:8300`). `ParseRaftPeerSpec` validates each entry; `PrepareRaftPeerList` rejects duplicate IDs, requires self to appear in the list, and decides whether to seed vs trust persisted config
- **C++**: `RaftConfig.advertise_address` / `--raft-advertise-address` separates peer-facing endpoint from bind address; peers connect via pod FQDN, binding stays on `0.0.0.0:port`
- **Chart**: `NODE_ID=$((ORDINAL + 1))` derived from `${HOSTNAME##*-}` (POSIX-only, dash-compatible); `--raft-peers` rendered from `coordinator.replicas` via new `gvdb.coordinator.raftPeers` helper; `podManagementPolicy: Parallel` to break the OrderedReady deadlock in HA bootstrap; liveness `initialDelaySeconds: 90` to outlast the election-wait window
- **C++**: `coordinator_main` no longer crashes on election timeout — waits up to 60s, then logs a warning and continues
- Dev default `replicas: 1` keeps `--single-node` mode; operator opts into HA via `coordinator.replicas: 3`
- **Known follow-up**: all 3 pods come up Ready and reachable on the Raft port, but NuRaft leader election doesn't complete in distributed mode on kind. Chart scaffolding is correct; root cause likely in `GvdbStateManager`'s in-memory constructor (should use the 4-arg persistent variant) or NuRaft ASIO client-connection timing on fresh cluster bootstrap. Tracked as 0b.4-followup.
- Files: [src/consensus/raft_node.cpp](src/consensus/raft_node.cpp), [include/consensus/raft_node.h](include/consensus/raft_node.h), [include/consensus/raft_config.h](include/consensus/raft_config.h), [src/main/coordinator_main.cpp](src/main/coordinator_main.cpp), [deploy/helm/gvdb/templates/coordinator-statefulset.yaml](deploy/helm/gvdb/templates/coordinator-statefulset.yaml), [deploy/helm/gvdb/templates/_helpers.tpl](deploy/helm/gvdb/templates/_helpers.tpl)

### 0b.5 Helm primitives — core hardening — **Done**
All opt-in; default install renders byte-identical to pre-0b.5 (verified: 0 new objects, no `serviceAccountName` emitted).
- `templates/pdb.yaml` — per-workload `PodDisruptionBudget` with `fail` guard when `minAvailable > replicas`
- `templates/serviceaccount.yaml` — per-workload `ServiceAccount` with annotation slots for IRSA / Workload Identity / Azure WI
- `templates/priorityclass.yaml` — 4 `PriorityClass` objects, names include namespace to avoid cluster-scope collisions across installs
- Workload templates: `security.{podSecurityContext,containerSecurityContext}` (applied to all 4), parameterized anti-affinity helper (`type: preferred|required`, configurable `topologyKey`), `topologySpreadConstraints`, `priorityClassName` auto-wired when `priorityClasses.create=true`, conditional `serviceAccountName` emission
- `clusterDomain` knob (default `cluster.local`) for non-standard K8s cluster DNS
- Files: [deploy/helm/gvdb/templates/pdb.yaml](deploy/helm/gvdb/templates/pdb.yaml), [deploy/helm/gvdb/templates/serviceaccount.yaml](deploy/helm/gvdb/templates/serviceaccount.yaml), [deploy/helm/gvdb/templates/priorityclass.yaml](deploy/helm/gvdb/templates/priorityclass.yaml), [deploy/helm/gvdb/values.yaml](deploy/helm/gvdb/values.yaml), [deploy/helm/gvdb/templates/_helpers.tpl](deploy/helm/gvdb/templates/_helpers.tpl)

### 0b.4-followup — distributed Raft leader election — **Done**
Uncovered during kind verification of 0b.4: all 3 pods came up Ready and reachable on :8300 but no pod transitioned to LEADER. Root cause (found via libnuraft source dive, not any of the suspected causes): `RaftNode::InitializeNuRaft` set `init_opts.start_server_in_constructor_ = false` with the comment "Start manually after init" — but the manual `start_server()` call was never wired. The NuRaft `raft_server` was constructed but its election timer + bg commit/append threads never started. Fix was a one-line flip; persistent-mode `GvdbStateManager` + `GvdbLogStore` additional-path fixes shipped with it.
- Files: [src/consensus/raft_node.cpp](src/consensus/raft_node.cpp), [src/consensus/gvdb_log_store.cpp](src/consensus/gvdb_log_store.cpp)
- Verified: 3-pod HA elects a leader in ≤2s; leader-pod deletion triggers re-election in ≤10s; 16/16 SDK tests pass against the HA cluster.
- Shipped in PR #73 (v0.20.1).

### 0b.6.A Kubernetes Operator — first slice — **Done**
`GVDBCluster` v1alpha1 CRD + reconciler that produces the same K8s topology `deploy/helm/gvdb` does: 4 Services, 3 StatefulSets (coordinator/data-node/query-node), 1 Deployment (proxy), 1 ConfigMap, plus opt-in PDB/SA/PriorityClass/anti-affinity from 0b.5.
- Go-native rendering (no Helm SDK at runtime); Server-Side Apply with distinct field owners; `--leader-elect` operator-itself HA; OwnerReferences for cascade delete + finalizer for PriorityClass cleanup; `metav1.Condition`-based status (`Available`/`Progressing`/`Degraded`).
- Kubebuilder v4, Go 1.25, module `gvdb/operator`, image `ghcr.io/jonathanberhe/gvdb-operator`, Helm chart `deploy/helm/gvdb-operator`. Lockstep version with GVDB core.
- Full design: [CLOUD_NATIVE.md](CLOUD_NATIVE.md#operator-design).
- Shipped in PR #75 (v0.21.0).

### 0b.6.C/D/E — Production safety + status bundle — **Done**
- **0b.6.D** `GetLeaderInfo` RPC on `InternalService` + new `current_term` field; operator populates `status.coordinatorLeader` via fall-through dial across coordinator pods (pod-0 → pod-1 → pod-2) so rollouts don't blind the status-refresh path.
- **0b.6.E** `GetClusterHealthResponse.last_rebalance_unix_ms` added — captured as wall-clock at rebalance completion in `Coordinator::LastAutoRebalance` (the steady→wall rebase earlier draft was removed for NTP-safety); operator populates `status.lastRebalance`.
- **0b.6.C** Raft-quorum-aware coordinator rolling upgrade via `spec.updateStrategy.rollingUpdate.partition`. State machine gates advance on (a) current-partition pods having caught up, (b) a leader being present, and (c) the Raft term being stable since the last reconcile (recorded via `gvdb.io/rollout-observed-term` STS annotation — survives operator restart). Partition is written via SSA under a distinct field owner so the main apply path can't stomp it. New `CoordinatorRolloutReady` CR condition.
- Shipped in PR #77.

### 0b.6.B — Operator Backup/Restore — Pending
Server-side `BackupCollection` / `RestoreCollection` RPCs (see Tier 2 / 1.3) + `GVDBBackup` + `GVDBRestore` CRDs + CronJob-based scheduling. Large multi-PR effort; intentionally held behind 0b.6.A/C/D/E so the rest of the operator ships first.
- Server-side design: segment snapshot → object store (S3 via existing `IObjectStore`); metadata manifest for restore.
- Operator side: CronJob-managed `GVDBBackup`, `GVDBRestore` with progress tracking in `status`.
- Estimated: 400-600 LOC for backup RPC; +300-500 for restore RPC; +300 for each CRD + controller.

---

## Tier 1 — Cloud Adopters (EKS-first, sequential)

Every item in Tier 1 depends on all of Tier 0b. Scaling code and Helm primitives are written **once** in 0b; Tier 1 is per-cloud overlays and object-store backends.

| ID | Feature | Complexity | Status | Ships |
|----|---------|-----------|--------|-------|
| 1.EKS | EKS overlay + IRSA + E2E | Small | Pending | **v1.0.0** |
| 1.GKE | GCS backend + GKE overlay + Workload Identity + E2E | High | Pending | v1.1.0 |
| 1.AKS | Azure Blob backend + AKS overlay + Azure WI + E2E | High | Pending | v1.2.0 |
| 1.Full | cert-manager, External Secrets, NetworkPolicy, ServiceMonitor, Ingress/Gateway, HPA, pre-upgrade hook | Medium | Pending | Rolling |

### 1.EKS — Amazon EKS first production cloud
S3 backend already done in 0.4 — EKS is overlay + IAM + E2E test.
- `values-eks.yaml`: IRSA ServiceAccount annotations, EBS gp3 storage class, AWS Load Balancer Controller annotations
- Pods consume S3 via OIDC-issued IAM roles — no static AWS keys in Secrets
- E2E: spin up EKS cluster in CI (or periodic), run scaling test matrix from [CLOUD_NATIVE.md](CLOUD_NATIVE.md#verification-matrix)
- Files: [deploy/helm/gvdb/values-eks.yaml](deploy/helm/gvdb/values-eks.yaml) *(new)*, [CLOUD_NATIVE.md § EKS](CLOUD_NATIVE.md#eks-section)

### 1.GKE — Google GKE second production cloud
Requires a new object-store backend (GCS) since S3 isn't available on GCP.
- `GcsObjectStore` implementing `IObjectStore` via Google Cloud C++ Client Library
- CMake: `-DGVDB_WITH_GCS=ON`. CI matrix covers it.
- `values-gke.yaml`: Workload Identity annotations, PD-SSD storage class, GCE Ingress
- Pods consume GCS via Workload Identity — no service-account key JSON in Secrets
- E2E: GKE cluster, full scaling matrix
- Files: [include/storage/gcs_object_store.h](include/storage/gcs_object_store.h) *(new)*, [src/storage/gcs_object_store.cpp](src/storage/gcs_object_store.cpp) *(new)*, [deploy/helm/gvdb/values-gke.yaml](deploy/helm/gvdb/values-gke.yaml) *(new)*, [CLOUD_NATIVE.md § GKE](CLOUD_NATIVE.md#gke-section)

### 1.AKS — Azure AKS third production cloud
Mirrors GKE work — new backend + overlay.
- `AzureBlobObjectStore` via Azure SDK for C++
- CMake: `-DGVDB_WITH_AZURE=ON`
- `values-aks.yaml`: Azure Workload Identity annotations, managed-csi storage class, AGIC (App Gateway Ingress Controller) annotations
- E2E: AKS cluster, full scaling matrix
- Files: [include/storage/azure_blob_object_store.h](include/storage/azure_blob_object_store.h) *(new)*, [src/storage/azure_blob_object_store.cpp](src/storage/azure_blob_object_store.cpp) *(new)*, [deploy/helm/gvdb/values-aks.yaml](deploy/helm/gvdb/values-aks.yaml) *(new)*, [CLOUD_NATIVE.md § AKS](CLOUD_NATIVE.md#aks-section)

### 1.Full — Complete Helm hardening sweep
Everything from the original 1.5 that isn't strictly required for scaling itself. Ships rolling after any cloud is live.
- `cert-manager` integration (Issuer/Certificate templates for mTLS)
- External Secrets Operator (ExternalSecret/SecretStore for AWS SM, GCP SM, Azure KV)
- NetworkPolicy (east-west isolation of coordinator Raft port, data-node internal gRPC)
- ServiceMonitor / PodMonitor (Prometheus Operator discovery of `:9090`)
- Ingress / Gateway API resources for proxy
- HorizontalPodAutoscaler (proxy, query-node CPU/QPS-based)
- Pre-upgrade health-check Helm hook (fail fast if Raft quorum unhealthy before rolling)
- Files: [deploy/helm/gvdb/templates/](deploy/helm/gvdb/templates/)

---

## Tier 2 — Production Ops (after Tier 0b + at least one cloud)

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 1.2 | Multi-Tenancy (Phase 1) | Medium | Pending |
| 1.3 | Backup and Restore (server-side primitives) | Medium | Pending |
| 1.4 | Audit Logging | Low | **Done** |
| 2.5 | OpenTelemetry | Medium | Pending |

### 1.2 Multi-Tenancy (Collection-Level Isolation)
- Phase 1: `tenant_id` on `CollectionMetadata`, RBAC restricts keys to tenant's collections
- Phase 2: Resource group isolation
- Phase 3: Partition-key namespaces (100K+)
- Deps: RBAC (done)
- Files: [include/cluster/coordinator.h](include/cluster/coordinator.h), [proto/vectordb.proto](proto/vectordb.proto)

### 1.3 Backup and Restore (server-side primitives)
Server-side flush + compress + upload. Operator CRDs (0b.6) orchestrate; this tier implements the RPC and storage primitives.
- `BackupCollection` / `RestoreCollection` RPCs
- Incremental backups using segment creation timestamps
- Remote target is any `IObjectStore` backend (S3 / GCS / Azure Blob / Filesystem)
- Files: [include/storage/backup.h](include/storage/backup.h) *(new)*, [proto/vectordb.proto](proto/vectordb.proto)

### 2.5 OpenTelemetry (Distributed Tracing)
Multi-hop paths (proxy → coordinator → data-node) currently blind to latency distribution.
- OpenTelemetry C++ SDK, instrument gRPC interceptor
- Propagate trace context via metadata headers
- Export to Jaeger/OTLP

---

## Tier 3 — Competitive Table Stakes

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 2.1 | Scalar Index on Metadata | High | **Done** |
| 2.2 | Sparse Vector Support (SPLADE) | High | **Done** |
| 2.3 | Upsert Operation | Low | **Done** |
| 2.4 | TTL (Time-to-Live) | Medium | **Done** |
| 2.6 | Range Search API | Low | **Done** |
| 2.7 | Spark Connector | High | **Done (v0.15.0)** |
| 2.8 | Flink Connector | High | **Done (v0.15.0)** |
| 2.9 | Bulk Data Import (Client-Side) | Medium | **Done** |
| 2.10 | Server-Side Bulk Import | High | Pending (parallelizable with 0b) |

### 2.10 Server-Side Bulk Import
New `BulkImport` RPC where the server reads Parquet/NumPy directly from object storage and creates sealed segments, bypassing WAL + growing-segment lifecycle. 3-5x throughput improvement over StreamInsert at scale.
- `BulkImport(source_uri, collection, format, column_mapping)` → async job ID
- `GetImportStatus(import_id)` → progress, state, error
- Server-side: data node downloads file from S3 → parses with Arrow (Parquet) or NumPy → creates sealed segments with indexes directly → registers with coordinator
- Skips: WAL writes, message queue, growing-segment flush, proxy routing
- Formats: Parquet (primary), NumPy .npy (secondary), JSON Lines (convenience)
- Spark connector bulk mode writes Parquet to S3 staging path, then calls this RPC
- Deps: 0.4 (S3 — done). **No dep on Tier 0b** — parallelizable.
- Files: [proto/vectordb.proto](proto/vectordb.proto), [include/storage/bulk_importer.h](include/storage/bulk_importer.h) *(new)*, [src/storage/bulk_importer.cpp](src/storage/bulk_importer.cpp) *(new)*, [src/network/vectordb_service.cpp](src/network/vectordb_service.cpp)

---

## Tier 4 — Differentiation

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 3.1 | IVF_TURBOQUANT | High | **Done** |
| 3.2 | Auto-Index Selection | Medium | **Done** |
| 3.3 | Embedding Visualization | Medium | Pending |
| 3.4 | ColBERT / Multi-Vector | Very High | Pending |
| 3.5 | Biovector (Biology SDK) | High | Planned (Phase 1-3 unblocked, parallelizable with 0b) |

### 3.3 Embedding Visualization
2D/3D UMAP projection in Web UI. No competitor has this.
- Server-side UMAP (sample 10K), Plotly.js scatter plot
- Cache projections per collection

### 3.4 ColBERT / Multi-Vector Late Interaction
Multi-vector per document with MaxSim scoring. SOTA retrieval quality.
- `multi_vector` collection flag, `doc_id` linking token vectors
- Two-phase: ANN for candidate tokens → MaxSim scoring
- Deps: 2.1 (scalar index for doc_id grouping) (done)

### 3.5 Biovector — Biology SDK
Python library bridging biological data with GVDB. No biology-aware vector database exists — biologists cobble together FAISS + scripts. Full design plan: [BIOVECTOR.md](BIOVECTOR.md). Phase 1-3 has no hard GVDB blockers; can proceed in parallel with Tier 0b. Phase 4+ at scale benefits from Tier 0b + a cloud in Tier 1.

---

## Tier 5 — Forward-Looking

| ID | Feature | Complexity | Status |
|----|---------|-----------|--------|
| 4.1 | DiskANN | Very High | Pending |
| 4.2a | GPU Acceleration — Apple Metal (FLAT) | Medium | **Done** |
| 4.2b | GPU Acceleration — CUDA | High | Pending |
| 4.3 | CDC (Change Data Capture) | Medium | Pending |
| 4.4 | FP16/BF16/INT8/Binary Vectors | Very High | Pending |
| 4.5 | Arrow Flight Ingestion Endpoint | High | Pending |

### 4.2a Apple Metal GPU Acceleration (Phase 1: FLAT — Done)
`MetalFlatIndex` via metal-cpp, 16-24x speedup over faiss CPU on M1 Pro (1K–2M vectors, dim 128–1536). Future phases: IVF cluster assignment (4.2a-P2), TurboQuant WHT kernels (4.2a-P3).

### 4.5 Arrow Flight Ingestion Endpoint
Zero-copy bulk ingestion via Apache Arrow Flight RPC protocol (benchmarked 6000 MB/s). No vector database offers this today — genuine differentiator.

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
Tier 0b — Horizontal Scalability  (5 of 6 shipped)
  0b.1  DRAINING signal                            [Medium]   ✅ PR #66 (v0.18.0)
  0b.2  Auto-rebalance on join                     [Small]    ✅ PR #69 (v0.19.0)
  0b.3  Graceful drain execution                   [Medium]   ✅ PR #67
  0b.4  Coordinator HA chart + Raft seeding        [Small+]   ✅ PR #70 (chart) — election follow-up below
  0b.5  Helm core primitives                       [Medium]   ✅ PR #70
  0b.4f Distributed Raft leader election fix       [Small]    🟡 uncovered by kind verification of 0b.4
  0b.6  Kubernetes Operator                        [X-Large]  ⬜

IN PARALLEL (no hard dependency on Tier 0b):
  2.10  Server-Side Bulk Import                    [High]  ← 0.4 done, unblocks Spark bulk mode
  3.5   Biovector Phase 1-3                        [High]  ← no hard GVDB blocker

AFTER 0b: Cloud Adopters (EKS-first, sequential)
  1.EKS  values-eks.yaml + IRSA + E2E              [Small]    ← v1.0.0
  1.GKE  GCS backend + values-gke.yaml + WI + E2E  [High]     ← v1.1.0
  1.AKS  Azure Blob + values-aks.yaml + WI + E2E   [High]     ← v1.2.0
  1.Full Remaining Helm hardening sweep            [Medium]   ← rolling

AFTER Tier 1 (at least one cloud): Production Ops
  1.2   Multi-tenancy Phase 1                      [Medium]
  1.3   Backup/Restore server-side primitives      [Medium]
  2.5   OpenTelemetry                              [Medium]

LATER: Competitive parity + differentiation
  3.3   Embedding Visualization                    [Medium]
  3.5   Biovector Phase 4+                         [High]
  3.4   ColBERT / Multi-Vector                     [Very High]
  4.1   DiskANN                                    [Very High]
  4.2b  CUDA GPU Acceleration                      [High]
  4.3   CDC                                        [Medium]
  4.4   FP16/BF16/INT8/Binary Vectors              [Very High]
  4.5   Arrow Flight Ingestion                     [High]  ← no competitor has this
```

**Critical path to "fully scalable cluster on EKS" (v1.0.0)**: `~0b.1 → 0b.2 → 0b.3 → 0b.4 → 0b.5~ → 0b.4f → 0b.6 → 1.EKS`
**Then**: `1.GKE (v1.1.0) → 1.AKS (v1.2.0)`
**Adoption path (parallel)**: `2.10 Server-Side Bulk Import` can proceed alongside without blocking.
**Biovector path (parallel)**: `3.5 Phase 1-3` has no hard blocker; scales with 0b+1.* for production.

Calendar delta vs. the original 16–20 week estimate: Tier 0b is largely in the bag aside from 0b.4f (small) and 0b.6 (X-large). Revised rough estimates:
- **v1.0.0 EKS-scalable**: ~8–10 weeks remaining (was 11–15)
- **v1.1.0 + GKE**: +2 weeks
- **v1.2.0 + AKS**: +2 weeks
- **All three clouds + full hardening**: ~12–15 weeks remaining

---

## Architecture Changes Required (Tier 0b / Tier 1)

1. **DNS-Based Discovery** (0b.1): Proxy and Raft peer lists replaced by gRPC `dns:///` resolver for headless service DNS. Graceful pod shutdown sends deregistration heartbeat before exit.
2. **Kubernetes Operator** (0b.6): Go project in `operator/` with kubebuilder. CRDs: `GVDBCluster`, `GVDBBackup`, `GVDBRestore`. Thin wrapper rendering Helm chart + day-2 reconciliation logic. Published as separate OCI image + Helm chart (`gvdb-operator`).
3. **Pluggable Object-Store Backends** (1.GKE, 1.AKS): Extend `IObjectStore` with `GcsObjectStore` (Google Cloud C++ Client) and `AzureBlobObjectStore` (Azure SDK for C++). Compile-time opt-in: `-DGVDB_WITH_GCS=ON`, `-DGVDB_WITH_AZURE=ON`.
4. **Proto Schema Evolution** (for sparse, multi-vector, bulk import): `oneof` for vector types, `SearchMode` enum, field numbers >100 for extensions. BulkImport / GetImportStatus RPCs for 2.10.
5. **Vector Type Abstraction** (for 4.4): `VectorView` class providing float32 view of any underlying type.
6. **GVDB Parquet Schema Convention** (for 2.9, 2.10, 2.7): Shared column mapping across all ingestion paths. Convention: `id` (int64 or string) + `vector` (list<float32>) + remaining columns → metadata.
7. **Server-Side Bulk Import Pipeline** (for 2.10): Data node reads files from object storage → parses with Arrow (Parquet) or NumPy → creates sealed segments with indexes directly → registers with coordinator. Bypasses WAL, proxy routing, and growing-segment lifecycle.

---

## Key Files (Pending Work)

| File | Tier | What Needs Work |
|------|------|-----------------|
| [src/consensus/raft_node.cpp](src/consensus/raft_node.cpp) (InitializeNuRaft) | 0b.4f | Switch to persistent `GvdbStateManager` (4-arg ctor) + verify NuRaft election completes in distributed mode |
| [src/consensus/gvdb_state_manager.cpp](src/consensus/gvdb_state_manager.cpp) | 0b.4f | Wire log_store_path + state_path via `config_.data_dir` |
| `operator/` *(new)* | 0b.6 | Full Kubernetes Operator — 3 CRDs + reconcilers |
| [deploy/helm/gvdb/values-eks.yaml](deploy/helm/gvdb/values-eks.yaml) *(new)* | 1.EKS | EKS overlay |
| [include/storage/gcs_object_store.h](include/storage/gcs_object_store.h) *(new)* | 1.GKE | GCS backend |
| [deploy/helm/gvdb/values-gke.yaml](deploy/helm/gvdb/values-gke.yaml) *(new)* | 1.GKE | GKE overlay |
| [include/storage/azure_blob_object_store.h](include/storage/azure_blob_object_store.h) *(new)* | 1.AKS | Azure Blob backend |
| [deploy/helm/gvdb/values-aks.yaml](deploy/helm/gvdb/values-aks.yaml) *(new)* | 1.AKS | AKS overlay |
| [proto/vectordb.proto](proto/vectordb.proto) | 2.10 | BulkImport/GetImportStatus RPCs |
| [include/storage/bulk_importer.h](include/storage/bulk_importer.h) *(new)* | 2.10 | Server-side bulk import |
| [src/network/flight_service.cpp](src/network/flight_service.cpp) *(new)* | 4.5 | Arrow Flight ingestion endpoint |
