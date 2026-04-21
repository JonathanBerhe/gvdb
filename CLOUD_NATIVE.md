# GVDB Cloud-Native Deployment

**Status**: Design doc + shipped-feature summary for Tier 0b (Horizontal Scalability) and Tier 1 (Cloud Adopters: EKS → GKE → AKS).
**Last Updated**: 2026-04-21 — after 0b.4-followup election fix (PR #73), operator first-slice (PR #75), and operator production-safety bundle (PR #77).
**See also**: [ROADMAP.md](ROADMAP.md) for tier structure and priorities.

This document expands [ROADMAP.md](ROADMAP.md) items **0b.1–0b.6** (cloud-agnostic scaling) and **1.EKS / 1.GKE / 1.AKS / 1.Full** (per-cloud overlays). It answers: *how does a GVDB cluster actually scale horizontally, and what's different across the three major managed Kubernetes services?*

## Status at a glance (as of 2026-04-21)

| Capability | Status | Notes |
|---|---|---|
| Graceful scale-down (data/query node) | ✅ Shipped | 0b.1 signal + 0b.3 execution + 60s `terminationGracePeriodSeconds` on data-node |
| Auto-rebalance on scale-up | ✅ Shipped | 0b.2 with 60s debounce, Prometheus metrics, detached worker joined on shutdown |
| Coordinator single-node (Raft in `--single-node`) | ✅ Works | Default chart install |
| Coordinator HA chart topology (3 pods up + Ready) | ✅ Shipped | 0b.4 chart: parallel bootstrap, Raft peers seeded, `podManagementPolicy: Parallel` (PR #70) |
| Coordinator HA leader election (distributed Raft) | ✅ Shipped | 0b.4-followup — `start_server_in_constructor_` flip + persistent state manager + log-store persistent-mode fixes (PR #73) |
| Production hardening primitives | ✅ Shipped | 0b.5: PDB, anti-affinity, topology spread, securityContext, SA, PriorityClass — all opt-in |
| Operator first slice — `GVDBCluster` CRD + reconciler | ✅ Shipped | 0b.6.A — Go-native rendering, SSA, OwnerReferences cascade, finalizer-managed PriorityClass cleanup, self-leader-election (PR #75) |
| Operator `status.coordinatorLeader` + `status.lastRebalance` | ✅ Shipped | 0b.6.D + 0b.6.E — new `GetLeaderInfo` RPC + `last_rebalance_unix_ms` on `GetClusterHealth` (PR #77) |
| Operator Raft-quorum-aware rolling upgrade | ✅ Shipped | 0b.6.C — partition-gated pod-by-pod drain with leader + term-stability gate, SSA field owner `gvdb-operator-rollout` (PR #77) |
| Operator Backup/Restore CRDs | ⬜ Not started | 0b.6.B — needs new C++ `BackupCollection` / `RestoreCollection` RPCs; held behind the rest of 0b.6 on purpose |

---

## Table of Contents

1. [Overview](#overview)
2. [Scaling Architecture](#scaling-architecture)
3. [Operator Design](#operator-design)
4. [EKS Section](#eks-section)
5. [GKE Section](#gke-section)
6. [AKS Section](#aks-section)
7. [Verification Matrix](#verification-matrix)

---

## Overview

### What's shared across EKS / GKE / AKS

All three managed Kubernetes services expose the same K8s API surface. GVDB's scaling mechanics are therefore **written once** and work on all three:

- `StatefulSet` ordinal-based identity for coordinator + data-node + query-node
- Headless service DNS for peer discovery (0b.1)
- `PodDisruptionBudget`, `topologySpreadConstraints`, `podAntiAffinity` (0b.5)
- The GVDB Kubernetes Operator (0b.6) and its CRDs

### What's cloud-specific

| Concern | EKS | GKE | AKS |
|---|---|---|---|
| Object storage | S3 (0.4 done) | GCS (1.GKE) | Azure Blob (1.AKS) |
| Pod → Object Store IAM | IRSA via OIDC | Workload Identity | Azure Workload Identity |
| Default block storage | EBS gp3 | PD-SSD | Managed Disk (managed-csi) |
| LB controller | AWS Load Balancer Controller | GCE / GCP LB | Application Gateway (AGIC) |
| Secret source (for External Secrets, Tier 1.Full) | AWS Secrets Manager | GCP Secret Manager | Azure Key Vault |

**Implication**: per-cloud Tier 1 work is small — a `values-<cloud>.yaml` overlay plus (for GKE/AKS) one new `IObjectStore` implementation.

---

## Scaling Architecture

### Component scaling model

| Component | Kind | Scales | How | Status |
|---|---|---|---|---|
| Coordinator | StatefulSet | Raft quorum (1 or 3+) | Parallel bootstrap, Raft peers seeded from headless DNS, ordinal-based node-id | ✅ topology, 🟡 election |
| Data Node | StatefulSet | Horizontally, any N | Heartbeat register + 0b.2 auto-rebalance on join + 0b.3 graceful drain on SIGTERM | ✅ Shipped |
| Query Node | StatefulSet | Horizontally, any N | Heartbeat auto-register + 0b.1 DRAINING signal on SIGTERM | ✅ Shipped |
| Proxy | Deployment | Horizontally, any N | Stateless; routes via coordinator's `RouteQuery` (drain-aware for reads via `prefer_routable_replica`) | ✅ Shipped |

### Scale-up flow (data-node) — shipped in 0b.2

```
1. helm upgrade --set dataNode.replicas=N   (or Operator CR update, or kubectl scale)
2. StatefulSet controller creates pod gvdb-data-node-(N-1)
3. Pod starts, HeartbeatSender registers with coordinator
4. Coordinator health-check loop: DetectNewDataNodes() spots id not in known_data_nodes_
5. Passes debounce (60s since last auto-rebalance) + in-flight guard
6. Spawns ONE detached worker: ExecuteRebalancePlan (bounded by kMaxMovesPerCycle=4)
7. ReplicateSegmentData moves segments source → new node; DeleteSegment on source
8. Metrics: gvdb_auto_rebalance_triggered_total + moves_completed_total
9. GetLastAutoRebalance() exposes {status, completed_at, moves} for observability
```

### Scale-down flow (data-node) — shipped in 0b.1 + 0b.3

```
1. helm upgrade --set dataNode.replicas=N-1   (or kubectl scale)
2. StatefulSet controller sends SIGTERM to highest-ordinal pod
3. Pod's WaitForShutdown returns; DrainAndStop() runs:
   a. Joins the normal heartbeat loop (prevents race: no READY can overwrite DRAINING)
   b. Sends a final heartbeat with NODE_STATUS_DRAINING, retries once on RPC error
4. Coordinator's next health-check cycle: GetDrainingNodes() (only fresh draining)
5. HandleDrainingNode(id):
   - Primary role shard → promote a routable replica via SetPrimaryNode
   - Replica role shard → RemoveReplica
   - No routable replica for a solo primary → leave in place, fall back to heartbeat-timeout
6. All shards migrated off → ShardManager::UnregisterNode(graceful=true)
   known_data_nodes_ also pruned so a future rejoin is detected as scale-up
7. Meanwhile data-node polls GetShardAssignments until no shards assigned or
   --drain-wait-seconds (default 15s) elapses, then server->Shutdown()
8. K8s deletes the pod; PVC persists for potential scale-up back
```

### Coordinator HA flow (0b.4 — topology shipped, election pending)

**Before 0b.4**: `replicas: 1`, `--node-id 1` hardcoded, chart never emitted `--raft-peers`, and even if it did the C++ code never passed them to NuRaft.

**After 0b.4** (what works):
- `replicas: 3` renders correctly with `podManagementPolicy: Parallel` — all 3 pods start simultaneously (required to break the OrderedReady/quorum chicken-and-egg deadlock)
- Each pod derives `NODE_ID=$((ORDINAL + 1))` via POSIX-portable shell (dash-compatible, with explicit ordinal validation)
- `--raft-peers "1:host1:8300,2:host2:8300,3:host3:8300"` rendered from `coordinator.replicas` via the `gvdb.coordinator.raftPeers` helper
- `--raft-advertise-address` separates peer-facing endpoint (FQDN) from bind (`0.0.0.0:port`)
- `RaftNode::InitializeNuRaft` seeds `state_mgr_->cluster_config` with declared peers BEFORE `launcher_->init` (was a no-op before). Validates via `PrepareRaftPeerList`: parses each entry, rejects duplicate IDs, requires self to appear in the declared list
- On restart the persisted `cluster_config` wins; only fresh boots (`size ≤ 1`) get reseeded — runtime `add_srv` / `remove_srv` changes aren't clobbered
- `coordinator_main` waits up to 60s for leader election, then continues startup with a warning (was crash-looping before)
- Liveness probe `initialDelaySeconds: 90` outlasts the election window

**What's still pending (0b.4-followup)**:

All 3 pods reach Ready and are mutually reachable on `:8300`. `cluster_config` lists 3 voting members. But NuRaft's leader election doesn't complete — no pod transitions to LEADER. Verified with kind + `kubectl logs` — zero `Became LEADER` events on any pod.

Suspected causes (to diagnose in the follow-up PR):
1. `RaftNode::InitializeNuRaft` instantiates `GvdbStateManager` via the **2-arg in-memory constructor**. Switching to the 4-arg persistent variant (`log_store_path`, `state_path` under `config_.data_dir/raft`) may be required for the log-store handshake NuRaft expects during election.
2. `NuRaftLoggerAdapter` may be filtering DEBUG events at level 5 — couldn't reproduce election traffic in logs to diagnose further.
3. NuRaft ASIO client-connection sequencing when all 3 pods come up concurrently.

Impact: bare-metal / compose / single-coordinator Helm installs are unaffected. Only the `replicas: 3` HA topology is blocked on this follow-up.

### Failure / recovery flow (unchanged, documented for completeness)

- Data-node crashes → heartbeat timeout → `HandleFailedNode` promotes a replica
- Under-replicated shards → `Coordinator::CheckConsistency()` health loop re-replicates (0.2)
- Migrating shards crash-recover via `RecoverMigratingShards()` in the same health loop (0.3)
- Coordinator restart → `known_data_nodes_` seeded from registry on `Start()` → no spurious rebalance
- Rebalance worker in flight at shutdown → `Coordinator::Shutdown()` joins before destruction (no UAF)

### Failure / recovery flow (unchanged, documented for completeness)

- Data-node crashes → heartbeat timeout → coordinator promotes a replica to primary
- Under-replicated shards → `Coordinator::CheckConsistency()` health loop re-replicates (0.2)
- Migrating shards crash-recover via `RecoverMigratingShards()` in the same health loop (0.3)

---

## Operator Design

Full 1.8 scope (ROADMAP.md item 0b.6). Purpose-built Go operator using Kubebuilder.

**Important**: The Operator is *optional* and *additive*. Bare-metal, Docker Compose, and plain Helm deployments of GVDB are fully functional without it. Tier 0b has deliberately put the scaling *mechanism* in the Coordinator (in-process, always-present) — the Operator is a K8s-native *policy* layer on top that:
- Automates membership decisions via `GVDBCluster` CR (declarative scaling)
- Coordinates Raft-quorum-aware rolling upgrades
- Schedules backups / restores via CRDs
- Surfaces cluster state via `kubectl get gvdbcluster`

What the Operator does NOT need to implement (because 0b.1–0b.5 already cover it):
- **Shard rebalancing** — 0b.2 fires automatically in the Coordinator when a new node joins. Operator only observes; optional belt-and-braces RPC call.
- **Graceful drain on scale-down** — 0b.3 fires automatically on pod SIGTERM (DRAINING heartbeat + shard migration). Operator doesn't need to drive it.
- **Failure detection / replica promotion** — existing heartbeat-timeout path in the Coordinator.

This separation keeps GVDB usable without the Operator (an intentional product decision) and lets the Operator be a thin reconciler rather than reimplementing cluster logic.

### Directory

```
operator/
├── api/v1alpha1/
│   ├── gvdbcluster_types.go
│   ├── gvdbbackup_types.go
│   ├── gvdbrestore_types.go
│   └── zz_generated.deepcopy.go
├── controllers/
│   ├── gvdbcluster_controller.go
│   ├── gvdbbackup_controller.go
│   └── gvdbrestore_controller.go
├── config/
│   ├── crd/
│   ├── rbac/
│   └── manager/
├── internal/
│   ├── helm/          # renders the gvdb Helm chart
│   ├── raft/          # coordinator health & quorum checks
│   └── rebalance/     # calls ExecuteRebalancePlan RPC
├── Dockerfile
├── Makefile
└── PROJECT
```

### CRDs

**`GVDBCluster`** (`gvdb.io/v1alpha1`) — declarative cluster spec.
```yaml
apiVersion: gvdb.io/v1alpha1
kind: GVDBCluster
metadata:
  name: prod
spec:
  version: 1.0.0
  coordinator:
    replicas: 3
  dataNode:
    replicas: 6
    storage:
      size: 500Gi
      storageClassName: gp3
  queryNode:
    replicas: 3
  proxy:
    replicas: 2
  objectStore:
    type: s3
    bucket: gvdb-prod-segments
    region: us-east-1
  security:
    tls:
      enabled: true
      certManagerIssuer: letsencrypt-prod
status:
  phase: Ready
  coordinatorLeader: gvdb-coordinator-1
  nodeCounts:
    dataNode: {desired: 6, ready: 6}
    queryNode: {desired: 3, ready: 3}
  collectionCount: 42
  totalVectors: 1247000000
  lastRebalance: 2026-04-19T10:34:21Z
```

**`GVDBBackup`** — one-shot or cron-scheduled backup.
```yaml
apiVersion: gvdb.io/v1alpha1
kind: GVDBBackup
metadata:
  name: nightly
spec:
  clusterRef: prod
  schedule: "0 2 * * *"
  destination:
    type: s3
    uri: s3://gvdb-backups/prod/
  retention:
    days: 30
status:
  lastBackupTime: 2026-04-19T02:00:15Z
  lastBackupSizeBytes: 84000000000
```

**`GVDBRestore`** — recreate collections from a backup.
```yaml
apiVersion: gvdb.io/v1alpha1
kind: GVDBRestore
metadata:
  name: restore-prod-20260418
spec:
  clusterRef: prod
  source:
    uri: s3://gvdb-backups/prod/2026-04-18/
  collections: ["*"]   # or explicit list
status:
  phase: Completed
  restoredCollections: 42
```

### Reconciliation loops

- **GVDBCluster controller**: renders the Helm chart from `spec`, reconciles StatefulSets/Deployments. On scale-up, calls `ExecuteRebalancePlan` RPC via coordinator (belt-and-braces over 0b.2). On scale-down, calls `Unregister(graceful=true)` on the draining data-node before pod deletion (belt-and-braces over 0b.3 SIGTERM). Rolling upgrades are Raft-quorum-aware for coordinator pods.
- **GVDBBackup controller**: manages CronJobs that invoke `BackupCollection` RPC. Tracks retention.
- **GVDBRestore controller**: invokes `RestoreCollection` RPC, tracks progress in `status`.

### Distribution

- Separate OCI image: `ghcr.io/jonathanberhe/gvdb-operator:<ver>`
- Separate Helm chart: `gvdb-operator` (for installing the operator itself)
- CRDs installed by the operator chart
- Users install `gvdb-operator` once per cluster, then create `GVDBCluster` CRs

### What the operator deliberately does NOT do

- Does not re-implement Raft — delegates to NuRaft in the coordinator C++
- Does not re-implement rebalancing — delegates to `ExecuteRebalancePlan` RPC
- Does not re-implement backup/restore logic — delegates to server RPCs
- Is a thin orchestration layer that makes Tier 0b + Tier 1 safely declarative

---

## EKS Section

**Goal**: v1.0.0 — first production-grade cloud deployment.
**Dep**: All of Tier 0b complete. S3 backend (0.4) already done.

### What's needed beyond Tier 0b

Just a values overlay + IAM wiring. No C++ changes.

### `values-eks.yaml` overlay

```yaml
# IRSA: pods assume IAM role via OIDC, no static AWS keys
serviceAccount:
  create: true
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/gvdb-s3-access

# EBS gp3 for PVCs
persistence:
  storageClassName: gp3

# AWS Load Balancer Controller for proxy exposure
proxy:
  service:
    type: LoadBalancer
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: "external"
      service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: "ip"
      service.beta.kubernetes.io/aws-load-balancer-scheme: "internet-facing"

# Object storage
storage:
  objectStore:
    type: s3
    bucket: gvdb-prod
    region: us-east-1
    # No access key / secret key — IRSA handles auth

# Node placement hints
affinity:
  nodeAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      preference:
        matchExpressions:
        - key: node.kubernetes.io/instance-type
          operator: In
          values: ["m6i.2xlarge", "m6i.4xlarge"]
```

### IRSA setup (one-time per cluster)

1. Create IAM OIDC provider for the EKS cluster
2. Create IAM policy allowing `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on the GVDB bucket
3. Create IAM role trusted by the OIDC provider for the ServiceAccount
4. Install GVDB Operator, create `GVDBCluster` with the `role-arn` annotation

### E2E test

Run [CLOUD_NATIVE.md § Verification Matrix](#verification-matrix) on a real EKS cluster. Gate `v1.0.0` release on passing.

---

## GKE Section

**Goal**: v1.1.0.
**Dep**: All of Tier 0b + new GCS backend (1.GKE).

### New C++ work — `GcsObjectStore`

Implements [include/storage/object_store.h](include/storage/object_store.h) via Google Cloud C++ Client Library.

- `PutObject(key, data)` → `google::cloud::storage::Client::InsertObject()`
- `GetObject(key)` → `ReadObject()`
- `DeleteObject(key)` → `DeleteObject()`
- `ListObjects(prefix)` → `ListObjects()` with pagination

**CMake**:
```cmake
option(GVDB_WITH_GCS "Enable GCS object store" OFF)
if(GVDB_WITH_GCS)
  FetchContent_Declare(google-cloud-cpp ...)
  # ...
  target_link_libraries(gvdb_storage PRIVATE google-cloud-cpp::storage)
  target_compile_definitions(gvdb_storage PRIVATE GVDB_WITH_GCS=1)
endif()
```

**Factory registration**:
```cpp
// src/storage/storage_factory.cpp
#ifdef GVDB_WITH_GCS
  if (type == "gcs") return std::make_unique<GcsObjectStore>(config);
#endif
```

CI matrix adds a `GVDB_WITH_GCS=ON` build job.

### `values-gke.yaml` overlay

```yaml
# Workload Identity: pods authenticate as a GCP service account via KSA binding
serviceAccount:
  create: true
  annotations:
    iam.gke.io/gcp-service-account: gvdb-gcs@my-project.iam.gserviceaccount.com

# PD-SSD for PVCs
persistence:
  storageClassName: premium-rwo    # or pd-ssd on regional clusters

# GCE LB for proxy
proxy:
  service:
    type: LoadBalancer
    annotations:
      cloud.google.com/neg: '{"ingress": true}'
      cloud.google.com/backend-config: '{"default": "gvdb-backend-config"}'

# Object storage
storage:
  objectStore:
    type: gcs
    bucket: gvdb-prod
    # Workload Identity handles auth

# Regional spread for HA
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
```

### Workload Identity setup (one-time per cluster)

1. Enable Workload Identity on the GKE cluster
2. Create GCP service account with `roles/storage.objectAdmin` on the bucket
3. Bind the Kubernetes ServiceAccount (annotated above) to the GCP SA:
   ```bash
   gcloud iam service-accounts add-iam-policy-binding gvdb-gcs@my-project.iam.gserviceaccount.com \
     --role roles/iam.workloadIdentityUser \
     --member "serviceAccount:my-project.svc.id.goog[gvdb-ns/gvdb]"
   ```

### E2E test

Same matrix as EKS, on GKE. Gate `v1.1.0` release.

---

## AKS Section

**Goal**: v1.2.0.
**Dep**: All of Tier 0b + new Azure Blob backend (1.AKS).

### New C++ work — `AzureBlobObjectStore`

Implements [include/storage/object_store.h](include/storage/object_store.h) via Azure SDK for C++ (Storage Blob).

- `PutObject(key, data)` → `azure::storage::blobs::BlobClient::UploadFrom()`
- `GetObject(key)` → `DownloadTo()`
- `DeleteObject(key)` → `Delete()`
- `ListObjects(prefix)` → `ListBlobs()` via container client

**CMake**: `-DGVDB_WITH_AZURE=ON` mirrors the GCS pattern.

### `values-aks.yaml` overlay

```yaml
# Azure Workload Identity: pods authenticate via federated OIDC credential
serviceAccount:
  create: true
  annotations:
    azure.workload.identity/client-id: <managed-identity-client-id>
podLabels:
  azure.workload.identity/use: "true"

# managed-csi (or managed-csi-premium) for PVCs
persistence:
  storageClassName: managed-csi

# Application Gateway Ingress Controller (AGIC) for proxy
proxy:
  ingress:
    enabled: true
    className: azure-application-gateway
    annotations:
      appgw.ingress.kubernetes.io/backend-protocol: "GRPC"

# Object storage
storage:
  objectStore:
    type: azure-blob
    account: gvdbprod
    container: segments
    # Azure WI handles auth

# Availability Zone spread
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
```

### Azure Workload Identity setup (one-time per cluster)

1. Enable Workload Identity on the AKS cluster
2. Create an Azure AD application + service principal (or user-assigned managed identity)
3. Grant `Storage Blob Data Contributor` on the storage account
4. Create a federated credential binding the OIDC issuer + Kubernetes SA

### E2E test

Same matrix as EKS/GKE. Gate `v1.2.0` release.

---

## Verification Matrix

Every cloud (EKS, GKE, AKS) must pass this matrix before its release is cut. The matrix is the operational contract of "fully scalable cluster".

### Functional matrix

Status key: ✅ verified on kind  ·  🟡 infrastructure verified, upstream issue tracked  ·  ⬜ pending (needs real cloud cluster or 0b.6 Operator)

| Test | What it proves | Expected outcome | Status |
|---|---|---|---|
| `helm install gvdb` fresh (default) | Baseline install works | All pods Ready within 2 min; 0 new hardening objects rendered | ✅ verified |
| `helm install gvdb -f values-prod.yaml` (PDB/SA/PC/anti-affinity enabled) | Hardening primitives render | 4 PDBs + 4 ServiceAccounts + 4 PriorityClasses (namespace-qualified names) | ✅ verified |
| `helm upgrade --set coordinator.replicas=3` fresh install | Coordinator HA topology | 3 pods Ready in parallel, Raft peers seeded in `cluster_config` (seen via logs) | ✅ verified |
| Same, but verify actual Raft leader election | Coordinator HA election | One pod becomes LEADER, others FOLLOWER | 🟡 0b.4-followup — pods Ready but election doesn't complete in distributed mode |
| `kubectl scale sts/gvdb-data-node --replicas=+1` | Scale-up auto-rebalances (0b.2) | Coordinator logs `Auto-rebalance: new data node(s) [N] joined`; shards redistributed within debounce + move window | ✅ verified end-to-end via PR #66's live kind test + unit tests |
| `kubectl scale sts/gvdb-data-node --replicas=-1` | Scale-down graceful drain (0b.1 + 0b.3) | Coordinator logs `Node N entered DRAINING state`; shards migrated before pod exits; no data loss | ✅ DRAINING signal verified live; migration logic unit-tested |
| `kubectl drain <node>` (evict one pod) | PDB protection (0b.5) | PDB blocks if it would break quorum; pod reschedules after sibling is healthy | ⬜ pending live multi-node kind or cloud |
| `helm upgrade` to new image during 1k QPS | Rolling upgrade without client errors | p99 latency increase <2x; zero error responses | ⬜ pending cloud E2E |
| Chaos: `kubectl delete pod gvdb-data-node-1 --force` | Failure recovery + read repair (0.2) | Replica promoted; shard re-replicated; no data divergence after recovery | ⬜ pending cloud E2E |
| Chaos: partition coordinator leader | Raft leader election (0b.4) | New leader elected <10s; writes resume | ⬜ blocked on 0b.4-followup |
| `helm upgrade --set coordinator.replicas=3` from 1 | Coordinator HA scale-up (0b.4) | Quorum formed; no data loss | 🟡 topology verified; election blocked on 0b.4-followup |
| `kubectl apply -f GVDBBackup` nightly | Operator CR works (0b.6) | Backup object appears in object store; status shows `lastBackupSizeBytes` | ⬜ 0b.6 not started |
| `kubectl apply -f GVDBRestore` | Restore works (0b.6) | All collections recreated with same cardinality | ⬜ 0b.6 not started |

### Per-cloud IAM validation

| Check | EKS | GKE | AKS |
|---|---|---|---|
| No static cloud credentials in any Secret | ✅ IRSA | ✅ Workload Identity | ✅ Azure WI |
| Pods reach object store | ✅ S3 | ✅ GCS | ✅ Azure Blob |
| Revoke IAM role → pods lose access | ✅ | ✅ | ✅ |

### Performance baseline (on each cloud)

Run the existing Go e2e load test suite (`make test-e2e-kind` adapted for cloud) with scaled config:

- 10M vectors, dim 768, IVF_TURBOQUANT index
- 1000 concurrent search QPS for 10 minutes
- Target: p99 < 50ms, zero errors, p50 cache hit > 90%

Per-cloud performance tolerance is ±20%. Deviations beyond that are investigated before release.

---

## Release Cadence

| Release | Gates | Contents | Status |
|---|---|---|---|
| v0.18.0 | 0b.1 DRAINING signal | Single PR | ✅ Shipped |
| v0.19.0 | 0b.2 auto-rebalance | Single PR | ✅ Shipped |
| v0.20.0 | 0b.4 + 0b.5 (chart scaffolding + hardening) | PR #70 | 🟡 Open — waiting on merge |
| v0.21.0 | 0b.4-followup (Raft election works in distributed mode) | Small PR | ⬜ Next |
| v0.22.0 | 0b.6 Operator | X-Large PR | ⬜ Last Tier 0b item |
| **v1.0.0-rc.*** | Tier 0b complete (0b.1–0b.6 + followup), verification matrix passes on kind | Tier 0b code + Operator | ⬜ |
| **v1.0.0** | Verification matrix passes on real EKS | Above + 1.EKS overlay | ⬜ |
| **v1.1.0** | Verification matrix passes on real GKE | Above + 1.GKE (GCS + overlay) | ⬜ |
| **v1.2.0** | Verification matrix passes on real AKS | Above + 1.AKS (Azure Blob + overlay) | ⬜ |
| **v1.3.0+** | Rolling | Tier 1.Full hardening sweep + subsequent feature work | ⬜ |

Revised rough calendar (remaining work from 2026-04-20):
- v0.20.0 (0b.4 + 0b.5 merged): days
- v0.21.0 (0b.4-followup: persistent GvdbStateManager + verify election): 1 week
- v0.22.0 (0b.6 Operator): 6–8 weeks
- v1.0.0 (Tier 0b complete, kind matrix green, EKS overlay, EKS verified): +3 days overlay + 1 week EKS E2E → total ~8–10 weeks
- v1.1.0 (+ GKE backend): +2 weeks
- v1.2.0 (+ Azure Blob): +2 weeks

Total remaining time to "all three clouds + hardening" is ~12–15 weeks. Compared to the original 16–20 week estimate in the initial reorg, Tier 0b work is ~50% complete and tracking ahead of schedule.
- v1.0.0 (EKS validated): ~12 weeks
- v1.1.0 (+ GKE): ~14 weeks
- v1.2.0 (+ AKS): ~16 weeks
- v1.3.0+ (hardening complete): ~18–20 weeks
