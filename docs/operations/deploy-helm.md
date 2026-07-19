# Deploy with Helm

The official Helm chart is published to GitHub Container Registry as an OCI artifact.

## Install

```bash
helm install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --create-namespace
```

Connect from outside the cluster:

```bash
kubectl port-forward -n gvdb svc/gvdb-proxy 50050:50050
```

## Upgrade

```bash
helm upgrade gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb \
  --set image.tag=v1.2.0
```

## Values reference

The chart is intentionally minimal. Exposed keys mirror [`deploy/helm/gvdb/values.yaml`](https://github.com/JonathanBerhe/gvdb/blob/main/deploy/helm/gvdb/values.yaml) — follow that file for the source of truth.

### `image`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `image.repository` | `gvdb` | Container image name |
| `image.tag` | `""` (falls back to `Chart.appVersion`) | Image tag |
| `image.pullPolicy` | `IfNotPresent` | |

### `coordinator`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `coordinator.replicas` | `1` | Coordinators (use 3 for Raft quorum in production) |
| `coordinator.singleNode` | `true` | Run as single-node embedded coordinator |
| `coordinator.resources` | see values.yaml | CPU/memory requests and limits |
| `coordinator.storage.size` | `1Gi` | PVC size for Raft log |
| `coordinator.storage.storageClass` | `""` (cluster default) | StorageClass override |

### `dataNode`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dataNode.replicas` | `2` | Data nodes — scale for storage + insert throughput |
| `dataNode.memoryLimitGb` | `4` | Memory budget for vector storage |
| `dataNode.resources` | see values.yaml | CPU/memory requests and limits |
| `dataNode.storage.size` | `5Gi` | PVC size per data node |
| `dataNode.storage.storageClass` | `""` | StorageClass override |

### `queryNode`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `queryNode.replicas` | `1` | Query nodes — scale for QPS |
| `queryNode.memoryLimitGb` | `4` | Memory budget |
| `queryNode.resources` | see values.yaml | CPU/memory requests and limits |
| `queryNode.storage.size` | `2Gi` | PVC size |
| `queryNode.storage.storageClass` | `""` | StorageClass override |

### `proxy`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `proxy.replicas` | `1` | Proxy replicas (horizontally scalable) |
| `proxy.resources` | see values.yaml | CPU/memory requests and limits |
| `proxy.service.type` | `ClusterIP` | `ClusterIP`, `NodePort`, or `LoadBalancer` |
| `proxy.service.port` | `50050` | gRPC port |
| `proxy.service.nodePort` | `""` | Explicit NodePort when `type: NodePort` |

### `config` (server-side config ConfigMap)

| Parameter | Default |
|-----------|---------|
| `config.server.maxMessageSizeMb` | `256` |
| `config.server.maxConcurrentStreams` | `1000` |
| `config.storage.segmentMaxSizeMb` | `512` |
| `config.storage.walBufferSizeMb` | `64` |
| `config.storage.enableCompression` | `true` |
| `config.storage.compactionThreads` | `2` |
| `config.index.defaultIndexType` | `"HNSW"` |
| `config.index.hnswM` | `16` |
| `config.index.hnswEfConstruction` | `200` |
| `config.index.hnswEfSearch` | `100` |
| `config.logging.level` | `"info"` |
| `config.logging.consoleEnabled` | `true` |
| `config.logging.fileEnabled` | `false` |

### `ui`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ui.enabled` | `false` | Deploy the GVDB Web UI alongside the cluster |
| `ui.image.repository` | `ghcr.io/jonathanberhe/gvdb-ui` | UI image |
| `ui.image.tag` | `latest` | UI tag (the UI image is not yet built/pushed by CI; enabling currently requires manually publishing a tag) |
| `ui.port` | `8080` | Container port |
| `ui.service.type` | `ClusterIP` | |
| `ui.service.port` | `8080` | |

## Hardening primitives (production)

All hardening features default to `enabled: false` so the dev/kind install is unchanged. Enable them in a production overlay. Per-workload toggles let you adopt incrementally — e.g. PDB everywhere first, then NetworkPolicy.

### Per-workload: PDB, anti-affinity, zone-spread, ServiceAccount, PriorityClass

Each of `coordinator`, `dataNode`, `queryNode`, `proxy` exposes the same shape:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `<workload>.podDisruptionBudget.enabled` | `false` | Emit a PodDisruptionBudget. The chart fails the render if `minAvailable > replicas`. |
| `<workload>.podDisruptionBudget.minAvailable` | `1` | For HA Raft, set to `replicas - 1` (e.g. 2 with `replicas: 3`) so K8s cannot break quorum. |
| `<workload>.podAntiAffinity.enabled` | `false` | Spread replicas across `topologyKey`. |
| `<workload>.podAntiAffinity.type` | `preferred` | `preferred` (soft) or `required` (hard). |
| `<workload>.podAntiAffinity.topologyKey` | `kubernetes.io/hostname` | Use `topology.kubernetes.io/zone` for AZ spread. |
| `<workload>.zoneSpread.enabled` | `false` | Add a `topologySpreadConstraints` entry on `topology.kubernetes.io/zone`. StatefulSets default to `whenUnsatisfiable: DoNotSchedule` (refuse to colocate replicas in one AZ — data-safety contract); the proxy uses `ScheduleAnyway` (availability over balance). |
| `<workload>.zoneSpread.maxSkew` | `1` | |
| `<workload>.topologySpreadConstraints` | `[]` | Raw user-provided list, appended after `zoneSpread`. Explicit overrides win. |
| `<workload>.serviceAccount.create` | `false` | Create a per-workload ServiceAccount. |
| `<workload>.serviceAccount.name` | `""` | Override generated name. |
| `<workload>.serviceAccount.annotations` | `{}` | Cloud IAM annotations: `eks.amazonaws.com/role-arn` (IRSA), `iam.gke.io/gcp-service-account` (Workload Identity), `azure.workload.identity/client-id` (AKS). |
| `<workload>.priorityClassName` | `""` | Auto-wired to `<release>-<namespace>-<workload>` when `priorityClasses.create=true`. |
| `<workload>.nodeSelector` | `{}` | Pin pods to nodes matching these labels (e.g. dedicated vector-DB node pool). |
| `<workload>.tolerations` | `[]` | Tolerate taints on dedicated nodes (`key: dedicated, operator: Exists, effect: NoSchedule`). |
| `<workload>.podAnnotations` | `{}` | Propagated to pod template metadata for VPA, Datadog, Istio, Velero, Prometheus scrape hints, etc. |

Pods carry the standard recommended labels (`app.kubernetes.io/name`, `instance`, `version`, `component`, `part-of`, `managed-by`) so tools like Prometheus and Loki can scope queries by component (`coordinator`, `data-node`, `query-node`, `proxy`) without relying on the chart-internal `app` selector label.

Example: 3 coordinator replicas across 3 AZs, with a PDB protecting Raft quorum.

```yaml title="values.prod.yaml"
coordinator:
  replicas: 3
  singleNode: false
  podDisruptionBudget:
    enabled: true
    minAvailable: 2          # quorum = floor(3/2)+1 = 2; never go below
  podAntiAffinity:
    enabled: true
    type: required
    topologyKey: topology.kubernetes.io/zone
  zoneSpread:
    enabled: true
    maxSkew: 1
```

### `priorityClasses`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `priorityClasses.create` | `false` | Create cluster-scoped PriorityClasses for all four workloads. The names embed the namespace so two installs in different namespaces don't collide. |
| `priorityClasses.coordinator.value` | `1000000` | |
| `priorityClasses.dataNode.value` | `900000` | |
| `priorityClasses.queryNode.value` | `800000` | |
| `priorityClasses.proxy.value` | `700000` | |

All values reserve headroom under K8s `system-cluster-critical` (`2000000000`).

### `networkPolicy`

Per-workload `NetworkPolicy` resources allowing only the gRPC, Raft, and metrics traffic the chart documents, plus DNS egress to kube-system CoreDNS. Requires a CNI that enforces NetworkPolicy (kindnet 0.20+, Calico, Cilium, EKS VPC CNI with NP enabled, GKE NetworkPolicy add-on, AKS Calico, etc.).

| Parameter | Default | Description |
|-----------|---------|-------------|
| `networkPolicy.enabled` | `false` | Emit policies for all four workloads. |
| `networkPolicy.dns.namespaceSelectorLabels` | `{}` (defaults to `kubernetes.io/metadata.name: kube-system`) | Override if your cluster lacks the K8s 1.22+ automatic NS label or runs CoreDNS elsewhere. |
| `networkPolicy.dns.podSelectorLabels` | `{}` (defaults to `k8s-app: kube-dns`) | Override if CoreDNS uses non-standard labels. |
| `networkPolicy.proxy.clientCIDRs` | `[]` | **Empty blocks all external proxy traffic.** Set to e.g. `["10.0.0.0/8"]` for VPC-only, `["0.0.0.0/0"]` for fully public (only with auth). |
| `networkPolicy.<workload>.extraIngress` | `[]` | Additional ingress rules appended verbatim (e.g. allow scrape from a non-default Prometheus namespace). |
| `networkPolicy.<workload>.extraEgress` | `[]` | Additional egress rules appended verbatim. |

### `metrics` (Prometheus Operator)

Emits PodMonitor for the three StatefulSet workloads (coordinator, data-node, query-node) — each replica binds metrics on a different ordinal-derived port, so PodMonitor relabel rules rewrite `__address__` to `<pod_ip>:<port>`. The proxy uses ServiceMonitor (fixed port 9050).

| Parameter | Default | Description |
|-----------|---------|-------------|
| `metrics.serviceMonitor.enabled` | `false` | Emit Pod/ServiceMonitor resources. Requires Prometheus Operator CRDs (`monitoring.coreos.com/v1`). |
| `metrics.serviceMonitor.namespace` | `""` (release namespace) | Set when Prometheus does not select cross-namespace. |
| `metrics.serviceMonitor.interval` | `30s` | Scrape interval. |
| `metrics.serviceMonitor.scrapeTimeout` | `10s` | |
| `metrics.serviceMonitor.additionalLabels` | `{}` | Must include whatever your Prometheus's `serviceMonitorSelector` / `podMonitorSelector` matches (often `release: kube-prometheus-stack` for the kube-prometheus-stack default). |

Coordinator metrics ports: `9091, 9092, ...` (`9090 + node_id`, `node_id = ordinal + 1`). Data-node: `9101, 9102, ...`. Query-node: `9201, 9202, ...`. Proxy: fixed `9050`.

## Production overlay example

Combines every hardening gate. Pair with a CNI that enforces NetworkPolicy and a Prometheus Operator install (e.g. kube-prometheus-stack).

```yaml title="values.prod.yaml"
image:
  tag: v1.2.0

coordinator:
  replicas: 3
  singleNode: false
  podDisruptionBudget: { enabled: true, minAvailable: 2 }
  podAntiAffinity: { enabled: true, type: required, topologyKey: topology.kubernetes.io/zone }
  zoneSpread: { enabled: true, maxSkew: 1 }
  serviceAccount: { create: true }

dataNode:
  replicas: 3
  memoryLimitGb: 16
  storage: { size: 200Gi, storageClass: gp3 }
  podDisruptionBudget: { enabled: true, minAvailable: 2 }
  podAntiAffinity: { enabled: true, type: required, topologyKey: topology.kubernetes.io/zone }
  zoneSpread: { enabled: true, maxSkew: 1 }
  serviceAccount:
    create: true
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/gvdb-data-node

queryNode:
  replicas: 3
  podDisruptionBudget: { enabled: true, minAvailable: 1 }
  podAntiAffinity: { enabled: true, type: preferred, topologyKey: topology.kubernetes.io/zone }
  zoneSpread: { enabled: true }

proxy:
  replicas: 2
  service: { type: LoadBalancer }
  podDisruptionBudget: { enabled: true, minAvailable: 1 }
  zoneSpread: { enabled: true }

priorityClasses:
  create: true

networkPolicy:
  enabled: true
  proxy:
    clientCIDRs: ["10.0.0.0/8"]    # VPC-only — set to your client CIDR

metrics:
  serviceMonitor:
    enabled: true
    additionalLabels:
      release: kube-prometheus-stack

security:
  podSecurityContext:
    runAsNonRoot: true
    runAsUser: 1000
    fsGroup: 1000
    seccompProfile: { type: RuntimeDefault }
  containerSecurityContext:
    allowPrivilegeEscalation: false
    capabilities: { drop: [ALL] }
    readOnlyRootFilesystem: true
```

## Cloud overlays

A starter overlay for AWS EKS ships with the chart at [`deploy/helm/gvdb/values-eks.yaml`](https://github.com/JonathanBerhe/gvdb/blob/main/deploy/helm/gvdb/values-eks.yaml). It sets only the AWS-specific knobs (gp3 storage classes, AWS Load Balancer Controller NLB annotations on the proxy service with an internal scheme, a data-node ServiceAccount ready for either EKS Pod Identity or an IRSA `eks.amazonaws.com/role-arn`, zone spread on all stateful workloads). Compose it with your prod values overlay:

```bash
helm install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --create-namespace \
  -f deploy/helm/gvdb/values-eks.yaml \
  -f values.prod.yaml
```

A Google GKE overlay ships alongside it at [`deploy/helm/gvdb/values-gke.yaml`](https://github.com/JonathanBerhe/gvdb/blob/main/deploy/helm/gvdb/values-gke.yaml): `standard-rwo`/`premium-rwo` persistent-disk storage classes, an internal L4 load balancer for the proxy (`networking.gke.io/load-balancer-type: Internal`), a data-node ServiceAccount ready for a Workload Identity `iam.gke.io/gcp-service-account` binding (which the [GCS backend](../features/tiered-storage.md) authenticates through), and zone spread. Compose it the same way:

```bash
helm install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --create-namespace \
  -f deploy/helm/gvdb/values-gke.yaml \
  -f values.prod.yaml
```

An AKS overlay is not yet shipped (pending the Azure Blob object-store backend).

## Pre-upgrade health check

Set `preUpgradeHook.enabled: true` to install a Helm `pre-upgrade` hook that verifies every GVDB workload is healthy before the upgrade proceeds. The hook is a short-lived `Job` (with a scoped `ServiceAccount` + `Role` + `RoleBinding` scoped to `get` on `statefulsets` and `deployments` in the release namespace) that runs `kubectl rollout status` against the coordinator, data-node, and query-node StatefulSets and the proxy Deployment. If any one is mid-rollout or missing Ready replicas past `preUpgradeHook.timeoutSeconds` (default 60s per workload), the hook exits non-zero and Helm aborts the upgrade.

The hook does not run on `helm install` (only `helm upgrade`), and the SA/Role/RoleBinding/Job are deleted automatically on success (`helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded`). Pin `preUpgradeHook.image` to a `bitnami/kubectl` tag compatible with your cluster's K8s minor version.

## Ingress / Gateway API

For client access without a cloud LoadBalancer, set `ingress.enabled: true` and pick one of two backends.

**Legacy Ingress** (`ingress.kind: Ingress`, the default) for `networking.k8s.io/v1` controllers like nginx-ingress, AWS Load Balancer Controller, or GCE Ingress. gRPC needs the controller-specific backend-protocol annotation:

```yaml
ingress:
  enabled: true
  host: gvdb.example.com
  className: nginx
  annotations:
    nginx.ingress.kubernetes.io/backend-protocol: GRPC
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
  tls:
    secretName: gvdb-tls
```

**Gateway API GRPCRoute** (`ingress.kind: GRPCRoute`) for `gateway.networking.k8s.io/v1` implementations (Istio, Envoy Gateway, Cilium, GKE Gateway, etc.). The `Gateway` itself must already exist in the cluster:

```yaml
ingress:
  enabled: true
  kind: GRPCRoute
  host: gvdb.example.com
  gateway:
    name: public-gw
    namespace: gateway-system    # optional
```

In both cases the chart routes traffic to the `<release>-proxy` Service on `proxy.service.port`. `ingress.host` is required when `ingress.enabled: true`; the render fails fast if it's empty.

## TLS (cert-manager or an existing Secret)

Set `tls.enabled: true` to turn on TLS. Every workload then mounts a TLS Secret at `/etc/gvdb/tls` and starts with `--config /etc/gvdb/config.yaml`, and the rendered server config gains a `server.tls` block pointing at `tls.crt` / `tls.key` / `ca.crt`. Consensus and routing still come from CLI flags, so enabling TLS does not disturb distributed mode.

Provide the keypair one of two ways. **cert-manager** (`tls.certManager.enabled: true`) emits a `Certificate` that a `ClusterIssuer`/`Issuer` you run signs into the `<release>-tls` Secret, with SANs covering every workload Service and the headless per-pod FQDNs (so one cert validates all inter-node connections):

```yaml
tls:
  enabled: true
  mutualTls: true                 # require + verify client certs
  certManager:
    enabled: true
    issuerRef:
      name: gvdb-ca
      kind: ClusterIssuer
```

Or point at a **Secret you manage** (from any source) and skip cert-manager:

```yaml
tls:
  enabled: true
  existingSecret: my-gvdb-tls     # must hold tls.crt / tls.key (+ ca.crt for mTLS)
```

`tls.certManager.issuerRef.name` is required when cert-manager is enabled; the render fails fast if it's empty. cert-manager and its issuer must already be installed in the cluster.

## External Secrets Operator

Set `externalSecrets.enabled: true` to emit an `ExternalSecret` (`external-secrets.io/v1`) that syncs secrets from AWS Secrets Manager / GCP Secret Manager / Azure Key Vault / Vault into a Kubernetes Secret, via a `(Cluster)SecretStore` you configure. Common uses: supply the TLS keypair (point `tls.existingSecret` at `externalSecrets.target.name`) or an API-key file.

```yaml
externalSecrets:
  enabled: true
  secretStoreRef:
    name: aws-secrets-manager
    kind: ClusterSecretStore
  target:
    name: gvdb-tls               # consumed by tls.existingSecret, or mounted yourself
  data:
    - secretKey: tls.crt
      remoteRef: { key: prod/gvdb/tls, property: cert }
    - secretKey: tls.key
      remoteRef: { key: prod/gvdb/tls, property: key }
```

Requires the External Secrets Operator CRDs installed. `externalSecrets.secretStoreRef.name` is required when enabled.

## Object storage (tiered storage)

Set `objectStore.enabled: true` to render a `storage.object_store` block into the server config and start the config-consuming workloads (coordinator, data-node, proxy) with `--config`. Sealed segments then upload to the object store automatically; see [Tiered storage](../features/tiered-storage.md) for how the tiering works.

```yaml
objectStore:
  enabled: true
  type: s3                       # "s3" | "minio" | "gcs"
  endpoint: http://minio.storage.svc.cluster.local:9000
  bucket: gvdb-segments
  region: us-east-1              # S3 only
  prefix: gvdb
  useSsl: false                  # plain-HTTP MinIO
  cacheSizeMb: 256               # local LRU cache for downloaded segments
  uploadThreads: 2
  # GCS only:
  project: ""                    # usually supplied by ADC
  credentialsPath: ""            # service-account JSON; prefer Workload Identity
```

For S3/MinIO auth in production, leave `accessKey` / `secretKey` empty and grant bucket access via IRSA (EKS) or Workload Identity (GKE). The static credential values are for dev/MinIO only: they render into the ConfigMap the pods read. The S3 and GCS backends require an image built with `-DGVDB_WITH_S3=ON` / `-DGVDB_WITH_GCS=ON` (the published images include S3).

## What the chart does **not** surface (yet)

The following are **not Helm-parameterized**. Configure them by mounting a custom `gvdb-config.yaml` ConfigMap / Secret that overrides the values the chart renders, or patch the StatefulSet directly:

- **Authentication** / **RBAC**: API keys, RBAC users (see [Security](security.md))
- **Audit logging**

Contributions to expose these in the Helm chart are welcome.

```bash
helm upgrade --install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --create-namespace \
  -f values.prod.yaml
```

## See also

- [Distributed cluster](../getting-started/distributed-cluster.md) — walkthrough
- [Configuration](configuration.md) — the server-side YAML the chart renders
- [Security](security.md) — how to layer auth/TLS on top of the chart
- [Monitoring](monitoring.md) — Prometheus scrape setup with the chart's PodMonitor/ServiceMonitor
