# Deploy with the Operator

The GVDB Operator manages clusters declaratively via the `GVDBCluster`, `GVDBBackup`, and `GVDBRestore` custom resources. It is the recommended path for production: it handles Raft-quorum-aware rolling upgrades, coordinator scale-up/down, data-node pre-drain on scale-in, and backup / restore.

## When to use the Operator vs raw Helm

| Need | Recommended path |
|------|------------------|
| Single-cluster dev / quick smoke test | Helm chart (`oci://ghcr.io/jonathanberhe/charts/gvdb`) |
| Multiple clusters, rolling upgrades, scale-in safety, backup/restore as CRs | Operator |
| GitOps with cluster spec in source control | Operator |
| You already operate other K8s operators (Prometheus, cert-manager, ...) | Operator |

The two paths are not exclusive: the operator chart only installs the controller and the CRDs. Existing Helm-installed clusters keep working.

## Install the operator

```bash
helm install gvdb-operator oci://ghcr.io/jonathanberhe/charts/gvdb-operator \
  --namespace gvdb-operator-system --create-namespace
```

This installs the controller Deployment, the `GVDBCluster` / `GVDBBackup` / `GVDBRestore` CRDs, and the RBAC the operator needs to reconcile clusters across namespaces.

Verify:

```bash
kubectl get deployment -n gvdb-operator-system
kubectl get crd | grep gvdb.io
```

## Create a cluster

```yaml title="gvdb-cluster.yaml"
apiVersion: gvdb.io/v1alpha1
kind: GVDBCluster
metadata:
  name: production
  namespace: gvdb
spec:
  image:
    repository: ghcr.io/jonathanberhe/gvdb
    tag: "0.34.0"
  coordinator:
    replicas: 3                  # Raft HA quorum
    storage:
      size: 2Gi
  dataNode:
    replicas: 3
    memoryLimitGb: 16
    terminationGracePeriodSeconds: 60
    storage:
      size: 50Gi
      storageClassName: gp3      # or premium-rwo (GKE) / managed-csi-premium (AKS)
  queryNode:
    replicas: 2
    memoryLimitGb: 16
    storage:
      size: 10Gi
  proxy:
    replicas: 2
    service:
      type: LoadBalancer
```

```bash
kubectl create namespace gvdb
kubectl apply -f gvdb-cluster.yaml
```

## Verify the cluster is healthy

```bash
kubectl get gvdbcluster -n gvdb
# NAME         PHASE
# production   Ready

kubectl get pods -n gvdb
# production-coordinator-0..2   Running
# production-data-node-0..2     Running
# production-query-node-0..1    Running
# production-proxy-...          Running
```

Resources are named `<cluster-name>-<workload>`, so the `production` cluster produces `production-coordinator`, `production-data-node`, and so on.

The operator surfaces per-subsystem status as conditions on the CR:

```bash
kubectl get gvdbcluster production -n gvdb -o jsonpath='{.status.conditions[*].type}'
```

You should see `Available`, `Progressing`, `CoordinatorRolloutReady`, `DataNodeRolloutReady`, `QueryNodeRolloutReady`, `ProxyRolloutReady`, `CoordinatorScaleReady`. Each carries a `reason` and `message` that pinpoints the gate when it isn't ready (for example, `WaitingForReplicaSafety: shard 42 has no ready replica besides node 102` during a data-node rollout).

## Connect

```bash
kubectl port-forward -n gvdb svc/production-proxy 50050:50050
```

Or, for a `type: LoadBalancer` service, point your client at the external IP:

```bash
kubectl get svc -n gvdb production-proxy
```

## Day-2 operations

### Rolling upgrade

```bash
kubectl patch gvdbcluster production -n gvdb --type=merge \
  -p '{"spec":{"image":{"tag":"0.35.0"}}}'
```

The operator walks each StatefulSet partition by partition, gated by:

- **Coordinator**: Raft leader present + term stable across the new pod
- **Data node**: cluster health + rebalance quiescence + per-shard replica safety (RF=1 surfaces an `RF1Blocked` Event rather than risking data loss)
- **Query node** / **Proxy**: sequenced after coordinator + data-node, mechanical because they are stateless

Watch progress:

```bash
kubectl get gvdbcluster production -n gvdb -w
```

### Scale data nodes

```bash
kubectl patch gvdbcluster production -n gvdb --type=merge \
  -p '{"spec":{"dataNode":{"replicas":5}}}'
```

- **Scale up**: existing rebalancer migrates shards onto new pods once they join the registry.
- **Scale down**: operator holds `spec.replicas` at the current value and drains shard assignments off the target ordinals **before** the StatefulSet shrinks, so no shard is left without a replica.

### Scale coordinators

```bash
kubectl patch gvdbcluster production -n gvdb --type=merge \
  -p '{"spec":{"coordinator":{"replicas":5}}}'
```

Scale-up: new pods auto-join the Raft cluster via `JoinCluster` RPC. Scale-down: operator transfers leadership off any ordinal being removed, then calls `RemovePeer` so the Raft config does not accumulate ghost members. The `CoordinatorScaleReady` condition surfaces progress.

### Backup a collection

```yaml title="gvdb-backup.yaml"
apiVersion: gvdb.io/v1alpha1
kind: GVDBBackup
metadata:
  name: products-nightly
  namespace: gvdb
spec:
  clusterRef:
    name: production
  collection: products
  target:
    s3:
      bucket: gvdb-backups
      prefix: prod
```

```bash
kubectl apply -f gvdb-backup.yaml
kubectl get gvdbbackup products-nightly -n gvdb -w
```

For PVC-backed backups (`target.local.path`) the data-node pods must be configured with `storage.local_backup_dir` covering that path. See [Backup and restore](../features/backup-restore.md) for the full data-flow.

### Restore from a backup

```yaml title="gvdb-restore.yaml"
apiVersion: gvdb.io/v1alpha1
kind: GVDBRestore
metadata:
  name: products-rollback
  namespace: gvdb
spec:
  clusterRef:
    name: production
  fromBackupRef:
    name: products-nightly
  # Optional: restore into a different collection name. Empty uses the
  # name from the backup manifest.
  targetCollection: products
  # NewCollection (default) refuses if the target already exists;
  # Overwrite drops and recreates it from the backup.
  mode: NewCollection
```

For DR across clusters, omit `fromBackupRef` and point at the source directly with `target.s3` + `backupID` instead.

```bash
kubectl apply -f gvdb-restore.yaml
```

## Uninstall

Delete the cluster CR first; the operator's finalizer removes cluster-scoped `PriorityClass` objects before the namespaced resources cascade.

```bash
kubectl delete gvdbcluster production -n gvdb
kubectl delete namespace gvdb
helm uninstall gvdb-operator -n gvdb-operator-system
```

## What the operator does **not** cover (yet)

- A pause-reconciliation annotation for break-glass manual changes
- `HorizontalPodAutoscaler` wiring for proxy / query-node
- GCS / Azure object-store backends for backup destinations (S3 / MinIO and PVC-local only today)

See [`CLOUD_NATIVE.md`](https://github.com/JonathanBerhe/gvdb/blob/main/CLOUD_NATIVE.md) for the in-flight roadmap.

## See also

- [Deploy with Helm](deploy-helm.md): the standalone Helm chart for clusters not managed by the operator
- [Distributed cluster](../getting-started/distributed-cluster.md): topology overview and client connection
- [Backup and restore](../features/backup-restore.md): protocol-level detail behind `GVDBBackup` / `GVDBRestore`
