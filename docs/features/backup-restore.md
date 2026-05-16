# Backup and Restore

GVDB takes consistent point-in-time backups of a collection and restores
them into the same or a different cluster. Single-shard, multi-shard,
and operator-driven flows all use the same on-disk format.

## When to use it

- **Disaster recovery.** Restore a collection after a region outage or
  an operator mistake.
- **Cross-cluster migration.** Copy a collection from staging into prod
  or onto a freshly-rebuilt cluster.
- **Compliance snapshots.** Retain a tamper-evident copy of the
  collection on object storage.

The point-in-time semantics are **per shard**: each shard is briefly
write-fenced while its segments are uploaded, then immediately released.
Reads continue throughout.

## Architecture in one paragraph

A backup is a fan-out: the coordinator picks each shard's primary, calls
`FreezeWrites` on it, dispatches `BackupShard` to upload that shard's
segments to the target object store, then `UnfreezeWrites`. Once every
shard succeeds, the coordinator writes a top-level
`backup.manifest.json` that ties the per-shard manifests together. A
restore is the inverse: read the top manifest, drop-and-recreate the
target collection from the recorded metadata, then dispatch
`RestoreShard` to each primary.

The top-level manifest is written **last**. A backup that crashes
mid-flight leaves orphan segment objects but no manifest — so a restore
that can't find the top manifest treats the backup as nonexistent.

## Backup targets

`BackupTarget` is a `oneof` of two shapes:

```text
target:
  s3:
    bucket: gvdb-backups
    prefix: prod          # optional; final layout is <prefix>/backups/<id>/...
# or
  local:
    path: /var/lib/gvdb/backups   # must lie under storage.local_backup_dir
```

- **S3 / MinIO.** The cluster's data-nodes and coordinator must be
  configured with credentials for the bucket (typically via IRSA /
  Workload Identity). The operator does not transport credentials in
  the CR.
- **Local filesystem.** A path on a PVC mounted by the data-node pods.
  Must lie under the server-side allowlist root
  (`storage.local_backup_dir` in the YAML config); paths that escape
  it are rejected.

## Operator-driven flow (GVDBBackup / GVDBRestore)

The recommended path. The operator handles dialing, polling, and status
plumbing.

```yaml
apiVersion: gvdb.io/v1alpha1
kind: GVDBBackup
metadata:
  name: products-nightly
  namespace: default
spec:
  clusterRef:
    name: gvdb-prod
  collection: products
  target:
    s3:
      bucket: gvdb-backups
      prefix: prod
```

Apply it (`kubectl apply -f ...`); the operator starts the backup,
polls `GetBackupStatus` every 5 s, and lands a Kubernetes status:

```text
NAME              PHASE      COLLECTION  SHARDS  SIZE         AGE
products-nightly  Completed  products    4       2147483648   42s
```

To restore, point a `GVDBRestore` at the produced backup CR:

```yaml
apiVersion: gvdb.io/v1alpha1
kind: GVDBRestore
metadata:
  name: products-restore
spec:
  clusterRef:
    name: gvdb-prod
  fromBackupRef:
    name: products-nightly
  targetCollection: products
  mode: NewCollection     # or "Overwrite" to drop-and-recreate
```

For DR scenarios where the backup was taken in a different cluster (so
there's no `GVDBBackup` CR to reference), set the source explicitly:

```yaml
spec:
  clusterRef:
    name: gvdb-prod-dr
  target:
    s3:
      bucket: gvdb-backups
      prefix: prod
  backupID: bk-20260101T000000Z-abc123
  targetCollection: products
  mode: Overwrite
```

## Direct gRPC flow

When you don't have the operator, call the proxy directly:

```python
from gvdb.proto import vectordb_pb2 as pb

# Start
resp = client.BackupCollection(pb.BackupCollectionRequest(
    collection_name="products",
    target=pb.BackupTarget(s3=pb.S3Target(bucket="gvdb-backups", prefix="prod")),
))
backup_id = resp.backup_id

# Poll
while True:
    s = client.GetBackupStatus(pb.GetBackupStatusRequest(backup_id=backup_id))
    if s.state in (pb.BACKUP_COMPLETED, pb.BACKUP_FAILED, pb.BACKUP_CANCELLED):
        break
    time.sleep(1)
print(s.state, s.manifest_uri)
```

The proxy forwards admin RPCs to the coordinator, which fans out per
shard. Single-node binaries skip the fan-out and run the backup
in-process against the local segment store.

## Permissions

| Role | BACKUP | RESTORE |
|------|:------:|:-------:|
| `admin` | yes | yes |
| `collection_admin` | yes | no |
| `readwrite` | yes | no |
| `readonly` | no | no |

`RESTORE` is gated like `CREATE_COLLECTION` / `DROP_COLLECTION` —
admin-only — because every restore either creates or replaces a
collection.

## What is and isn't captured

| | Captured |
|---|---|
| Vectors + IDs | yes |
| Per-vector metadata | yes |
| Sparse vectors | yes |
| TTL expiry timestamps | yes |
| Collection metadata (dimension, metric, index type, shard count, replication factor) | yes |
| Indexes (`index.faiss`) | yes — copied as-is for sealed segments |
| GROWING segment state | yes — snapshotted via `Segment::SerializeToBytes` under a shared lock |
| Coordinator metadata for collections **other** than the one being backed up | no |
| Raft state / cluster topology | no |
| API keys / RBAC config | no |

## Operating notes

- **Write fence is per shard, not per collection.** Each shard is
  frozen only while its own upload is in flight (~milliseconds to
  seconds, depending on segment size and bandwidth). A multi-shard
  backup interleaves the fences; the collection as a whole is never
  fully blocked.
- **Lease auto-expiry caps the blast radius.** A `FreezeWrites` lease
  is 60 s by default — if the coordinator crashes mid-backup, the
  data-node releases the fence on its own and writes resume.
- **Crash-safety across coordinator restart is best-effort.** The
  multi-shard job state lives in the coordinator's memory; if the
  coordinator restarts mid-backup, the operator marks the CR
  `Failed` (status RPC returns NotFound) and the backup is retried.
  Partial uploads under the target prefix are cleaned up on failure.

## Non-goals (today)

- **Incremental backups.** The proto and manifest carry
  `incremental_from` / `parent_segment_ids` placeholders, but full
  backups are always taken.
- **WAL-based PITR.** Backups are point-in-time snapshots, not
  log-based PITR.
- **Client-side encryption.** Rely on S3 SSE / MinIO encrypt-at-rest.
- **In-place restore over a live collection.** `Overwrite` mode is
  implemented as drop + create; in-flight writes during the window
  return `NotFound` and retry.

## Testing

```bash
# C++ unit + integration tests for backup/restore primitives
make test                                              # all suites
ctest --test-dir build -R "Backup|Restore|ShardWriteGate"

# Coordinator-orchestrated fan-out test
ctest --test-dir build -R "CoordinatorBackup"

# Operator envtest controllers
cd operator && make test
```

## Further reading

- [Persistence](persistence.md) — segment lifecycle backups rely on
- [Tiered storage](tiered-storage.md) — same `IObjectStore` abstraction
- [RBAC](rbac.md) — permission model for `BACKUP` / `RESTORE`
