# Persistence

GVDB survives restarts without data loss. Every insert is journaled, segments flush to disk, and indexes rebuild from the persisted vectors on startup.

## Segment lifecycle

```
GROWING  →  SEALING  →  SEALED  →  FLUSHED
```

| State | Description |
|-------|-------------|
| `GROWING` | Accepts inserts. Backed by a memtable and WAL. |
| `SEALING` | No more inserts; index build is in progress. |
| `SEALED` | Index built, in memory. Queryable. |
| `FLUSHED` | Persisted to disk (and optionally [tiered storage](tiered-storage.md)). |

A collection has **one growing segment per shard** at a time; auto-seal rotates it when the size threshold is hit.

## Write-ahead log

- **16 MB buffer**, fsync every **1 s** by default.
- Every insert, upsert, update, delete, and TTL assignment is written to the WAL before returning.
- On startup, the WAL is replayed into the memtable before the node accepts traffic.

## Recovery on startup

1. Load the segment manifest (local or from object storage).
2. For each sealed segment: memory-map vectors, rebuild the index from raw vectors.
3. Replay the WAL into the growing segment's memtable.
4. Announce readiness.

This means **indexes are never persisted raw** — they are always rebuilt from vectors. This keeps the on-disk format stable across index-type migrations.

## Replication

- Data nodes replicate segments over gRPC (`ReplicateSegment` RPC).
- The coordinator auto-replicates under-replicated shards.
- Read repair runs in the background to fix divergence after failures.

## Flushing manually

In the rare cases where you want to force a flush:

```python
client.flush("my_collection")
```

This seals the current growing segment and blocks until it reaches `FLUSHED`.

## Further reading

- [Architecture — storage](../architecture/storage.md) for segment internals
- [Tiered storage](tiered-storage.md) for S3/MinIO offload
- [Architecture — consensus](../architecture/consensus.md) for metadata durability
