# Storage

Vector storage in GVDB is segment-based and LSM-style. New data lands in a growing in-memory segment, rotates to an immutable sealed segment, and flushes to disk (and optionally to object storage).

## Segment lifecycle

```
GROWING  →  SEALING  →  SEALED  →  FLUSHED
```

- **GROWING** — accepts inserts. Backed by a memtable + WAL.
- **SEALING** — no more inserts; index build in progress.
- **SEALED** — index built and in memory; queryable.
- **FLUSHED** — written to local disk, then optionally uploaded to object storage.

Auto-seal rotates the growing segment when a configurable size threshold is hit. A background build queue prioritizes sealed segments for index construction.

## Write path

1. Client insert → gRPC proxy → primary data node for the shard
2. Data node appends to WAL (16 MB buffer, fsync every 1 s)
3. Entry added to memtable of the growing segment
4. Ack returned to client
5. On seal, the memtable is frozen; index build kicks off
6. On flush, segment vectors + metadata + index-rebuild hints are written to `data_dir/`

## Read path

1. Client search → gRPC proxy → query node
2. Query node consults coordinator for shard→data-node mapping (cached)
3. Fan-out to data nodes holding the relevant shards (`ExecuteShardQuery`)
4. Each data node runs the index on each of its relevant segments, merges locally
5. Query node merges across shards, returns top-k

## Indexes

- Rebuilt from raw vectors on startup (not persisted as index binaries)
- This keeps the on-disk format stable across index-type migrations
- `IndexManager` enforces a memory budget; indexes for cold segments can be evicted and rebuilt on demand

## Scalar metadata indexes

Per-field structures to accelerate [metadata filtering](../features/metadata-filtering.md):

- **Bitmap inverted index** — for equality on strings/booleans
- **Sorted numeric index** — for range queries

## Tiered storage

With `-DGVDB_WITH_S3=ON` and `storage.object_store.enabled: true`:

- Sealed segments upload asynchronously to S3/MinIO
- A manifest in the bucket tracks every segment for fast startup discovery
- **LRU cache** on local disk holds recently-accessed segments
- Reads on a missing segment block briefly while the download completes

See [tiered storage](../features/tiered-storage.md).

## Replication

- Data nodes replicate segments over gRPC (`ReplicateSegment` RPC)
- Coordinator watches for under-replicated shards and triggers re-replication
- Read repair runs in the background to reconcile replicas after recovery

## Compaction

Background task merges small segments and skips deleted vectors to keep segment count bounded. Runs on data nodes with configurable idleness windows.

## Related code

- `include/storage/segment_store.h` — `ISegmentStore` interface
- `src/storage/segment_manager.cpp` — local-only impl
- `src/storage/tiered_segment_manager.cpp` — composes local + S3 + cache
- `include/storage/object_store.h` — `IObjectStore` abstract
- `src/storage/s3_object_store.cpp` — AWS SDK impl

## See also

- [Modules](modules.md)
- [Persistence](../features/persistence.md)
- [Tiered storage](../features/tiered-storage.md)
