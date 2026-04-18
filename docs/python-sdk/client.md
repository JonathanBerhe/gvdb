# Client API

Reference for `GVDBClient`. Signatures mirror [`clients/python/gvdb/client.py`](https://github.com/JonathanBerhe/gvdb/blob/main/clients/python/gvdb/client.py) — follow that file for the source of truth.

## Connection

```python
from gvdb import GVDBClient

client = GVDBClient(
    "localhost:50051",
    api_key="your-key",     # optional — required if RBAC is enabled
    timeout=30.0,           # per-RPC timeout in seconds
)

client.health_check()       # -> status string
client.get_stats()          # -> dict of server stats
client.close()
```

`GVDBClient` is a context manager:

```python
with GVDBClient("localhost:50051") as client:
    client.health_check()
```

The client uses an **insecure gRPC channel**. TLS is configured on the server side; TLS-enabled endpoints need a custom channel factory (see the source for details).

## Collection management

```python
client.create_collection(
    "my_collection",
    dimension=768,
    metric="l2",              # "l2" | "ip" | "cosine"
    index_type="hnsw",        # "flat" | "hnsw" | "ivf_flat" | "ivf_pq"
                              # | "ivf_sq" | "turboquant" | "ivf_turboquant" | "auto"
    num_shards=0,             # 0 = server default
)
# returns the new collection_id (int)

client.list_collections()     # -> list[CollectionInfo(name, id, dimension, vector_count)]
client.drop_collection("my_collection")
```

## Writes

### Insert

```python
inserted_count = client.insert(
    "my_collection",
    ids=[1, 2, 3],
    vectors=[[0.1]*768, [0.2]*768, [0.3]*768],
    metadata=[{"cat": "a"}, {"cat": "b"}, {"cat": "c"}],   # optional
    sparse_vectors=[{42: 0.8}, {137: 0.3}, {2048: 1.2}],   # optional, dict[int, float]
    ttl_seconds=[3600, 3600, 0],                           # optional; 0 = no expiration
)
```

Notes:

- **Sparse vectors** are plain `dict[int, float]` — there is no `SparseVector` class.
- **`ttl_seconds`** is a **relative** duration (not an absolute timestamp).
- Metadata values can be `int`, `float`, `str`, or `bool`. Nested dicts or lists are not preserved.

### Upsert

```python
result = client.upsert(
    "my_collection",
    ids=[1, 2],
    vectors=[[0.1]*768, [0.2]*768],
    metadata=[{"cat": "a"}, {"cat": "b"}],
)
# result == {"upserted_count": 2, "inserted_count": 1, "updated_count": 1}
```

### Stream insert

For large batches, use `stream_insert` — it chunks the ids/vectors into `batch_size` pieces and uses a gRPC client-streaming RPC:

```python
total = client.stream_insert(
    "my_collection",
    ids=list(range(1_000_000)),
    vectors=big_vector_list,
    batch_size=10_000,
    metadata=None,
)
```

### Delete

```python
deleted_count = client.delete("my_collection", ids=[1, 2])
```

Deletion by metadata filter is not exposed in the current SDK.

### Update metadata

Update the metadata for a single vector:

```python
client.update_metadata(
    "my_collection",
    vector_id=42,
    metadata={"cat": "updated"},
    merge=True,     # True: patch existing keys; False: replace entire metadata
)
```

## Reads

### Get by ID

```python
rows = client.get("my_collection", ids=[1, 2, 3])
# rows == [{"id": 1, "vector": [...], "metadata": {...}}, ...]
```

Returns a list of dicts with `id`, `vector`, and `metadata` (present only if the vector has metadata).

### Search

```python
results = client.search(
    "my_collection",
    query_vector=[0.1]*768,
    top_k=10,
    filter_expression="cat = 'a' AND price < 100",   # optional; empty string disables
    return_metadata=True,
)
for r in results:
    print(r.id, r.distance, r.metadata)
```

`SearchResult` is a dataclass with `id: int`, `distance: float`, and `metadata: dict | None`.

### Range search (by radius)

```python
results = client.range_search(
    "my_collection",
    query_vector=[0.1]*768,
    radius=0.8,
    max_results=1000,
    filter_expression="",
    return_metadata=False,
)
```

### Hybrid search

```python
results = client.hybrid_search(
    "my_collection",
    query_vector=[0.1]*768,          # optional
    text_query="running shoes",       # optional
    sparse_query={42: 0.7, 137: 0.5}, # optional; dict[int, float]
    top_k=10,
    vector_weight=0.5,                # weights are linear, not RRF
    text_weight=0.5,
    sparse_weight=0.0,
    text_field="text",                # metadata field for BM25
    filter_expression="",
    return_metadata=True,
)
```

Any combination of `query_vector`, `text_query`, and `sparse_query` may be provided. See [hybrid search](../features/hybrid-search.md) for the weighting behaviour.

## Server-side bulk import

For large-scale loads, the server can read directly from S3/MinIO — much faster than streaming bytes over gRPC:

```python
import_id = client.bulk_import(
    "my_collection",
    source_uri="s3://my-bucket/embeddings.parquet",
    format="parquet",                # "parquet" or "numpy"
    vector_column="vector",
    id_column="id",
)

status = client.wait_for_import(import_id, poll_interval=2.0, timeout=3600.0)
# status["state"] -> 2 (COMPLETED) on success; see ImportState below
```

Polling and cancellation:

```python
status = client.get_import_status(import_id)
# keys: import_id, state, total_vectors, imported_vectors,
#       progress_percent, error_message, elapsed_seconds, segments_created

client.cancel_import(import_id)      # -> True if accepted
```

`ImportState` integer values (module constant):

| Value | Meaning |
|-------|---------|
| 0 | PENDING |
| 1 | RUNNING |
| 2 | COMPLETED |
| 3 | FAILED |
| 4 | CANCELLED |

For client-side imports (Parquet, NumPy, pandas, CSV, h5ad) that go over the gRPC insert path, see [bulk import](bulk-import.md).

## Errors

The SDK does not wrap errors — it re-raises `grpc.RpcError` directly. Check `.code()` for the gRPC status:

```python
import grpc

try:
    client.drop_collection("nope")
except grpc.RpcError as e:
    if e.code() == grpc.StatusCode.NOT_FOUND:
        pass                          # collection didn't exist
    elif e.code() == grpc.StatusCode.PERMISSION_DENIED:
        raise                         # RBAC rejection
    else:
        raise
```

## See also

- [Bulk import](bulk-import.md) — client-side importers for Parquet / NumPy / pandas / h5ad
- [Examples](examples.md)
- [`clients/python/gvdb/client.py`](https://github.com/JonathanBerhe/gvdb/blob/main/clients/python/gvdb/client.py) — authoritative source
