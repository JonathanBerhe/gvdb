# Client API

Reference for `GVDBClient`. Methods group into **connection**, **collection management**, **writes**, **reads**, and **bulk import** (see [bulk import](bulk-import.md)).

## Connection

```python
from gvdb import GVDBClient

client = GVDBClient(
    "localhost:50051",
    api_key="your-key",          # optional; required if RBAC is on
    timeout_seconds=30,
    tls=False,                    # True for mutual-TLS endpoints
)

client.health_check()             # returns server status dict
client.close()                    # shut down the gRPC channel
```

`GVDBClient` is a context manager:

```python
with GVDBClient("localhost:50051") as client:
    client.health_check()
```

## Collection management

```python
client.create_collection(
    "my_collection",
    dimension=768,
    metric="cosine",                   # "l2" | "ip" | "cosine"
    index_type="auto",                 # "auto" | "flat" | "hnsw" | "ivf_flat" | ...
    # HNSW tuning (optional):
    # hnsw_m=16, hnsw_ef_construction=200, hnsw_ef_search=64,
    # IVF tuning (optional):
    # nlist=16384, nprobe=32,
)

client.list_collections()              # -> ["collection_a", ...]
client.describe_collection("my_collection")
client.drop_collection("my_collection")
client.flush("my_collection")          # force-seal the growing segment
```

## Writes

### Insert

```python
inserted = client.insert(
    "my_collection",
    ids=[1, 2, 3],
    vectors=[[0.1]*768, [0.2]*768, [0.3]*768],
    metadata=[{"cat": "a"}, {"cat": "b"}, {"cat": "c"}],  # optional
    expire_at=None,                    # int unix ts, or list of int; see features/ttl.md
)
```

### Upsert (idempotent)

```python
result = client.upsert("my_collection", ids=[1, 2], vectors=[...], metadata=[...])
# result.upsertedCount, result.insertedCount, result.updatedCount
```

### Update metadata

```python
client.update_metadata("my_collection", ids=[1], metadata=[{"cat": "new"}])
```

### Delete

```python
client.delete("my_collection", ids=[1, 2])
client.delete_by_filter("my_collection", filter_expression="cat = 'a'")
```

### Streaming insert (for large batches)

```python
client.stream_insert(
    "my_collection",
    iterator=((id_i, vec_i, meta_i) for id_i, vec_i, meta_i in source),
    batch_size=5000,
)
```

gRPC client-streaming — ~1.9× faster than unary for > 100K vectors.

## Reads

### Get by ID

```python
vectors = client.get("my_collection", ids=[1, 2, 3], return_metadata=True)
for v in vectors:
    print(v.id, v.vector, v.metadata)
```

### Search

```python
results = client.search(
    "my_collection",
    query_vector=[0.1]*768,
    top_k=10,
    filter_expression="cat = 'a' AND price < 100",   # optional
    return_metadata=True,
    return_vector=False,
)
for r in results:
    print(r.id, r.distance, r.metadata)
```

### Range search (by radius)

```python
results = client.range_search(
    "my_collection",
    query_vector=[0.1]*768,
    radius=0.8,
    max_results=100,
)
```

### Hybrid search

```python
results = client.hybrid_search(
    "my_collection",
    query_vector=[0.1]*768,
    text_query="running shoes",
    text_field="description",
    sparse_vector=None,                # optional SparseVector
    top_k=10,
    filter_expression=None,
    return_metadata=True,
    rrf_k=60,
)
```

See [hybrid search](../features/hybrid-search.md).

### List

```python
page = client.list("my_collection", limit=100, offset=0, return_metadata=True)
```

## Sparse vectors

```python
from gvdb import SparseVector

client.create_collection_sparse("sparse_col")
client.insert_sparse("sparse_col", ids=[1], sparse_vectors=[SparseVector({42: 0.8})])
results = client.search_sparse("sparse_col", SparseVector({42: 0.7}), top_k=10)
```

See [sparse vectors](../features/sparse-vectors.md).

## Errors

`GVDBClient` raises `GVDBError` for all server-side errors. Subclasses:

- `NotFoundError` — collection or vector doesn't exist
- `AlreadyExistsError` — collection already exists
- `PermissionDeniedError` — RBAC rejection
- `InvalidArgumentError` — malformed request

```python
from gvdb import GVDBError, NotFoundError

try:
    client.drop_collection("nope")
except NotFoundError:
    pass
except GVDBError as e:
    print(f"Server error: {e}")
```

## See also

- [Bulk import](bulk-import.md)
- [Examples](examples.md)
