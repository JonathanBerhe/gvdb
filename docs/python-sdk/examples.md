# Examples

Runnable scripts that exercise common Python SDK workflows. For the authoritative API surface, see [Client API](client.md).

## Quickstart — end-to-end CRUD + search

```python title="quickstart.py"
import random
from gvdb import GVDBClient


def main():
    client = GVDBClient("localhost:50051")
    print(f"Health: {client.health_check()}")

    collection = "quickstart_demo"
    try:
        client.drop_collection(collection)
    except Exception:
        pass

    client.create_collection(
        collection, dimension=128, metric="l2", index_type="hnsw"
    )
    print(f"Created collection: {collection}")

    # Insert 100 vectors with metadata
    ids = list(range(1, 101))
    vectors = [[random.gauss(0, 1) for _ in range(128)] for _ in range(100)]
    metadata = [
        {"category": f"cat_{i % 5}", "score": random.random()}
        for i in range(100)
    ]
    inserted = client.insert(collection, ids, vectors, metadata=metadata)
    print(f"Inserted {inserted} vectors")

    # Search
    query = [random.gauss(0, 1) for _ in range(128)]
    results = client.search(collection, query, top_k=5, return_metadata=True)
    print("\nTop 5 results:")
    for r in results:
        print(f"  ID={r.id}, distance={r.distance:.4f}, metadata={r.metadata}")

    # Filtered search
    results = client.search(
        collection, query, top_k=5,
        filter_expression="category = 'cat_0'",
        return_metadata=True,
    )
    print("\nFiltered (category='cat_0'):")
    for r in results:
        print(f"  ID={r.id}, distance={r.distance:.4f}")

    # Get by ID
    fetched = client.get(collection, [1, 2, 3])
    print(f"\nFetched {len(fetched)} vectors by ID")

    client.drop_collection(collection)
    client.close()


if __name__ == "__main__":
    main()
```

Source: [`clients/python/examples/quickstart.py`](https://github.com/JonathanBerhe/gvdb/blob/main/clients/python/examples/quickstart.py).

## Hybrid search (dense + BM25)

```python
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")

client.create_collection("products", dimension=768, metric="cosine")

client.insert(
    "products",
    ids=[1, 2, 3],
    vectors=[dense_emb_1, dense_emb_2, dense_emb_3],
    metadata=[
        {"text": "Running shoes — marathon ready", "price": 129.0},
        {"text": "Kitchen knife set, 8 pieces", "price": 89.0},
        {"text": "Trail running shoe, waterproof", "price": 159.0},
    ],
)

results = client.hybrid_search(
    "products",
    query_vector=query_dense,
    text_query="running shoes",
    text_field="text",
    top_k=5,
    vector_weight=0.6,
    text_weight=0.4,
    return_metadata=True,
)

for r in results:
    print(r.id, r.distance, r.metadata["text"])
```

## Three-way hybrid (dense + sparse + BM25)

Sparse vectors are plain `dict[int, float]` — no special class to import:

```python
client.insert(
    "products",
    ids=[1, 2, 3],
    vectors=[dense_emb_1, dense_emb_2, dense_emb_3],
    sparse_vectors=[
        {42: 0.8, 137: 0.3, 2048: 1.2},   # non-zero SPLADE dimensions
        {7: 0.5, 42: 0.9},
        {137: 1.1, 9999: 0.4},
    ],
    metadata=[{"text": "..."}, {"text": "..."}, {"text": "..."}],
)

results = client.hybrid_search(
    "products",
    query_vector=query_dense,
    text_query="running shoes",
    sparse_query={42: 0.7, 137: 0.5},
    text_field="text",
    top_k=5,
    vector_weight=0.5,
    text_weight=0.3,
    sparse_weight=0.2,
    return_metadata=True,
)
```

## Server-side bulk import from S3

```python
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")

# Create the target collection (bulk_import does NOT auto-create)
client.create_collection("catalog", dimension=768)

import_id = client.bulk_import(
    "catalog",
    source_uri="s3://my-bucket/embeddings.parquet",
    format="parquet",
    vector_column="vector",
    id_column="id",
)

status = client.wait_for_import(import_id, poll_interval=2.0, timeout=3600.0)
print(status)
# {"state": 2, "imported_vectors": 1_000_000, "segments_created": 12, ...}
```

## Per-vector TTL

```python
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")
client.create_collection("sessions", dimension=384)

# ttl_seconds is RELATIVE. 0 means "no expiration".
client.insert(
    "sessions",
    ids=[1, 2, 3],
    vectors=[[0.1]*384, [0.2]*384, [0.3]*384],
    ttl_seconds=[3600, 3600, 0],   # first two expire in 1 hour; third never
)
```

## RBAC

```python
admin = GVDBClient("localhost:50051", api_key="admin-key")
admin.create_collection("shared", dimension=384)

analyst = GVDBClient("localhost:50051", api_key="analyst-key")
analyst.search("shared", query_vector=[0.1]*384, top_k=5)   # OK: readonly on 'shared'

try:
    analyst.drop_collection("shared")
except Exception as e:
    # grpc.StatusCode.PERMISSION_DENIED
    print(f"Rejected: {e}")
```

See [RBAC](../features/rbac.md).

## Error handling

The SDK re-raises `grpc.RpcError` directly. Branch on `.code()`:

```python
import grpc
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")
try:
    client.drop_collection("does-not-exist")
except grpc.RpcError as e:
    if e.code() == grpc.StatusCode.NOT_FOUND:
        pass
    elif e.code() == grpc.StatusCode.PERMISSION_DENIED:
        raise
    else:
        raise
```

## See also

- [Client API](client.md)
- [Bulk import](bulk-import.md)
- The [`clients/python/examples/`](https://github.com/JonathanBerhe/gvdb/tree/main/clients/python/examples) directory for the source of truth.
