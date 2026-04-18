# Examples

Runnable scripts that exercise common Python SDK workflows.

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

client.create_collection("products", dimension=768)

client.insert(
    "products",
    ids=[1, 2, 3],
    vectors=[dense_emb_1, dense_emb_2, dense_emb_3],
    metadata=[
        {"title": "Running shoes — marathon ready", "price": 129.0},
        {"title": "Kitchen knife set, 8 pieces", "price": 89.0},
        {"title": "Trail running shoe, waterproof", "price": 159.0},
    ],
)

results = client.hybrid_search(
    "products",
    query_vector=query_dense,
    text_query="running shoes",
    text_field="title",
    top_k=5,
    return_metadata=True,
)

for r in results:
    print(r.id, r.score, r.metadata["title"])
```

## Bulk import from Parquet

```python
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")
result = client.import_parquet(
    "s3://my-bucket/embeddings.parquet",   # or local path
    collection="catalog",
    batch_size=10_000,
)
print(result)
```

## Per-vector TTL

```python
import time
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")
client.create_collection("sessions", dimension=384)

now = int(time.time())
client.insert(
    "sessions",
    ids=[1, 2, 3],
    vectors=[[0.1]*384, [0.2]*384, [0.3]*384],
    expire_at=now + 3600,   # all vectors expire in 1 hour
)
```

## RBAC

```python
admin = GVDBClient("localhost:50051", api_key="admin-key")
admin.create_collection("shared", dimension=384)

analyst = GVDBClient("localhost:50051", api_key="analyst-key")
analyst.search("shared", query_vector=[...], top_k=5)  # OK: readonly on 'shared'
# analyst.drop_collection("shared")                      # PermissionDeniedError
```

See [RBAC](../features/rbac.md).

## See also

- [Client API](client.md)
- [Bulk import](bulk-import.md)
- The [`clients/python/examples/`](https://github.com/JonathanBerhe/gvdb/tree/main/clients/python/examples) directory for the source of truth.
