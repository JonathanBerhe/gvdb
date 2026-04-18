# Quickstart

Get a GVDB server running and execute your first search in under 5 minutes.

## 1. Start a server

=== "Docker"

    ```bash
    docker run -d --name gvdb -p 50051:50051 \
      -v "$PWD/gvdb-data:/var/lib/gvdb" \
      ghcr.io/jonathanberhe/gvdb:latest \
      gvdb-single-node --port 50051 --data-dir /var/lib/gvdb
    ```

=== "From source"

    ```bash
    ./build/bin/gvdb-single-node --port 50051 --data-dir /tmp/gvdb
    ```

## 2. Install the Python SDK

```bash
pip install gvdb
```

## 3. Create a collection, insert, and search

```python title="quickstart.py"
import random
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")
print(f"Health: {client.health_check()}")

# Create a 128-dim collection with an HNSW index and L2 metric
client.create_collection(
    "quickstart_demo",
    dimension=128,
    metric="l2",
    index_type="hnsw",
)

# Insert 100 vectors with metadata
ids = list(range(1, 101))
vectors = [[random.gauss(0, 1) for _ in range(128)] for _ in range(100)]
metadata = [
    {"category": f"cat_{i % 5}", "score": random.random()}
    for i in range(100)
]
inserted = client.insert("quickstart_demo", ids, vectors, metadata=metadata)
print(f"Inserted {inserted} vectors")

# Search
query = [random.gauss(0, 1) for _ in range(128)]
results = client.search("quickstart_demo", query, top_k=5, return_metadata=True)
for r in results:
    print(f"ID={r.id}  distance={r.distance:.4f}  metadata={r.metadata}")

# Filtered search (SQL-like predicate)
results = client.search(
    "quickstart_demo",
    query,
    top_k=5,
    filter_expression="category = 'cat_0'",
    return_metadata=True,
)

# Clean up
client.drop_collection("quickstart_demo")
client.close()
```

Run it:

```bash
python quickstart.py
```

## What just happened

1. **`create_collection`** allocated an HNSW index with dimension 128 and L2 distance.
2. **`insert`** streamed the vectors to a growing segment, which will seal and flush to disk on rotation.
3. **`search`** returned the top-5 approximate nearest neighbours by L2 distance.
4. **Filtered search** intersected ANN results with a [metadata predicate](../features/metadata-filtering.md).
5. **`drop_collection`** removed the segments and index.

## Next steps

- Try [hybrid search](../features/hybrid-search.md) combining dense, sparse, and BM25 text
- Load millions of vectors with [bulk import](../python-sdk/bulk-import.md) (Parquet, NumPy, pandas, h5ad)
- Scale out to a [distributed cluster](distributed-cluster.md)
- Stream real-time embeddings from [Spark](../connectors/spark.md) or [Flink](../connectors/flink.md)
