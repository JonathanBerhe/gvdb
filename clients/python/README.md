# gvdb

Python client for [GVDB](https://github.com/JonathanBerhe/gvdb) distributed vector database.

## Install

```bash
pip install gvdb
```

## Quick Start

```python
from gvdb import GVDBClient

client = GVDBClient("localhost:50051", api_key="your-key")  # api_key is optional

# Create a collection
client.create_collection("my_vectors", dimension=768)

# Insert vectors
vectors = [[0.1, 0.2, ...], [0.3, 0.4, ...]]  # list of float lists
ids = [1, 2]
client.insert("my_vectors", ids, vectors)

# Search
results = client.search("my_vectors", query_vector=[0.1, 0.2, ...], top_k=10)
for r in results:
    print(f"ID: {r.id}, distance: {r.distance}")

# Hybrid search (BM25 + vector)
results = client.hybrid_search(
    "my_vectors",
    query_vector=[0.1, 0.2, ...],
    text_query="running shoes",
    top_k=10,
    text_field="description",   # metadata field to search
    return_metadata=True,
)

# Clean up
client.drop_collection("my_vectors")
client.close()
```
