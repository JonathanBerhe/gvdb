# Python SDK

Official Python client for GVDB. Full CRUD, hybrid search, streaming inserts, per-vector TTL, and bulk import from Parquet, NumPy, pandas, CSV, and AnnData.

## Install

```bash
pip install gvdb

# With bulk import extras (Parquet, NumPy, Pandas, progress bar)
pip install gvdb[import]

# Everything including AnnData for single-cell workflows
pip install gvdb[import-all]
```

See [client API](client.md) for the full method reference and [bulk import](bulk-import.md) for loading large datasets.

## Optional dependency extras

| Extra | Dependencies | For |
|-------|--------------|-----|
| `gvdb[parquet]` | pyarrow | `import_parquet` |
| `gvdb[numpy]` | numpy | `import_numpy` |
| `gvdb[pandas]` | pandas, pyarrow | `import_dataframe`, `import_csv` |
| `gvdb[h5ad]` | anndata, numpy | `import_h5ad` |
| `gvdb[progress]` | tqdm | Progress bars during bulk imports |
| `gvdb[import]` | All above except anndata | Common ML workflows |
| `gvdb[import-all]` | Everything + polars | All formats |

## Quick start

```python
from gvdb import GVDBClient

client = GVDBClient("localhost:50051", api_key="your-key")  # api_key optional

# Create a collection
client.create_collection("my_vectors", dimension=768)

# Insert vectors
vectors = [[0.1]*768, [0.3]*768]
client.insert("my_vectors", ids=[1, 2], vectors=vectors)

# Search
results = client.search("my_vectors", query_vector=[0.1]*768, top_k=10)
for r in results:
    print(f"ID: {r.id}, distance: {r.distance}")

# Hybrid search (BM25 + vector)
results = client.hybrid_search(
    "my_vectors",
    query_vector=[0.1]*768,
    text_query="running shoes",
    top_k=10,
    text_field="description",
    return_metadata=True,
)

# Clean up
client.drop_collection("my_vectors")
client.close()
```

## Next

- [Client API](client.md) — every method and its parameters
- [Bulk import](bulk-import.md) — Parquet, NumPy, pandas, CSV, h5ad
- [Examples](examples.md) — runnable scripts

## See also

- [Quickstart](../getting-started/quickstart.md)
- [Features: hybrid search](../features/hybrid-search.md)
