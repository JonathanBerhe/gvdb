# gvdb

Python client for [GVDB](https://github.com/JonathanBerhe/gvdb) distributed vector database.

## Install

```bash
pip install gvdb

# With bulk import extras (Parquet, NumPy, Pandas, progress bar)
pip install gvdb[import]

# All optional dependencies
pip install gvdb[import-all]
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

## Bulk Import

Import vectors from common ML formats. Auto-creates collections, supports resume via upsert idempotency, and shows progress bars (with `tqdm`).

```python
import numpy as np

# From NumPy array
vectors = np.random.rand(100_000, 768).astype(np.float32)
result = client.import_numpy(vectors, "embeddings")
print(result)  # ImportResult(total=100000, batches=10, elapsed=12.3s, ...)

# From Parquet (GVDB schema: id + vector + metadata columns)
result = client.import_parquet("vectors.parquet", "embeddings")

# From Pandas DataFrame
result = client.import_dataframe(df, "embeddings", vector_column="embedding")

# From CSV (JSON-encoded or dimension-prefixed vector columns)
result = client.import_csv("data.csv", "embeddings")

# From AnnData h5ad (scRNA-seq embeddings)
result = client.import_h5ad("adata.h5ad", "cells", embedding_key="X_pca")
```

All importers accept `mode="upsert"` (default, idempotent) or `mode="stream_insert"` (faster, no resume). See `ImportResult` for batch counts, timing, and failure tracking.

### Optional dependency extras

| Extra | Dependencies | For |
|-------|-------------|-----|
| `gvdb[parquet]` | pyarrow | `import_parquet` |
| `gvdb[numpy]` | numpy | `import_numpy` |
| `gvdb[pandas]` | pandas, pyarrow | `import_dataframe`, `import_csv` |
| `gvdb[h5ad]` | anndata, numpy | `import_h5ad` |
| `gvdb[progress]` | tqdm | Progress bars |
| `gvdb[import]` | All above except anndata | Common ML workflows |
| `gvdb[import-all]` | Everything + polars | All formats |
