# Bulk import

Two paths for loading many vectors:

1. **Client-side importers** — Python reads the file, streams over gRPC. Good for most workloads up to a few million rows.
2. **Server-side bulk import** — the server reads directly from S3/MinIO. 3–5× faster for billion-scale loads.

## Client-side importers

### Install extras

```bash
pip install gvdb[import]       # Parquet, NumPy, Pandas, tqdm
pip install gvdb[import-all]   # plus AnnData + polars
```

| Extra | Deps |
|-------|------|
| `gvdb[parquet]` | pyarrow |
| `gvdb[numpy]` | numpy |
| `gvdb[pandas]` | pandas, pyarrow |
| `gvdb[h5ad]` | anndata, numpy |
| `gvdb[progress]` | tqdm |

### Shared parameters

Every `import_*` helper accepts these kwargs:

| Arg | Default | Description |
|-----|---------|-------------|
| `batch_size` | `10_000` | Rows per insert RPC |
| `mode` | `"upsert"` | `"upsert"` (idempotent, safe to re-run) or `"stream_insert"` (faster) |
| `metric` | `"cosine"` | Distance metric when auto-creating the collection |
| `index_type` | `"auto"` | Index type when auto-creating the collection |
| `max_retries` | `3` | Retries per batch on transient errors |
| `show_progress` | `True` | tqdm progress bar (requires `gvdb[progress]`) |

### NumPy

```python
import numpy as np
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")

vectors = np.random.rand(100_000, 768).astype(np.float32)
result = client.import_numpy(
    vectors,
    "embeddings",
    ids=None,                       # None → sequential IDs starting from 0
    metadata=None,
    batch_size=10_000,
)
print(result)
# ImportResult(total=100000, batches=10, failed=0, elapsed=12.3s, ...)
```

### Parquet

Expected schema: `id` (int), `vector` (list<float>), plus any metadata columns.

```python
result = client.import_parquet(
    "vectors.parquet",
    "embeddings",
    vector_column="vector",
    id_column="id",
)
```

### Pandas / Polars DataFrame

```python
import pandas as pd

df = pd.DataFrame({
    "id": range(1000),
    "vector": [[...] for _ in range(1000)],
    "category": [...],
    "price": [...],
})

result = client.import_dataframe(
    df,
    "products",
    vector_column="vector",
    id_column="id",
)
```

Non-vector, non-id columns become per-vector metadata (scalars only: int, float, str, bool).

### CSV

Two vector encodings are auto-detected:

```python
# 1. JSON array in a single column: "[0.1, 0.2, 0.3]"
result = client.import_csv("data.csv", "embeddings", vector_column="vector", id_column="id")

# 2. Dimension-prefixed columns: vector_0, vector_1, ..., vector_N
result = client.import_csv("wide.csv", "embeddings", vector_column="vector")
```

### AnnData (`.h5ad`)

For single-cell workflows — import cell embeddings, `obs` columns as metadata:

```python
result = client.import_h5ad(
    "adata.h5ad",
    "cells",
    embedding_key="X_pca",          # or "X_umap", "X_scvi", ...
    id_column=None,                 # defaults to row index
    metadata_columns=None,          # None = include all obs columns
)
```

### `ImportResult`

```python
ImportResult(
    total_count=100_000,
    batch_count=10,
    failed_count=0,
    elapsed_seconds=12.3,
    collection="embeddings",
    dimension=768,
    created_collection=True,
)
```

## Server-side bulk import

For S3/MinIO-backed loads, skip gRPC entirely — the server downloads the file and writes segments directly:

```python
import_id = client.bulk_import(
    "my_collection",
    source_uri="s3://my-bucket/embeddings.parquet",
    format="parquet",           # "parquet" or "numpy"
    vector_column="vector",
    id_column="id",
)

status = client.wait_for_import(import_id, poll_interval=2.0, timeout=3600.0)
# status == {"state": 2, "total_vectors": 1_000_000, "imported_vectors": 1_000_000,
#            "progress_percent": 100.0, "segments_created": 12, ...}
```

Polling and cancellation:

```python
status = client.get_import_status(import_id)
client.cancel_import(import_id)      # -> True if accepted
```

`state` is an integer:

| Value | Meaning |
|-------|---------|
| 0 | PENDING |
| 1 | RUNNING |
| 2 | COMPLETED |
| 3 | FAILED |
| 4 | CANCELLED |

**The collection must already exist** when you call `bulk_import` — unlike client-side importers, the server-side path does not auto-create collections.

## Alternatives for very large workloads

- **Spark** for parallel loads from data lakes — see the [Spark connector](../connectors/spark.md).
- **Flink** for streaming ingestion — see the [Flink connector](../connectors/flink.md).

## See also

- [Client API](client.md)
- [Use case: bulk ingestion](../use-cases/bulk-ingestion.md)
