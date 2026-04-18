# Bulk ingestion

Load millions of vectors in one shot from common ML formats. The Python SDK's `import_*` helpers auto-create the collection, handle retries, and show a progress bar.

## Supported formats

| Format | Helper | Optional extra |
|--------|--------|----------------|
| NumPy array | `import_numpy` | `pip install gvdb[numpy]` |
| Parquet | `import_parquet` | `pip install gvdb[parquet]` |
| Pandas DataFrame | `import_dataframe` | `pip install gvdb[pandas]` |
| CSV | `import_csv` | `pip install gvdb[pandas]` |
| AnnData `.h5ad` | `import_h5ad` | `pip install gvdb[h5ad]` |

Install everything with `pip install gvdb[import-all]`.

## NumPy

```python
import numpy as np
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")

vectors = np.random.rand(1_000_000, 768).astype(np.float32)
result = client.import_numpy(vectors, "embeddings")
print(result)
# ImportResult(total=1000000, batches=100, elapsed=38.2s, ...)
```

## Parquet

GVDB expects a schema with `id`, `vector`, and optional metadata columns:

```python
result = client.import_parquet("vectors.parquet", "embeddings")
```

## Pandas DataFrame

```python
import pandas as pd
df = pd.read_csv("embeddings.csv")  # has 'id', 'embedding', 'category', 'price'
result = client.import_dataframe(
    df,
    collection="products",
    vector_column="embedding",
)
```

Non-vector columns become metadata automatically.

## CSV

```python
# Vector encoded as a JSON array string in a single column
result = client.import_csv("data.csv", collection="embeddings")

# Or dimension-prefixed columns (vec_0, vec_1, ..., vec_767)
result = client.import_csv("wide.csv", collection="embeddings", vector_prefix="vec_")
```

## AnnData (`.h5ad`) for scRNA-seq

Import cell embeddings from a single-cell experiment:

```python
result = client.import_h5ad(
    "adata.h5ad",
    collection="cells",
    embedding_key="X_pca",   # or "X_umap", "X_scvi", ...
)
```

Cell IDs become vector IDs; `obs` columns become metadata.

## Idempotency and resume

All importers default to `mode="upsert"`, which is idempotent — re-running a partial import will not produce duplicates. Use `mode="stream_insert"` for the fastest path when you don't need resume.

## ImportResult

Every importer returns an `ImportResult`:

```python
ImportResult(
    total=1_000_000,
    inserted=998_742,
    updated=1_258,
    failed=0,
    batches=100,
    elapsed=38.2,
    failures=[],
)
```

## Alternatives for very large workloads

- **Spark** for parallel loads from data lakes — see the [Spark connector](../connectors/spark.md).
- **Flink** for streaming ingestion — see the [Flink connector](../connectors/flink.md).

## See also

- [Python SDK — bulk import](../python-sdk/bulk-import.md)
