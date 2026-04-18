# Bulk import

Load millions of vectors from common ML formats in a single call. Auto-creates the collection, supports resume via upsert idempotency, and shows progress bars (with `tqdm`).

## Install extras

```bash
pip install gvdb[import]       # Parquet, NumPy, Pandas, tqdm
pip install gvdb[import-all]   # plus AnnData + polars
```

Individual extras:

| Extra | Deps |
|-------|------|
| `gvdb[parquet]` | pyarrow |
| `gvdb[numpy]` | numpy |
| `gvdb[pandas]` | pandas, pyarrow |
| `gvdb[h5ad]` | anndata, numpy |
| `gvdb[progress]` | tqdm |

## NumPy

```python
import numpy as np
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")

vectors = np.random.rand(100_000, 768).astype(np.float32)
result = client.import_numpy(vectors, "embeddings")
print(result)
# ImportResult(total=100000, batches=10, elapsed=12.3s, ...)
```

Pass `ids=...` to control the IDs; otherwise they default to `range(len(vectors))`.

## Parquet

Expected schema: `id` (int), `vector` (list<float>), and optional metadata columns.

```python
result = client.import_parquet("vectors.parquet", "embeddings")
```

Custom column names:

```python
result = client.import_parquet(
    "vectors.parquet",
    "embeddings",
    id_column="row_id",
    vector_column="embedding",
)
```

## Pandas DataFrame

```python
import pandas as pd

df = pd.DataFrame({
    "id": range(1000),
    "embedding": [[...] for _ in range(1000)],
    "category": [...],
    "price": [...],
})

result = client.import_dataframe(
    df,
    collection="products",
    vector_column="embedding",
)
```

Non-vector columns (other than `id`) become per-vector metadata automatically.

## CSV

Two CSV formats are supported:

```python
# 1. Vector encoded as JSON array in a single column
result = client.import_csv("data.csv", "embeddings")   # expects 'id' + 'vector' JSON

# 2. Dimension-prefixed columns (vec_0 ... vec_767)
result = client.import_csv(
    "wide.csv",
    "embeddings",
    vector_prefix="vec_",
)
```

## AnnData (`.h5ad`)

For single-cell workflows — import cell embeddings as vectors, `obs` columns as metadata:

```python
result = client.import_h5ad(
    "adata.h5ad",
    "cells",
    embedding_key="X_pca",             # or "X_umap", "X_scvi", ...
)
```

## Tuning

All importers accept:

| Arg | Default | Description |
|-----|---------|-------------|
| `batch_size` | 5000 | Rows per insert RPC |
| `mode` | `"upsert"` | `"upsert"` (idempotent, retries-safe) or `"stream_insert"` (faster) |
| `workers` | 1 | Parallel insert workers (pyarrow only for Parquet) |
| `show_progress` | `True` | tqdm bar if installed |

## Return value

Every importer returns an `ImportResult`:

```python
ImportResult(
    total=100_000,
    inserted=98_742,
    updated=1_258,
    failed=0,
    batches=20,
    elapsed=12.3,
    failures=[],
)
```

## See also

- [Use case: bulk ingestion](../use-cases/bulk-ingestion.md)
- [Client API](client.md)
