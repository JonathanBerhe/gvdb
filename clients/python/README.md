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

## Backup & Restore

Server-side collection backups to S3 or a local path on the data-node. Pass exactly one of `s3_bucket` or `local_path` to select the target.

```python
# Start a backup and block until it finishes
backup_id = client.backup_collection("my_vectors", s3_bucket="my-bucket", s3_prefix="prod")
status = client.wait_for_backup(backup_id)
print(status.manifest_uri)  # s3://my-bucket/prod/backups/<id>/backup.manifest.json

# List backups stored in a target
for info in client.list_backups(s3_bucket="my-bucket", s3_prefix="prod"):
    print(info.backup_id, info.collection_name, info.vector_count)

# Restore into a new collection (omit target_collection to reuse the original name)
restore_id = client.restore_collection(
    backup_id,
    s3_bucket="my-bucket",
    s3_prefix="prod",
    target_collection="my_vectors_copy",
)
client.wait_for_restore(restore_id)

# Local filesystem target (path must be under the server-side allow-list)
backup_id = client.backup_collection("my_vectors", local_path="/backups")

# Cancel a running backup
client.cancel_backup(backup_id)
```

`get_backup_status` / `get_restore_status` return `BackupStatus` / `RestoreStatus` dataclasses with shard progress and error details. `wait_for_backup` and `wait_for_restore` raise `RuntimeError` if the job fails or is cancelled, and `TimeoutError` if it does not finish within `timeout` (default 3600s).
