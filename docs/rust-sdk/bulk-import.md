# Bulk import

GVDB supports two bulk loading paths in Rust:

1. Client-side importers read local data and send batched gRPC requests. Use this path for datasets up to a few million vectors.
2. Server-side bulk import reads Parquet or NumPy files directly from S3 or MinIO without sending vectors over gRPC.

## Client-side importers

Enable optional Cargo features in `Cargo.toml`:

```toml
[dependencies]
gvdb = { path = "clients/rust", features = ["import"] }
```

| Feature | Dependencies | Method |
|---------|--------------|--------|
| `csv` | `csv`, `serde_json` | `import_csv` |
| `parquet` | `parquet`, `arrow-array`, `arrow-schema` | `import_parquet` |
| `import` | `csv`, `parquet` | Both methods |

`import_records` is available without optional features.

### Shared configuration (`ImportOptions`)

| Field | Default | Purpose |
|-------|---------|---------|
| `batch_size` | `10_000` | Vectors per RPC batch |
| `mode` | `ImportMode::Upsert` | `Upsert` retries transient errors. `StreamInsert` skips retries. |
| `metric` | `Metric::Cosine` | Distance metric when the importer creates a new collection |
| `index_type` | `IndexType::Auto` | Index type when the importer creates a new collection |
| `max_retries` | `3` | Retry attempts per failed `Upsert` batch |
| `retry_base_delay` | `500ms` | Initial exponential backoff delay before the first retry |

If the target collection does not exist, the importer creates it using the dimension of the first vector. If the collection already exists with a different dimension, the importer returns `Error::InvalidArgument`.

### In-memory records

```rust
use gvdb::{ImportOptions, VectorRecord};

let result = client
    .import_records("embeddings", &records, ImportOptions::default().batch_size(5_000))
    .await?;
println!("imported={} failed={}", result.total_count, result.failed_count);
```

### CSV

`import_csv` detects two vector layouts from the CSV header:

1. One JSON array column named `vector` (or your custom `vector_column`), such as `"[0.1, 0.2, 0.3]"`.
2. Dimension-prefixed columns such as `vector_0`, `vector_1`, and `vector_2`.

All other non-empty columns become vector metadata.

```rust
use gvdb::CsvImportOptions;

let result = client
    .import_csv("data.csv", "embeddings", CsvImportOptions::default())
    .await?;
```

### Parquet

`import_parquet` reads an integer ID column (`Int32`, `Int64`, `UInt32`, or `UInt64`), a list or fixed-size list vector column (`Float32` or `Float64`), and scalar metadata columns.

```rust
use gvdb::ParquetImportOptions;

let result = client
    .import_parquet("vectors.parquet", "embeddings", ParquetImportOptions::default())
    .await?;
```

## Server-side bulk import

Create the collection first, then start the server-side job:

```rust
use gvdb::{BulkImportFormat, BulkImportOptions, CreateCollectionOptions, PollOptions};

client
    .create_collection("catalog", 768, CreateCollectionOptions::default())
    .await?;

let import_id = client
    .bulk_import(
        "catalog",
        "s3://my-bucket/embeddings.parquet",
        BulkImportOptions::default()
            .format(BulkImportFormat::Parquet)
            .vector_column("vector")
            .id_column("id"),
    )
    .await?;

let status = client.wait_for_import(&import_id, PollOptions::default()).await?;
println!("state={:?} imported={}", status.state, status.imported_vectors);
```

Unlike client-side importers, `bulk_import` requires the target collection to already exist.

## See also

- [Client API](client.md)
- [Use case: bulk ingestion](../use-cases/bulk-ingestion.md)
