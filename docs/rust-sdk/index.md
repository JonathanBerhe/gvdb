# Rust SDK

Official async Rust client for GVDB. The crate supports collection management, CRUD operations, streaming inserts, hybrid search, range search, per-vector TTL, backup and restore, server-side bulk import, and optional local CSV and Parquet importers.

## Requirements

- Rust 1.85 or newer (edition 2024)
- `protoc` on `PATH` when building from this repository

## Install

Add the crate to `Cargo.toml`:

```toml
[dependencies]
gvdb = { path = "clients/rust" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

See [client API](client.md) for the full method list and [bulk import](bulk-import.md) for loading large datasets.

## Optional Cargo features

| Feature | Dependencies | Adds |
|---------|--------------|------|
| `csv` | `csv`, `serde_json` | `import_csv` |
| `parquet` | `parquet`, `arrow-array`, `arrow-schema` | `import_parquet` |
| `import` | `csv`, `parquet` | Both local file importers |

The default build enables only the core gRPC client with TLS support.

## Quick start

```rust
use gvdb::{
    ClientConfig, CreateCollectionOptions, HybridSearchOptions, IndexType, Metadata,
    MetadataValue, Metric, SearchOptions, VectorRecord,
};

#[tokio::main]
async fn main() -> gvdb::Result<()> {
    let client = ClientConfig::new("localhost:50051")
        .api_key("your-key")
        .connect()
        .await?;

    client
        .create_collection(
            "my_vectors",
            768,
            CreateCollectionOptions::default()
                .metric(Metric::Cosine)
                .index_type(IndexType::Hnsw),
        )
        .await?;

    let mut meta = Metadata::new();
    meta.insert("description".to_owned(), MetadataValue::from("running shoes"));

    let records = vec![VectorRecord::new(1, vec![0.1; 768]).with_metadata(meta)];
    client.insert("my_vectors", &records).await?;

    let results = client
        .search(
            "my_vectors",
            &vec![0.1; 768],
            SearchOptions::default().top_k(10),
        )
        .await?;
    for hit in results {
        println!("ID: {}, distance: {}", hit.id, hit.distance);
    }

    let hybrid = client
        .hybrid_search(
            "my_vectors",
            HybridSearchOptions::default()
                .query_vector(vec![0.1; 768])
                .text_query("running shoes")
                .text_field("description")
                .top_k(10)
                .return_metadata(true),
        )
        .await?;
    println!("Hybrid hits: {}", hybrid.len());

    client.drop_collection("my_vectors").await?;
    Ok(())
}
```

## Next

- [Client API](client.md) covers every client method and configuration type.
- [Bulk import](bulk-import.md) covers local and server-side bulk loading.
- [Examples](examples.md) shows complete runnable patterns.

## See also

- [Quickstart](../getting-started/quickstart.md)
- [Features: hybrid search](../features/hybrid-search.md)
