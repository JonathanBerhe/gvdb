# gvdb

Rust client for the [GVDB](https://github.com/JonathanBerhe/gvdb) distributed vector database.

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

Enable optional Cargo features for client-side file importers:

| Feature | Dependencies | Adds |
|---------|--------------|------|
| `csv` | `csv`, `serde_json` | `import_csv` |
| `parquet` | `parquet`, `arrow-array`, `arrow-schema` | `import_parquet` |
| `import` | `csv`, `parquet` | Both file importers |

## Quick start

```rust
use gvdb::{
    ClientConfig, CreateCollectionOptions, GvdbClient, HybridSearchOptions, IndexType, Metadata,
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

    let hits = client
        .search(
            "my_vectors",
            &vec![0.1; 768],
            SearchOptions::default().top_k(10).return_metadata(true),
        )
        .await?;
    for hit in hits {
        println!("id={} distance={}", hit.id, hit.distance);
    }

    let hybrid_hits = client
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
    println!("hybrid hits: {}", hybrid_hits.len());

    client.drop_collection("my_vectors").await?;
    Ok(())
}
```

## TLS connections

Pass an `https://` address or call `.tls(true)` on `ClientConfig` to connect with the root certificates of the operating system:

```rust
use gvdb::ClientConfig;

let client = ClientConfig::new("https://gvdb.example.com:50051")
    .api_key("your-key")
    .connect()
    .await?;
```

For a private certificate authority or mutual TLS, pass a `tonic::transport::ClientTlsConfig` to `ClientConfig::tls_config`.

## Tests

Run offline unit tests and importer tests without a GVDB server:

```bash
cargo test --all-features
```

To run integration tests against a local `gvdb-single-node` binary, set `GVDB_SERVER_BIN` or `GVDB_SERVER_ADDR`:

```bash
GVDB_SERVER_BIN=../../build/dev/bin/gvdb-single-node cargo test --all-features
```
