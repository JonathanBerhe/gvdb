# Client API

Reference for `GvdbClient` and `ClientConfig`. Method signatures mirror [`clients/rust/src/client.rs`](https://github.com/JonathanBerhe/gvdb/blob/main/clients/rust/src/client.rs).

## Connection

Use `GvdbClient::connect` for a plain connection with default settings:

```rust
use gvdb::GvdbClient;

let client = GvdbClient::connect("localhost:50051").await?;
let health = client.health_check().await?;
let stats = client.get_stats().await?;
```

Use `ClientConfig` to set an API key, a timeout, or TLS:

```rust
use std::time::Duration;
use gvdb::ClientConfig;

let client = ClientConfig::new("https://gvdb.example.com:50051")
    .api_key("your-key")
    .timeout(Duration::from_secs(30))
    .connect()
    .await?;
```

`GvdbClient` implements `Clone`. Cloned clients share the same HTTP/2 gRPC channel, so you can pass clones across Tokio tasks without a lock.

If you connect with a private certificate authority or mutual TLS, pass a `tonic::transport::ClientTlsConfig` to `ClientConfig::tls_config`. If you manage your own `tonic::transport::Channel`, call `GvdbClient::from_channel`.

## Collection management

```rust
use gvdb::{CreateCollectionOptions, IndexType, Metric};

let id = client
    .create_collection(
        "my_collection",
        768,
        CreateCollectionOptions::default()
            .metric(Metric::L2)
            .index_type(IndexType::Hnsw)
            .num_shards(0),
    )
    .await?;

let collections = client.list_collections().await?;
client.drop_collection("my_collection").await?;
```

Available metrics: `Metric::L2`, `Metric::InnerProduct`, and `Metric::Cosine`.

Available index types: `Flat`, `Hnsw`, `IvfFlat`, `IvfPq`, `IvfSq`, `IvfTurboQuant`, `TurboQuant`, and `Auto`.

## Writes

### Insert

Build each vector with `VectorRecord`. Attach metadata, a sparse vector, or a relative TTL in seconds when needed:

```rust
use gvdb::{Metadata, MetadataValue, SparseVector, VectorRecord};

let mut meta = Metadata::new();
meta.insert("cat".to_owned(), MetadataValue::from("a"));

let record = VectorRecord::new(1, vec![0.1; 768])
    .with_metadata(meta)
    .with_sparse_vector(SparseVector::from_pairs([(42, 0.8), (137, 0.3)]))
    .with_ttl_seconds(3600);

let inserted = client.insert("my_collection", &[record]).await?;
```

`MetadataValue` supports `Int(i64)`, `Float(f64)`, `String(String)`, and `Bool(bool)`. Zero `ttl_seconds` disables expiration.

### Upsert

```rust
let result = client.upsert("my_collection", &records).await?;
println!(
    "upserted={} inserted={} updated={}",
    result.upserted_count, result.inserted_count, result.updated_count
);
```

### Stream insert

Use `stream_insert` to split a large slice into gRPC client-streaming chunks:

```rust
let total = client.stream_insert("my_collection", &records, 10_000).await?;
```

### Delete

```rust
let deleted = client.delete("my_collection", &[1, 2]).await?;
```

### Update metadata

```rust
use gvdb::{Metadata, MetadataValue};

let mut meta = Metadata::new();
meta.insert("cat".to_owned(), MetadataValue::from("updated"));

let updated = client.update_metadata("my_collection", 42, meta, true).await?;
```

Pass `true` for `merge` to keep existing keys not present in `meta`. Pass `false` to replace all metadata for that vector.

## Reads

### Get by ID

```rust
let records = client.get("my_collection", &[1, 2, 3], true).await?;
```

Pass `false` as the third argument to omit metadata from the response.

### List vectors

```rust
let page = client.list_vectors("my_collection", 20, 0, true).await?;
println!("returned={} total={} has_more={}", page.vectors.len(), page.total_count, page.has_more);
```

### Search

```rust
use gvdb::SearchOptions;

let hits = client
    .search(
        "my_collection",
        &vec![0.1; 768],
        SearchOptions::default()
            .top_k(10)
            .filter("cat = 'a' AND price < 100.0")
            .return_metadata(true),
    )
    .await?;
```

Results arrive closest first. A lower `distance` means a closer match.

### Range search

For an L2 or cosine collection, set `radius` to the largest accepted distance:

```rust
use gvdb::RangeSearchOptions;

let hits = client
    .range_search(
        "my_collection",
        &vec![0.1; 768],
        RangeSearchOptions::default().radius(0.8).max_results(1000),
    )
    .await?;
```

For an inner-product collection, set `similarity_threshold` to the smallest accepted dot product:

```rust
let hits = client
    .range_search(
        "my_ip_collection",
        &vec![0.1; 768],
        RangeSearchOptions::default().similarity_threshold(0.75),
    )
    .await?;
```

### Hybrid search

```rust
use gvdb::{HybridSearchOptions, SparseVector};

let hits = client
    .hybrid_search(
        "my_collection",
        HybridSearchOptions::default()
            .query_vector(vec![0.1; 768])
            .text_query("running shoes")
            .sparse_query(SparseVector::from_pairs([(42, 0.7), (137, 0.5)]))
            .top_k(10)
            .vector_weight(0.5)
            .text_weight(0.3)
            .sparse_weight(0.2)
            .text_field("text")
            .return_metadata(true),
    )
    .await?;
```

## Server-side bulk import

```rust
use gvdb::{BulkImportFormat, BulkImportOptions, PollOptions};

let import_id = client
    .bulk_import(
        "my_collection",
        "s3://my-bucket/embeddings.parquet",
        BulkImportOptions::default().format(BulkImportFormat::Parquet),
    )
    .await?;

let status = client.wait_for_import(&import_id, PollOptions::default()).await?;
```

Use `get_import_status` to poll once and `cancel_import` to stop a pending or running job.

## Backup and restore

Select an S3 or local filesystem target with `BackupTarget`:

```rust
use gvdb::{BackupTarget, PollOptions};

let target = BackupTarget::s3("my-bucket", Some("prod"));
let backup_id = client.backup_collection("my_collection", target.clone(), None).await?;
let status = client.wait_for_backup(&backup_id, PollOptions::default()).await?;
println!("manifest={}", status.manifest_uri);

let backups = client.list_backups(target.clone()).await?;
let restore_id = client
    .restore_collection(&backup_id, target, Some("my_collection_copy".to_owned()), false)
    .await?;
client.wait_for_restore(&restore_id, PollOptions::default()).await?;
```

Use `get_backup_status`, `get_restore_status`, and `cancel_backup` for manual job control.

## Errors

All client methods return `gvdb::Result<T>`. Server rejections arrive as `Error::Status(tonic::Status)`:

```rust
use tonic::Code;

match client.drop_collection("missing").await {
    Ok(()) => {}
    Err(err) if err.status().map(|s| s.code()) == Some(Code::NotFound) => {}
    Err(err) => return Err(err),
}
```
