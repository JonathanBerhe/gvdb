# Examples

Common Rust SDK patterns. See [Client API](client.md) for the full API reference.

## Quickstart CRUD and search

```rust
use gvdb::{
    CreateCollectionOptions, GvdbClient, IndexType, Metadata, MetadataValue, Metric,
    SearchOptions, VectorRecord,
};

#[tokio::main]
async fn main() -> gvdb::Result<()> {
    let client = GvdbClient::connect("localhost:50051").await?;
    println!("Health: {}", client.health_check().await?);

    let collection = "quickstart_demo";
    let _ = client.drop_collection(collection).await;
    client
        .create_collection(
            collection,
            128,
            CreateCollectionOptions::default()
                .metric(Metric::L2)
                .index_type(IndexType::Hnsw),
        )
        .await?;

    let records: Vec<VectorRecord> = (1..=100u64)
        .map(|id| {
            let mut meta = Metadata::new();
            meta.insert("category".to_owned(), MetadataValue::from(format!("cat_{}", id % 5)));
            meta.insert("score".to_owned(), MetadataValue::from(id as f64));
            let values: Vec<f32> = (0..128).map(|j| ((id as usize + j + 1) as f32) / 128.0).collect();
            VectorRecord::new(id, values).with_metadata(meta)
        })
        .collect();
    let inserted = client.insert(collection, &records).await?;
    println!("Inserted {inserted} vectors");

    let query: Vec<f32> = (0..128).map(|j| ((j + 2) as f32) / 128.0).collect();
    let hits = client
        .search(
            collection,
            &query,
            SearchOptions::default()
                .top_k(5)
                .filter("category = 'cat_0'")
                .return_metadata(true),
        )
        .await?;
    for hit in hits {
        println!("ID={} distance={:.4} metadata={:?}", hit.id, hit.distance, hit.metadata);
    }

    let fetched = client.get(collection, &[1, 2, 3], true).await?;
    println!("Fetched {} vectors", fetched.len());

    client.drop_collection(collection).await?;
    Ok(())
}
```

## Three-way hybrid search

Combine dense vectors, BM25 text search, and sparse vectors:

```rust
use gvdb::{HybridSearchOptions, Metadata, MetadataValue, SparseVector, VectorRecord};

let mut meta = Metadata::new();
meta.insert("text".to_owned(), MetadataValue::from("Waterproof trail running shoe"));

let record = VectorRecord::new(1, dense_embedding)
    .with_metadata(meta)
    .with_sparse_vector(SparseVector::from_pairs([(42, 0.8), (137, 0.3), (2048, 1.2)]));
client.insert("products", &[record]).await?;

let hits = client
    .hybrid_search(
        "products",
        HybridSearchOptions::default()
            .query_vector(query_dense)
            .text_query("running shoes")
            .sparse_query(SparseVector::from_pairs([(42, 0.7), (137, 0.5)]))
            .text_field("text")
            .top_k(5)
            .vector_weight(0.5)
            .text_weight(0.3)
            .sparse_weight(0.2)
            .return_metadata(true),
    )
    .await?;
```

## Per-vector TTL

```rust
use gvdb::VectorRecord;

let records = vec![
    VectorRecord::new(1, vec![0.1; 384]).with_ttl_seconds(3600),
    VectorRecord::new(2, vec![0.2; 384]).with_ttl_seconds(0),
];
client.insert("sessions", &records).await?;
```

## RBAC and error handling

```rust
use gvdb::{ClientConfig, SearchOptions};
use tonic::Code;

let analyst = ClientConfig::new("localhost:50051")
    .api_key("analyst-key")
    .connect()
    .await?;

let _hits = analyst
    .search("shared", &vec![0.1; 384], SearchOptions::default().top_k(5))
    .await?;

match analyst.drop_collection("shared").await {
    Ok(()) => {}
    Err(err) if err.status().map(|s| s.code()) == Some(Code::PermissionDenied) => {
        println!("Write rejected by RBAC: {err}");
    }
    Err(err) => return Err(err),
}
```

## See also

- [Client API](client.md)
- [Bulk import](bulk-import.md)
- [RBAC](../features/rbac.md)
