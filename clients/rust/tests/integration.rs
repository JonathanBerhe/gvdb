//! End-to-end integration tests against a real `gvdb-single-node` binary.
//!
//! Set `GVDB_SERVER_ADDR` to run against an external server, or set
//! `GVDB_SERVER_BIN` to point to a compiled `gvdb-single-node` binary.
//! When neither is available locally, the tests return early so offline
//! `cargo test` passes without a C++ build.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use gvdb::{
    ClientConfig, CreateCollectionOptions, GvdbClient, HybridSearchOptions, ImportOptions,
    IndexType, Metadata, MetadataValue, Metric, RangeSearchOptions, SearchOptions, VectorRecord,
};
use tempfile::TempDir;
use tonic::Code;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct ServerGuard {
    child: Child,
    address: String,
    _data_dir: TempDir,
    _config_file: Option<tempfile::NamedTempFile>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> TestResult<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn find_server_binary() -> TestResult<Option<PathBuf>> {
    if let Ok(explicit) = std::env::var("GVDB_SERVER_BIN") {
        let path = PathBuf::from(&explicit);
        if !path.is_file() {
            return Err(format!(
                "GVDB_SERVER_BIN is set to '{explicit}', but the file does not exist."
            )
            .into());
        }
        return Ok(Some(path));
    }

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let default_bin = manifest.join("../../build/dev/bin/gvdb-single-node");
    Ok(default_bin.is_file().then_some(default_bin))
}

async fn wait_for_server(address: &str, timeout: Duration) -> TestResult {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or("wait_for_server timeout overflow")?;
    while Instant::now() < deadline {
        if let Ok(client) = ClientConfig::new(address)
            .timeout(Duration::from_secs(2))
            .connect()
            .await
            && client.health_check().await.is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(format!("Server at {address} did not become ready within {timeout:?}").into())
}

async fn start_local_server(binary: &Path, config_yaml: Option<String>) -> TestResult<ServerGuard> {
    let port = free_port()?;
    let data_dir = TempDir::new()?;
    let address = format!("127.0.0.1:{port}");

    let mut cmd = Command::new(binary);
    let mut config_file = None;

    if let Some(yaml) = config_yaml {
        let mut tmp = tempfile::NamedTempFile::new()?;
        std::io::Write::write_all(&mut tmp, yaml.as_bytes())?;
        cmd.arg("--config")
            .arg(tmp.path())
            .arg("--port")
            .arg(port.to_string())
            .arg("--data-dir")
            .arg(data_dir.path());
        config_file = Some(tmp);
    } else {
        cmd.arg("--port")
            .arg(port.to_string())
            .arg("--data-dir")
            .arg(data_dir.path())
            .arg("--node-id")
            .arg("1");
    }

    let child = cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn()?;

    let guard = ServerGuard {
        child,
        address: address.clone(),
        _data_dir: data_dir,
        _config_file: config_file,
    };
    wait_for_server(&address, Duration::from_secs(15)).await?;
    Ok(guard)
}

fn deterministic_vector(seed: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|j| ((seed + j + 1) as f32) / (dim as f32))
        .collect()
}

#[tokio::test]
async fn end_to_end_sdk_operations() -> TestResult {
    let _guard;
    let address = if let Ok(external) = std::env::var("GVDB_SERVER_ADDR") {
        wait_for_server(&external, Duration::from_secs(15)).await?;
        external
    } else if let Some(bin) = find_server_binary()? {
        let g = start_local_server(&bin, None).await?;
        let addr = g.address.clone();
        _guard = g;
        addr
    } else {
        eprintln!("Skipping integration test: set GVDB_SERVER_BIN or GVDB_SERVER_ADDR.");
        return Ok(());
    };

    let client = GvdbClient::connect(&address).await?;
    let collection = "rust_sdk_test_collection";
    let dim = 32usize;

    let health = client.health_check().await?;
    assert!(!health.is_empty());

    let _stats = client.get_stats().await?;

    let _ = client.drop_collection(collection).await;
    let cid = client
        .create_collection(
            collection,
            dim as u32,
            CreateCollectionOptions::default()
                .metric(Metric::L2)
                .index_type(IndexType::Flat),
        )
        .await?;
    assert!(cid > 0);

    let colls = client.list_collections().await?;
    assert!(colls.iter().any(|c| c.name == collection));

    // Insert 40 vectors with metadata.
    let records: Vec<VectorRecord> = (1..=40u64)
        .map(|id| {
            let mut meta = Metadata::new();
            meta.insert(
                "category".to_owned(),
                MetadataValue::from(format!("cat_{}", id % 4)),
            );
            meta.insert("score".to_owned(), MetadataValue::from(id as f64));
            VectorRecord::new(id, deterministic_vector(id as usize, dim)).with_metadata(meta)
        })
        .collect();
    assert_eq!(client.insert(collection, &records).await?, 40);

    // Stream insert another 60 vectors in chunks of 20.
    let stream_records: Vec<VectorRecord> = (41..=100u64)
        .map(|id| VectorRecord::new(id, deterministic_vector(id as usize, dim)))
        .collect();
    assert_eq!(
        client
            .stream_insert(collection, &stream_records, 20)
            .await?,
        60
    );

    // Nearest-neighbor search.
    let query = deterministic_vector(1, dim);
    let results = client
        .search(collection, &query, SearchOptions::default().top_k(5))
        .await?;
    assert_eq!(results.len(), 5);
    for pair in results.windows(2) {
        if let (Some(prev), Some(next)) = (pair.first(), pair.get(1)) {
            assert!(next.distance >= prev.distance);
        }
    }

    // Filtered search with metadata.
    let filtered = client
        .search(
            collection,
            &query,
            SearchOptions::default()
                .top_k(5)
                .filter("category = 'cat_0'")
                .return_metadata(true),
        )
        .await?;
    assert!(!filtered.is_empty());
    for hit in &filtered {
        assert_eq!(
            hit.metadata.as_ref().and_then(|m| m.get("category")),
            Some(&MetadataValue::String("cat_0".to_owned()))
        );
    }

    // Hybrid search and range search.
    let _hybrid = client
        .hybrid_search(
            collection,
            HybridSearchOptions::default()
                .query_vector(query.clone())
                .top_k(5),
        )
        .await?;

    let range_hits = client
        .range_search(
            collection,
            &query,
            RangeSearchOptions::default()
                .radius(100_000.0)
                .max_results(20),
        )
        .await?;
    assert!(!range_hits.is_empty());

    // Get with and without metadata.
    let got_with_meta = client.get(collection, &[1, 2, 3], true).await?;
    assert_eq!(got_with_meta.len(), 3);
    assert!(got_with_meta.iter().all(|v| v.metadata.is_some()));

    let got_no_meta = client.get(collection, &[1], false).await?;
    assert_eq!(got_no_meta.len(), 1);
    assert!(
        got_no_meta
            .first()
            .ok_or("missing record from get")?
            .metadata
            .is_none()
    );

    // List vectors pagination.
    let page = client.list_vectors(collection, 10, 0, false).await?;
    assert!(!page.vectors.is_empty());
    assert!(page.total_count >= 100);

    // Upsert existing + new vector.
    let upsert_res = client
        .upsert(
            collection,
            &[
                VectorRecord::new(1, deterministic_vector(1, dim)),
                VectorRecord::new(99_999, deterministic_vector(99, dim)),
            ],
        )
        .await?;
    assert_eq!(upsert_res.upserted_count, 2);

    // Update metadata and confirm via filtered search.
    let mut patch = Metadata::new();
    patch.insert("tag".to_owned(), MetadataValue::from("updated"));
    patch.insert("price".to_owned(), MetadataValue::from(42.0f64));
    assert!(client.update_metadata(collection, 5, patch, true).await?);
    let updated_hits = client
        .search(
            collection,
            &query,
            SearchOptions::default()
                .top_k(1)
                .filter("tag = 'updated'")
                .return_metadata(true),
        )
        .await?;
    assert_eq!(updated_hits.len(), 1);
    assert_eq!(updated_hits.first().map(|h| h.id), Some(5));

    // Client-side import_records into a new collection.
    let import_coll = "rust_sdk_import_records";
    let _ = client.drop_collection(import_coll).await;
    let imp_records: Vec<VectorRecord> = (1..=10u64)
        .map(|id| VectorRecord::new(id, deterministic_vector(id as usize, 8)))
        .collect();
    let imp_res = client
        .import_records(
            import_coll,
            &imp_records,
            ImportOptions::default()
                .batch_size(4)
                .metric(Metric::Cosine)
                .index_type(IndexType::Flat),
        )
        .await?;
    assert_eq!(imp_res.total_count, 10);
    assert!(imp_res.created_collection);
    client.drop_collection(import_coll).await?;

    // Delete and drop.
    assert_eq!(client.delete(collection, &[5, 6, 7]).await?, 3);
    assert!(client.get(collection, &[5, 6, 7], false).await?.is_empty());

    client.drop_collection(collection).await?;
    let remaining = client.list_collections().await?;
    assert!(!remaining.iter().any(|c| c.name == collection));
    Ok(())
}

#[tokio::test]
async fn end_to_end_rbac_enforcement() -> TestResult {
    if std::env::var("GVDB_SERVER_ADDR").is_ok() {
        eprintln!("Skipping RBAC test against external server.");
        return Ok(());
    }
    let Some(bin) = find_server_binary()? else {
        eprintln!("Skipping RBAC integration test: set GVDB_SERVER_BIN.");
        return Ok(());
    };

    let collection = "rust_rbac_test";
    let dim = 4usize;
    let port = free_port()?;
    let yaml = format!(
        r#"
server:
  grpc_port: {port}
  auth:
    enabled: true
    api_keys: ["legacy-key"]
    roles:
      - key: "admin-key"
        role: admin
      - key: "rw-key"
        role: readwrite
        collections: ["{collection}"]
      - key: "ro-key"
        role: readonly
        collections: ["*"]
consensus:
  node_id: 1
  single_node_mode: true
logging:
  level: info
index:
  default_index_type: flat
"#
    );

    let guard = start_local_server(&bin, Some(yaml)).await?;
    let addr = guard.address.clone();

    let admin = ClientConfig::new(&addr)
        .api_key("admin-key")
        .connect()
        .await?;
    let legacy = ClientConfig::new(&addr)
        .api_key("legacy-key")
        .connect()
        .await?;
    let rw = ClientConfig::new(&addr).api_key("rw-key").connect().await?;
    let ro = ClientConfig::new(&addr).api_key("ro-key").connect().await?;
    let no_auth = GvdbClient::connect(&addr).await?;

    admin
        .create_collection(
            collection,
            dim as u32,
            CreateCollectionOptions::default()
                .metric(Metric::L2)
                .index_type(IndexType::Flat),
        )
        .await?;
    admin
        .insert(
            collection,
            &[VectorRecord::new(1, deterministic_vector(1, dim))],
        )
        .await?;

    // Missing key is rejected.
    let Err(err) = no_auth.list_collections().await else {
        return Err("expected unauthenticated error".into());
    };
    let code = err.status().ok_or("expected gRPC status")?.code();
    assert!(matches!(code, Code::Unauthenticated | Code::Cancelled));

    // Legacy key acts as admin.
    let hits = legacy
        .search(
            collection,
            &deterministic_vector(1, dim),
            SearchOptions::default().top_k(1),
        )
        .await?;
    assert_eq!(hits.len(), 1);

    // Readonly can read but cannot write.
    assert_eq!(ro.get(collection, &[1], false).await?.len(), 1);
    let Err(err) = ro
        .upsert(
            collection,
            &[VectorRecord::new(1, deterministic_vector(1, dim))],
        )
        .await
    else {
        return Err("expected permission denied on readonly upsert".into());
    };
    assert_eq!(
        err.status().ok_or("expected gRPC status")?.code(),
        Code::PermissionDenied
    );

    // Readwrite can write to allowed collection, denied on others.
    assert_eq!(
        rw.insert(
            collection,
            &[VectorRecord::new(2, deterministic_vector(2, dim))],
        )
        .await?,
        1
    );

    admin
        .create_collection(
            "forbidden_coll",
            dim as u32,
            CreateCollectionOptions::default()
                .metric(Metric::L2)
                .index_type(IndexType::Flat),
        )
        .await?;
    let Err(err) = rw
        .insert(
            "forbidden_coll",
            &[VectorRecord::new(1, deterministic_vector(1, dim))],
        )
        .await
    else {
        return Err("expected permission denied on forbidden collection".into());
    };
    assert_eq!(
        err.status().ok_or("expected gRPC status")?.code(),
        Code::PermissionDenied
    );

    admin.drop_collection("forbidden_coll").await?;
    admin.drop_collection(collection).await?;
    drop(guard);
    Ok(())
}
