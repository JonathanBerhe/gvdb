//! Offline unit tests for `GvdbClient` using an in-process mock gRPC server.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gvdb::pb::vector_db_service_server::{VectorDbService, VectorDbServiceServer};
use gvdb::{
    BackupState, BackupTarget, BulkImportFormat, BulkImportOptions, ClientConfig,
    CreateCollectionOptions, Error, GvdbClient, HybridSearchOptions, ImportState, IndexType,
    Metadata, MetadataValue, Metric, PollOptions, RangeSearchOptions, SearchOptions, SparseVector,
    VectorRecord, pb,
};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};

#[derive(Default)]
struct MockState {
    auth_headers: Vec<(&'static str, Option<String>)>,
    last_create: Option<pb::CreateCollectionRequest>,
    last_insert: Option<pb::InsertRequest>,
    stream_batches: Vec<pb::InsertRequest>,
    last_get: Option<pb::GetRequest>,
    last_range: Option<pb::RangeSearchRequest>,
    last_hybrid: Option<pb::HybridSearchRequest>,
    last_update_meta: Option<pb::UpdateMetadataRequest>,
    last_bulk_import: Option<pb::BulkImportRequest>,
    last_backup: Option<pb::BackupCollectionRequest>,
    last_restore: Option<pb::RestoreCollectionRequest>,
    import_statuses: VecDeque<pb::GetImportStatusResponse>,
    backup_statuses: VecDeque<pb::GetBackupStatusResponse>,
    restore_statuses: VecDeque<pb::GetRestoreStatusResponse>,
}

#[derive(Clone, Default)]
struct MockServer {
    state: Arc<Mutex<MockState>>,
}

impl MockServer {
    fn record_auth<T>(&self, method: &'static str, req: &Request<T>) {
        let header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        self.state
            .lock()
            .unwrap()
            .auth_headers
            .push((method, header));
    }
}

#[tonic::async_trait]
impl VectorDbService for MockServer {
    async fn create_collection(
        &self,
        request: Request<pb::CreateCollectionRequest>,
    ) -> Result<Response<pb::CreateCollectionResponse>, Status> {
        self.record_auth("CreateCollection", &request);
        self.state.lock().unwrap().last_create = Some(request.into_inner());
        Ok(Response::new(pb::CreateCollectionResponse {
            collection_id: 7,
            message: "created".to_owned(),
        }))
    }

    async fn drop_collection(
        &self,
        request: Request<pb::DropCollectionRequest>,
    ) -> Result<Response<pb::DropCollectionResponse>, Status> {
        self.record_auth("DropCollection", &request);
        Ok(Response::new(pb::DropCollectionResponse {
            message: "dropped".to_owned(),
        }))
    }

    async fn list_collections(
        &self,
        request: Request<pb::ListCollectionsRequest>,
    ) -> Result<Response<pb::ListCollectionsResponse>, Status> {
        self.record_auth("ListCollections", &request);
        Ok(Response::new(pb::ListCollectionsResponse {
            collections: vec![pb::CollectionInfo {
                collection_id: 7,
                collection_name: "docs".to_owned(),
                dimension: 4,
                metric_type: "COSINE".to_owned(),
                vector_count: 10,
            }],
        }))
    }

    async fn insert(
        &self,
        request: Request<pb::InsertRequest>,
    ) -> Result<Response<pb::InsertResponse>, Status> {
        self.record_auth("Insert", &request);
        let inner = request.into_inner();
        let count = inner.vectors.len() as u64;
        self.state.lock().unwrap().last_insert = Some(inner);
        Ok(Response::new(pb::InsertResponse {
            inserted_count: count,
            message: "ok".to_owned(),
        }))
    }

    async fn stream_insert(
        &self,
        request: Request<Streaming<pb::InsertRequest>>,
    ) -> Result<Response<pb::InsertResponse>, Status> {
        self.record_auth("StreamInsert", &request);
        let mut stream = request.into_inner();
        let mut total = 0u64;
        let mut batches = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            total += chunk.vectors.len() as u64;
            batches.push(chunk);
        }
        self.state.lock().unwrap().stream_batches = batches;
        Ok(Response::new(pb::InsertResponse {
            inserted_count: total,
            message: "ok".to_owned(),
        }))
    }

    async fn upsert(
        &self,
        request: Request<pb::UpsertRequest>,
    ) -> Result<Response<pb::UpsertResponse>, Status> {
        self.record_auth("Upsert", &request);
        let n = request.into_inner().vectors.len() as u64;
        Ok(Response::new(pb::UpsertResponse {
            upserted_count: n,
            inserted_count: n.saturating_sub(1),
            updated_count: u64::from(n > 0),
            message: "ok".to_owned(),
        }))
    }

    async fn search(
        &self,
        request: Request<pb::SearchRequest>,
    ) -> Result<Response<pb::SearchResponse>, Status> {
        self.record_auth("Search", &request);
        let req = request.into_inner();
        let metadata = if req.return_metadata {
            Some(pb::Metadata {
                fields: HashMap::from([(
                    "tag".to_owned(),
                    pb::MetadataValue {
                        value: Some(pb::metadata_value::Value::StringValue("alpha".to_owned())),
                    },
                )]),
            })
        } else {
            None
        };
        Ok(Response::new(pb::SearchResponse {
            results: vec![pb::SearchResultEntry {
                id: 1,
                distance: 0.25,
                metadata,
            }],
            query_time_ms: 0.5,
        }))
    }

    async fn range_search(
        &self,
        request: Request<pb::RangeSearchRequest>,
    ) -> Result<Response<pb::RangeSearchResponse>, Status> {
        self.record_auth("RangeSearch", &request);
        self.state.lock().unwrap().last_range = Some(request.into_inner());
        Ok(Response::new(pb::RangeSearchResponse {
            results: vec![pb::SearchResultEntry {
                id: 2,
                distance: 0.4,
                metadata: None,
            }],
            query_time_ms: 0.3,
        }))
    }

    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetResponse>, Status> {
        self.record_auth("Get", &request);
        let req = request.into_inner();
        let include_meta = req.return_metadata;
        self.state.lock().unwrap().last_get = Some(req);
        let metadata = if include_meta {
            Some(pb::Metadata {
                fields: HashMap::from([(
                    "score".to_owned(),
                    pb::MetadataValue {
                        value: Some(pb::metadata_value::Value::IntValue(99)),
                    },
                )]),
            })
        } else {
            None
        };
        Ok(Response::new(pb::GetResponse {
            vectors: vec![pb::VectorWithId {
                id: 1,
                vector: Some(pb::Vector {
                    values: vec![1.0, 2.0],
                    dimension: 2,
                }),
                metadata,
                sparse_vector: None,
                ttl_seconds: 60,
            }],
            not_found_ids: vec![],
        }))
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::DeleteResponse>, Status> {
        self.record_auth("Delete", &request);
        let n = request.into_inner().ids.len() as u64;
        Ok(Response::new(pb::DeleteResponse {
            deleted_count: n,
            not_found_ids: vec![],
            message: "ok".to_owned(),
        }))
    }

    async fn update_metadata(
        &self,
        request: Request<pb::UpdateMetadataRequest>,
    ) -> Result<Response<pb::UpdateMetadataResponse>, Status> {
        self.record_auth("UpdateMetadata", &request);
        self.state.lock().unwrap().last_update_meta = Some(request.into_inner());
        Ok(Response::new(pb::UpdateMetadataResponse {
            updated: true,
            message: "ok".to_owned(),
        }))
    }

    async fn list_vectors(
        &self,
        request: Request<pb::ListVectorsRequest>,
    ) -> Result<Response<pb::ListVectorsResponse>, Status> {
        self.record_auth("ListVectors", &request);
        Ok(Response::new(pb::ListVectorsResponse {
            vectors: vec![pb::VectorWithId {
                id: 10,
                vector: Some(pb::Vector {
                    values: vec![0.5, 0.5],
                    dimension: 2,
                }),
                metadata: None,
                sparse_vector: None,
                ttl_seconds: 0,
            }],
            total_count: 25,
            has_more: true,
        }))
    }

    async fn hybrid_search(
        &self,
        request: Request<pb::HybridSearchRequest>,
    ) -> Result<Response<pb::HybridSearchResponse>, Status> {
        self.record_auth("HybridSearch", &request);
        self.state.lock().unwrap().last_hybrid = Some(request.into_inner());
        Ok(Response::new(pb::HybridSearchResponse {
            results: vec![pb::SearchResultEntry {
                id: 3,
                distance: 0.1,
                metadata: None,
            }],
            query_time_ms: 1.0,
        }))
    }

    async fn bulk_import(
        &self,
        request: Request<pb::BulkImportRequest>,
    ) -> Result<Response<pb::BulkImportResponse>, Status> {
        self.record_auth("BulkImport", &request);
        self.state.lock().unwrap().last_bulk_import = Some(request.into_inner());
        Ok(Response::new(pb::BulkImportResponse {
            import_id: "imp-1".to_owned(),
            message: "started".to_owned(),
        }))
    }

    async fn get_import_status(
        &self,
        request: Request<pb::GetImportStatusRequest>,
    ) -> Result<Response<pb::GetImportStatusResponse>, Status> {
        self.record_auth("GetImportStatus", &request);
        let next = self.state.lock().unwrap().import_statuses.pop_front();
        Ok(Response::new(next.unwrap_or(pb::GetImportStatusResponse {
            import_id: "imp-1".to_owned(),
            state: pb::ImportState::ImportCompleted as i32,
            total_vectors: 100,
            imported_vectors: 100,
            progress_percent: 100.0,
            error_message: String::new(),
            elapsed_seconds: 1.2,
            segments_created: 2,
        })))
    }

    async fn cancel_import(
        &self,
        request: Request<pb::CancelImportRequest>,
    ) -> Result<Response<pb::CancelImportResponse>, Status> {
        self.record_auth("CancelImport", &request);
        Ok(Response::new(pb::CancelImportResponse {
            success: true,
            message: "cancelled".to_owned(),
        }))
    }

    async fn backup_collection(
        &self,
        request: Request<pb::BackupCollectionRequest>,
    ) -> Result<Response<pb::BackupCollectionResponse>, Status> {
        self.record_auth("BackupCollection", &request);
        self.state.lock().unwrap().last_backup = Some(request.into_inner());
        Ok(Response::new(pb::BackupCollectionResponse {
            backup_id: "bk-1".to_owned(),
            message: "started".to_owned(),
        }))
    }

    async fn restore_collection(
        &self,
        request: Request<pb::RestoreCollectionRequest>,
    ) -> Result<Response<pb::RestoreCollectionResponse>, Status> {
        self.record_auth("RestoreCollection", &request);
        self.state.lock().unwrap().last_restore = Some(request.into_inner());
        Ok(Response::new(pb::RestoreCollectionResponse {
            restore_id: "rs-1".to_owned(),
            message: "started".to_owned(),
        }))
    }

    async fn get_backup_status(
        &self,
        request: Request<pb::GetBackupStatusRequest>,
    ) -> Result<Response<pb::GetBackupStatusResponse>, Status> {
        self.record_auth("GetBackupStatus", &request);
        let next = self.state.lock().unwrap().backup_statuses.pop_front();
        Ok(Response::new(next.unwrap_or(pb::GetBackupStatusResponse {
            backup_id: "bk-1".to_owned(),
            state: pb::BackupState::BackupCompleted as i32,
            shards_total: 2,
            shards_completed: 2,
            bytes_uploaded: 4096,
            error_message: String::new(),
            elapsed_seconds: 0.8,
            manifest_uri: "s3://bkt/backups/bk-1/backup.manifest.json".to_owned(),
        })))
    }

    async fn get_restore_status(
        &self,
        request: Request<pb::GetRestoreStatusRequest>,
    ) -> Result<Response<pb::GetRestoreStatusResponse>, Status> {
        self.record_auth("GetRestoreStatus", &request);
        let next = self.state.lock().unwrap().restore_statuses.pop_front();
        Ok(Response::new(next.unwrap_or(
            pb::GetRestoreStatusResponse {
                restore_id: "rs-1".to_owned(),
                state: pb::BackupState::BackupCompleted as i32,
                shards_total: 2,
                shards_completed: 2,
                error_message: String::new(),
                elapsed_seconds: 0.4,
            },
        )))
    }

    async fn list_backups(
        &self,
        request: Request<pb::ListBackupsRequest>,
    ) -> Result<Response<pb::ListBackupsResponse>, Status> {
        self.record_auth("ListBackups", &request);
        Ok(Response::new(pb::ListBackupsResponse {
            backups: vec![pb::BackupInfo {
                backup_id: "bk-1".to_owned(),
                collection_name: "docs".to_owned(),
                collection_id: 7,
                created_at_unix_ms: 1_700_000_000_000,
                vector_count: 1000,
                size_bytes: 8192,
                manifest_version: 1,
            }],
        }))
    }

    async fn cancel_backup(
        &self,
        request: Request<pb::CancelBackupRequest>,
    ) -> Result<Response<pb::CancelBackupResponse>, Status> {
        self.record_auth("CancelBackup", &request);
        Ok(Response::new(pb::CancelBackupResponse {
            success: true,
            message: "cancelled".to_owned(),
        }))
    }

    async fn health_check(
        &self,
        request: Request<pb::HealthCheckRequest>,
    ) -> Result<Response<pb::HealthCheckResponse>, Status> {
        self.record_auth("HealthCheck", &request);
        Ok(Response::new(pb::HealthCheckResponse {
            status: pb::health_check_response::Status::Serving as i32,
            message: "SERVING".to_owned(),
        }))
    }

    async fn get_stats(
        &self,
        request: Request<pb::GetStatsRequest>,
    ) -> Result<Response<pb::GetStatsResponse>, Status> {
        self.record_auth("GetStats", &request);
        Ok(Response::new(pb::GetStatsResponse {
            total_vectors: 42,
            total_collections: 2,
            total_queries: 7,
            avg_query_time_ms: 1.5,
        }))
    }
}

async fn spawn_mock_server() -> (String, Arc<Mutex<MockState>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = MockServer::default();
    let state = server.state.clone();
    let incoming = TcpListenerStream::new(listener);

    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(VectorDbServiceServer::new(server))
            .serve_with_incoming(incoming)
            .await;
    });

    (format!("127.0.0.1:{}", addr.port()), state)
}

#[tokio::test]
async fn every_rpc_sends_bearer_auth_and_maps_fields() {
    let (addr, state) = spawn_mock_server().await;
    let client = ClientConfig::new(addr)
        .api_key("test-secret")
        .timeout(Duration::from_secs(5))
        .connect()
        .await
        .unwrap();

    assert_eq!(client.health_check().await.unwrap(), "SERVING");
    let stats = client.get_stats().await.unwrap();
    assert_eq!(stats.total_vectors, 42);
    assert_eq!(stats.total_collections, 2);

    let cid = client
        .create_collection(
            "docs",
            4,
            CreateCollectionOptions::default()
                .metric(Metric::Cosine)
                .index_type(IndexType::IvfTurboQuant)
                .num_shards(3),
        )
        .await
        .unwrap();
    assert_eq!(cid, 7);
    {
        let s = state.lock().unwrap();
        let req = s.last_create.as_ref().unwrap();
        assert_eq!(req.collection_name, "docs");
        assert_eq!(req.dimension, 4);
        assert_eq!(
            req.metric,
            pb::create_collection_request::MetricType::Cosine as i32
        );
        assert_eq!(
            req.index_type,
            pb::create_collection_request::IndexType::IvfTurboquant as i32
        );
        assert_eq!(req.num_shards, 3);
    }

    let colls = client.list_collections().await.unwrap();
    assert_eq!(colls.len(), 1);
    assert_eq!(colls[0].name, "docs");

    let mut meta = Metadata::new();
    meta.insert("count".to_owned(), MetadataValue::from(5i64));
    meta.insert("score".to_owned(), MetadataValue::from(2.5f64));
    meta.insert("label".to_owned(), MetadataValue::from("shoe"));
    meta.insert("active".to_owned(), MetadataValue::from(true));

    let unsorted_sparse = SparseVector {
        indices: vec![20, 5, 12],
        values: vec![0.2, 0.9, 0.4],
    };
    let rec = VectorRecord::new(1, vec![0.1, 0.2, 0.3, 0.4])
        .with_metadata(meta.clone())
        .with_sparse_vector(unsorted_sparse)
        .with_ttl_seconds(120);

    assert_eq!(client.insert("docs", &[rec]).await.unwrap(), 1);
    {
        let s = state.lock().unwrap();
        let inserted = &s.last_insert.as_ref().unwrap().vectors[0];
        let sv = inserted.sparse_vector.as_ref().unwrap();
        assert_eq!(sv.indices, vec![5, 12, 20]);
        assert_eq!(sv.values, vec![0.9, 0.4, 0.2]);
        assert_eq!(inserted.ttl_seconds, 120);
    }

    let records = vec![
        VectorRecord::new(1, vec![1.0, 0.0]),
        VectorRecord::new(2, vec![0.0, 1.0]),
        VectorRecord::new(3, vec![1.0, 1.0]),
    ];
    assert_eq!(client.stream_insert("docs", &records, 2).await.unwrap(), 3);
    assert_eq!(state.lock().unwrap().stream_batches.len(), 2);

    let upserted = client.upsert("docs", &records).await.unwrap();
    assert_eq!(upserted.upserted_count, 3);

    let fetched = client.get("docs", &[1], true).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(
        fetched[0].metadata.as_ref().unwrap().get("score"),
        Some(&MetadataValue::Int(99))
    );
    assert!(
        state
            .lock()
            .unwrap()
            .last_get
            .as_ref()
            .unwrap()
            .return_metadata
    );

    let fetched_no_meta = client.get("docs", &[1], false).await.unwrap();
    assert!(fetched_no_meta[0].metadata.is_none());
    assert!(
        !state
            .lock()
            .unwrap()
            .last_get
            .as_ref()
            .unwrap()
            .return_metadata
    );

    assert!(
        client
            .update_metadata("docs", 1, meta, false)
            .await
            .unwrap()
    );
    assert!(
        !state
            .lock()
            .unwrap()
            .last_update_meta
            .as_ref()
            .unwrap()
            .merge
    );

    let page = client.list_vectors("docs", 10, 0, false).await.unwrap();
    assert_eq!(page.total_count, 25);
    assert!(page.has_more);

    let results = client
        .search(
            "docs",
            &[0.1, 0.2],
            SearchOptions::default().return_metadata(true),
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].metadata.as_ref().unwrap().get("tag"),
        Some(&MetadataValue::String("alpha".to_owned()))
    );

    let range_res = client
        .range_search(
            "docs",
            &[0.1, 0.2],
            RangeSearchOptions::default()
                .similarity_threshold(0.8)
                .max_results(50),
        )
        .await
        .unwrap();
    assert_eq!(range_res.len(), 1);
    assert_eq!(
        state
            .lock()
            .unwrap()
            .last_range
            .as_ref()
            .unwrap()
            .similarity_threshold,
        Some(0.8)
    );

    let hybrid_res = client
        .hybrid_search(
            "docs",
            HybridSearchOptions::default()
                .query_vector(vec![0.1, 0.2])
                .text_query("shoes")
                .sparse_query(SparseVector::from_pairs([(7, 0.5)]))
                .sparse_weight(0.2)
                .text_field("description"),
        )
        .await
        .unwrap();
    assert_eq!(hybrid_res.len(), 1);
    assert_eq!(
        state
            .lock()
            .unwrap()
            .last_hybrid
            .as_ref()
            .unwrap()
            .text_field,
        "description"
    );

    let imp_id = client
        .bulk_import(
            "docs",
            "s3://bkt/data.npy",
            BulkImportOptions::default().format(BulkImportFormat::Numpy),
        )
        .await
        .unwrap();
    assert_eq!(imp_id, "imp-1");
    assert_eq!(
        state
            .lock()
            .unwrap()
            .last_bulk_import
            .as_ref()
            .unwrap()
            .format,
        pb::ImportFormat::Numpy as i32
    );
    assert_eq!(
        client.get_import_status("imp-1").await.unwrap().state,
        ImportState::Completed
    );
    assert!(client.cancel_import("imp-1").await.unwrap());

    let bk_id = client
        .backup_collection(
            "docs",
            BackupTarget::s3("bkt", Some("prod")),
            Some("bk-1".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(bk_id, "bk-1");
    let rs_id = client
        .restore_collection(
            "bk-1",
            BackupTarget::local("/backups"),
            Some("docs_restored".to_owned()),
            true,
        )
        .await
        .unwrap();
    assert_eq!(rs_id, "rs-1");
    assert_eq!(
        client.get_backup_status("bk-1").await.unwrap().state,
        BackupState::Completed
    );
    assert_eq!(
        client.get_restore_status("rs-1").await.unwrap().state,
        BackupState::Completed
    );
    let backups = client
        .list_backups(BackupTarget::s3("bkt", None::<String>))
        .await
        .unwrap();
    assert_eq!(backups.len(), 1);
    assert_eq!(backups[0].backup_id, "bk-1");
    assert!(client.cancel_backup("bk-1").await.unwrap());

    assert_eq!(client.delete("docs", &[1, 2]).await.unwrap(), 2);
    client.drop_collection("docs").await.unwrap();

    let headers = state.lock().unwrap().auth_headers.clone();
    assert!(headers.len() >= 22);
    for (method, header) in headers {
        assert_eq!(
            header.as_deref(),
            Some("Bearer test-secret"),
            "Method {method} did not send the authorization header"
        );
    }
}

#[tokio::test]
async fn polling_helpers_handle_completion_failure_and_timeout() {
    let (addr, state) = spawn_mock_server().await;
    let client = GvdbClient::connect(addr).await.unwrap();
    let fast_poll = PollOptions::default()
        .poll_interval(Duration::from_millis(5))
        .timeout(Duration::from_millis(200));

    // wait_for_import transitions Running -> Completed
    {
        let mut s = state.lock().unwrap();
        s.import_statuses.push_back(pb::GetImportStatusResponse {
            import_id: "imp-1".to_owned(),
            state: pb::ImportState::ImportRunning as i32,
            ..Default::default()
        });
        s.import_statuses.push_back(pb::GetImportStatusResponse {
            import_id: "imp-1".to_owned(),
            state: pb::ImportState::ImportCompleted as i32,
            imported_vectors: 50,
            ..Default::default()
        });
    }
    let imp = client.wait_for_import("imp-1", fast_poll).await.unwrap();
    assert_eq!(imp.state, ImportState::Completed);
    assert_eq!(imp.imported_vectors, 50);

    // wait_for_backup returns JobFailed on BackupFailed
    state
        .lock()
        .unwrap()
        .backup_statuses
        .push_back(pb::GetBackupStatusResponse {
            backup_id: "bk-1".to_owned(),
            state: pb::BackupState::BackupFailed as i32,
            error_message: "disk full".to_owned(),
            ..Default::default()
        });
    let err = client.wait_for_backup("bk-1", fast_poll).await.unwrap_err();
    assert!(matches!(err, Error::JobFailed { kind: "Backup", .. }));
    assert!(err.to_string().contains("disk full"));

    // wait_for_restore times out when stuck in Running
    for _ in 0..20 {
        state
            .lock()
            .unwrap()
            .restore_statuses
            .push_back(pb::GetRestoreStatusResponse {
                restore_id: "rs-1".to_owned(),
                state: pb::BackupState::BackupRunning as i32,
                ..Default::default()
            });
    }
    let timeout_poll = PollOptions::default()
        .poll_interval(Duration::from_millis(5))
        .timeout(Duration::from_millis(25));
    let err = client
        .wait_for_restore("rs-1", timeout_poll)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Timeout(_)));
}

#[tokio::test]
async fn client_side_validation_rejects_bad_arguments() {
    let (addr, _) = spawn_mock_server().await;

    let bad_key = ClientConfig::new(addr.clone())
        .api_key("bad\nkey")
        .connect()
        .await
        .unwrap_err();
    assert!(matches!(bad_key, Error::InvalidArgument(_)));

    let bad_tls = ClientConfig::new("http://127.0.0.1:50051")
        .tls(true)
        .connect()
        .await
        .unwrap_err();
    assert!(matches!(bad_tls, Error::InvalidArgument(_)));

    let client = GvdbClient::connect(addr).await.unwrap();
    let err = client.stream_insert("docs", &[], 0).await.unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));

    let err = client
        .backup_collection("docs", BackupTarget::s3("", None::<String>), None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));

    let bad_sparse = VectorRecord::new(1, vec![1.0]).with_sparse_vector(SparseVector {
        indices: vec![1, 2],
        values: vec![0.5],
    });
    let err = client.insert("docs", &[bad_sparse]).await.unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
}
