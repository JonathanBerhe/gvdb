//! Offline tests for client-side batch importers (in-memory, CSV, Parquet).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gvdb::pb::vector_db_service_server::{VectorDbService, VectorDbServiceServer};
use gvdb::{Error, GvdbClient, ImportMode, ImportOptions, MetadataValue, Metric, VectorRecord, pb};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Response, Status, Streaming};

#[derive(Default)]
struct ImportMockState {
    collections: Vec<pb::CollectionInfo>,
    upserted: Vec<pb::VectorWithId>,
    streamed: Vec<pb::VectorWithId>,
    upsert_errors: VecDeque<Status>,
    upsert_calls: usize,
    stream_errors: VecDeque<Status>,
    stream_calls: usize,
}

#[derive(Clone, Default)]
struct ImportMockServer {
    state: Arc<Mutex<ImportMockState>>,
}

#[tonic::async_trait]
impl VectorDbService for ImportMockServer {
    async fn create_collection(
        &self,
        request: Request<pb::CreateCollectionRequest>,
    ) -> Result<Response<pb::CreateCollectionResponse>, Status> {
        let req = request.into_inner();
        let mut s = self.state.lock().unwrap();
        let id = (s.collections.len() + 1) as u32;
        s.collections.push(pb::CollectionInfo {
            collection_id: id,
            collection_name: req.collection_name,
            dimension: req.dimension,
            metric_type: format!("{}", req.metric),
            vector_count: 0,
        });
        Ok(Response::new(pb::CreateCollectionResponse {
            collection_id: id,
            message: "ok".to_owned(),
        }))
    }

    async fn drop_collection(
        &self,
        _: Request<pb::DropCollectionRequest>,
    ) -> Result<Response<pb::DropCollectionResponse>, Status> {
        Ok(Response::new(pb::DropCollectionResponse::default()))
    }

    async fn list_collections(
        &self,
        _: Request<pb::ListCollectionsRequest>,
    ) -> Result<Response<pb::ListCollectionsResponse>, Status> {
        let collections = self.state.lock().unwrap().collections.clone();
        Ok(Response::new(pb::ListCollectionsResponse { collections }))
    }

    async fn insert(
        &self,
        _: Request<pb::InsertRequest>,
    ) -> Result<Response<pb::InsertResponse>, Status> {
        Ok(Response::new(pb::InsertResponse::default()))
    }

    async fn stream_insert(
        &self,
        request: Request<Streaming<pb::InsertRequest>>,
    ) -> Result<Response<pb::InsertResponse>, Status> {
        let mut stream = request.into_inner();
        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await {
            collected.extend(chunk?.vectors);
        }
        let mut s = self.state.lock().unwrap();
        s.stream_calls += 1;
        if let Some(err) = s.stream_errors.pop_front() {
            return Err(err);
        }
        let n = collected.len() as u64;
        s.streamed.extend(collected);
        Ok(Response::new(pb::InsertResponse {
            inserted_count: n,
            message: "ok".to_owned(),
        }))
    }

    async fn upsert(
        &self,
        request: Request<pb::UpsertRequest>,
    ) -> Result<Response<pb::UpsertResponse>, Status> {
        let req = request.into_inner();
        let mut s = self.state.lock().unwrap();
        s.upsert_calls += 1;
        if let Some(err) = s.upsert_errors.pop_front() {
            return Err(err);
        }
        let n = req.vectors.len() as u64;
        s.upserted.extend(req.vectors);
        Ok(Response::new(pb::UpsertResponse {
            upserted_count: n,
            inserted_count: n,
            updated_count: 0,
            message: "ok".to_owned(),
        }))
    }

    async fn search(
        &self,
        _: Request<pb::SearchRequest>,
    ) -> Result<Response<pb::SearchResponse>, Status> {
        Ok(Response::new(pb::SearchResponse::default()))
    }

    async fn range_search(
        &self,
        _: Request<pb::RangeSearchRequest>,
    ) -> Result<Response<pb::RangeSearchResponse>, Status> {
        Ok(Response::new(pb::RangeSearchResponse::default()))
    }

    async fn get(&self, _: Request<pb::GetRequest>) -> Result<Response<pb::GetResponse>, Status> {
        Ok(Response::new(pb::GetResponse::default()))
    }

    async fn delete(
        &self,
        _: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::DeleteResponse>, Status> {
        Ok(Response::new(pb::DeleteResponse::default()))
    }

    async fn update_metadata(
        &self,
        _: Request<pb::UpdateMetadataRequest>,
    ) -> Result<Response<pb::UpdateMetadataResponse>, Status> {
        Ok(Response::new(pb::UpdateMetadataResponse::default()))
    }

    async fn list_vectors(
        &self,
        _: Request<pb::ListVectorsRequest>,
    ) -> Result<Response<pb::ListVectorsResponse>, Status> {
        Ok(Response::new(pb::ListVectorsResponse::default()))
    }

    async fn hybrid_search(
        &self,
        _: Request<pb::HybridSearchRequest>,
    ) -> Result<Response<pb::HybridSearchResponse>, Status> {
        Ok(Response::new(pb::HybridSearchResponse::default()))
    }

    async fn bulk_import(
        &self,
        _: Request<pb::BulkImportRequest>,
    ) -> Result<Response<pb::BulkImportResponse>, Status> {
        Ok(Response::new(pb::BulkImportResponse::default()))
    }

    async fn get_import_status(
        &self,
        _: Request<pb::GetImportStatusRequest>,
    ) -> Result<Response<pb::GetImportStatusResponse>, Status> {
        Ok(Response::new(pb::GetImportStatusResponse::default()))
    }

    async fn cancel_import(
        &self,
        _: Request<pb::CancelImportRequest>,
    ) -> Result<Response<pb::CancelImportResponse>, Status> {
        Ok(Response::new(pb::CancelImportResponse::default()))
    }

    async fn backup_collection(
        &self,
        _: Request<pb::BackupCollectionRequest>,
    ) -> Result<Response<pb::BackupCollectionResponse>, Status> {
        Ok(Response::new(pb::BackupCollectionResponse::default()))
    }

    async fn restore_collection(
        &self,
        _: Request<pb::RestoreCollectionRequest>,
    ) -> Result<Response<pb::RestoreCollectionResponse>, Status> {
        Ok(Response::new(pb::RestoreCollectionResponse::default()))
    }

    async fn get_backup_status(
        &self,
        _: Request<pb::GetBackupStatusRequest>,
    ) -> Result<Response<pb::GetBackupStatusResponse>, Status> {
        Ok(Response::new(pb::GetBackupStatusResponse::default()))
    }

    async fn get_restore_status(
        &self,
        _: Request<pb::GetRestoreStatusRequest>,
    ) -> Result<Response<pb::GetRestoreStatusResponse>, Status> {
        Ok(Response::new(pb::GetRestoreStatusResponse::default()))
    }

    async fn list_backups(
        &self,
        _: Request<pb::ListBackupsRequest>,
    ) -> Result<Response<pb::ListBackupsResponse>, Status> {
        Ok(Response::new(pb::ListBackupsResponse::default()))
    }

    async fn cancel_backup(
        &self,
        _: Request<pb::CancelBackupRequest>,
    ) -> Result<Response<pb::CancelBackupResponse>, Status> {
        Ok(Response::new(pb::CancelBackupResponse::default()))
    }

    async fn health_check(
        &self,
        _: Request<pb::HealthCheckRequest>,
    ) -> Result<Response<pb::HealthCheckResponse>, Status> {
        Ok(Response::new(pb::HealthCheckResponse::default()))
    }

    async fn get_stats(
        &self,
        _: Request<pb::GetStatsRequest>,
    ) -> Result<Response<pb::GetStatsResponse>, Status> {
        Ok(Response::new(pb::GetStatsResponse::default()))
    }
}

async fn spawn_import_server() -> (GvdbClient, Arc<Mutex<ImportMockState>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = ImportMockServer::default();
    let state = server.state.clone();
    let incoming = TcpListenerStream::new(listener);

    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(VectorDbServiceServer::new(server))
            .serve_with_incoming(incoming)
            .await;
    });

    let client = GvdbClient::connect(format!("127.0.0.1:{}", addr.port()))
        .await
        .unwrap();
    (client, state)
}

#[tokio::test]
async fn import_records_batches_retries_and_validates_dimension() {
    let (client, state) = spawn_import_server().await;
    state
        .lock()
        .unwrap()
        .upsert_errors
        .push_back(Status::unavailable("temporary outage"));

    let records = vec![
        VectorRecord::new(1, vec![1.0, 2.0, 3.0]),
        VectorRecord::new(2, vec![4.0, 5.0, 6.0]),
        VectorRecord::new(3, vec![7.0, 8.0, 9.0]),
    ];

    let result = client
        .import_records(
            "items",
            &records,
            ImportOptions::default()
                .batch_size(2)
                .metric(Metric::L2)
                .retry_base_delay(Duration::from_millis(1)),
        )
        .await
        .unwrap();

    assert_eq!(result.total_count, 3);
    assert_eq!(result.batch_count, 2);
    assert_eq!(result.failed_count, 0);
    assert_eq!(result.dimension, 3);
    assert!(result.created_collection);
    assert_eq!(state.lock().unwrap().upsert_calls, 3);

    // Re-importing into existing collection with wrong dimension fails cleanly.
    let wrong_dim = vec![VectorRecord::new(4, vec![1.0, 2.0])];
    let err = client
        .import_records("items", &wrong_dim, ImportOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
}

#[tokio::test]
async fn import_records_skips_retries_for_resource_exhausted_and_stream_insert() {
    let (client, state) = spawn_import_server().await;
    state
        .lock()
        .unwrap()
        .upsert_errors
        .push_back(Status::new(Code::ResourceExhausted, "batch too large"));

    let records = vec![VectorRecord::new(1, vec![1.0, 2.0])];
    let res = client
        .import_records(
            "re_coll",
            &records,
            ImportOptions::default()
                .max_retries(3)
                .retry_base_delay(Duration::from_millis(1)),
        )
        .await
        .unwrap();
    assert_eq!(res.failed_count, 1);
    assert_eq!(state.lock().unwrap().upsert_calls, 1);

    state
        .lock()
        .unwrap()
        .stream_errors
        .push_back(Status::unavailable("network drop"));
    let res = client
        .import_records(
            "stream_coll",
            &records,
            ImportOptions::default()
                .mode(ImportMode::StreamInsert)
                .max_retries(3)
                .retry_base_delay(Duration::from_millis(1)),
        )
        .await
        .unwrap();
    assert_eq!(res.failed_count, 1);
    assert_eq!(state.lock().unwrap().stream_calls, 1);
}

#[cfg(feature = "csv")]
#[tokio::test]
async fn import_csv_supports_json_and_prefixed_columns() {
    use gvdb::CsvImportOptions;
    use std::io::Write;

    let (client, state) = spawn_import_server().await;

    let mut json_csv = tempfile::NamedTempFile::new().unwrap();
    writeln!(json_csv, "id,vector,category,count,score,active").unwrap();
    writeln!(json_csv, "10,\"[0.1, 0.2, 0.3]\",books,42,9.5,true").unwrap();
    writeln!(json_csv, "11,\"[0.4, 0.5, 0.6]\",music,,3.25,false").unwrap();

    let res = client
        .import_csv(
            json_csv.path(),
            "csv_json",
            CsvImportOptions::default().import(ImportOptions::default().batch_size(1)),
        )
        .await
        .unwrap();
    assert_eq!(res.total_count, 2);
    assert_eq!(res.batch_count, 2);
    assert_eq!(res.dimension, 3);
    assert!(res.created_collection);

    {
        let s = state.lock().unwrap();
        let first = &s.upserted[0];
        assert_eq!(first.id, 10);
        let fields = &first.metadata.as_ref().unwrap().fields;
        assert_eq!(
            fields.get("count").and_then(|v| v.value.as_ref()),
            Some(&pb::metadata_value::Value::IntValue(42))
        );
        assert_eq!(
            fields.get("active").and_then(|v| v.value.as_ref()),
            Some(&pb::metadata_value::Value::BoolValue(true))
        );
        let second_fields = &s.upserted[1].metadata.as_ref().unwrap().fields;
        assert!(!second_fields.contains_key("count"));
    }

    // Dimension-prefixed columns out of order must sort numerically.
    let mut pref_csv = tempfile::NamedTempFile::new().unwrap();
    writeln!(pref_csv, "id,emb_2,emb_0,emb_1,tag").unwrap();
    writeln!(pref_csv, "1,3.0,1.0,2.0,first").unwrap();

    let res = client
        .import_csv(
            pref_csv.path(),
            "csv_pref",
            CsvImportOptions::default().vector_column("emb"),
        )
        .await
        .unwrap();
    assert_eq!(res.total_count, 1);
    assert_eq!(res.dimension, 3);
    let last = state.lock().unwrap().upserted.last().unwrap().clone();
    assert_eq!(last.vector.unwrap().values, vec![1.0, 2.0, 3.0]);
}

#[cfg(feature = "parquet")]
#[tokio::test]
async fn import_parquet_reads_vectors_and_typed_metadata() {
    use std::sync::Arc;

    use arrow_array::builder::{Float32Builder, ListBuilder};
    use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use gvdb::ParquetImportOptions;
    use parquet::arrow::ArrowWriter;

    let (client, state) = spawn_import_server().await;

    let mut list_builder = ListBuilder::new(Float32Builder::new());
    list_builder.values().append_slice(&[1.0, 2.0, 3.0]);
    list_builder.append(true);
    list_builder.values().append_slice(&[4.0, 5.0, 6.0]);
    list_builder.append(true);
    let vec_array = Arc::new(list_builder.finish());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("vector", vec_array.data_type().clone(), false),
        Field::new("category", DataType::Utf8, true),
        Field::new("rank", DataType::Int64, true),
        Field::new("weight", DataType::Float64, true),
        Field::new("enabled", DataType::Boolean, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![100, 200])),
            vec_array,
            Arc::new(StringArray::from(vec![Some("cat_a"), None])),
            Arc::new(Int64Array::from(vec![Some(7), Some(8)])),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5)])),
            Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
        ],
    )
    .unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    {
        let file = std::fs::File::create(tmp.path()).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let res = client
        .import_parquet(
            tmp.path(),
            "pq_items",
            ParquetImportOptions::default().import(ImportOptions::default().batch_size(1)),
        )
        .await
        .unwrap();
    assert_eq!(res.total_count, 2);
    assert_eq!(res.batch_count, 2);
    assert_eq!(res.dimension, 3);
    assert!(res.created_collection);

    let s = state.lock().unwrap();
    assert_eq!(s.upserted.len(), 2);
    assert_eq!(s.upserted[0].id, 100);
    assert_eq!(
        s.upserted[0].vector.as_ref().unwrap().values,
        vec![1.0, 2.0, 3.0]
    );
    let meta0 = &s.upserted[0].metadata.as_ref().unwrap().fields;
    assert_eq!(
        meta0.get("category").and_then(|v| v.value.as_ref()),
        Some(&pb::metadata_value::Value::StringValue("cat_a".to_owned()))
    );
    assert_eq!(
        meta0.get("rank").and_then(|v| v.value.as_ref()),
        Some(&pb::metadata_value::Value::IntValue(7))
    );
    assert_eq!(
        meta0.get("enabled").and_then(|v| v.value.as_ref()),
        Some(&pb::metadata_value::Value::BoolValue(true))
    );
    // Null string cell on row 2 must be omitted.
    let meta1 = &s.upserted[1].metadata.as_ref().unwrap().fields;
    assert!(!meta1.contains_key("category"));
    assert_eq!(MetadataValue::from(8i64), MetadataValue::Int(8));
}
