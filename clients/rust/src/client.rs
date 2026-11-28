//! Async gRPC client for GVDB.

use std::fmt;
use std::time::{Duration, Instant};

use tonic::Request;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::error::{Error, Result};
use crate::importers::{self, ImportOptions, ImportResult};
use crate::pb;
use crate::pb::vector_db_service_client::VectorDbServiceClient;
use crate::types::{
    BackupInfo, BackupState, BackupStatus, BackupTarget, BulkImportOptions, CollectionInfo,
    CreateCollectionOptions, HybridSearchOptions, ImportState, ImportStatus, ListVectorsResult,
    Metadata, PollOptions, RangeSearchOptions, RestoreStatus, SearchOptions, SearchResult,
    ServerStats, UpsertResult, VectorRecord, from_proto_metadata, to_proto_metadata,
};

#[cfg(feature = "csv")]
use crate::importers::CsvImportOptions;
#[cfg(feature = "parquet")]
use crate::importers::ParquetImportOptions;
#[cfg(any(feature = "csv", feature = "parquet"))]
use std::path::Path;

const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
// Tonic formats gRPC timeouts up to 99,999,999 hours and panics above that limit.
const MAX_GRPC_TIMEOUT: Duration = Duration::from_secs(99_999_999 * 3600);

/// Connection configuration for [`GvdbClient`].
#[derive(Clone)]
pub struct ClientConfig {
    address: String,
    api_key: Option<String>,
    timeout: Duration,
    tls: Option<bool>,
    tls_config: Option<ClientTlsConfig>,
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("address", &self.address)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("timeout", &self.timeout)
            .field("tls", &self.tls)
            .field(
                "tls_config",
                &self.tls_config.as_ref().map(|_| "<configured>"),
            )
            .finish()
    }
}

impl ClientConfig {
    /// Create a configuration for the target server address.
    ///
    /// Pass either a bare `host:port` string or a full `http://` or `https://` URI.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            api_key: None,
            timeout: DEFAULT_TIMEOUT,
            tls: None,
            tls_config: None,
        }
    }

    /// Set the API key sent in the `authorization: Bearer <key>` header.
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Set the connect and per-request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Enable or disable TLS.
    ///
    /// Enabling TLS uses the root certificates of the operating system unless
    /// `tls_config` is also set. Disabling TLS clears any previously set `tls_config`.
    pub fn tls(mut self, enabled: bool) -> Self {
        self.tls = Some(enabled);
        if !enabled {
            self.tls_config = None;
        }
        self
    }

    /// Set a custom TLS configuration and enable TLS.
    pub fn tls_config(mut self, tls_config: ClientTlsConfig) -> Self {
        self.tls = Some(true);
        self.tls_config = Some(tls_config);
        self
    }

    /// Connect to the GVDB server using this configuration.
    pub async fn connect(self) -> Result<GvdbClient> {
        GvdbClient::connect_with_config(self).await
    }
}

/// Async client for the GVDB distributed vector database.
///
/// Cloning `GvdbClient` is cheap because the underlying gRPC channel is shared.
#[derive(Clone)]
pub struct GvdbClient {
    stub: VectorDbServiceClient<Channel>,
    auth_header: Option<MetadataValue<Ascii>>,
    timeout: Duration,
}

impl fmt::Debug for GvdbClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GvdbClient")
            .field(
                "auth_header",
                &self.auth_header.as_ref().map(|_| "<redacted>"),
            )
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl GvdbClient {
    /// Connect to a GVDB server with default settings.
    ///
    /// Accepts `host:port`, `http://host:port`, or `https://host:port`.
    pub async fn connect(address: impl Into<String>) -> Result<Self> {
        Self::connect_with_config(ClientConfig::new(address)).await
    }

    /// Connect to a GVDB server with explicit [`ClientConfig`] settings.
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        validate_rpc_timeout(config.timeout)?;
        let effective_tls_override = if config.tls_config.is_some() {
            Some(true)
        } else {
            config.tls
        };
        let uri = normalize_endpoint_uri(&config.address, effective_tls_override)?;
        let effective_tls = effective_tls_override == Some(true) || uri.starts_with("https://");

        let mut endpoint = Endpoint::from_shared(uri)
            .map_err(|err| Error::InvalidArgument(format!("Invalid server address: {err}")))?;

        if !config.timeout.is_zero() {
            endpoint = endpoint.connect_timeout(config.timeout);
        }

        if effective_tls {
            let mut tls_cfg = config
                .tls_config
                .unwrap_or_else(|| ClientTlsConfig::new().with_native_roots());
            if !config.timeout.is_zero() {
                tls_cfg = tls_cfg.timeout(config.timeout);
            }
            endpoint = endpoint.tls_config(tls_cfg)?;
        }

        let channel = if config.timeout.is_zero() {
            endpoint.connect().await?
        } else {
            tokio::time::timeout(config.timeout, endpoint.connect())
                .await
                .map_err(|_| {
                    Error::Timeout(format!(
                        "Connecting to server did not complete within {}s.",
                        config.timeout.as_secs_f64()
                    ))
                })??
        };
        Self::from_channel(channel, config.api_key.as_deref(), config.timeout)
    }

    /// Build a client from an existing [`Channel`].
    pub fn from_channel(
        channel: Channel,
        api_key: Option<&str>,
        timeout: Duration,
    ) -> Result<Self> {
        validate_rpc_timeout(timeout)?;
        let auth_header = match api_key {
            Some(key) => {
                if key.is_empty() || key.bytes().any(|b| b.is_ascii_whitespace()) {
                    return Err(Error::InvalidArgument(
                        "API key cannot be empty or contain whitespace.".to_owned(),
                    ));
                }
                let header = format!("Bearer {key}");
                let mut value = MetadataValue::try_from(header).map_err(|_| {
                    Error::InvalidArgument(
                        "API key contains characters that are not valid in an HTTP header."
                            .to_owned(),
                    )
                })?;
                value.set_sensitive(true);
                Some(value)
            }
            None => None,
        };

        let stub = VectorDbServiceClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);

        Ok(Self {
            stub,
            auth_header,
            timeout,
        })
    }

    fn prepare_request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(auth) = &self.auth_header {
            request.metadata_mut().insert("authorization", auth.clone());
        }
        if !self.timeout.is_zero() {
            request.set_timeout(self.timeout);
        }
        request
    }

    // -----------------------------------------------------------------------
    // Health and stats
    // -----------------------------------------------------------------------

    /// Check server health and return the status message.
    pub async fn health_check(&self) -> Result<String> {
        let mut stub = self.stub.clone();
        let response = stub
            .health_check(self.prepare_request(pb::HealthCheckRequest {}))
            .await?
            .into_inner();
        Ok(response.message)
    }

    /// Fetch global statistics from the server.
    pub async fn get_stats(&self) -> Result<ServerStats> {
        let mut stub = self.stub.clone();
        let response = stub
            .get_stats(self.prepare_request(pb::GetStatsRequest {}))
            .await?
            .into_inner();
        Ok(ServerStats {
            total_vectors: response.total_vectors,
            total_collections: response.total_collections,
            total_queries: response.total_queries,
            avg_query_time_ms: response.avg_query_time_ms,
        })
    }

    // -----------------------------------------------------------------------
    // Collection management
    // -----------------------------------------------------------------------

    /// Create a collection and return its numeric ID.
    pub async fn create_collection(
        &self,
        name: impl Into<String>,
        dimension: u32,
        options: CreateCollectionOptions,
    ) -> Result<u32> {
        let mut stub = self.stub.clone();
        let request = pb::CreateCollectionRequest {
            collection_name: name.into(),
            dimension,
            metric: options.metric.to_proto(),
            index_type: options.index_type.to_proto(),
            num_shards: options.num_shards,
        };
        let response = stub
            .create_collection(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(response.collection_id)
    }

    /// Drop a collection by name.
    pub async fn drop_collection(&self, name: impl Into<String>) -> Result<()> {
        let mut stub = self.stub.clone();
        let request = pb::DropCollectionRequest {
            collection_name: name.into(),
        };
        stub.drop_collection(self.prepare_request(request)).await?;
        Ok(())
    }

    /// List all collections on the server.
    pub async fn list_collections(&self) -> Result<Vec<CollectionInfo>> {
        let mut stub = self.stub.clone();
        let response = stub
            .list_collections(self.prepare_request(pb::ListCollectionsRequest {}))
            .await?
            .into_inner();
        Ok(response
            .collections
            .into_iter()
            .map(|c| CollectionInfo {
                id: c.collection_id,
                name: c.collection_name,
                dimension: c.dimension,
                metric_type: c.metric_type,
                vector_count: c.vector_count,
            })
            .collect())
    }

    // -----------------------------------------------------------------------
    // Vector operations
    // -----------------------------------------------------------------------

    /// Insert vectors into a collection and return the inserted count.
    pub async fn insert(
        &self,
        collection: impl Into<String>,
        vectors: &[VectorRecord],
    ) -> Result<u64> {
        let proto_vectors = vectors
            .iter()
            .map(VectorRecord::to_proto)
            .collect::<Result<Vec<_>>>()?;
        let mut stub = self.stub.clone();
        let request = pb::InsertRequest {
            collection_name: collection.into(),
            vectors: proto_vectors,
        };
        let response = stub
            .insert(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(response.inserted_count)
    }

    /// Stream vectors to the server in chunks of `batch_size` and return the inserted count.
    pub async fn stream_insert(
        &self,
        collection: impl Into<String>,
        vectors: &[VectorRecord],
        batch_size: usize,
    ) -> Result<u64> {
        if batch_size == 0 {
            return Err(Error::InvalidArgument(
                "batch_size must be greater than zero.".to_owned(),
            ));
        }
        if vectors.is_empty() {
            return Err(Error::InvalidArgument(
                "vectors cannot be empty.".to_owned(),
            ));
        }
        let collection_name = collection.into();
        let mut chunks = Vec::with_capacity(vectors.len().div_ceil(batch_size));
        for slice in vectors.chunks(batch_size) {
            let proto_vectors = slice
                .iter()
                .map(VectorRecord::to_proto)
                .collect::<Result<Vec<_>>>()?;
            chunks.push(pb::InsertRequest {
                collection_name: collection_name.clone(),
                vectors: proto_vectors,
            });
        }
        let mut stub = self.stub.clone();
        let stream = tokio_stream::iter(chunks);
        let response = stub
            .stream_insert(self.prepare_request(stream))
            .await?
            .into_inner();
        Ok(response.inserted_count)
    }

    /// Insert new vectors or replace existing vectors with the same IDs.
    pub async fn upsert(
        &self,
        collection: impl Into<String>,
        vectors: &[VectorRecord],
    ) -> Result<UpsertResult> {
        let proto_vectors = vectors
            .iter()
            .map(VectorRecord::to_proto)
            .collect::<Result<Vec<_>>>()?;
        let mut stub = self.stub.clone();
        let request = pb::UpsertRequest {
            collection_name: collection.into(),
            vectors: proto_vectors,
        };
        let response = stub
            .upsert(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(UpsertResult {
            upserted_count: response.upserted_count,
            inserted_count: response.inserted_count,
            updated_count: response.updated_count,
        })
    }

    /// Fetch vectors by ID.
    pub async fn get(
        &self,
        collection: impl Into<String>,
        ids: &[u64],
        return_metadata: bool,
    ) -> Result<Vec<VectorRecord>> {
        let mut stub = self.stub.clone();
        let request = pb::GetRequest {
            collection_name: collection.into(),
            ids: ids.to_vec(),
            return_metadata,
        };
        let response = stub.get(self.prepare_request(request)).await?.into_inner();
        Ok(response
            .vectors
            .into_iter()
            .map(VectorRecord::from_proto)
            .collect())
    }

    /// Delete vectors by ID and return the deleted count.
    pub async fn delete(&self, collection: impl Into<String>, ids: &[u64]) -> Result<u64> {
        let mut stub = self.stub.clone();
        let request = pb::DeleteRequest {
            collection_name: collection.into(),
            ids: ids.to_vec(),
        };
        let response = stub
            .delete(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(response.deleted_count)
    }

    /// Update metadata for one vector.
    ///
    /// Set `merge` to `true` to keep existing keys that are not in `metadata`.
    /// Set `merge` to `false` to replace all metadata for that vector.
    pub async fn update_metadata(
        &self,
        collection: impl Into<String>,
        id: u64,
        metadata: Metadata,
        merge: bool,
    ) -> Result<bool> {
        let mut stub = self.stub.clone();
        let request = pb::UpdateMetadataRequest {
            collection_name: collection.into(),
            id,
            metadata: Some(to_proto_metadata(&metadata)),
            merge,
        };
        let response = stub
            .update_metadata(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(response.updated)
    }

    /// Browse vectors in a collection with offset pagination.
    pub async fn list_vectors(
        &self,
        collection: impl Into<String>,
        limit: u32,
        offset: u64,
        include_metadata: bool,
    ) -> Result<ListVectorsResult> {
        let mut stub = self.stub.clone();
        let request = pb::ListVectorsRequest {
            collection_name: collection.into(),
            limit,
            offset,
            include_metadata,
        };
        let response = stub
            .list_vectors(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(ListVectorsResult {
            vectors: response
                .vectors
                .into_iter()
                .map(VectorRecord::from_proto)
                .collect(),
            total_count: response.total_count,
            has_more: response.has_more,
        })
    }

    // -----------------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------------

    /// Search for the nearest vectors to `query_vector`.
    pub async fn search(
        &self,
        collection: impl Into<String>,
        query_vector: &[f32],
        options: SearchOptions,
    ) -> Result<Vec<SearchResult>> {
        let dimension = u32::try_from(query_vector.len()).map_err(|_| {
            Error::InvalidArgument("Query vector dimension exceeds u32::MAX.".to_owned())
        })?;
        let mut stub = self.stub.clone();
        let request = pb::SearchRequest {
            collection_name: collection.into(),
            query_vector: Some(pb::Vector {
                values: query_vector.to_vec(),
                dimension,
            }),
            top_k: options.top_k,
            filter: options.filter.unwrap_or_default(),
            return_metadata: options.return_metadata,
        };
        let response = stub
            .search(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(map_search_results(response.results))
    }

    /// Find all vectors within a distance radius or inner-product similarity threshold.
    pub async fn range_search(
        &self,
        collection: impl Into<String>,
        query_vector: &[f32],
        options: RangeSearchOptions,
    ) -> Result<Vec<SearchResult>> {
        let dimension = u32::try_from(query_vector.len()).map_err(|_| {
            Error::InvalidArgument("Query vector dimension exceeds u32::MAX.".to_owned())
        })?;
        let mut stub = self.stub.clone();
        let request = pb::RangeSearchRequest {
            collection_name: collection.into(),
            query_vector: Some(pb::Vector {
                values: query_vector.to_vec(),
                dimension,
            }),
            radius: options.radius.unwrap_or(0.0),
            filter: options.filter.unwrap_or_default(),
            return_metadata: options.return_metadata,
            max_results: options.max_results,
            similarity_threshold: options.similarity_threshold,
        };
        let response = stub
            .range_search(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(map_search_results(response.results))
    }

    /// Run hybrid search combining dense vectors, BM25 text, and sparse vectors.
    pub async fn hybrid_search(
        &self,
        collection: impl Into<String>,
        options: HybridSearchOptions,
    ) -> Result<Vec<SearchResult>> {
        let query_vector = match options.query_vector {
            Some(values) if !values.is_empty() => {
                let dimension = u32::try_from(values.len()).map_err(|_| {
                    Error::InvalidArgument("Query vector dimension exceeds u32::MAX.".to_owned())
                })?;
                Some(pb::Vector { values, dimension })
            }
            _ => None,
        };
        let sparse_query = match options.sparse_query {
            Some(sv) if !sv.indices.is_empty() || !sv.values.is_empty() => Some(sv.to_proto()?),
            _ => None,
        };

        let mut stub = self.stub.clone();
        let request = pb::HybridSearchRequest {
            collection_name: collection.into(),
            query_vector,
            text_query: options.text_query.unwrap_or_default(),
            top_k: options.top_k,
            filter: options.filter.unwrap_or_default(),
            return_metadata: options.return_metadata,
            vector_weight: options.vector_weight,
            text_weight: options.text_weight,
            text_field: options.text_field,
            sparse_query,
            sparse_weight: options.sparse_weight,
        };
        let response = stub
            .hybrid_search(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(map_search_results(response.results))
    }

    // -----------------------------------------------------------------------
    // Client-side bulk import
    // -----------------------------------------------------------------------

    /// Import in-memory records in batches with automatic collection setup and retries.
    pub async fn import_records(
        &self,
        collection: impl Into<String>,
        records: &[VectorRecord],
        options: ImportOptions,
    ) -> Result<ImportResult> {
        importers::import_records(self, collection, records, options).await
    }

    /// Import vectors from a local CSV file.
    #[cfg(feature = "csv")]
    pub async fn import_csv(
        &self,
        path: impl AsRef<Path>,
        collection: impl Into<String>,
        options: CsvImportOptions,
    ) -> Result<ImportResult> {
        importers::import_csv(self, path, collection, options).await
    }

    /// Import vectors from a local Parquet file.
    #[cfg(feature = "parquet")]
    pub async fn import_parquet(
        &self,
        path: impl AsRef<Path>,
        collection: impl Into<String>,
        options: ParquetImportOptions,
    ) -> Result<ImportResult> {
        importers::import_parquet(self, path, collection, options).await
    }

    // -----------------------------------------------------------------------
    // Server-side bulk import
    // -----------------------------------------------------------------------

    /// Start a server-side bulk import from S3 or MinIO and return the job ID.
    pub async fn bulk_import(
        &self,
        collection: impl Into<String>,
        source_uri: impl Into<String>,
        options: BulkImportOptions,
    ) -> Result<String> {
        let mut stub = self.stub.clone();
        let request = pb::BulkImportRequest {
            collection_name: collection.into(),
            source_uri: source_uri.into(),
            format: options.format.to_proto(),
            vector_column: options.vector_column,
            id_column: options.id_column,
        };
        let response = stub
            .bulk_import(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(response.import_id)
    }

    /// Get the current status of a server-side bulk import job.
    pub async fn get_import_status(&self, import_id: impl Into<String>) -> Result<ImportStatus> {
        let mut stub = self.stub.clone();
        let request = pb::GetImportStatusRequest {
            import_id: import_id.into(),
        };
        let response = stub
            .get_import_status(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(ImportStatus {
            import_id: response.import_id,
            state: ImportState::from_proto(response.state),
            total_vectors: response.total_vectors,
            imported_vectors: response.imported_vectors,
            progress_percent: response.progress_percent,
            error_message: response.error_message,
            elapsed_seconds: response.elapsed_seconds,
            segments_created: response.segments_created,
        })
    }

    /// Cancel a pending or running server-side bulk import job.
    pub async fn cancel_import(&self, import_id: impl Into<String>) -> Result<bool> {
        let mut stub = self.stub.clone();
        let request = pb::CancelImportRequest {
            import_id: import_id.into(),
        };
        let response = stub
            .cancel_import(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(response.success)
    }

    /// Poll a server-side bulk import job until it reaches a terminal state.
    pub async fn wait_for_import(
        &self,
        import_id: impl Into<String>,
        poll: PollOptions,
    ) -> Result<ImportStatus> {
        let import_id = import_id.into();
        let deadline = validate_poll_options(poll)?;
        loop {
            let status = run_with_deadline(
                deadline,
                poll.timeout,
                "Import",
                &import_id,
                self.get_import_status(&import_id),
            )
            .await?;
            if status.state.is_terminal() {
                return Ok(status);
            }
            sleep_before_deadline(deadline, poll, "Import", &import_id).await?;
        }
    }

    // -----------------------------------------------------------------------
    // Backup and restore
    // -----------------------------------------------------------------------

    /// Start a server-side backup for a collection and return the backup ID.
    pub async fn backup_collection(
        &self,
        collection: impl Into<String>,
        target: BackupTarget,
        backup_id: Option<String>,
    ) -> Result<String> {
        let mut stub = self.stub.clone();
        let request = pb::BackupCollectionRequest {
            collection_name: collection.into(),
            target: Some(target.to_proto()?),
            incremental_from: String::new(),
            backup_id: backup_id.unwrap_or_default(),
        };
        let response = stub
            .backup_collection(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(response.backup_id)
    }

    /// Start a server-side restore from a backup and return the restore ID.
    pub async fn restore_collection(
        &self,
        backup_id: impl Into<String>,
        source: BackupTarget,
        target_collection: Option<String>,
        overwrite: bool,
    ) -> Result<String> {
        let mut stub = self.stub.clone();
        let request = pb::RestoreCollectionRequest {
            source: Some(source.to_proto()?),
            backup_id: backup_id.into(),
            target_collection_name: target_collection.unwrap_or_default(),
            overwrite,
        };
        let response = stub
            .restore_collection(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(response.restore_id)
    }

    /// Get the current status of a server-side backup job.
    pub async fn get_backup_status(&self, backup_id: impl Into<String>) -> Result<BackupStatus> {
        let mut stub = self.stub.clone();
        let request = pb::GetBackupStatusRequest {
            backup_id: backup_id.into(),
        };
        let response = stub
            .get_backup_status(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(BackupStatus {
            backup_id: response.backup_id,
            state: BackupState::from_proto(response.state),
            shards_total: response.shards_total,
            shards_completed: response.shards_completed,
            bytes_uploaded: response.bytes_uploaded,
            error_message: response.error_message,
            elapsed_seconds: response.elapsed_seconds,
            manifest_uri: response.manifest_uri,
        })
    }

    /// Get the current status of a server-side restore job.
    pub async fn get_restore_status(&self, restore_id: impl Into<String>) -> Result<RestoreStatus> {
        let mut stub = self.stub.clone();
        let request = pb::GetRestoreStatusRequest {
            restore_id: restore_id.into(),
        };
        let response = stub
            .get_restore_status(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(RestoreStatus {
            restore_id: response.restore_id,
            state: BackupState::from_proto(response.state),
            shards_total: response.shards_total,
            shards_completed: response.shards_completed,
            error_message: response.error_message,
            elapsed_seconds: response.elapsed_seconds,
        })
    }

    /// Poll a backup job until it completes.
    ///
    /// Returns `Error::JobFailed` when the job ends in `Failed` or `Cancelled`.
    pub async fn wait_for_backup(
        &self,
        backup_id: impl Into<String>,
        poll: PollOptions,
    ) -> Result<BackupStatus> {
        let backup_id = backup_id.into();
        let deadline = validate_poll_options(poll)?;
        loop {
            let status = run_with_deadline(
                deadline,
                poll.timeout,
                "Backup",
                &backup_id,
                self.get_backup_status(&backup_id),
            )
            .await?;
            match status.state {
                BackupState::Completed => return Ok(status),
                BackupState::Failed | BackupState::Cancelled => {
                    return Err(Error::JobFailed {
                        kind: "Backup",
                        id: backup_id,
                        state: status.state.as_str().to_owned(),
                        message: status.error_message,
                    });
                }
                BackupState::Pending | BackupState::Running => {}
            }
            sleep_before_deadline(deadline, poll, "Backup", &backup_id).await?;
        }
    }

    /// Poll a restore job until it completes.
    ///
    /// Returns `Error::JobFailed` when the job ends in `Failed` or `Cancelled`.
    pub async fn wait_for_restore(
        &self,
        restore_id: impl Into<String>,
        poll: PollOptions,
    ) -> Result<RestoreStatus> {
        let restore_id = restore_id.into();
        let deadline = validate_poll_options(poll)?;
        loop {
            let status = run_with_deadline(
                deadline,
                poll.timeout,
                "Restore",
                &restore_id,
                self.get_restore_status(&restore_id),
            )
            .await?;
            match status.state {
                BackupState::Completed => return Ok(status),
                BackupState::Failed | BackupState::Cancelled => {
                    return Err(Error::JobFailed {
                        kind: "Restore",
                        id: restore_id,
                        state: status.state.as_str().to_owned(),
                        message: status.error_message,
                    });
                }
                BackupState::Pending | BackupState::Running => {}
            }
            sleep_before_deadline(deadline, poll, "Restore", &restore_id).await?;
        }
    }

    /// List backups stored in `source`.
    pub async fn list_backups(&self, source: BackupTarget) -> Result<Vec<BackupInfo>> {
        let mut stub = self.stub.clone();
        let request = pb::ListBackupsRequest {
            source: Some(source.to_proto()?),
        };
        let response = stub
            .list_backups(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(response
            .backups
            .into_iter()
            .map(|b| BackupInfo {
                backup_id: b.backup_id,
                collection_name: b.collection_name,
                collection_id: b.collection_id,
                created_at_unix_ms: b.created_at_unix_ms,
                vector_count: b.vector_count,
                size_bytes: b.size_bytes,
                manifest_version: b.manifest_version,
            })
            .collect())
    }

    /// Cancel a pending or running backup job.
    pub async fn cancel_backup(&self, backup_id: impl Into<String>) -> Result<bool> {
        let mut stub = self.stub.clone();
        let request = pb::CancelBackupRequest {
            backup_id: backup_id.into(),
        };
        let response = stub
            .cancel_backup(self.prepare_request(request))
            .await?
            .into_inner();
        Ok(response.success)
    }
}

fn map_search_results(results: Vec<pb::SearchResultEntry>) -> Vec<SearchResult> {
    results
        .into_iter()
        .map(|entry| SearchResult {
            id: entry.id,
            distance: entry.distance,
            metadata: entry.metadata.and_then(from_proto_metadata),
        })
        .collect()
}

fn validate_rpc_timeout(timeout: Duration) -> Result<()> {
    if timeout > MAX_GRPC_TIMEOUT {
        return Err(Error::InvalidArgument(format!(
            "Timeout {}s exceeds the maximum gRPC timeout of {}s.",
            timeout.as_secs(),
            MAX_GRPC_TIMEOUT.as_secs()
        )));
    }
    Ok(())
}

fn validate_poll_options(poll: PollOptions) -> Result<Option<Instant>> {
    if poll.poll_interval.is_zero() {
        return Err(Error::InvalidArgument(
            "poll_interval must be greater than zero.".to_owned(),
        ));
    }
    Ok(Instant::now().checked_add(poll.timeout))
}

async fn run_with_deadline<F, T>(
    deadline: Option<Instant>,
    timeout: Duration,
    kind: &str,
    id: &str,
    fut: F,
) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let Some(deadline) = deadline else {
        return fut.await;
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Error::Timeout(format!(
            "{kind} {id} did not complete within {}s.",
            timeout.as_secs_f64()
        )));
    }
    tokio::time::timeout(remaining, fut).await.map_err(|_| {
        Error::Timeout(format!(
            "{kind} {id} did not complete within {}s.",
            timeout.as_secs_f64()
        ))
    })?
}

async fn sleep_before_deadline(
    deadline: Option<Instant>,
    poll: PollOptions,
    kind: &str,
    id: &str,
) -> Result<()> {
    let sleep_for = match deadline {
        Some(deadline) => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout(format!(
                    "{kind} {id} did not complete within {}s.",
                    poll.timeout.as_secs_f64()
                )));
            }
            poll.poll_interval.min(remaining)
        }
        None => poll.poll_interval,
    };
    tokio::time::sleep(sleep_for).await;
    Ok(())
}

fn normalize_endpoint_uri(address: &str, tls_override: Option<bool>) -> Result<String> {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidArgument(
            "Server address cannot be empty.".to_owned(),
        ));
    }
    if let Some((scheme, rest)) = trimmed.split_once("://") {
        if scheme.eq_ignore_ascii_case("http") {
            if tls_override == Some(true) {
                return Err(Error::InvalidArgument(
                    "You cannot use an http:// address with TLS enabled. Use https:// or omit the scheme."
                        .to_owned(),
                ));
            }
            return Ok(format!("http://{rest}"));
        }
        if scheme.eq_ignore_ascii_case("https") {
            if tls_override == Some(false) {
                return Err(Error::InvalidArgument(
                    "You cannot use an https:// address with TLS disabled. Use http:// or omit the scheme."
                        .to_owned(),
                ));
            }
            return Ok(format!("https://{rest}"));
        }
        return Err(Error::InvalidArgument(format!(
            "Unsupported URI scheme in '{trimmed}'. Use http://, https://, or host:port."
        )));
    }
    let scheme = if tls_override == Some(true) {
        "https"
    } else {
        "http"
    };
    Ok(format!("{scheme}://{trimmed}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_uri_adds_default_scheme() -> Result<()> {
        assert_eq!(
            normalize_endpoint_uri("localhost:50051", None)?,
            "http://localhost:50051"
        );
        assert_eq!(
            normalize_endpoint_uri("localhost:50051", Some(true))?,
            "https://localhost:50051"
        );
        assert_eq!(
            normalize_endpoint_uri("https://db.example.com:50051", None)?,
            "https://db.example.com:50051"
        );
        assert_eq!(
            normalize_endpoint_uri("HTTPS://db.example.com:50051", None)?,
            "https://db.example.com:50051"
        );
        assert_eq!(
            normalize_endpoint_uri("HTTP://db.example.com:50051", Some(false))?,
            "http://db.example.com:50051"
        );
        Ok(())
    }

    #[test]
    fn normalize_uri_rejects_conflicting_scheme() {
        assert!(normalize_endpoint_uri("http://localhost:50051", Some(true)).is_err());
        assert!(normalize_endpoint_uri("https://localhost:50051", Some(false)).is_err());
        assert!(normalize_endpoint_uri("ftp://localhost:50051", None).is_err());
        assert!(normalize_endpoint_uri("   ", None).is_err());
    }

    #[test]
    fn client_config_debug_redacts_secrets() {
        let cfg = ClientConfig::new("localhost:50051")
            .api_key("super-secret-token")
            .tls_config(ClientTlsConfig::new());
        let formatted = format!("{cfg:?}");
        assert!(!formatted.contains("super-secret-token"));
        assert!(formatted.contains("<redacted>"));
        assert!(formatted.contains("<configured>"));
    }

    #[test]
    fn tls_false_clears_tls_config() {
        let cfg = ClientConfig::new("localhost:50051")
            .tls_config(ClientTlsConfig::new())
            .tls(false);
        assert_eq!(cfg.tls, Some(false));
        assert!(cfg.tls_config.is_none());
    }
}
