//! Public data types for GVDB requests and responses.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::pb;

/// Key-value metadata map attached to a vector.
pub type Metadata = HashMap<String, MetadataValue>;

/// Scalar value stored in vector metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

impl From<i64> for MetadataValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<i32> for MetadataValue {
    fn from(value: i32) -> Self {
        Self::Int(i64::from(value))
    }
}

impl From<f64> for MetadataValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<f32> for MetadataValue {
    fn from(value: f32) -> Self {
        Self::Float(f64::from(value))
    }
}

impl From<String> for MetadataValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for MetadataValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<bool> for MetadataValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

/// Distance metric for a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Metric {
    #[default]
    L2,
    InnerProduct,
    Cosine,
}

impl Metric {
    pub(crate) fn to_proto(self) -> i32 {
        match self {
            Self::L2 => pb::create_collection_request::MetricType::L2 as i32,
            Self::InnerProduct => pb::create_collection_request::MetricType::InnerProduct as i32,
            Self::Cosine => pb::create_collection_request::MetricType::Cosine as i32,
        }
    }
}

/// Index algorithm for a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexType {
    Flat,
    #[default]
    Hnsw,
    IvfFlat,
    IvfPq,
    IvfSq,
    IvfTurboQuant,
    TurboQuant,
    Auto,
}

impl IndexType {
    pub(crate) fn to_proto(self) -> i32 {
        match self {
            Self::Flat => pb::create_collection_request::IndexType::Flat as i32,
            Self::Hnsw => pb::create_collection_request::IndexType::Hnsw as i32,
            Self::IvfFlat => pb::create_collection_request::IndexType::IvfFlat as i32,
            Self::IvfPq => pb::create_collection_request::IndexType::IvfPq as i32,
            Self::IvfSq => pb::create_collection_request::IndexType::IvfSq as i32,
            Self::IvfTurboQuant => pb::create_collection_request::IndexType::IvfTurboquant as i32,
            Self::TurboQuant => pb::create_collection_request::IndexType::Turboquant as i32,
            Self::Auto => pb::create_collection_request::IndexType::Auto as i32,
        }
    }
}

/// Sparse vector for learned sparse retrieval such as SPLADE.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SparseVector {
    /// Non-zero dimension indices sorted in ascending order.
    pub indices: Vec<u32>,
    /// Non-zero values matching `indices`.
    pub values: Vec<f32>,
}

impl SparseVector {
    /// Build a sparse vector from `(index, value)` pairs sorted by index.
    pub fn from_pairs(pairs: impl IntoIterator<Item = (u32, f32)>) -> Self {
        let mut sorted: Vec<(u32, f32)> = pairs.into_iter().collect();
        sorted.sort_by_key(|(idx, _)| *idx);
        let mut indices = Vec::with_capacity(sorted.len());
        let mut values = Vec::with_capacity(sorted.len());
        for (idx, val) in sorted {
            indices.push(idx);
            values.push(val);
        }
        Self { indices, values }
    }

    pub(crate) fn to_proto(&self) -> Result<pb::SparseVector> {
        if self.indices.len() != self.values.len() {
            return Err(Error::InvalidArgument(format!(
                "Sparse vector has {} indices and {} values. Both lengths must match.",
                self.indices.len(),
                self.values.len()
            )));
        }
        if self.values.iter().any(|v| !v.is_finite()) {
            return Err(Error::InvalidArgument(
                "Sparse vector values must be finite numbers.".to_owned(),
            ));
        }

        let (indices, values) = if self
            .indices
            .windows(2)
            .all(|w| matches!((w.first(), w.get(1)), (Some(a), Some(b)) if a < b))
        {
            (self.indices.clone(), self.values.clone())
        } else {
            let sorted = Self::from_pairs(
                self.indices
                    .iter()
                    .copied()
                    .zip(self.values.iter().copied()),
            );
            if sorted
                .indices
                .windows(2)
                .any(|w| matches!((w.first(), w.get(1)), (Some(a), Some(b)) if a == b))
            {
                return Err(Error::InvalidArgument(
                    "Sparse vector contains duplicate dimension indices.".to_owned(),
                ));
            }
            (sorted.indices, sorted.values)
        };

        Ok(pb::SparseVector { indices, values })
    }
}

impl From<BTreeMap<u32, f32>> for SparseVector {
    fn from(map: BTreeMap<u32, f32>) -> Self {
        let mut indices = Vec::with_capacity(map.len());
        let mut values = Vec::with_capacity(map.len());
        for (idx, val) in map {
            indices.push(idx);
            values.push(val);
        }
        Self { indices, values }
    }
}

impl From<HashMap<u32, f32>> for SparseVector {
    fn from(map: HashMap<u32, f32>) -> Self {
        Self::from_pairs(map)
    }
}

/// Vector record with an ID, dense values, and optional metadata, sparse vector, and TTL.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorRecord {
    pub id: u64,
    pub values: Vec<f32>,
    pub metadata: Option<Metadata>,
    pub sparse_vector: Option<SparseVector>,
    /// Optional time-to-live in seconds. Zero disables expiration.
    pub ttl_seconds: u32,
}

impl VectorRecord {
    /// Create a vector record with an ID and dense float values.
    pub fn new(id: u64, values: Vec<f32>) -> Self {
        Self {
            id,
            values,
            metadata: None,
            sparse_vector: None,
            ttl_seconds: 0,
        }
    }

    /// Attach metadata to this vector record.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Attach a sparse vector to this vector record.
    pub fn with_sparse_vector(mut self, sparse_vector: SparseVector) -> Self {
        self.sparse_vector = Some(sparse_vector);
        self
    }

    /// Set a relative time-to-live in seconds.
    pub fn with_ttl_seconds(mut self, ttl_seconds: u32) -> Self {
        self.ttl_seconds = ttl_seconds;
        self
    }

    pub(crate) fn to_proto(&self) -> Result<pb::VectorWithId> {
        let dimension = u32::try_from(self.values.len())
            .map_err(|_| Error::InvalidArgument("Vector dimension exceeds u32::MAX.".to_owned()))?;
        let metadata = self
            .metadata
            .as_ref()
            .filter(|m| !m.is_empty())
            .map(to_proto_metadata);
        let sparse_vector = match &self.sparse_vector {
            Some(sv) if !sv.indices.is_empty() => Some(sv.to_proto()?),
            Some(sv) if !sv.values.is_empty() => Some(sv.to_proto()?),
            _ => None,
        };
        Ok(pb::VectorWithId {
            id: self.id,
            vector: Some(pb::Vector {
                values: self.values.clone(),
                dimension,
            }),
            metadata,
            sparse_vector,
            ttl_seconds: self.ttl_seconds,
        })
    }

    pub(crate) fn from_proto(proto: pb::VectorWithId) -> Self {
        let values = proto.vector.map(|v| v.values).unwrap_or_default();
        let metadata = proto.metadata.and_then(from_proto_metadata);
        let sparse_vector = proto
            .sparse_vector
            .filter(|sv| !sv.indices.is_empty())
            .map(|sv| SparseVector {
                indices: sv.indices,
                values: sv.values,
            });
        Self {
            id: proto.id,
            values,
            metadata,
            sparse_vector,
            ttl_seconds: proto.ttl_seconds,
        }
    }
}

/// Summary information for one collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionInfo {
    pub id: u32,
    pub name: String,
    pub dimension: u32,
    pub metric_type: String,
    pub vector_count: u64,
}

/// Server-wide statistics returned by `get_stats`.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerStats {
    pub total_vectors: u64,
    pub total_collections: u64,
    pub total_queries: u64,
    pub avg_query_time_ms: f32,
}

/// Single match returned by vector, range, or hybrid search.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub id: u64,
    pub distance: f32,
    pub metadata: Option<Metadata>,
}

/// Counts returned by `upsert`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpsertResult {
    pub upserted_count: u64,
    pub inserted_count: u64,
    pub updated_count: u64,
}

/// Page of vectors returned by `list_vectors`.
#[derive(Debug, Clone, PartialEq)]
pub struct ListVectorsResult {
    pub vectors: Vec<VectorRecord>,
    pub total_count: u64,
    pub has_more: bool,
}

/// Configuration for `create_collection`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateCollectionOptions {
    pub metric: Metric,
    pub index_type: IndexType,
    /// Number of shards. Zero uses the server default.
    pub num_shards: u32,
}

impl Default for CreateCollectionOptions {
    fn default() -> Self {
        Self {
            metric: Metric::L2,
            index_type: IndexType::Hnsw,
            num_shards: 0,
        }
    }
}

impl CreateCollectionOptions {
    /// Set the distance metric.
    pub fn metric(mut self, metric: Metric) -> Self {
        self.metric = metric;
        self
    }

    /// Set the index algorithm.
    pub fn index_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Set the shard count.
    pub fn num_shards(mut self, num_shards: u32) -> Self {
        self.num_shards = num_shards;
        self
    }
}

/// Configuration for `search`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOptions {
    pub top_k: u32,
    pub filter: Option<String>,
    pub return_metadata: bool,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            top_k: 10,
            filter: None,
            return_metadata: false,
        }
    }
}

impl SearchOptions {
    /// Set the maximum number of matches to return.
    pub fn top_k(mut self, top_k: u32) -> Self {
        self.top_k = top_k;
        self
    }

    /// Set the metadata filter expression.
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Request metadata fields in search results.
    pub fn return_metadata(mut self, return_metadata: bool) -> Self {
        self.return_metadata = return_metadata;
        self
    }
}

/// Configuration for `range_search`.
///
/// For an L2 or cosine collection, set `radius`.
/// For an inner-product collection, set `similarity_threshold`.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeSearchOptions {
    pub radius: Option<f32>,
    pub similarity_threshold: Option<f32>,
    pub filter: Option<String>,
    pub return_metadata: bool,
    pub max_results: u32,
}

impl Default for RangeSearchOptions {
    fn default() -> Self {
        Self {
            radius: None,
            similarity_threshold: None,
            filter: None,
            return_metadata: false,
            max_results: 1000,
        }
    }
}

impl RangeSearchOptions {
    /// Set the maximum distance for L2 or cosine collections.
    pub fn radius(mut self, radius: f32) -> Self {
        self.radius = Some(radius);
        self
    }

    /// Set the minimum dot product for inner-product collections.
    pub fn similarity_threshold(mut self, threshold: f32) -> Self {
        self.similarity_threshold = Some(threshold);
        self
    }

    /// Set the metadata filter expression.
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Request metadata fields in range search results.
    pub fn return_metadata(mut self, return_metadata: bool) -> Self {
        self.return_metadata = return_metadata;
        self
    }

    /// Set the maximum number of nearest results to keep.
    pub fn max_results(mut self, max_results: u32) -> Self {
        self.max_results = max_results;
        self
    }
}

/// Configuration for `hybrid_search`.
#[derive(Debug, Clone, PartialEq)]
pub struct HybridSearchOptions {
    pub query_vector: Option<Vec<f32>>,
    pub text_query: Option<String>,
    pub sparse_query: Option<SparseVector>,
    pub top_k: u32,
    pub vector_weight: f32,
    pub text_weight: f32,
    pub sparse_weight: f32,
    pub text_field: String,
    pub filter: Option<String>,
    pub return_metadata: bool,
}

impl Default for HybridSearchOptions {
    fn default() -> Self {
        Self {
            query_vector: None,
            text_query: None,
            sparse_query: None,
            top_k: 10,
            vector_weight: 0.5,
            text_weight: 0.5,
            sparse_weight: 0.0,
            text_field: "text".to_owned(),
            filter: None,
            return_metadata: false,
        }
    }
}

impl HybridSearchOptions {
    /// Set the dense query vector.
    pub fn query_vector(mut self, vector: Vec<f32>) -> Self {
        self.query_vector = Some(vector);
        self
    }

    /// Set the BM25 text query.
    pub fn text_query(mut self, query: impl Into<String>) -> Self {
        self.text_query = Some(query.into());
        self
    }

    /// Set the sparse query vector.
    pub fn sparse_query(mut self, sparse: SparseVector) -> Self {
        self.sparse_query = Some(sparse);
        self
    }

    /// Set the maximum number of matches to return.
    pub fn top_k(mut self, top_k: u32) -> Self {
        self.top_k = top_k;
        self
    }

    /// Set the weight for dense vector similarity.
    pub fn vector_weight(mut self, weight: f32) -> Self {
        self.vector_weight = weight;
        self
    }

    /// Set the weight for BM25 text search.
    pub fn text_weight(mut self, weight: f32) -> Self {
        self.text_weight = weight;
        self
    }

    /// Set the weight for sparse vector search.
    pub fn sparse_weight(mut self, weight: f32) -> Self {
        self.sparse_weight = weight;
        self
    }

    /// Set the metadata field used for BM25 text search.
    pub fn text_field(mut self, field: impl Into<String>) -> Self {
        self.text_field = field.into();
        self
    }

    /// Set the metadata filter expression.
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Request metadata fields in hybrid search results.
    pub fn return_metadata(mut self, return_metadata: bool) -> Self {
        self.return_metadata = return_metadata;
        self
    }
}

/// File format for server-side `bulk_import`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BulkImportFormat {
    #[default]
    Parquet,
    Numpy,
}

impl BulkImportFormat {
    pub(crate) fn to_proto(self) -> i32 {
        match self {
            Self::Parquet => pb::ImportFormat::Parquet as i32,
            Self::Numpy => pb::ImportFormat::Numpy as i32,
        }
    }
}

/// Configuration for server-side `bulk_import`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkImportOptions {
    pub format: BulkImportFormat,
    pub vector_column: String,
    pub id_column: String,
}

impl Default for BulkImportOptions {
    fn default() -> Self {
        Self {
            format: BulkImportFormat::Parquet,
            vector_column: "vector".to_owned(),
            id_column: "id".to_owned(),
        }
    }
}

impl BulkImportOptions {
    /// Set the input file format.
    pub fn format(mut self, format: BulkImportFormat) -> Self {
        self.format = format;
        self
    }

    /// Set the Parquet column name that holds vectors.
    pub fn vector_column(mut self, column: impl Into<String>) -> Self {
        self.vector_column = column.into();
        self
    }

    /// Set the Parquet column name that holds vector IDs.
    pub fn id_column(mut self, column: impl Into<String>) -> Self {
        self.id_column = column.into();
        self
    }
}

/// State of a server-side bulk import job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl ImportState {
    pub(crate) fn from_proto(value: i32) -> Self {
        match pb::ImportState::try_from(value) {
            Ok(pb::ImportState::ImportPending) => Self::Pending,
            Ok(pb::ImportState::ImportRunning) => Self::Running,
            Ok(pb::ImportState::ImportCompleted) => Self::Completed,
            Ok(pb::ImportState::ImportFailed) => Self::Failed,
            Ok(pb::ImportState::ImportCancelled) => Self::Cancelled,
            Err(_) => Self::Pending,
        }
    }

    /// Return `true` when the job reached a final state.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Status of a server-side bulk import job.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportStatus {
    pub import_id: String,
    pub state: ImportState,
    pub total_vectors: u64,
    pub imported_vectors: u64,
    pub progress_percent: f32,
    pub error_message: String,
    pub elapsed_seconds: f32,
    pub segments_created: u32,
}

/// Destination or source location for server-side backup and restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupTarget {
    /// S3 bucket with an optional key prefix.
    S3 {
        bucket: String,
        prefix: Option<String>,
    },
    /// Absolute directory path on the data node.
    Local { path: String },
}

impl BackupTarget {
    /// Create an S3 backup target.
    pub fn s3(bucket: impl Into<String>, prefix: Option<impl Into<String>>) -> Self {
        Self::S3 {
            bucket: bucket.into(),
            prefix: prefix.map(Into::into),
        }
    }

    /// Create a local filesystem backup target.
    pub fn local(path: impl Into<String>) -> Self {
        Self::Local { path: path.into() }
    }

    pub(crate) fn to_proto(&self) -> Result<pb::BackupTarget> {
        match self {
            Self::S3 { bucket, prefix } => {
                if bucket.is_empty() {
                    return Err(Error::InvalidArgument(
                        "S3 backup bucket cannot be empty.".to_owned(),
                    ));
                }
                Ok(pb::BackupTarget {
                    target: Some(pb::backup_target::Target::S3(pb::S3Target {
                        bucket: bucket.clone(),
                        prefix: prefix.clone().unwrap_or_default(),
                    })),
                })
            }
            Self::Local { path } => {
                if path.is_empty() {
                    return Err(Error::InvalidArgument(
                        "Local backup path cannot be empty.".to_owned(),
                    ));
                }
                Ok(pb::BackupTarget {
                    target: Some(pb::backup_target::Target::Local(pb::LocalTarget {
                        path: path.clone(),
                    })),
                })
            }
        }
    }
}

/// State of a server-side backup or restore job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl BackupState {
    pub(crate) fn from_proto(value: i32) -> Self {
        match pb::BackupState::try_from(value) {
            Ok(pb::BackupState::BackupPending) => Self::Pending,
            Ok(pb::BackupState::BackupRunning) => Self::Running,
            Ok(pb::BackupState::BackupCompleted) => Self::Completed,
            Ok(pb::BackupState::BackupFailed) => Self::Failed,
            Ok(pb::BackupState::BackupCancelled) => Self::Cancelled,
            Err(_) => Self::Pending,
        }
    }

    /// Return the proto enum name for display in error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "BACKUP_PENDING",
            Self::Running => "BACKUP_RUNNING",
            Self::Completed => "BACKUP_COMPLETED",
            Self::Failed => "BACKUP_FAILED",
            Self::Cancelled => "BACKUP_CANCELLED",
        }
    }
}

/// Status of a server-side backup job.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupStatus {
    pub backup_id: String,
    pub state: BackupState,
    pub shards_total: u32,
    pub shards_completed: u32,
    pub bytes_uploaded: u64,
    pub error_message: String,
    pub elapsed_seconds: f32,
    pub manifest_uri: String,
}

/// Status of a server-side restore job.
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreStatus {
    pub restore_id: String,
    pub state: BackupState,
    pub shards_total: u32,
    pub shards_completed: u32,
    pub error_message: String,
    pub elapsed_seconds: f32,
}

/// Metadata for a backup stored in a backup target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupInfo {
    pub backup_id: String,
    pub collection_name: String,
    pub collection_id: u32,
    pub created_at_unix_ms: i64,
    pub vector_count: u64,
    pub size_bytes: u64,
    pub manifest_version: u32,
}

/// Polling intervals for `wait_for_import`, `wait_for_backup`, and `wait_for_restore`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollOptions {
    pub poll_interval: Duration,
    pub timeout: Duration,
}

impl Default for PollOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(2),
            timeout: Duration::from_secs(3600),
        }
    }
}

impl PollOptions {
    /// Set the delay between status requests.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set the total deadline before returning `Error::Timeout`.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

pub(crate) fn to_proto_metadata(metadata: &Metadata) -> pb::Metadata {
    let fields = metadata
        .iter()
        .map(|(key, val)| {
            let proto_val = match val {
                MetadataValue::Int(v) => pb::metadata_value::Value::IntValue(*v),
                MetadataValue::Float(v) => pb::metadata_value::Value::DoubleValue(*v),
                MetadataValue::String(v) => pb::metadata_value::Value::StringValue(v.clone()),
                MetadataValue::Bool(v) => pb::metadata_value::Value::BoolValue(*v),
            };
            (
                key.clone(),
                pb::MetadataValue {
                    value: Some(proto_val),
                },
            )
        })
        .collect();
    pb::Metadata { fields }
}

pub(crate) fn from_proto_metadata(proto: pb::Metadata) -> Option<Metadata> {
    if proto.fields.is_empty() {
        return None;
    }
    let mut result = HashMap::with_capacity(proto.fields.len());
    for (key, val) in proto.fields {
        match val.value {
            Some(pb::metadata_value::Value::IntValue(v)) => {
                result.insert(key, MetadataValue::Int(v));
            }
            Some(pb::metadata_value::Value::DoubleValue(v)) => {
                result.insert(key, MetadataValue::Float(v));
            }
            Some(pb::metadata_value::Value::StringValue(v)) => {
                result.insert(key, MetadataValue::String(v));
            }
            Some(pb::metadata_value::Value::BoolValue(v)) => {
                result.insert(key, MetadataValue::Bool(v));
            }
            None => {}
        }
    }
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}
