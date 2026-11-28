//! Client-side batch importers for in-memory records, CSV files, and Parquet files.

use std::time::{Duration, Instant};

use tonic::Code;

use crate::client::GvdbClient;
use crate::error::{Error, Result};
use crate::types::{CreateCollectionOptions, IndexType, Metric, VectorRecord};

#[cfg(any(feature = "csv", feature = "parquet"))]
use crate::types::{Metadata, MetadataValue};
#[cfg(any(feature = "csv", feature = "parquet"))]
use std::path::Path;

/// Write mode used by client-side importers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ImportMode {
    /// Idempotent upsert with automatic retries on transient gRPC errors.
    #[default]
    Upsert,
    /// Streaming insert without retries because partial writes are not idempotent.
    StreamInsert,
}

/// Shared configuration for client-side batch importers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImportOptions {
    pub batch_size: usize,
    pub mode: ImportMode,
    pub metric: Metric,
    pub index_type: IndexType,
    pub max_retries: usize,
    pub retry_base_delay: Duration,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            mode: ImportMode::Upsert,
            metric: Metric::Cosine,
            index_type: IndexType::Auto,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(500),
        }
    }
}

impl ImportOptions {
    /// Set the number of vectors sent per RPC batch.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the write mode.
    pub fn mode(mut self, mode: ImportMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set the distance metric used when the importer creates a new collection.
    pub fn metric(mut self, metric: Metric) -> Self {
        self.metric = metric;
        self
    }

    /// Set the index type used when the importer creates a new collection.
    pub fn index_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Set the maximum retry count for failed `Upsert` batches.
    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the initial backoff delay before the first retry.
    pub fn retry_base_delay(mut self, delay: Duration) -> Self {
        self.retry_base_delay = delay;
        self
    }
}

/// Summary returned after a client-side bulk import completes.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportResult {
    pub total_count: u64,
    pub batch_count: usize,
    pub failed_count: u64,
    pub elapsed_seconds: f64,
    pub collection: String,
    pub dimension: u32,
    pub created_collection: bool,
}

/// Configuration for [`import_csv`].
#[cfg(feature = "csv")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvImportOptions {
    pub vector_column: String,
    pub id_column: String,
    pub import: ImportOptions,
}

#[cfg(feature = "csv")]
impl Default for CsvImportOptions {
    fn default() -> Self {
        Self {
            vector_column: "vector".to_owned(),
            id_column: "id".to_owned(),
            import: ImportOptions::default(),
        }
    }
}

#[cfg(feature = "csv")]
impl CsvImportOptions {
    /// Set the name of the JSON vector column or the prefix for dimension columns.
    pub fn vector_column(mut self, column: impl Into<String>) -> Self {
        self.vector_column = column.into();
        self
    }

    /// Set the name of the vector ID column.
    pub fn id_column(mut self, column: impl Into<String>) -> Self {
        self.id_column = column.into();
        self
    }

    /// Set batching, retry, and collection creation options.
    pub fn import(mut self, import: ImportOptions) -> Self {
        self.import = import;
        self
    }
}

/// Configuration for [`import_parquet`].
#[cfg(feature = "parquet")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetImportOptions {
    pub vector_column: String,
    pub id_column: String,
    pub import: ImportOptions,
}

#[cfg(feature = "parquet")]
impl Default for ParquetImportOptions {
    fn default() -> Self {
        Self {
            vector_column: "vector".to_owned(),
            id_column: "id".to_owned(),
            import: ImportOptions::default(),
        }
    }
}

#[cfg(feature = "parquet")]
impl ParquetImportOptions {
    /// Set the name of the list column that holds vectors.
    pub fn vector_column(mut self, column: impl Into<String>) -> Self {
        self.vector_column = column.into();
        self
    }

    /// Set the name of the integer column that holds vector IDs.
    pub fn id_column(mut self, column: impl Into<String>) -> Self {
        self.id_column = column.into();
        self
    }

    /// Set batching, retry, and collection creation options.
    pub fn import(mut self, import: ImportOptions) -> Self {
        self.import = import;
        self
    }
}

/// Import in-memory [`VectorRecord`] slices in batches.
pub async fn import_records(
    client: &GvdbClient,
    collection: impl Into<String>,
    records: &[VectorRecord],
    options: ImportOptions,
) -> Result<ImportResult> {
    let mut importer = BatchImporter::new(client, collection.into(), options)?;
    for chunk in records.chunks(options.batch_size) {
        importer.send_batch(chunk).await?;
    }
    Ok(importer.finish())
}

struct BatchImporter<'a> {
    client: &'a GvdbClient,
    collection: String,
    options: ImportOptions,
    started_at: Instant,
    total_count: u64,
    batch_count: usize,
    failed_count: u64,
    dimension: u32,
    created_collection: bool,
}

impl<'a> BatchImporter<'a> {
    fn new(client: &'a GvdbClient, collection: String, options: ImportOptions) -> Result<Self> {
        if options.batch_size == 0 {
            return Err(Error::InvalidArgument(
                "batch_size must be greater than zero.".to_owned(),
            ));
        }
        Ok(Self {
            client,
            collection,
            options,
            started_at: Instant::now(),
            total_count: 0,
            batch_count: 0,
            failed_count: 0,
            dimension: 0,
            created_collection: false,
        })
    }

    async fn send_batch(&mut self, batch: &[VectorRecord]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        if self.batch_count == 0 {
            let dim = u32::try_from(batch[0].values.len()).map_err(|_| {
                Error::InvalidArgument("Vector dimension exceeds u32::MAX.".to_owned())
            })?;
            if dim == 0 {
                return Err(Error::InvalidArgument(
                    "Vectors cannot be empty during import.".to_owned(),
                ));
            }
            self.dimension = dim;
            self.created_collection = self.ensure_collection(dim).await?;
        }

        let retries = match self.options.mode {
            ImportMode::Upsert => self.options.max_retries,
            ImportMode::StreamInsert => 0,
        };

        match self.send_with_retry(batch, retries).await? {
            Some(count) => self.total_count += count,
            None => self.failed_count += batch.len() as u64,
        }
        self.batch_count += 1;
        Ok(())
    }

    async fn ensure_collection(&self, dimension: u32) -> Result<bool> {
        for existing in self.client.list_collections().await? {
            if existing.name == self.collection {
                if existing.dimension != dimension {
                    return Err(Error::InvalidArgument(format!(
                        "Collection '{}' already exists with dimension {}, but input data has dimension {}.",
                        self.collection, existing.dimension, dimension
                    )));
                }
                return Ok(false);
            }
        }
        self.client
            .create_collection(
                &self.collection,
                dimension,
                CreateCollectionOptions::default()
                    .metric(self.options.metric)
                    .index_type(self.options.index_type),
            )
            .await?;
        Ok(true)
    }

    async fn send_with_retry(
        &self,
        batch: &[VectorRecord],
        max_retries: usize,
    ) -> Result<Option<u64>> {
        for attempt in 0..=max_retries {
            let outcome = match self.options.mode {
                ImportMode::Upsert => self
                    .client
                    .upsert(&self.collection, batch)
                    .await
                    .map(|res| res.upserted_count),
                ImportMode::StreamInsert => {
                    self.client
                        .stream_insert(&self.collection, batch, batch.len())
                        .await
                }
            };

            match outcome {
                Ok(count) => return Ok(Some(count)),
                Err(Error::Status(status)) => {
                    if status.code() == Code::ResourceExhausted {
                        return Ok(None);
                    }
                    if attempt == max_retries {
                        return Ok(None);
                    }
                    let factor = 1u32.checked_shl(attempt as u32).unwrap_or(u32::MAX);
                    let delay = self.options.retry_base_delay.saturating_mul(factor);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(other) => return Err(other),
            }
        }
        Ok(None)
    }

    fn finish(self) -> ImportResult {
        let elapsed = (self.started_at.elapsed().as_secs_f64() * 100.0).round() / 100.0;
        ImportResult {
            total_count: self.total_count,
            batch_count: self.batch_count,
            failed_count: self.failed_count,
            elapsed_seconds: elapsed,
            collection: self.collection,
            dimension: self.dimension,
            created_collection: self.created_collection,
        }
    }
}

// ---------------------------------------------------------------------------
// CSV importer
// ---------------------------------------------------------------------------

#[cfg(feature = "csv")]
enum CsvVectorColumns {
    Json(usize),
    Prefixed(Vec<usize>),
}

/// Import vectors from a CSV file.
///
/// Supports either one JSON-encoded vector column (`"[0.1, 0.2, 0.3]"`) or
/// dimension-prefixed columns (`vector_0, vector_1, ...`). Remaining columns
/// are imported as vector metadata.
#[cfg(feature = "csv")]
pub async fn import_csv(
    client: &GvdbClient,
    path: impl AsRef<Path>,
    collection: impl Into<String>,
    options: CsvImportOptions,
) -> Result<ImportResult> {
    let mut importer = BatchImporter::new(client, collection.into(), options.import)?;
    let mut reader = csv::Reader::from_path(path.as_ref())?;
    let headers: Vec<String> = reader
        .headers()?
        .iter()
        .map(std::borrow::ToOwned::to_owned)
        .collect();

    let id_idx = headers
        .iter()
        .position(|h| h == &options.id_column)
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "ID column '{}' was not found. Available columns: {:?}",
                options.id_column, headers
            ))
        })?;

    let vec_cols = detect_csv_vector_columns(&headers, &options.vector_column)?;
    let meta_cols: Vec<(usize, String)> = headers
        .iter()
        .enumerate()
        .filter(|(idx, _)| {
            if *idx == id_idx {
                return false;
            }
            match &vec_cols {
                CsvVectorColumns::Json(v_idx) => idx != v_idx,
                CsvVectorColumns::Prefixed(indices) => !indices.contains(idx),
            }
        })
        .map(|(idx, name)| (idx, name.clone()))
        .collect();

    let batch_size = options.import.batch_size;
    let mut batch = Vec::with_capacity(batch_size);

    for (row_num, record) in reader.records().enumerate() {
        let row = record?;
        let id_str = row.get(id_idx).unwrap_or("").trim();
        let id = id_str.parse::<u64>().map_err(|err| {
            Error::InvalidArgument(format!(
                "Row {} has invalid ID '{}' in column '{}': {}",
                row_num + 1,
                id_str,
                options.id_column,
                err
            ))
        })?;

        let values = match &vec_cols {
            CsvVectorColumns::Json(v_idx) => {
                let raw = row.get(*v_idx).unwrap_or("").trim();
                serde_json::from_str::<Vec<f32>>(raw)?
            }
            CsvVectorColumns::Prefixed(indices) => {
                let mut vec = Vec::with_capacity(indices.len());
                for &idx in indices {
                    let raw = row.get(idx).unwrap_or("").trim();
                    let val = raw.parse::<f32>().map_err(|err| {
                        Error::InvalidArgument(format!(
                            "Row {} has invalid float '{}' in column '{}': {}",
                            row_num + 1,
                            raw,
                            headers[idx],
                            err
                        ))
                    })?;
                    vec.push(val);
                }
                vec
            }
        };

        let mut rec = VectorRecord::new(id, values);
        if !meta_cols.is_empty() {
            let mut meta = Metadata::new();
            for (idx, col_name) in &meta_cols {
                let raw = row.get(*idx).unwrap_or("").trim();
                if let Some(parsed) = parse_csv_metadata_value(raw) {
                    meta.insert(col_name.clone(), parsed);
                }
            }
            if !meta.is_empty() {
                rec = rec.with_metadata(meta);
            }
        }

        batch.push(rec);
        if batch.len() == batch_size {
            importer.send_batch(&batch).await?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        importer.send_batch(&batch).await?;
    }

    Ok(importer.finish())
}

#[cfg(feature = "csv")]
fn detect_csv_vector_columns(headers: &[String], vector_column: &str) -> Result<CsvVectorColumns> {
    if let Some(idx) = headers.iter().position(|h| h == vector_column) {
        return Ok(CsvVectorColumns::Json(idx));
    }

    let prefix = format!("{vector_column}_");
    let mut prefixed: Vec<(usize, usize)> = headers
        .iter()
        .enumerate()
        .filter_map(|(idx, name)| {
            let suffix = name.strip_prefix(&prefix)?;
            let dim_idx = suffix.parse::<usize>().ok()?;
            Some((dim_idx, idx))
        })
        .collect();

    if prefixed.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "Vector column '{vector_column}' was not found, and no '{vector_column}_0' columns exist. Available columns: {headers:?}"
        )));
    }

    prefixed.sort_by_key(|(dim_idx, _)| *dim_idx);
    Ok(CsvVectorColumns::Prefixed(
        prefixed.into_iter().map(|(_, idx)| idx).collect(),
    ))
}

#[cfg(feature = "csv")]
fn parse_csv_metadata_value(raw: &str) -> Option<MetadataValue> {
    if raw.is_empty() {
        return None;
    }
    if raw.eq_ignore_ascii_case("true") {
        return Some(MetadataValue::Bool(true));
    }
    if raw.eq_ignore_ascii_case("false") {
        return Some(MetadataValue::Bool(false));
    }
    if let Ok(int_val) = raw.parse::<i64>() {
        return Some(MetadataValue::Int(int_val));
    }
    if let Ok(float_val) = raw.parse::<f64>()
        && float_val.is_finite()
    {
        return Some(MetadataValue::Float(float_val));
    }
    Some(MetadataValue::String(raw.to_owned()))
}

// ---------------------------------------------------------------------------
// Parquet importer
// ---------------------------------------------------------------------------

/// Import vectors from a Parquet file.
///
/// Expects an integer `id` column, a list or fixed-size list `vector` column
/// of `float32` or `float64` values, and optional scalar metadata columns.
#[cfg(feature = "parquet")]
pub async fn import_parquet(
    client: &GvdbClient,
    path: impl AsRef<Path>,
    collection: impl Into<String>,
    options: ParquetImportOptions,
) -> Result<ImportResult> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut importer = BatchImporter::new(client, collection.into(), options.import)?;
    let file = std::fs::File::open(path.as_ref())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();

    let all_columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let vector_idx = schema.index_of(&options.vector_column).map_err(|_| {
        Error::InvalidArgument(format!(
            "Vector column '{}' was not found. Available columns: {:?}",
            options.vector_column, all_columns
        ))
    })?;
    let id_idx = schema.index_of(&options.id_column).map_err(|_| {
        Error::InvalidArgument(format!(
            "ID column '{}' was not found. Available columns: {:?}",
            options.id_column, all_columns
        ))
    })?;

    let meta_cols: Vec<(usize, String)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != vector_idx && *idx != id_idx)
        .map(|(idx, field)| (idx, field.name().clone()))
        .collect();

    let reader = builder.with_batch_size(options.import.batch_size).build()?;
    for batch_result in reader {
        let record_batch = batch_result?;
        let records = decode_parquet_batch(&record_batch, id_idx, vector_idx, &meta_cols)?;
        importer.send_batch(&records).await?;
    }

    Ok(importer.finish())
}

#[cfg(feature = "parquet")]
fn decode_parquet_batch(
    batch: &arrow_array::RecordBatch,
    id_idx: usize,
    vector_idx: usize,
    meta_cols: &[(usize, String)],
) -> Result<Vec<VectorRecord>> {
    let n_rows = batch.num_rows();
    let id_col = batch.column(id_idx).as_ref();
    let vec_col = batch.column(vector_idx).as_ref();
    let mut records = Vec::with_capacity(n_rows);

    for row in 0..n_rows {
        let id = extract_arrow_u64(id_col, row)?;
        let values = extract_arrow_vector(vec_col, row)?;
        let mut rec = VectorRecord::new(id, values);

        if !meta_cols.is_empty() {
            let mut meta = Metadata::new();
            for (col_idx, col_name) in meta_cols {
                if let Some(val) = extract_arrow_metadata(batch.column(*col_idx).as_ref(), row) {
                    meta.insert(col_name.clone(), val);
                }
            }
            if !meta.is_empty() {
                rec = rec.with_metadata(meta);
            }
        }
        records.push(rec);
    }

    Ok(records)
}

#[cfg(feature = "parquet")]
fn extract_arrow_u64(col: &dyn arrow_array::Array, row: usize) -> Result<u64> {
    use arrow_array::{Int32Array, Int64Array, UInt32Array, UInt64Array};

    if col.is_null(row) {
        return Err(Error::InvalidArgument(format!(
            "Row {} has a null ID value.",
            row + 1
        )));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        return Ok(arr.value(row));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
        return Ok(u64::from(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return u64::try_from(arr.value(row)).map_err(|_| {
            Error::InvalidArgument(format!("Row {} has a negative ID value.", row + 1))
        });
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return u64::try_from(arr.value(row)).map_err(|_| {
            Error::InvalidArgument(format!("Row {} has a negative ID value.", row + 1))
        });
    }
    Err(Error::InvalidArgument(format!(
        "ID column has unsupported Arrow type {:?}. Use Int32, Int64, UInt32, or UInt64.",
        col.data_type()
    )))
}

#[cfg(feature = "parquet")]
fn extract_arrow_vector(col: &dyn arrow_array::Array, row: usize) -> Result<Vec<f32>> {
    use arrow_array::{FixedSizeListArray, LargeListArray, ListArray};

    if col.is_null(row) {
        return Err(Error::InvalidArgument(format!(
            "Row {} has a null vector value.",
            row + 1
        )));
    }
    if let Some(arr) = col.as_any().downcast_ref::<ListArray>() {
        return arrow_slice_to_f32(arr.value(row).as_ref());
    }
    if let Some(arr) = col.as_any().downcast_ref::<LargeListArray>() {
        return arrow_slice_to_f32(arr.value(row).as_ref());
    }
    if let Some(arr) = col.as_any().downcast_ref::<FixedSizeListArray>() {
        return arrow_slice_to_f32(arr.value(row).as_ref());
    }
    Err(Error::InvalidArgument(format!(
        "Vector column has unsupported Arrow type {:?}. Use List, LargeList, or FixedSizeList of floats.",
        col.data_type()
    )))
}

#[cfg(feature = "parquet")]
fn arrow_slice_to_f32(values: &dyn arrow_array::Array) -> Result<Vec<f32>> {
    use arrow_array::{Float32Array, Float64Array};

    if let Some(arr) = values.as_any().downcast_ref::<Float32Array>() {
        return Ok(arr.values().to_vec());
    }
    if let Some(arr) = values.as_any().downcast_ref::<Float64Array>() {
        return Ok(arr.values().iter().map(|&v| v as f32).collect());
    }
    Err(Error::InvalidArgument(format!(
        "Vector elements have unsupported Arrow type {:?}. Use Float32 or Float64.",
        values.data_type()
    )))
}

#[cfg(feature = "parquet")]
fn extract_arrow_metadata(col: &dyn arrow_array::Array, row: usize) -> Option<MetadataValue> {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeStringArray, StringArray, StringViewArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };

    if col.is_null(row) {
        return None;
    }
    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
        return Some(MetadataValue::Bool(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(MetadataValue::Int(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return Some(MetadataValue::Int(i64::from(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int16Array>() {
        return Some(MetadataValue::Int(i64::from(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int8Array>() {
        return Some(MetadataValue::Int(i64::from(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        return i64::try_from(arr.value(row)).ok().map(MetadataValue::Int);
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
        return Some(MetadataValue::Int(i64::from(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt16Array>() {
        return Some(MetadataValue::Int(i64::from(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt8Array>() {
        return Some(MetadataValue::Int(i64::from(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        let v = arr.value(row);
        return v.is_finite().then_some(MetadataValue::Float(v));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
        let v = f64::from(arr.value(row));
        return v.is_finite().then_some(MetadataValue::Float(v));
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Some(MetadataValue::String(arr.value(row).to_owned()));
    }
    if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
        return Some(MetadataValue::String(arr.value(row).to_owned()));
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
        return Some(MetadataValue::String(arr.value(row).to_owned()));
    }
    None
}
