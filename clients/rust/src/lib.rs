//! Official Rust client for the GVDB distributed vector database.
//!
//! Connect to a GVDB server with [`GvdbClient::connect`] for plain connections,
//! or use [`ClientConfig`] to set an API key, a timeout, or TLS.

/// Generated gRPC and protobuf definitions for `gvdb.proto`.
pub mod pb {
    tonic::include_proto!("gvdb.proto");
}

mod client;
mod error;
pub mod importers;
mod types;

pub use client::{ClientConfig, GvdbClient};
pub use error::{Error, Result};
#[cfg(feature = "csv")]
pub use importers::CsvImportOptions;
#[cfg(feature = "parquet")]
pub use importers::ParquetImportOptions;
pub use importers::{ImportMode, ImportOptions, ImportResult};
pub use types::{
    BackupInfo, BackupState, BackupStatus, BackupTarget, BulkImportFormat, BulkImportOptions,
    CollectionInfo, CreateCollectionOptions, HybridSearchOptions, ImportState, ImportStatus,
    IndexType, ListVectorsResult, Metadata, MetadataValue, Metric, PollOptions, RangeSearchOptions,
    RestoreStatus, SearchOptions, SearchResult, ServerStats, SparseVector, UpsertResult,
    VectorRecord,
};
