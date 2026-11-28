//! Error types for the GVDB client.

use thiserror::Error;

/// Result type alias for GVDB client operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by [`crate::GvdbClient`] and client-side importers.
#[derive(Debug, Error)]
pub enum Error {
    /// The GVDB server returned a gRPC error status.
    #[error("gRPC error ({}): {}", .0.code(), .0.message())]
    Status(#[from] tonic::Status),

    /// The gRPC transport failed to connect or send a request.
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// A caller passed an invalid argument or configuration value.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A polling operation did not finish before its deadline.
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// A server-side backup or restore job ended in a failed or cancelled state.
    #[error("{kind} {id} ended in {state}: {message}")]
    JobFailed {
        kind: &'static str,
        id: String,
        state: String,
        message: String,
    },

    /// A local file operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A CSV file could not be parsed.
    #[cfg(feature = "csv")]
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

    /// A JSON vector value in a CSV file could not be parsed.
    #[cfg(feature = "csv")]
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// A Parquet file could not be read.
    #[cfg(feature = "parquet")]
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// An Arrow batch could not be decoded.
    #[cfg(feature = "parquet")]
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
}

impl Error {
    /// Return the underlying gRPC status if this error came from the server.
    pub fn status(&self) -> Option<&tonic::Status> {
        match self {
            Self::Status(status) => Some(status),
            _ => None,
        }
    }
}
