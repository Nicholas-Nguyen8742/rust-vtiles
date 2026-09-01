//! Pipeline error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error(transparent)]
    Ingest(#[from] vtile_ingest::IngestError),

    #[error(transparent)]
    Tile(#[from] vtile_core::TileError),

    #[error("job error: {0}")]
    Job(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("manifest error: {0}")]
    Manifest(String),

    /// Sequence 1 US-01: conditional job creation lost — a record with this
    /// `jobId` already exists. Local analog of DynamoDB's
    /// `ConditionalCheckFailedException` on `attribute_not_exists(jobId)`.
    #[error("job already exists: {0}")]
    JobAlreadyExists(String),

    /// Sequence 1 US-04: a conditional state transition was rejected (stale
    /// status, or the edge is not in the state machine).
    #[error("state conflict: {0}")]
    StateConflict(String),

    /// Sequence 1 US-04: the job is owned by another worker's active lease.
    #[error("lease conflict: {0}")]
    LeaseConflict(String),

    /// Sequence 1 US-05: replay attempted while the job is still being
    /// processed (`JOB_ALREADY_ACTIVE`).
    #[error("job already active: {0}")]
    JobAlreadyActive(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type PipelineResult<T> = std::result::Result<T, PipelineError>;
