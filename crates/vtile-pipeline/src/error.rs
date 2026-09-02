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

    /// Sequence 2 US-AP-02: the candidate tile version failed completeness
    /// verification (missing tiles, zero-byte tiles, checksum mismatch).
    #[error("publish validation failed: {0}")]
    PublishValidation(String),

    /// Sequence 2 US-AP-03: conditional promotion lost — the layer's current
    /// version changed underneath the publisher.
    #[error("promotion conflict: {0}")]
    PromotionConflict(String),

    /// Sequence 2 US-AP-05: rollback rejected (unknown layer, missing or
    /// corrupt target version).
    #[error("rollback failed: {0}")]
    RollbackFailed(String),

    /// Sequence 3 US-03: replay refused — the original failure is permanent
    /// (fix the source and submit a new upload), the replay limit is
    /// exhausted, or the job is cancelled (`REPLAY_NOT_ALLOWED`).
    #[error("replay not allowed: {0}")]
    ReplayNotAllowed(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type PipelineResult<T> = std::result::Result<T, PipelineError>;
