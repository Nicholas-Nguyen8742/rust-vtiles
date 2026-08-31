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

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type PipelineResult<T> = std::result::Result<T, PipelineError>;
