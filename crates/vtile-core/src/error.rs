//! Error types for the tile-generation engine.

use thiserror::Error;

/// Errors produced by the MVT generation engine.
#[derive(Debug, Error)]
pub enum TileError {
    #[error("invalid geometry: {0}")]
    Geometry(String),

    #[error("tile encoding failed: {0}")]
    Encoding(String),

    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("tile sink write failed: {0}")]
    Sink(String),

    #[error("gzip compression failed: {0}")]
    Compression(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, TileError>;
