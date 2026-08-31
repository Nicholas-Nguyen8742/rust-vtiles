//! Error types for the tile-generation engine.

use thiserror::Error;

use crate::tilemath::TileId;

/// Errors produced by the MVT generation engine.
#[derive(Debug, Error)]
pub enum TileError {
    #[error("invalid geometry: {0}")]
    Geometry(String),

    /// A tile exceeds the hard size cap after the full mitigation ladder.
    ///
    /// Note: the current ladder (TRD §5) always converges — as a last resort
    /// it publishes a single oversized feature-tile and flags it in
    /// `TileStats` (TRD §17 "tile size explosion"). This variant is reserved
    /// for configurations that choose to fail instead of publish oversized
    /// tiles, and is mapped to the `TILE_SIZE_EXCEEDED` taxonomy code.
    #[error("tile size exceeded: z{tile.z}/{tile.x}/{tile.y} is {bytes} bytes gzipped (limit {limit})")]
    SizeExceeded { tile: TileId, bytes: usize, limit: usize },

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
