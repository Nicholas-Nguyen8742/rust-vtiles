//! Ingestion error types, mapped to TRD §8/§10 HTTP error codes upstream.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    /// TRD: reject with `422 INVALID_SHAPEFILE`.
    #[error("invalid shapefile: {0}")]
    InvalidShapefile(String),

    /// TRD: reject with `422 INVALID_GEOJSON`.
    #[error("invalid geojson: {0}")]
    InvalidGeoJson(String),

    /// TRD: reject with `422 EMPTY_DATASET`.
    #[error("empty dataset: {0}")]
    EmptyDataset(String),

    /// TRD: reject with `413 PAYLOAD_TOO_LARGE`.
    #[error("payload too large: {size} bytes exceeds limit of {max} bytes")]
    PayloadTooLarge { size: u64, max: u64 },

    /// TRD: unsupported CRS without a safe reprojection path.
    #[error("unsupported CRS: {0}")]
    UnsupportedCrs(String),

    /// Missing `.prj`: CRS unknown, user confirmation required (TRD §10).
    #[error("unknown CRS: {0}")]
    UnknownCrs(String),

    #[error("geometry errors: {unrecoverable} unrecoverable feature(s); first: {first}")]
    GeometryErrors { unrecoverable: u64, first: String },

    #[error("encoding error: {0}")]
    Encoding(String),

    #[error("zip error: {0}")]
    Zip(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type IngestResult<T> = std::result::Result<T, IngestError>;
