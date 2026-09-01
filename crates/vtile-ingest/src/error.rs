//! Ingestion error types, mapped to TRD §8/§10 HTTP error codes upstream.
//!
//! Every variant carries a stable taxonomy code via [`IngestError::error_code`]
//! (see `docs/ERRORS.md`), used consistently in `job.failed` events (TRD §9),
//! API error responses (TRD §8), job records, and the quarantine/DLQ
//! workflow.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    /// TRD: reject with `422 INVALID_SHAPEFILE`.
    #[error("invalid shapefile: {0}")]
    InvalidShapefile(String),

    /// Shapefile bundle is missing mandatory members (TRD §10: reject with
    /// `422 MISSING_SHAPEFILE_COMPONENTS`).
    #[error("missing shapefile components: {0}")]
    MissingShapefileComponents(String),

    /// Upload extension/content-type does not match the declared source
    /// format (Recommendation 3 US-01 input gate).
    #[error("invalid file type: {0}")]
    InvalidFileType(String),

    /// TRD: reject with `422 INVALID_GEOJSON`.
    #[error("invalid geojson: {0}")]
    InvalidGeoJson(String),

    /// TRD: reject with `422 EMPTY_DATASET`.
    #[error("empty dataset: {0}")]
    EmptyDataset(String),

    /// TRD: reject with `413 PAYLOAD_TOO_LARGE`.
    #[error("payload too large: {size} bytes exceeds limit of {max} bytes")]
    PayloadTooLarge { size: u64, max: u64 },

    /// Feature count exceeds the pipeline limit (TRD §14 scalability:
    /// datasets up to 1M features). Mapped to `FILE_TOO_LARGE`.
    #[error("dataset too large: {features} features exceeds limit of {max}")]
    DatasetTooLarge { features: u64, max: u64 },

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

impl IngestError {
    /// Machine-readable taxonomy code shared by `job.failed` events
    /// (TRD §9), API error bodies (TRD §8), and the quarantine/DLQ error
    /// report. Full table in `docs/ERRORS.md`.
    pub fn error_code(&self) -> &'static str {
        match self {
            IngestError::InvalidShapefile(_) => "INVALID_SHAPEFILE",
            IngestError::MissingShapefileComponents(_) => "MISSING_SHAPEFILE_COMPONENTS",
            IngestError::InvalidFileType(_) => "INVALID_FILE_TYPE",
            IngestError::InvalidGeoJson(_) => "INVALID_GEOJSON",
            IngestError::EmptyDataset(_) => "EMPTY_DATASET",
            IngestError::PayloadTooLarge { .. } => "PAYLOAD_TOO_LARGE",
            IngestError::DatasetTooLarge { .. } => "FILE_TOO_LARGE",
            IngestError::UnsupportedCrs(_) => "UNSUPPORTED_CRS",
            IngestError::UnknownCrs(_) => "UNKNOWN_CRS",
            IngestError::GeometryErrors { .. } => "GEOMETRY_ERRORS",
            IngestError::Encoding(_) => "ENCODING_ERROR",
            IngestError::Zip(_) => "INVALID_SHAPEFILE",
            IngestError::Io(_) => "INTERNAL_ERROR",
            IngestError::Other(_) => "INGEST_FAILED",
        }
    }
}

pub type IngestResult<T> = std::result::Result<T, IngestError>;
