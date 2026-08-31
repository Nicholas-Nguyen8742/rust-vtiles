//! `vtile-ingest` — source-format readers and normalization.
//!
//! Implements TRD §3 (supported formats), §4 (normalization rules) and §10
//! (validation rules) for the MVP formats GeoJSON and zipped Shapefile.
//!
//! Pipeline position:
//!
//! ```text
//! raw upload ─▶ detect format ─▶ read (geojson | shapefile)
//!            ─▶ validate CRS / geometry ─▶ repair ─▶ reproject to EPSG:4326
//!            ─▶ clean properties ─▶ NormalizedDataset
//! ```

pub mod crs;
pub mod error;
pub mod geojson;
pub mod normalize;
pub mod repair;
pub mod shapefile;
pub mod validate;

pub use error::{IngestError, IngestResult};
pub use normalize::{
    normalize_source, write_normalized_geojson, NormalizedDataset, NormalizedFeature,
    NormalizeOptions, SourceFile,
};
