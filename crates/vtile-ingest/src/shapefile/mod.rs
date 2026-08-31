//! Shapefile ingestion (TRD §3).

pub mod bundle;
pub mod dbf;
pub mod shp;

pub use bundle::{extract_bundle, read_bundle, ShapefileBundle};
