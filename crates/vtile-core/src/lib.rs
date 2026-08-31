//! `vtile-core` — Mapbox Vector Tile (MVT v2) generation engine.
//!
//! This crate is the technical heart of the vector-tile pipeline (see TRD §5).
//! It converts `geo_types` geometries in EPSG:4326 into gzip-compressed MVT v2
//! tiles addressed by Web Mercator XYZ (`z/x/y.pbf`).
//!
//! Design notes:
//! - The protobuf wire format is encoded directly (see [`mvt::pbf`]) instead of
//!   pulling in a code-generated protobuf stack. MVT's schema is tiny and
//!   stable, and a hand-rolled encoder keeps the hot path dependency-free.
//! - Feature-to-tile assignment is bbox-based with a configurable buffer: a
//!   feature is written into every tile whose extent intersects the feature
//!   bounding box expanded by `buffer`. Coordinates are allowed to exceed the
//!   `[0, extent]` range, exactly as the MVT spec permits; map clients clip at
//!   render time. This trades a small amount of tile weight for correctness
//!   without a polygon-clipping dependency. See `docs/MVT.md` for the
//!   trade-offs and `docs/PRECISION.md` for the precision analysis.
//!
//! ## Quick example
//!
//! ```ignore
//! use vtile_core::config::TileConfig;
//! use vtile_core::model::ZoomRange;
//! use vtile_core::tileset::{prepare_features, generate_tiles, RawFeature};
//! use vtile_core::sink::MemoryTileSink;
//!
//! let config = TileConfig { layer_name: "parcel_boundary".into(), ..Default::default() };
//! let dataset = prepare_features(raw_features, &config);
//! let sink = MemoryTileSink::default();
//! let stats = generate_tiles(&dataset, &config, &sink).unwrap();
//! ```

pub mod config;
pub mod error;
pub mod model;
pub mod mvt;
pub mod properties;
pub mod simplify;
pub mod sink;
pub mod tilemath;
pub mod tileset;

pub use error::{Result, TileError};
