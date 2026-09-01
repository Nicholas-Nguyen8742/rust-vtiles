//! HTTP route handlers.

pub mod health;
pub mod jobs;
pub mod layers;
pub mod tiles;
pub mod uploads;

use vtile_core::model::LayerCategory;

/// TRD layer naming convention `{source}_{type}` (US-09) for MVT layer names.
/// Delegates to the pipeline crate so the API, CLI, and replay path share
/// one implementation.
pub fn mvt_layer_name(category: Option<LayerCategory>, layer_id: &str) -> String {
    vtile_pipeline::job::default_mvt_layer_name(category, layer_id)
}
