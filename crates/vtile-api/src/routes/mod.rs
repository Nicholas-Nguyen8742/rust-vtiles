//! HTTP route handlers.

pub mod health;
pub mod jobs;
pub mod layers;
pub mod tiles;
pub mod uploads;

use vtile_core::model::LayerCategory;

/// TRD layer naming convention `{source}_{type}` (US-09) for MVT layer names.
pub fn mvt_layer_name(category: Option<LayerCategory>, layer_id: &str) -> String {
    match category {
        Some(LayerCategory::Parcel) => "parcel_boundary".to_string(),
        Some(LayerCategory::Zoning) => "zoning_district".to_string(),
        Some(LayerCategory::FloodRisk) => "flood_100yr".to_string(),
        Some(LayerCategory::Submarket) => "submarket_area".to_string(),
        Some(LayerCategory::AssetPoint) => "asset_point".to_string(),
        Some(LayerCategory::Macro) => "macro_region".to_string(),
        Some(LayerCategory::Other) | None => format!("{layer_id}_features"),
    }
}
