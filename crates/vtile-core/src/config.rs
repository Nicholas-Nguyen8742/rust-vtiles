//! Tile-generation configuration (TRD §5 requirements encoded as defaults).

use crate::model::ZoomRange;
use serde::{Deserialize, Serialize};

/// Standard tile extent (MVT spec default).
pub const DEFAULT_EXTENT: u32 = 4096;
/// Preferred max gzipped tile size (TRD §5: 250 KB).
pub const TARGET_MAX_TILE_BYTES: usize = 250_000;
/// Hard max gzipped tile size (TRD §5: 750 KB).
pub const HARD_MAX_TILE_BYTES: usize = 750_000;
/// Do not simplify geometry at or above this zoom (TRD §4: preserve parcel
/// boundaries at zoom 14–16).
pub const SIMPLIFY_BELOW_ZOOM: u8 = 14;

/// Configuration for one tile-generation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TileConfig {
    /// MVT layer name written into every tile.
    /// TRD convention: `{source}_{type}`, e.g. `parcel_boundary`.
    pub layer_name: String,
    /// Tile extent in integer units (4096 by spec default).
    pub extent: u32,
    /// Buffer in extent units included around each tile edge.
    pub buffer: u32,
    /// Zoom range to generate.
    pub zoom_range: ZoomRange,
    /// Zooms below this value may be simplified; at/above it geometry is
    /// preserved verbatim (TRD §4 precision rules).
    pub simplify_below_zoom: u8,
    /// Preferred gzipped tile size target used for size mitigation.
    pub target_max_tile_bytes: usize,
    /// Hard gzipped tile size cap; tiles are pruned until under it.
    pub hard_max_tile_bytes: usize,
    /// gzip compression level (1–9).
    pub gzip_level: u32,
    /// Property policy (allowlist/denylist, payload caps).
    #[serde(default)]
    pub property_policy: PropertyPolicy,
    /// When true, tiles are generated in parallel worker threads.
    #[serde(default = "default_true")]
    pub parallel: bool,
}

fn default_true() -> bool {
    true
}

impl Default for TileConfig {
    fn default() -> Self {
        Self {
            layer_name: "vis_layer".to_string(),
            extent: DEFAULT_EXTENT,
            buffer: 256,
            zoom_range: ZoomRange::new(0, 16),
            simplify_below_zoom: SIMPLIFY_BELOW_ZOOM,
            target_max_tile_bytes: TARGET_MAX_TILE_BYTES,
            hard_max_tile_bytes: HARD_MAX_TILE_BYTES,
            gzip_level: 6,
            property_policy: PropertyPolicy::default(),
            parallel: true,
        }
    }
}

impl TileConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.extent == 0 {
            return Err("extent must be > 0".into());
        }
        if self.zoom_range.max_zoom > 20 {
            return Err("max_zoom > 20 is unsupported (integer overflow)".into());
        }
        if self.gzip_level == 0 || self.gzip_level > 9 {
            return Err("gzip_level must be in 1..=9".into());
        }
        if self.target_max_tile_bytes > self.hard_max_tile_bytes {
            return Err("target_max_tile_bytes must be <= hard_max_tile_bytes".into());
        }
        Ok(())
    }
}

/// Attribute policy applied during normalization and tile assembly.
///
/// TRD §4: preserve analysis attributes, strip PII, enforce a max
/// property payload per feature. TRD §18 mitigation: property allowlists and
/// zoom-based attribute pruning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PropertyPolicy {
    /// If `Some`, only these properties survive (case-insensitive).
    #[serde(default)]
    pub allowlist: Option<Vec<String>>,
    /// Always dropped, even when no allowlist is set (PII per TRD §13).
    #[serde(default = "default_denylist")]
    pub denylist: Vec<String>,
    /// Properties kept during size-mitigation level 1 (identifiers).
    #[serde(default = "default_core_properties")]
    pub core_properties: Vec<String>,
    /// Max serialized bytes of key/value payload per feature.
    #[serde(default = "default_max_property_bytes")]
    pub max_property_bytes_per_feature: usize,
    /// Max number of properties per feature.
    #[serde(default = "default_max_fields")]
    pub max_fields_per_feature: usize,
}

fn default_denylist() -> Vec<String> {
    vec![
        "owner_name".into(),
        "ownername".into(),
        "owner".into(),
        "owner_first".into(),
        "owner_last".into(),
        "mailing_address".into(),
        "email".into(),
        "phone".into(),
        "ssn".into(),
        "tax_id".into(),
    ]
}

fn default_core_properties() -> Vec<String> {
    vec![
        "assetId".into(),
        "parcelId".into(),
        "propertyId".into(),
        "propertyType".into(),
        "market".into(),
        "submarket".into(),
    ]
}

fn default_max_property_bytes() -> usize {
    512
}

fn default_max_fields() -> usize {
    32
}

impl Default for PropertyPolicy {
    fn default() -> Self {
        Self {
            allowlist: None,
            denylist: default_denylist(),
            core_properties: default_core_properties(),
            max_property_bytes_per_feature: default_max_property_bytes(),
            max_fields_per_feature: default_max_fields(),
        }
    }
}

impl PropertyPolicy {
    /// Case-insensitive membership helper.
    fn in_list(list: &[String], key: &str) -> bool {
        let lower = key.to_lowercase();
        list.iter().any(|k| k.to_lowercase() == lower)
    }

    pub fn is_denied(&self, key: &str) -> bool {
        Self::in_list(&self.denylist, key)
    }

    pub fn is_allowed(&self, key: &str) -> bool {
        match &self.allowlist {
            Some(list) => Self::in_list(list, key),
            None => !self.is_denied(key),
        }
    }

    pub fn is_core(&self, key: &str) -> bool {
        Self::in_list(&self.core_properties, key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_filters_pii_case_insensitively() {
        let p = PropertyPolicy::default();
        assert!(!p.is_allowed("ownerName"));
        assert!(!p.is_allowed("OWNER_NAME"));
        assert!(p.is_allowed("parcelId"));
    }

    #[test]
    fn policy_allowlist_wins() {
        let mut p = PropertyPolicy::default();
        p.allowlist = Some(vec!["parcelId".into()]);
        assert!(p.is_allowed("parcelId"));
        assert!(!p.is_allowed("market"));
    }

    #[test]
    fn default_config_validates() {
        TileConfig::default().validate().unwrap();
    }

    #[test]
    fn layer_categories_have_zoom_ranges() {
        use crate::model::LayerCategory::*;
        assert_eq!(Parcel.default_zoom_range(), ZoomRange::new(10, 16));
        assert_eq!(Macro.default_zoom_range(), ZoomRange::new(0, 8));
    }
}
