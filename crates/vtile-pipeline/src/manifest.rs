//! Tile manifests (TRD §6 `manifests/{tenantId}/{layerId}/manifest.json`).
//!
//! The manifest is the atomic publish pointer (TRD §14 reliability: "Atomic
//! publish using tile version prefix or manifest swap"): tiles live under a
//! versioned prefix and the manifest records which version is live. Rollback
//! = rewrite the manifest to the previous version (retained 90 days, TRD §6).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use vtile_core::model::Bbox;

use crate::error::{PipelineError, PipelineResult};

pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TileManifest {
    pub schema_version: u32,
    pub tenant_id: String,
    pub layer_id: String,
    /// Live tile version (timestamp by default; see TRD open question 3).
    pub tile_version: String,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub tile_count: u64,
    pub total_gzip_bytes: u64,
    pub bounding_box: Bbox,
    pub generated_at: DateTime<Utc>,
    /// URL template for clients, e.g.
    /// `https://cdn.example.com/tiles/{tenant}/{layer}/{version}/{z}/{x}/{y}.pbf`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tile_url_template: Option<String>,
}

impl TileManifest {
    pub fn to_json(&self) -> PipelineResult<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub fn from_json(json: &str) -> PipelineResult<Self> {
        serde_json::from_str(json).map_err(|e| PipelineError::Manifest(e.to_string()))
    }
}
