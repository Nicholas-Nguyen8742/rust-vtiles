//! Validation rules (TRD §10 table) that apply before normalization.
//!
//! HTTP mapping happens in the API layer:
//! * `INVALID_SHAPEFILE` → 422
//! * `EMPTY_DATASET` → 422
//! * `PAYLOAD_TOO_LARGE` → 413

use std::collections::HashSet;

use crate::error::{IngestError, IngestResult};

/// TRD §10: "File exceeds max size". Default 2 GB (TRD §14 scalability).
pub const DEFAULT_MAX_UPLOAD_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Rejects oversized uploads with `413 PAYLOAD_TOO_LARGE`.
pub fn check_payload_size(size: u64, max: u64) -> IngestResult<()> {
    if size > max {
        return Err(IngestError::PayloadTooLarge { size, max });
    }
    Ok(())
}

/// Features sitting exactly at null island — a classic geocoding failure
/// (TRD §17). Counted and warned, not rejected.
pub fn count_null_island<'a>(
    features: impl Iterator<Item = &'a geo_types::Geometry<f64>>,
) -> u64 {
    use geo_types::CoordsIter;
    features
        .filter(|g| {
            let mut coords = g.coords_iter();
            match coords.next() {
                Some(c) => c.x == 0.0 && c.y == 0.0 && coords.next().is_none(),
                None => false,
            }
        })
        .count() as u64
}

/// Duplicate identifiers (TRD §17 "duplicate parcel IDs"). Returns the
/// number of duplicated keys so callers can warn.
pub fn count_duplicate_ids(properties: &[serde_json::Map<String, serde_json::Value>]) -> u64 {
    let mut seen = HashSet::new();
    let mut dupes = 0u64;
    for props in properties {
        for key in ["parcelId", "assetId", "propertyId"] {
            if let Some(v) = props.get(key) {
                let marker = format!("{key}:{v}");
                if !seen.insert(marker) {
                    dupes += 1;
                }
            }
        }
    }
    dupes
}

/// Rejects an empty normalized dataset with `422 EMPTY_DATASET`.
pub fn require_non_empty(feature_count: usize, format: &str) -> IngestResult<()> {
    if feature_count == 0 {
        return Err(IngestError::EmptyDataset(format!(
            "{format} contained no usable features"
        )));
    }
    Ok(())
}
