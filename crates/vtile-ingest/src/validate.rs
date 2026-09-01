//! Validation rules (TRD §10 table) that apply before normalization.
//!
//! HTTP mapping happens in the API layer:
//! * `INVALID_SHAPEFILE` / `MISSING_SHAPEFILE_COMPONENTS` → 422
//! * `INVALID_FILE_TYPE` → 422
//! * `EMPTY_DATASET` → 422
//! * `PAYLOAD_TOO_LARGE` / `FILE_TOO_LARGE` → 413

use std::collections::HashSet;

use vtile_core::model::SourceFormat;

use crate::error::{IngestError, IngestResult};

/// TRD §10: "File exceeds max size". Default 2 GB (TRD §14 scalability).
pub const DEFAULT_MAX_UPLOAD_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// TRD §14 scalability: "Support datasets up to 1M features".
pub const DEFAULT_MAX_FEATURES: u64 = 1_000_000;

/// Rejects oversized uploads with `413 PAYLOAD_TOO_LARGE`.
pub fn check_payload_size(size: u64, max: u64) -> IngestResult<()> {
    if size > max {
        return Err(IngestError::PayloadTooLarge { size, max });
    }
    Ok(())
}

/// Rejects oversized datasets with `FILE_TOO_LARGE` before the expensive
/// tile-generation stage (Recommendation 3 US-01: fail fast).
pub fn check_feature_count(count: u64, max: u64) -> IngestResult<()> {
    if count > max {
        return Err(IngestError::DatasetTooLarge {
            features: count,
            max,
        });
    }
    Ok(())
}

/// Input validation gate (Recommendation 3 US-01): the file name extension
/// and, when supplied, the content type must match the declared source
/// format. Runs at upload time, before any parsing work.
pub fn validate_file_type(
    file_name: &str,
    content_type: Option<&str>,
    format: SourceFormat,
) -> IngestResult<()> {
    let lower = file_name.to_lowercase();
    let (ext_ok, expected) = match format {
        SourceFormat::GeoJson => (lower.ends_with(".geojson") || lower.ends_with(".json"), ".geojson/.json"),
        SourceFormat::Shapefile => (lower.ends_with(".zip"), ".zip"),
        // Post-MVP formats are rejected at the upload API already; keep a
        // deterministic error here as well.
        _ => (false, "no supported extension"),
    };
    if !ext_ok {
        return Err(IngestError::InvalidFileType(format!(
            "file {file_name:?} does not match declared format {} (expected {expected})",
            format.as_str()
        )));
    }

    if let Some(ct) = content_type {
        // Strip parameters (`application/zip; charset=...`).
        let ct = ct.split(';').next().unwrap_or(ct).trim().to_lowercase();
        let ct_ok = match format {
            SourceFormat::GeoJson => matches!(
                ct.as_str(),
                "application/geo+json"
                    | "application/json"
                    | "text/plain"
                    | "application/octet-stream"
            ),
            SourceFormat::Shapefile => matches!(
                ct.as_str(),
                "application/zip" | "application/x-zip-compressed" | "application/octet-stream"
            ),
            _ => false,
        };
        if !ct_ok {
            return Err(IngestError::InvalidFileType(format!(
                "content type {ct:?} does not match declared format {}",
                format.as_str()
            )));
        }
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
