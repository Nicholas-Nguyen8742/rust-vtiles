//! Normalization orchestrator (TRD §10 workflow steps 4–7).
//!
//! `SourceFile` → parse → repair → reproject to EPSG:4326 → clean properties
//! → [`NormalizedDataset`], plus serialization of the normalized artifact
//! (workflow step 5: "Normalize to GeoJSON").

use geo_types::{Coord, Geometry};
use serde::{Deserialize, Serialize};

use vtile_core::config::PropertyPolicy;
use vtile_core::model::Bbox;

use crate::crs::{detect_crs, reproject_xy, CrsInfo, CrsKind};
use crate::error::{IngestError, IngestResult};
use crate::geojson::parse_geojson;
use crate::repair::repair_geometry;
use crate::shapefile::{extract_bundle, read_bundle};
use crate::validate::{check_payload_size, count_duplicate_ids, count_null_island};

/// What to do when a shapefile has no `.prj` (TRD §10 requires explicit user
/// confirmation; US-04 allows assuming WGS84 with a metadata flag).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum CrsPolicy {
    /// Reject unknown CRS (TRD §10 default).
    #[default]
    RequireKnown,
    /// Assume EPSG:4326 and flag `assumed_crs` in metadata (US-04).
    AssumeWgs84,
}

#[derive(Debug, Clone)]
pub enum SourceFile {
    GeoJson { bytes: Vec<u8> },
    ShapefileZip { bytes: Vec<u8> },
}

#[derive(Debug, Clone)]
pub struct NormalizeOptions {
    pub max_upload_bytes: u64,
    /// Feature-count cap (TRD §14 scalability). Exceeding it fails fast with
    /// `FILE_TOO_LARGE` before tile generation.
    pub max_features: u64,
    pub crs_policy: CrsPolicy,
    pub property_policy: PropertyPolicy,
}

impl Default for NormalizeOptions {
    fn default() -> Self {
        Self {
            max_upload_bytes: crate::validate::DEFAULT_MAX_UPLOAD_BYTES,
            max_features: crate::validate::DEFAULT_MAX_FEATURES,
            crs_policy: CrsPolicy::RequireKnown,
            property_policy: PropertyPolicy::default(),
        }
    }
}

/// One normalized feature: EPSG:4326 geometry + cleaned JSON properties.
#[derive(Debug, Clone)]
pub struct NormalizedFeature {
    pub id: Option<u64>,
    pub geometry: Geometry<f64>,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

/// Result of normalization, carrying everything the tiler and catalog need.
#[derive(Debug, Default)]
pub struct NormalizedDataset {
    pub features: Vec<NormalizedFeature>,
    pub bbox: Option<Bbox>,
    pub crs: CrsInfo,
    pub warnings: Vec<String>,
    pub rejected_features: u64,
}

impl NormalizedDataset {
    pub fn feature_count(&self) -> usize {
        self.features.len()
    }
}

/// Runs the full normalization pipeline.
pub fn normalize_source(source: SourceFile, opts: &NormalizeOptions) -> IngestResult<NormalizedDataset> {
    match source {
        SourceFile::GeoJson { bytes } => {
            check_payload_size(bytes.len() as u64, opts.max_upload_bytes)?;
            normalize_geojson(&bytes, opts)
        }
        SourceFile::ShapefileZip { bytes } => {
            check_payload_size(bytes.len() as u64, opts.max_upload_bytes)?;
            normalize_shapefile(&bytes, opts)
        }
    }
}

fn normalize_geojson(bytes: &[u8], opts: &NormalizeOptions) -> IngestResult<NormalizedDataset> {
    let parsed = parse_geojson(bytes)?;
    crate::validate::check_feature_count(parsed.len() as u64, opts.max_features)?;
    // RFC 7946: GeoJSON is always WGS84 lon/lat.
    let crs = CrsInfo {
        kind: CrsKind::Wgs84,
        epsg: Some(4326),
        raw: None,
        assumed: false,
    };
    finish_normalization(
        parsed.into_iter().map(|f| (f.id, f.geometry, f.properties)),
        &crs,
        opts,
        "GEOJSON",
    )
}

fn normalize_shapefile(bytes: &[u8], opts: &NormalizeOptions) -> IngestResult<NormalizedDataset> {
    let bundle = extract_bundle(bytes)?;
    let crs_info = detect_crs(bundle.prj.as_deref());

    let crs = match crs_info.kind {
        CrsKind::Unknown => match opts.crs_policy {
            CrsPolicy::RequireKnown => {
                return Err(IngestError::UnknownCrs(
                    "missing .prj; resubmit with CRS confirmation (AssumeWgs84) or a .prj file"
                        .into(),
                ));
            }
            CrsPolicy::AssumeWgs84 => CrsInfo {
                kind: CrsKind::Wgs84,
                epsg: Some(4326),
                raw: None,
                assumed: true,
            },
        },
        _ => crs_info,
    };

    let pairs = read_bundle(&bundle)?;
    crate::validate::check_feature_count(pairs.len() as u64, opts.max_features)?;
    let features = pairs.into_iter().enumerate().map(|(idx, (geom, props))| {
        // Synthetic sequential ids preserve row linkage (see bundle docs).
        (Some(idx as u64 + 1), geom, props)
    });
    finish_normalization(features, &crs, opts, "SHAPEFILE")
}

/// Shared tail: repair → reproject → clean properties → collect stats.
fn finish_normalization(
    features: impl Iterator<Item = (Option<u64>, Geometry<f64>, serde_json::Map<String, serde_json::Value>)>,
    crs: &CrsInfo,
    opts: &NormalizeOptions,
    format: &str,
) -> IngestResult<NormalizedDataset> {
    let mut dataset = NormalizedDataset {
        crs: crs.clone(),
        ..Default::default()
    };
    let mut bbox: Option<Bbox> = None;
    let mut null_island = 0u64;
    let mut all_props: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();

    for (id, mut geometry, properties) in features {

        // 1. Repair (ST_MakeValid equivalent, TRD §4 rule 3).
        let Some(mut repaired) = repair_geometry(geometry) else {
            dataset.rejected_features += 1;
            continue;
        };

        // 2. Reproject to EPSG:4326 (TRD §4 rule 1).
        if reproject_in_place(&mut repaired, crs).is_err() {
            return Err(IngestError::UnsupportedCrs(crs.label().to_string()));
        }
        geometry = repaired;

        // 3. Track bbox / null island.
        {
            use geo_types::CoordsIter;
            let mut first = true;
            for c in geometry.coords_iter() {
                let b = Bbox::new(c.x, c.y, c.x, c.y);
                bbox = Some(match bbox.take() {
                    Some(existing) => existing.union(&b),
                    None => b,
                });
                first = false;
            }
            if first {
                dataset.rejected_features += 1;
                continue;
            }
        }
        if is_null_island(&geometry) {
            null_island += 1;
        }

        // 4. Clean properties (TRD §4 rule 5).
        let cleaned = clean_properties(&properties, &opts.property_policy);
        all_props.push(cleaned.clone());

        dataset.features.push(NormalizedFeature {
            id,
            geometry,
            properties: cleaned,
        });
    }

    // TRD §4 rule 3: reject files whose features are all unrecoverable. The
    // distinction matters for operators: an empty *source* is a data-entry
    // problem (`EMPTY_DATASET`), a source whose geometries all failed repair
    // is a geometry problem (`GEOMETRY_ERRORS`).
    if dataset.feature_count() == 0 && dataset.rejected_features > 0 {
        return Err(IngestError::GeometryErrors {
            unrecoverable: dataset.rejected_features,
            first: "all features failed geometry repair (degenerate rings, non-finite coordinates)"
                .into(),
        });
    }
    crate::validate::require_non_empty(dataset.feature_count(), format)?;

    if crs.assumed {
        dataset
            .warnings
            .push("CRS missing in source; EPSG:4326 assumed (flagged in metadata)".into());
    }
    if null_island > 0 {
        dataset
            .warnings
            .push(format!("{null_island} feature(s) located exactly at null island (0,0)"));
    }
    let dupes = count_duplicate_ids(&all_props);
    if dupes > 0 {
        dataset
            .warnings
            .push(format!("{dupes} duplicate parcel/asset identifier(s) detected"));
    }
    if dataset.rejected_features > 0 {
        dataset.warnings.push(format!(
            "{} feature(s) rejected as unrecoverable",
            dataset.rejected_features
        ));
    }
    dataset.bbox = bbox;
    Ok(dataset)
}

fn is_null_island(geom: &Geometry<f64>) -> bool {
    use geo_types::CoordsIter;
    let mut coords = geom.coords_iter();
    match coords.next() {
        Some(c) => c.x == 0.0 && c.y == 0.0 && coords.next().is_none(),
        None => false,
    }
}

/// Applies the reprojection to every coordinate of a geometry in place.
fn reproject_in_place(geom: &mut Geometry<f64>, crs: &CrsInfo) -> IngestResult<()> {
    if matches!(crs.kind, CrsKind::Wgs84) {
        return Ok(());
    }
    use geo_types::CoordsIterMut;
    let mut failed = false;
    geom.coords_mut_iter().for_each(|c: &mut Coord<f64>| {
        if reproject_xy(crs, &mut c.x, &mut c.y).is_err() {
            failed = true;
        }
    });
    if failed {
        Err(IngestError::UnsupportedCrs(crs.label().to_string()))
    } else {
        Ok(())
    }
}

/// Property cleaning (TRD §4 rule 5): allowlist/denylist, field count and
/// payload caps. Deterministic key order (sorted).
pub fn clean_properties(
    props: &serde_json::Map<String, serde_json::Value>,
    policy: &PropertyPolicy,
) -> serde_json::Map<String, serde_json::Value> {
    let mut entries: Vec<(&String, &serde_json::Value)> =
        props.iter().filter(|(k, _)| policy.is_allowed(k)).collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    let mut out = serde_json::Map::new();
    let mut payload = 0usize;
    for (k, v) in entries {
        if out.len() >= policy.max_fields_per_feature {
            break;
        }
        let size = k.len() + v.to_string().len();
        payload += size;
        if payload > policy.max_property_bytes_per_feature {
            break;
        }
        out.insert(k.clone(), v.clone());
    }
    out
}

/// Serializes the normalized dataset as an RFC 7946 FeatureCollection
/// (workflow step 5 artifact, written under `staging/{tenant}/{job}/normalized/`).
pub fn write_normalized_geojson(dataset: &NormalizedDataset) -> serde_json::Value {
    let features: Vec<serde_json::Value> = dataset
        .features
        .iter()
        .map(|f| {
            serde_json::json!({
                "type": "Feature",
                "id": f.id,
                "geometry": geometry_to_geojson(&f.geometry),
                "properties": f.properties,
            })
        })
        .collect();
    let bbox = dataset.bbox.map(|b| b.to_vec());
    serde_json::json!({
        "type": "FeatureCollection",
        "bbox": bbox,
        "features": features,
    })
}

/// Converts a `geo_types` geometry into GeoJSON JSON (inverse of the reader).
pub fn geometry_to_geojson(geom: &Geometry<f64>) -> serde_json::Value {
    fn pos(c: &Coord<f64>) -> Vec<f64> {
        vec![c.x, c.y]
    }
    fn line(ls: &geo_types::LineString<f64>) -> Vec<Vec<f64>> {
        ls.0.iter().map(pos).collect()
    }
    fn polygon(p: &geo_types::Polygon<f64>) -> Vec<Vec<Vec<f64>>> {
        std::iter::once(p.exterior())
            .chain(p.interiors().iter())
            .map(line)
            .collect()
    }
    match geom {
        Geometry::Point(p) => {
            serde_json::json!({ "type": "Point", "coordinates": [p.x(), p.y()] })
        }
        Geometry::MultiPoint(mp) => {
            let coords: Vec<Vec<f64>> = mp.0.iter().map(|p| vec![p.x(), p.y()]).collect();
            serde_json::json!({ "type": "MultiPoint", "coordinates": coords })
        }
        Geometry::LineString(ls) => {
            serde_json::json!({ "type": "LineString", "coordinates": line(ls) })
        }
        Geometry::MultiLineString(mls) => {
            let coords: Vec<Vec<Vec<f64>>> = mls.0.iter().map(line).collect();
            serde_json::json!({ "type": "MultiLineString", "coordinates": coords })
        }
        Geometry::Polygon(p) => {
            serde_json::json!({ "type": "Polygon", "coordinates": polygon(p) })
        }
        Geometry::MultiPolygon(mp) => {
            let coords: Vec<Vec<Vec<Vec<f64>>>> = mp.0.iter().map(polygon).collect();
            serde_json::json!({ "type": "MultiPolygon", "coordinates": coords })
        }
        Geometry::Rect(r) => geometry_to_geojson(&Geometry::Polygon(r.to_polygon())),
        Geometry::Triangle(t) => geometry_to_geojson(&Geometry::Polygon(t.to_polygon())),
        Geometry::GeometryCollection(_) => serde_json::json!({ "type": "GeometryCollection", "geometries": [] }),
        _ => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn geojson_end_to_end() {
        let doc = json!({
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-73.98, 40.75]},
                "properties": {"parcelId": "NYC-1", "ownerName": "SECRET"}
            }]
        });
        let ds = normalize_source(
            SourceFile::GeoJson {
                bytes: doc.to_string().into_bytes(),
            },
            &NormalizeOptions::default(),
        )
        .unwrap();
        assert_eq!(ds.feature_count(), 1);
        assert!(ds.features[0].properties.get("ownerName").is_none());
        assert!(ds.features[0].properties.get("parcelId").is_some());
        assert!(!ds.crs.assumed);
    }

    #[test]
    fn empty_collection_rejected() {
        let doc = json!({"type": "FeatureCollection", "features": []});
        let err = normalize_source(
            SourceFile::GeoJson {
                bytes: doc.to_string().into_bytes(),
            },
            &NormalizeOptions::default(),
        )
        .unwrap_err();
        assert!(matches!(err, IngestError::EmptyDataset(_)));
    }

    #[test]
    fn null_island_warns() {
        let doc = json!({
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                "properties": {}
            }]
        });
        let ds = normalize_source(
            SourceFile::GeoJson {
                bytes: doc.to_string().into_bytes(),
            },
            &NormalizeOptions::default(),
        )
        .unwrap();
        assert!(ds.warnings.iter().any(|w| w.contains("null island")));
    }

    #[test]
    fn normalized_geojson_roundtrip() {
        let doc = json!({
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]},
                "properties": {"parcelId": "X"}
            }]
        });
        let ds = normalize_source(
            SourceFile::GeoJson {
                bytes: doc.to_string().into_bytes(),
            },
            &NormalizeOptions::default(),
        )
        .unwrap();
        let out = write_normalized_geojson(&ds);
        assert_eq!(out["type"], "FeatureCollection");
        assert_eq!(out["features"][0]["geometry"]["type"], "Polygon");
    }

    #[test]
    fn payload_too_large_rejected() {
        let mut opts = NormalizeOptions::default();
        opts.max_upload_bytes = 10;
        let err = normalize_source(
            SourceFile::GeoJson {
                bytes: vec![0u8; 100],
            },
            &opts,
        )
        .unwrap_err();
        assert!(matches!(err, IngestError::PayloadTooLarge { .. }));
    }
}
