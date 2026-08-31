//! GeoJSON ingestion (TRD §3: `.geojson`, `.json`; must be UTF-8).
//!
//! Per RFC 7946 a GeoJSON CRS is always EPSG:4326 with lon/lat axis order, so
//! no reprojection is needed for this format — only validation.

use geo_types::{
    Coord, Geometry, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon,
};
use geojson::{Feature, FeatureCollection, Value as GeoJsonValue};
use tracing::debug;

use crate::error::{IngestError, IngestResult};

/// A feature parsed from GeoJSON with raw JSON properties preserved.
#[derive(Debug)]
pub struct ParsedFeature {
    pub id: Option<u64>,
    pub geometry: Geometry<f64>,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

/// Parses a GeoJSON document into features.
///
/// Accepts a `FeatureCollection`, a single `Feature`, or a bare geometry
/// (clients sometimes upload the latter two; TRD §17 "mixed geometry types"
/// is handled naturally because each feature is converted independently).
pub fn parse_geojson(bytes: &[u8]) -> IngestResult<Vec<ParsedFeature>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| IngestError::InvalidGeoJson(format!("not UTF-8: {e}")))?;

    // Fast path for the common case: FeatureCollection. Fall back to a
    // single Feature, then to a bare Geometry (clients upload all three).
    let collection: FeatureCollection = if let Ok(fc) = text.parse::<FeatureCollection>() {
        fc
    } else if let Ok(feature) = text.parse::<Feature>() {
        FeatureCollection {
            features: vec![feature],
            bbox: None,
            foreign_members: None,
        }
    } else if let Ok(geometry) = text.parse::<geojson::Geometry>() {
        let feature = Feature {
            bbox: None,
            geometry: Some(geometry),
            id: None,
            properties: None,
            foreign_members: None,
        };
        FeatureCollection {
            features: vec![feature],
            bbox: None,
            foreign_members: None,
        }
    } else {
        return Err(IngestError::InvalidGeoJson(
            "document is not a FeatureCollection, Feature, or Geometry".into(),
        ));
    };

    let mut out = Vec::with_capacity(collection.features.len());
    let mut skipped = 0u64;
    for feature in collection.features {
        match convert_feature(&feature) {
            Ok(Some(f)) => out.push(f),
            Ok(None) => skipped += 1, // null geometry
            Err(e) => return Err(e),
        }
    }
    debug!(
        parsed = out.len(),
        skipped_null = skipped,
        "geojson parsed"
    );
    Ok(out)
}

fn convert_feature(feature: &Feature) -> IngestResult<Option<ParsedFeature>> {
    let Some(geojson_geom) = &feature.geometry else {
        return Ok(None);
    };
    let geometry = convert_geometry_value(&geojson_geom.value)?;
    let properties = feature.properties.clone().unwrap_or_default();
    let id = feature.id.as_ref().and_then(|id| match id {
        geojson::feature::Id::Number(n) => n.as_u64(),
        geojson::feature::Id::String(_) => None,
    });
    Ok(Some(ParsedFeature {
        id,
        geometry,
        properties,
    }))
}

/// Converts `geojson::Value` positions into `geo_types::Geometry`.
///
/// Done manually (rather than via the crate's `TryFrom` impls) so coordinate
/// validation and error messages are fully under pipeline control.
fn convert_geometry_value(value: &GeoJsonValue) -> IngestResult<Geometry<f64>> {
    match value {
        GeoJsonValue::Point(pos) => Ok(Geometry::Point(position_to_point(pos)?)),
        GeoJsonValue::MultiPoint(positions) => {
            let pts: IngestResult<Vec<Point>> =
                positions.iter().map(position_to_point).collect();
            Ok(Geometry::MultiPoint(MultiPoint(pts?)))
        }
        GeoJsonValue::LineString(positions) => {
            Ok(Geometry::LineString(positions_to_line(positions)?))
        }
        GeoJsonValue::MultiLineString(lines) => {
            let parts: IngestResult<Vec<LineString>> =
                lines.iter().map(|l| positions_to_line(l)).collect();
            Ok(Geometry::MultiLineString(MultiLineString(parts?)))
        }
        GeoJsonValue::Polygon(rings) => Ok(Geometry::Polygon(rings_to_polygon(rings)?)),
        GeoJsonValue::MultiPolygon(polys) => {
            let parts: IngestResult<Vec<Polygon>> =
                polys.iter().map(|rings| rings_to_polygon(rings)).collect();
            Ok(Geometry::MultiPolygon(MultiPolygon(parts?)))
        }
        GeoJsonValue::GeometryCollection(geoms) => {
            // Flatten: convert each member and, for MVP, keep only the first
            // non-geometry-collection member per feature is not possible;
            // instead reject with a clear error (TRD §17).
            let _ = geoms;
            Err(IngestError::InvalidGeoJson(
                "GeometryCollection is not supported by the MVP tiler; flatten upstream".into(),
            ))
        }
    }
}

fn position_to_point(pos: &Vec<f64>) -> IngestResult<Point> {
    if pos.len() < 2 {
        return Err(IngestError::InvalidGeoJson(
            "position must have at least 2 coordinates".into(),
        ));
    }
    validate_lonlat(pos[0], pos[1])?;
    Ok(Point::new(pos[0], pos[1]))
}

fn positions_to_line(positions: &[Vec<f64>]) -> IngestResult<LineString> {
    let coords: IngestResult<Vec<Coord>> = positions
        .iter()
        .map(|p| {
            if p.len() < 2 {
                return Err(IngestError::InvalidGeoJson("short position".into()));
            }
            validate_lonlat(p[0], p[1])?;
            Ok(Coord { x: p[0], y: p[1] })
        })
        .collect();
    Ok(LineString::new(coords?))
}

fn rings_to_polygon(rings: &[Vec<Vec<f64>>]) -> IngestResult<Polygon> {
    if rings.is_empty() {
        return Err(IngestError::InvalidGeoJson("polygon has no rings".into()));
    }
    let mut iter = rings.iter();
    let exterior = positions_to_line(iter.next().expect("non-empty checked above"))?;
    let interiors: IngestResult<Vec<LineString>> = iter.map(positions_to_line).collect();
    Ok(Polygon::new(exterior, interiors?))
}

/// RFC 7946 coordinate range validation (TRD §17 "features outside expected
/// bounding box").
fn validate_lonlat(lon: f64, lat: f64) -> IngestResult<()> {
    if !lon.is_finite() || !lat.is_finite() {
        return Err(IngestError::InvalidGeoJson(format!(
            "non-finite coordinate ({lon}, {lat})"
        )));
    }
    if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
        return Err(IngestError::InvalidGeoJson(format!(
            "coordinate ({lon}, {lat}) outside WGS84 range"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_feature_collection() {
        let doc = r#"{
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "id": 7,
                 "geometry": {"type": "Point", "coordinates": [-73.98, 40.75]},
                 "properties": {"parcelId": "NYC-1"}},
                {"type": "Feature",
                 "geometry": null,
                 "properties": {}}
            ]
        }"#;
        let feats = parse_geojson(doc.as_bytes()).unwrap();
        assert_eq!(feats.len(), 1);
        assert_eq!(feats[0].id, Some(7));
    }

    #[test]
    fn rejects_out_of_range_coordinates() {
        let doc = r#"{"type": "Point", "coordinates": [200.0, 0.0]}"#;
        assert!(parse_geojson(doc.as_bytes()).is_err());
    }

    #[test]
    fn parses_polygon_with_hole() {
        let doc = r#"{
            "type": "Polygon",
            "coordinates": [
                [[0,0],[10,0],[10,10],[0,10],[0,0]],
                [[2,2],[3,2],[3,3],[2,3],[2,2]]
            ]
        }"#;
        let feats = parse_geojson(doc.as_bytes()).unwrap();
        assert_eq!(feats.len(), 1);
        match &feats[0].geometry {
            Geometry::Polygon(p) => assert_eq!(p.interiors().len(), 1),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn rejects_geometry_collections() {
        let doc = r#"{"type": "GeometryCollection", "geometries": []}"#;
        assert!(parse_geojson(doc.as_bytes()).is_err());
    }
}
