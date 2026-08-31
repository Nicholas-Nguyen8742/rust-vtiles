//! Tile generation orchestration: feature preparation, per-zoom tile
//! assignment, MVT assembly, gzip compression, and size mitigation.
//!
//! Implements TRD §5 (tile size targets and mitigation order) and the TRD §4
//! precision rules (no simplification at zoom ≥ 14).

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use geo_types::{Geometry, Point};
use rayon::prelude::*;
use tracing::{debug, warn};

use crate::config::TileConfig;
use crate::error::{Result, TileError};
use crate::model::{Bbox, GeometryKind};
use crate::mvt::{encode_lines, encode_points, encode_rings, GeomKind, MvtLayer, MvtValue, Ring};
use crate::properties::{json_to_mvt_properties, AttrMode};
use crate::simplify::{epsilon_for_zoom, simplify_geometry};
use crate::sink::{TileObjectMeta, TileSink};
use crate::tilemath::{lonlat_to_world, tile_range_for_world_bbox, TileId, TileTransform, WorldPos};

/// Safety valve: a single feature assigned to more tiles than this at one
/// zoom is skipped at that zoom with a warning (prevents pathological
/// low-zoom explosions; see TRD §17 "tile size explosion").
const MAX_TILES_PER_FEATURE: u64 = 65_536;

/// Input feature before preparation: EPSG:4326 geometry + raw JSON properties.
#[derive(Debug, Clone)]
pub struct RawFeature {
    pub id: Option<u64>,
    pub geometry: Geometry<f64>,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

/// A feature prepared for tiling.
#[derive(Debug, Clone)]
pub struct PreparedFeature {
    pub id: Option<u64>,
    pub geometry: Geometry<f64>,
    /// Policy-filtered properties in MVT form (sorted by key).
    pub properties: Vec<(String, MvtValue)>,
    pub world_min: WorldPos,
    pub world_max: WorldPos,
    pub vertex_count: usize,
}

/// Dataset ready for tile generation.
#[derive(Debug, Default)]
pub struct PreparedDataset {
    pub features: Vec<PreparedFeature>,
    /// lon/lat bbox over all retained features.
    pub bbox: Option<Bbox>,
    pub skipped_empty: u64,
    pub geometry_kind: GeometryKind,
    pub warnings: Vec<String>,
}

impl PreparedDataset {
    pub fn feature_count(&self) -> u64 {
        self.features.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.features.is_empty()
    }
}

/// Converts raw features into a prepared dataset:
/// * drops empty geometries,
/// * computes per-feature world bboxes (Mercator),
/// * applies the property policy (PII stripping, payload caps).
pub fn prepare_features(raw: Vec<RawFeature>, config: &TileConfig) -> PreparedDataset {
    let mut dataset = PreparedDataset {
        geometry_kind: GeometryKind::Mixed,
        ..Default::default()
    };
    let mut kinds: HashMap<&'static str, u64> = HashMap::new();
    let mut bbox: Option<Bbox> = None;

    for feature in raw {
        let mut coord_count = 0usize;
        let mut lon_min = f64::INFINITY;
        let mut lon_max = f64::NEG_INFINITY;
        let mut lat_min = f64::INFINITY;
        let mut lat_max = f64::NEG_INFINITY;
        let mut invalid = false;

        {
            use geo_types::CoordsIter;
            for c in feature.geometry.coords_iter() {
                if !c.x.is_finite() || !c.y.is_finite() {
                    invalid = true;
                    break;
                }
                coord_count += 1;
                lon_min = lon_min.min(c.x);
                lon_max = lon_max.max(c.x);
                lat_min = lat_min.min(c.y);
                lat_max = lat_max.max(c.y);
            }
        }

        if invalid || coord_count == 0 {
            dataset.skipped_empty += 1;
            continue;
        }

        // World-space bbox (clamps latitude to the Mercator limit).
        let w_min = lonlat_to_world(lon_min, lat_max);
        let w_max = lonlat_to_world(lon_max, lat_min);

        let props = json_to_mvt_properties(&feature.properties, &config.property_policy, AttrMode::Full);

        let kind_name = match &feature.geometry {
            Geometry::Point(_) => "POINT",
            Geometry::MultiPoint(_) => "MULTIPOINT",
            Geometry::LineString(_) => "LINESTRING",
            Geometry::MultiLineString(_) => "MULTILINESTRING",
            Geometry::Polygon(_) => "POLYGON",
            Geometry::MultiPolygon(_) => "MULTIPOLYGON",
            _ => "OTHER",
        };
        *kinds.entry(kind_name).or_insert(0) += 1;

        bbox = Some(match bbox {
            Some(b) => b.union(&Bbox::new(lon_min, lat_min, lon_max, lat_max)),
            None => Bbox::new(lon_min, lat_min, lon_max, lat_max),
        });

        dataset.features.push(PreparedFeature {
            id: feature.id,
            geometry: feature.geometry,
            properties: props,
            world_min: w_min,
            world_max: w_max,
            vertex_count: coord_count,
        });
    }

    dataset.bbox = bbox;
    dataset.geometry_kind = match kinds.len() {
        0 => GeometryKind::Mixed,
        1 => match kinds.keys().next().copied() {
            Some("POINT") => GeometryKind::Point,
            Some("MULTIPOINT") => GeometryKind::MultiPoint,
            Some("LINESTRING") => GeometryKind::LineString,
            Some("MULTILINESTRING") => GeometryKind::MultiLineString,
            Some("POLYGON") => GeometryKind::Polygon,
            Some("MULTIPOLYGON") => GeometryKind::MultiPolygon,
            _ => GeometryKind::Mixed,
        },
        _ => GeometryKind::Mixed,
    };
    dataset
}

/// Per-zoom generation statistics.
#[derive(Debug, Default, Clone)]
pub struct ZoomStats {
    pub tiles: u64,
    pub gzip_bytes: u64,
    pub max_gzip_bytes: usize,
}

/// Aggregated statistics for a generation run (TRD §15 metrics feed).
#[derive(Debug, Default)]
pub struct TileStats {
    pub tiles_written: u64,
    pub empty_tiles_skipped: u64,
    pub total_gzip_bytes: u64,
    pub max_gzip_bytes: usize,
    pub largest_tile: Option<TileId>,
    /// Feature × tile instances encoded.
    pub feature_instances: u64,
    /// Tiles that needed mitigation beyond the default attribute set.
    pub mitigations_applied: u64,
    /// Features dropped entirely to satisfy the hard size cap.
    pub features_dropped: u64,
    pub per_zoom: BTreeMap<u8, ZoomStats>,
    pub elapsed_ms: u64,
}

/// Generates all tiles for the dataset and streams them into `sink`.
pub fn generate_tiles<S: TileSink>(
    dataset: &PreparedDataset,
    config: &TileConfig,
    meta: &TileObjectMeta,
    sink: &S,
) -> Result<TileStats> {
    config
        .validate()
        .map_err(TileError::Config)?;
    if dataset.is_empty() {
        return Err(TileError::Geometry("dataset has no features".into()));
    }

    let started = Instant::now();
    let mut stats = TileStats::default();

    for z in config.zoom_range.iter() {
        let zoom_started = Instant::now();
        // 1. Assign features to tiles via buffered bbox intersection.
        let mut by_tile: HashMap<TileId, Vec<usize>> = HashMap::new();
        let mut skipped_explosive = 0u64;
        for (fi, f) in dataset.features.iter().enumerate() {
            let Some((x0, x1, y0, y1)) =
                tile_range_for_world_bbox(&f.world_min, &f.world_max, z, config.buffer, config.extent)
            else {
                continue;
            };
            let count = (x1 - x0 + 1) as u64 * (y1 - y0 + 1) as u64;
            if count > MAX_TILES_PER_FEATURE {
                skipped_explosive += 1;
                continue;
            }
            for x in x0..=x1 {
                for y in y0..=y1 {
                    by_tile.entry(TileId::new(z, x, y)).or_default().push(fi);
                }
            }
        }
        if skipped_explosive > 0 {
            warn!(
                zoom = z,
                skipped = skipped_explosive,
                "features skipped at zoom (exceeded tile fan-out limit)"
            );
        }

        // 2. Build tiles (optionally in parallel) and stream into the sink.
        let entries: Vec<(TileId, Vec<usize>)> = by_tile.into_iter().collect();
        let zoom_stats = if config.parallel {
            build_tiles_parallel(dataset, config, meta, sink, &entries, &mut stats)?
        } else {
            build_tiles_sequential(dataset, config, meta, sink, &entries, &mut stats)?
        };

        stats.per_zoom.insert(
            z,
            ZoomStats {
                tiles: zoom_stats.tiles_written,
                gzip_bytes: zoom_stats.gzip_bytes,
                max_gzip_bytes: zoom_stats.max_gzip_bytes,
            },
        );

        debug!(
            zoom = z,
            tiles = zoom_stats.tiles_written,
            bytes = zoom_stats.gzip_bytes,
            ms = zoom_started.elapsed().as_millis() as u64,
            "zoom completed"
        );
    }

    sink.finish().map_err(|e| TileError::Sink(e.to_string()))?;
    stats.elapsed_ms = started.elapsed().as_millis() as u64;
    Ok(stats)
}

#[derive(Debug, Default)]
struct ZoomAccum {
    tiles_written: u64,
    empty: u64,
    gzip_bytes: u64,
    max_gzip_bytes: usize,
    largest: Option<(TileId, usize)>,
    feature_instances: u64,
    mitigations: u64,
    dropped: u64,
    first_error: Option<TileError>,
}

struct ZoomOutcome {
    tiles_written: u64,
    gzip_bytes: u64,
    max_gzip_bytes: usize,
}

fn merge_accum(mut a: ZoomAccum, b: ZoomAccum) -> ZoomAccum {
    a.tiles_written += b.tiles_written;
    a.empty += b.empty;
    a.gzip_bytes += b.gzip_bytes;
    if b.max_gzip_bytes > a.max_gzip_bytes {
        a.max_gzip_bytes = b.max_gzip_bytes;
        a.largest = b.largest;
    } else if a.largest.is_none() {
        a.largest = b.largest;
    }
    a.feature_instances += b.feature_instances;
    a.mitigations += b.mitigations;
    a.dropped += b.dropped;
    if a.first_error.is_none() {
        a.first_error = b.first_error;
    }
    a
}

fn finish_accum(accum: ZoomAccum, stats: &mut TileStats) -> Result<ZoomOutcome> {
    if let Some(err) = accum.first_error {
        return Err(err);
    }
    stats.tiles_written += accum.tiles_written;
    stats.empty_tiles_skipped += accum.empty;
    stats.total_gzip_bytes += accum.gzip_bytes;
    if accum.max_gzip_bytes > stats.max_gzip_bytes {
        stats.max_gzip_bytes = accum.max_gzip_bytes;
        stats.largest_tile = accum.largest.map(|(t, _)| t);
    }
    stats.feature_instances += accum.feature_instances;
    stats.mitigations_applied += accum.mitigations;
    stats.features_dropped += accum.dropped;
    Ok(ZoomOutcome {
        tiles_written: accum.tiles_written,
        gzip_bytes: accum.gzip_bytes,
        max_gzip_bytes: accum.max_gzip_bytes,
    })
}

fn build_tiles_parallel<S: TileSink>(
    dataset: &PreparedDataset,
    config: &TileConfig,
    meta: &TileObjectMeta,
    sink: &S,
    entries: &[(TileId, Vec<usize>)],
    stats: &mut TileStats,
) -> Result<ZoomOutcome> {
    let accum = entries
        .par_iter()
        .fold(ZoomAccum::default, |mut acc, (tile, idxs)| {
            match build_tile(dataset, config, *tile, idxs) {
                Ok(Some((gz, mitigated, dropped, instances))) => {
                    if let Err(e) = sink.write_tile(tile, &gz, meta) {
                        if acc.first_error.is_none() {
                            acc.first_error = Some(e);
                        }
                        return acc;
                    }
                    acc.tiles_written += 1;
                    acc.gzip_bytes += gz.len() as u64;
                    if gz.len() > acc.max_gzip_bytes {
                        acc.max_gzip_bytes = gz.len();
                        acc.largest = Some((*tile, gz.len()));
                    }
                    acc.feature_instances += instances;
                    if mitigated {
                        acc.mitigations += 1;
                    }
                    acc.dropped += dropped;
                }
                Ok(None) => acc.empty += 1,
                Err(e) => {
                    if acc.first_error.is_none() {
                        acc.first_error = Some(e);
                    }
                }
            }
            acc
        })
        .reduce(ZoomAccum::default, merge_accum);
    finish_accum(accum, stats)
}

fn build_tiles_sequential<S: TileSink>(
    dataset: &PreparedDataset,
    config: &TileConfig,
    meta: &TileObjectMeta,
    sink: &S,
    entries: &[(TileId, Vec<usize>)],
    stats: &mut TileStats,
) -> Result<ZoomOutcome> {
    let mut acc = ZoomAccum::default();
    for (tile, idxs) in entries {
        match build_tile(dataset, config, *tile, idxs)? {
            Some((gz, mitigated, dropped, instances)) => {
                sink.write_tile(tile, &gz, meta)?;
                acc.tiles_written += 1;
                acc.gzip_bytes += gz.len() as u64;
                if gz.len() > acc.max_gzip_bytes {
                    acc.max_gzip_bytes = gz.len();
                    acc.largest = Some((*tile, gz.len()));
                }
                acc.feature_instances += instances;
                if mitigated {
                    acc.mitigations += 1;
                }
                acc.dropped += dropped;
            }
            None => acc.empty += 1,
        }
    }
    finish_accum(acc, stats)
}

/// Builds one tile with TRD §5 size mitigation.
///
/// Returns `Ok(None)` for empty tiles (TRD §8.5: served as `204 No Content`),
/// otherwise the gzipped MVT bytes plus mitigation bookkeeping:
/// `(bytes, was_mitigated, features_dropped, feature_instances)`.
fn build_tile(
    dataset: &PreparedDataset,
    config: &TileConfig,
    tile: TileId,
    feature_idxs: &[usize],
) -> Result<Option<(Vec<u8>, bool, u64, u64)>> {
    let transform = TileTransform::new(tile, config.extent, config.buffer);
    let base_eps = epsilon_for_zoom(tile.z, config.simplify_below_zoom, config.extent);

    // Mitigation ladder (TRD §5 order): attributes → geometry (low zooms
    // only) → feature drops.
    let mut mode = AttrMode::Full;
    let mut extra_simplify = false;
    let mut dropped: u64 = 0;
    let mut active: Vec<usize> = feature_idxs.to_vec();

    loop {
        let eps = if extra_simplify { base_eps * 4.0 } else { base_eps };
        let (layer, instances) =
            encode_tile_layer(dataset, config, &transform, &active, eps, mode);

        if layer.is_empty() {
            return Ok(None);
        }

        let raw = layer.to_tile_bytes();
        let gz = gzip(&raw, config.gzip_level)?;

        if gz.len() <= config.hard_max_tile_bytes {
            let mitigated = mode != AttrMode::Full || extra_simplify || dropped > 0;
            if mitigated {
                warn!(
                    z = tile.z,
                    x = tile.x,
                    y = tile.y,
                    gzip = gz.len(),
                    ?mode,
                    extra_simplify,
                    dropped,
                    "tile required size mitigation"
                );
            }
            return Ok(Some((gz, mitigated, dropped, instances)));
        }

        // Escalate.
        match mode {
            AttrMode::Full => mode = AttrMode::Core,
            AttrMode::Core => mode = AttrMode::None,
            AttrMode::None => {
                if !extra_simplify && tile.z < config.simplify_below_zoom {
                    // TRD §4: extra simplification only at low zooms.
                    extra_simplify = true;
                } else if active.len() > 1 {
                    // Drop the heaviest features until under the hard cap.
                    active.sort_by(|a, b| {
                        dataset.features[*b]
                            .vertex_count
                            .cmp(&dataset.features[*a].vertex_count)
                    });
                    let cut = (active.len() / 2).max(1);
                    dropped += cut as u64;
                    active.truncate(active.len() - cut);
                } else {
                    // A single feature still exceeds the cap: publish it and
                    // flag via stats (TRD §17 "tile size explosion").
                    warn!(
                        z = tile.z,
                        x = tile.x,
                        y = tile.y,
                        gzip = gz.len(),
                        "single-feature tile exceeds hard size cap; publishing anyway"
                    );
                    let mitigated = true;
                    return Ok(Some((gz, mitigated, dropped, instances)));
                }
            }
        }
    }
}

/// Encodes the active features of one tile into an MVT layer.
fn encode_tile_layer(
    dataset: &PreparedDataset,
    config: &TileConfig,
    transform: &TileTransform,
    active: &[usize],
    epsilon: f64,
    mode: AttrMode,
) -> (MvtLayer, u64) {
    let mut layer = MvtLayer::new(config.layer_name.clone(), config.extent);
    let mut instances = 0u64;

    for &fi in active {
        let f = &dataset.features[fi];
        let geom = match simplify_geometry(&f.geometry, epsilon) {
            Some(g) => g,
            None => continue,
        };
        let Some((kind, commands)) = encode_geometry(&geom, transform) else {
            continue;
        };
        let props = match mode {
            AttrMode::Full => f.properties.clone(),
            AttrMode::Core => f
                .properties
                .iter()
                .filter(|(k, _)| config.property_policy.is_core(k))
                .cloned()
                .collect(),
            AttrMode::None => Vec::new(),
        };
        layer.add_feature(f.id, kind, commands, &props);
        instances += 1;
    }
    (layer, instances)
}

/// Converts a geo geometry into MVT command integers for a target tile.
/// Returns `None` when nothing survives the transform (e.g. a point outside
/// the buffered tile).
pub fn encode_geometry(geom: &Geometry<f64>, transform: &TileTransform) -> Option<(GeomKind, Vec<u32>)> {
    match geom {
        Geometry::Point(p) => {
            let world = lonlat_to_world(p.x(), p.y());
            if !transform.contains_buffered(&world) {
                return None;
            }
            let (x, y) = transform.to_tile_xy(&world);
            Some((GeomKind::Point, encode_points(&[(x, y)])))
        }
        Geometry::MultiPoint(mp) => {
            let mut pts = Vec::with_capacity(mp.0.len());
            for Point(coord) in &mp.0 {
                let world = lonlat_to_world(coord.x, coord.y);
                if transform.contains_buffered(&world) {
                    pts.push(transform.to_tile_xy(&world));
                }
            }
            (!pts.is_empty()).then_some((GeomKind::Point, encode_points(&pts)))
        }
        Geometry::LineString(ls) => {
            let pts = transform_line(&ls.0, transform);
            (pts.len() >= 2).then_some((GeomKind::LineString, encode_lines(&[pts])))
        }
        Geometry::MultiLineString(mls) => {
            let lines: Vec<Vec<(i32, i32)>> = mls
                .0
                .iter()
                .map(|ls| transform_line(&ls.0, transform))
                .filter(|pts| pts.len() >= 2)
                .collect();
            (!lines.is_empty()).then_some((GeomKind::LineString, encode_lines(&lines)))
        }
        Geometry::Polygon(p) => {
            let rings = transform_polygon_rings(p.exterior(), p.interiors(), transform);
            (!rings.is_empty()).then_some((GeomKind::Polygon, encode_rings(&rings)))
        }
        Geometry::MultiPolygon(mp) => {
            let mut rings = Vec::new();
            for poly in &mp.0 {
                rings.extend(transform_polygon_rings(
                    poly.exterior(),
                    poly.interiors(),
                    transform,
                ));
            }
            (!rings.is_empty()).then_some((GeomKind::Polygon, encode_rings(&rings)))
        }
        Geometry::Rect(r) => {
            let poly = r.to_polygon();
            let rings = transform_polygon_rings(poly.exterior(), poly.interiors(), transform);
            (!rings.is_empty()).then_some((GeomKind::Polygon, encode_rings(&rings)))
        }
        Geometry::Triangle(t) => {
            let poly = t.to_polygon();
            let rings = transform_polygon_rings(poly.exterior(), poly.interiors(), transform);
            (!rings.is_empty()).then_some((GeomKind::Polygon, encode_rings(&rings)))
        }
        _ => None,
    }
}

/// Transforms a line's coordinates into tile space, dropping consecutive
/// duplicates introduced by rounding.
fn transform_line(coords: &[geo_types::Coord<f64>], transform: &TileTransform) -> Vec<(i32, i32)> {
    let mut pts: Vec<(i32, i32)> = coords
        .iter()
        .map(|c| transform.to_tile_xy(&lonlat_to_world(c.x, c.y)))
        .collect();
    pts.dedup();
    pts
}

/// Transforms polygon rings into tile space. The repeated closing vertex is
/// removed (MVT uses an explicit ClosePath command instead).
fn transform_polygon_rings(
    exterior: &geo_types::LineString<f64>,
    interiors: &[geo_types::LineString<f64>],
    transform: &TileTransform,
) -> Vec<Ring> {
    let mut rings = Vec::new();
    if let Some(pts) = transform_ring(&exterior.0, transform) {
        rings.push(Ring {
            points: pts,
            exterior: true,
        });
        for hole in interiors {
            if let Some(pts) = transform_ring(&hole.0, transform) {
                rings.push(Ring {
                    points: pts,
                    exterior: false,
                });
            }
        }
    }
    rings
}

fn transform_ring(coords: &[geo_types::Coord<f64>], transform: &TileTransform) -> Option<Vec<(i32, i32)>> {
    let mut pts: Vec<(i32, i32)> = coords
        .iter()
        .map(|c| transform.to_tile_xy(&lonlat_to_world(c.x, c.y)))
        .collect();
    pts.dedup();
    // Drop the repeated closing vertex; MVT ClosePath replaces it.
    if pts.len() > 1 && pts.first() == pts.last() {
        pts.pop();
    }
    (pts.len() >= 3).then_some(pts)
}

/// Gzip-compresses raw tile bytes (TRD §5: "gzip-compressed PBF").
pub fn gzip(data: &[u8], level: u32) -> Result<Vec<u8>> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::with_capacity(data.len() / 2), Compression::new(level));
    encoder.write_all(data)?;
    encoder
        .finish()
        .map_err(|e| TileError::Compression(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PropertyPolicy;
    use crate::model::ZoomRange;
    use crate::sink::MemoryTileSink;
    use geo_types::{coord, LineString, Polygon};
    use serde_json::json;

    fn nyc_square(size: f64) -> Geometry<f64> {
        let (lon, lat) = (-73.9855, 40.7580);
        Geometry::Polygon(Polygon::new(
            LineString::new(vec![
                coord! { x: lon, y: lat },
                coord! { x: lon + size, y: lat },
                coord! { x: lon + size, y: lat + size },
                coord! { x: lon, y: lat + size },
                coord! { x: lon, y: lat },
            ]),
            vec![],
        ))
    }

    fn config(max_zoom: u8) -> TileConfig {
        TileConfig {
            layer_name: "parcel_boundary".into(),
            zoom_range: ZoomRange::new(0, max_zoom),
            parallel: false,
            ..Default::default()
        }
    }

    fn dataset_with(n: usize, size: f64) -> PreparedDataset {
        let raw: Vec<RawFeature> = (0..n)
            .map(|i| RawFeature {
                id: Some(i as u64),
                geometry: nyc_square(size),
                properties: serde_json::from_value(json!({
                    "parcelId": format!("NYC-{i}"),
                    "ownerName": "SECRET",
                }))
                .unwrap(),
            })
            .collect();
        prepare_features(raw, &config(16))
    }

    #[test]
    fn prepare_strips_pii_and_computes_bbox() {
        let ds = dataset_with(2, 0.001);
        assert_eq!(ds.feature_count(), 2);
        let bbox = ds.bbox.unwrap();
        assert!(bbox.min_lon <= -73.9855 && bbox.max_lon >= -73.9855);
        assert_eq!(ds.geometry_kind, GeometryKind::Polygon);
        // PII removed:
        for f in &ds.features {
            assert!(f.properties.iter().all(|(k, _)| k != "ownerName"));
            assert!(f.properties.iter().any(|(k, _)| k == "parcelId"));
        }
    }

    #[test]
    fn generates_expected_tiles_for_small_polygon() {
        let ds = dataset_with(1, 0.001);
        let cfg = config(2);
        let sink = MemoryTileSink::new();
        let meta = TileObjectMeta::default();
        let stats = generate_tiles(&ds, &cfg, &meta, &sink).unwrap();
        assert!(stats.tiles_written > 0);
        // The polygon is tiny; at every zoom it lands in a small tile cluster.
        assert_eq!(sink.len(), stats.tiles_written as usize);
        // Tiles must decode and carry our layer name.
        let any_tile = sink.tiles.lock().unwrap().values().next().cloned().unwrap();
        let decoded = crate::mvt::decode::decode_gzipped_tile(&any_tile).unwrap();
        assert_eq!(decoded.layers[0].name, "parcel_boundary");
        assert_eq!(decoded.layers[0].extent, 4096);
        assert_eq!(decoded.layers[0].version, 2);
    }

    #[test]
    fn empty_dataset_errors() {
        let ds = PreparedDataset::default();
        let cfg = config(2);
        let sink = MemoryTileSink::new();
        let meta = TileObjectMeta::default();
        assert!(generate_tiles(&ds, &cfg, &meta, &sink).is_err());
    }

    #[test]
    fn high_zoom_tiles_skip_simplification() {
        // At z >= 14 the epsilon must be zero (TRD §4).
        assert_eq!(epsilon_for_zoom(16, 14, 4096), 0.0);
    }

    #[test]
    fn gzip_roundtrip() {
        let data = b"hello vector tiles";
        let gz = gzip(data, 6).unwrap();
        let decoded = crate::mvt::decode::DecodedTile::default();
        let _ = decoded;
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut out = Vec::new();
        GzDecoder::new(&gz[..]).read_to_end(&mut out).unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn property_policy_applies_during_preparation() {
        let policy = PropertyPolicy::default();
        assert!(policy.is_denied("ownerName"));
    }
}
