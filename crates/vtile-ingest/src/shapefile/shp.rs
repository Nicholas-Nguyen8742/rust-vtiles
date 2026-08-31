//! Minimal `.shp` binary parser for the MVP geometry types.
//!
//! Reference: ESRI Shapefile Technical Description (July 1998). Only what the
//! pipeline needs is implemented: 2D Point, MultiPoint, PolyLine and Polygon
//! (plus their Z/M variants, whose extra ordinates are read past and
//! discarded). `.shx` is not required because the file is scanned
//! sequentially; its presence is validated by the bundle check (TRD §3).

use geo_types::{
    Coord, Geometry, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon,
};

use crate::error::{IngestError, IngestResult};

/// Shape type codes (little-endian i32 at the start of each record content).
const NULL_SHAPE: i32 = 0;
const POINT: i32 = 1;
const POLYLINE: i32 = 3;
const POLYGON: i32 = 5;
const MULTIPOINT: i32 = 8;
const POINT_Z: i32 = 11;
const POLYLINE_Z: i32 = 13;
const POLYGON_Z: i32 = 15;
const MULTIPOINT_Z: i32 = 18;
const POINT_M: i32 = 21;
const POLYLINE_M: i32 = 23;
const POLYGON_M: i32 = 25;
const MULTIPOINT_M: i32 = 28;

/// A parsed shapefile record geometry (coordinates still in the source CRS).
#[derive(Debug, Clone)]
pub enum ShpGeometry {
    Point(f64, f64),
    MultiPoint(Vec<Coord<f64>>),
    Polyline { parts: Vec<Vec<Coord<f64>>> },
    Polygon { parts: Vec<Vec<Coord<f64>>> },
}

/// Big/little-endian cursor over a byte buffer.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn take(&mut self, n: usize) -> IngestResult<&'a [u8]> {
        if self.remaining() < n {
            return Err(IngestError::InvalidShapefile(format!(
                "unexpected EOF at offset {}",
                self.pos
            )));
        }
        let out = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn be_i32(&mut self) -> IngestResult<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn le_i32(&mut self) -> IngestResult<i32> {
        let b = self.take(4)?;
        Ok(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn le_f64(&mut self) -> IngestResult<f64> {
        let b = self.take(8)?;
        Ok(f64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn skip(&mut self, n: usize) -> IngestResult<()> {
        self.take(n)?;
        Ok(())
    }
}

/// Parses an entire `.shp` file into geometries.
pub fn parse_shp(data: &[u8]) -> IngestResult<Vec<ShpGeometry>> {
    if data.len() < 100 {
        return Err(IngestError::InvalidShapefile(
            "file smaller than the 100-byte header".into(),
        ));
    }
    let mut header = Reader::new(&data[..100]);
    let file_code = header.be_i32()?;
    if file_code != 9994 {
        return Err(IngestError::InvalidShapefile(format!(
            "bad magic number {file_code} (expected 9994)"
        )));
    }
    header.skip(20)?; // unused
    let file_length_words = header.be_i32()?;
    header.skip(4)?; // version
    let header_shape_type = header.le_i32()?;
    header.skip(64)?; // bbox + z/m ranges
    let _ = header_shape_type; // record types are checked individually

    let declared_len = (file_length_words as usize) * 2;
    let body_end = declared_len.min(data.len());
    let mut r = Reader::new(&data[100..body_end]);

    let mut shapes = Vec::new();
    while r.remaining() >= 8 {
        let _record_number = r.be_i32()?;
        let content_length_words = r.be_i32()?;
        let content_bytes = (content_length_words as usize) * 2;
        if r.remaining() < content_bytes {
            return Err(IngestError::InvalidShapefile(
                "record length exceeds remaining file".into(),
            ));
        }
        let content = r.take(content_bytes)?;
        if let Some(geom) = parse_record_content(content)? {
            shapes.push(geom);
        }
    }
    Ok(shapes)
}

fn parse_record_content(content: &[u8]) -> IngestResult<Option<ShpGeometry>> {
    let mut r = Reader::new(content);
    let shape_type = r.le_i32()?;
    match shape_type {
        NULL_SHAPE => Ok(None),
        POINT | POINT_Z | POINT_M => {
            let x = r.le_f64()?;
            let y = r.le_f64()?;
            Ok(Some(ShpGeometry::Point(x, y)))
        }
        MULTIPOINT | MULTIPOINT_Z | MULTIPOINT_M => {
            r.skip(32)?; // bbox
            let n = r.le_i32()?;
            if n < 0 || n > 10_000_000 {
                return Err(IngestError::InvalidShapefile(format!(
                    "implausible multipoint count {n}"
                )));
            }
            let mut pts = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let x = r.le_f64()?;
                let y = r.le_f64()?;
                pts.push(Coord { x, y });
            }
            Ok(Some(ShpGeometry::MultiPoint(pts)))
        }
        POLYLINE | POLYLINE_Z | POLYLINE_M => {
            Ok(Some(ShpGeometry::Polyline { parts: read_parts(&mut r)? }))
        }
        POLYGON | POLYGON_Z | POLYGON_M => {
            Ok(Some(ShpGeometry::Polygon { parts: read_parts(&mut r)? }))
        }
        other => Err(IngestError::InvalidShapefile(format!(
            "unsupported shape type {other} (MVP supports point/multipoint/polyline/polygon)"
        ))),
    }
}

/// Reads the shared PolyLine/Polygon record layout: bbox, parts, points.
fn read_parts(r: &mut Reader<'_>) -> IngestResult<Vec<Vec<Coord<f64>>>> {
    r.skip(32)?; // bbox
    let num_parts = r.le_i32()?;
    let num_points = r.le_i32()?;
    if num_parts < 0 || num_parts > 1_000_000 || num_points < 0 || num_points > 100_000_000 {
        return Err(IngestError::InvalidShapefile(
            "implausible part/point counts".into(),
        ));
    }
    let mut offsets = Vec::with_capacity(num_parts as usize);
    for _ in 0..num_parts {
        offsets.push(r.le_i32()?);
    }
    let mut points = Vec::with_capacity(num_points as usize);
    for _ in 0..num_points {
        let x = r.le_f64()?;
        let y = r.le_f64()?;
        points.push(Coord { x, y });
    }
    // Z/M ordinates (if any) follow; we deliberately stop reading here.

    let mut parts = Vec::with_capacity(offsets.len());
    for (i, &start) in offsets.iter().enumerate() {
        let end = if i + 1 < offsets.len() {
            offsets[i + 1] as usize
        } else {
            points.len()
        };
        let start = start as usize;
        if start > end || end > points.len() {
            return Err(IngestError::InvalidShapefile(
                "corrupt part offset table".into(),
            ));
        }
        parts.push(points[start..end].to_vec());
    }
    Ok(parts)
}

/// Twice the signed shoelace area of a ring in the source plane.
/// Shapefile convention: exterior rings are clockwise (negative area in a
/// y-up plane), interior rings counter-clockwise (positive area).
fn signed_area2(ring: &[Coord<f64>]) -> f64 {
    let n = ring.len();
    let mut area = 0.0;
    for i in 0..n {
        let a = ring[i];
        let b = ring[(i + 1) % n];
        area += a.x * b.y - b.x * a.y;
    }
    area
}

/// Converts a parsed `ShpGeometry` into a `geo_types::Geometry`.
///
/// Polygon rings are grouped by the shapefile orientation convention; the
/// final winding fix-up happens in the MVT encoder regardless, so grouping
/// mistakes degrade gracefully (a hole rendered as its own polygon).
pub fn shp_to_geometry(geom: ShpGeometry) -> Option<Geometry<f64>> {
    match geom {
        ShpGeometry::Point(x, y) => Some(Geometry::Point(Point::new(x, y))),
        ShpGeometry::MultiPoint(pts) => {
            let points: Vec<Point> = pts.into_iter().map(|c| Point::new(c.x, c.y)).collect();
            (!points.is_empty()).then_some(Geometry::MultiPoint(MultiPoint(points)))
        }
        ShpGeometry::Polyline { parts } => {
            let lines: Vec<LineString> = parts
                .into_iter()
                .filter(|p| p.len() >= 2)
                .map(LineString::new)
                .collect();
            (!lines.is_empty()).then_some(Geometry::MultiLineString(MultiLineString(lines)))
        }
        ShpGeometry::Polygon { parts } => group_polygon_parts(parts),
    }
}

fn group_polygon_parts(parts: Vec<Vec<Coord<f64>>>) -> Option<Geometry<f64>> {
    let mut polygons: Vec<(LineString<f64>, Vec<LineString<f64>>)> = Vec::new();

    for ring in parts {
        if ring.len() < 4 {
            continue;
        }
        let line = LineString::new(ring.clone());
        let area = signed_area2(&ring);
        if area.abs() <= f64::EPSILON {
            continue; // degenerate ring
        }
        if area < 0.0 {
            // Exterior ring (clockwise per shapefile spec).
            polygons.push((line, Vec::new()));
        } else if let Some((_, holes)) = polygons.last_mut() {
            holes.push(line);
        } else {
            // Hole before any exterior: promote to exterior (defensive).
            polygons.push((line, Vec::new()));
        }
    }

    let polys: Vec<Polygon<f64>> = polygons
        .into_iter()
        .map(|(ext, holes)| Polygon::new(ext, holes))
        .collect();
    match polys.len() {
        0 => None,
        1 => polys.into_iter().next().map(Geometry::Polygon),
        _ => Some(Geometry::MultiPolygon(MultiPolygon(polys))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_point_shp(x: f64, y: f64) -> Vec<u8> {
        let mut data = Vec::new();
        // 100-byte header
        data.extend_from_slice(&9994i32.to_be_bytes());
        data.extend_from_slice(&[0u8; 20]);
        data.extend_from_slice(&58i32.to_be_bytes()); // file length words (116 bytes)
        data.extend_from_slice(&1000i32.to_le_bytes());
        data.extend_from_slice(&1i32.to_le_bytes()); // point type
        data.extend_from_slice(&[0u8; 64]);
        // Record: header (10 bytes => 5 words) + content (20 bytes => 10 words)
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend_from_slice(&10i32.to_be_bytes());
        data.extend_from_slice(&1i32.to_le_bytes()); // shape type POINT
        data.extend_from_slice(&x.to_le_bytes());
        data.extend_from_slice(&y.to_le_bytes());
        data
    }

    #[test]
    fn parses_point_record() {
        let data = build_point_shp(-73.9855, 40.7580);
        let shapes = parse_shp(&data).unwrap();
        assert_eq!(shapes.len(), 1);
        match &shapes[0] {
            ShpGeometry::Point(x, y) => {
                assert_eq!(*x, -73.9855);
                assert_eq!(*y, 40.7580);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn rejects_bad_magic() {
        let mut data = build_point_shp(0.0, 0.0);
        data[0] = 0;
        assert!(parse_shp(&data).is_err());
    }

    #[test]
    fn groups_rings_by_orientation() {
        // Outer ring clockwise (negative area in y-up), hole CCW.
        let outer = vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 0.0, y: 10.0 },
            Coord { x: 10.0, y: 10.0 },
            Coord { x: 10.0, y: 0.0 },
            Coord { x: 0.0, y: 0.0 },
        ];
        assert!(signed_area2(&outer) < 0.0);
        let hole = vec![
            Coord { x: 2.0, y: 2.0 },
            Coord { x: 3.0, y: 2.0 },
            Coord { x: 3.0, y: 3.0 },
            Coord { x: 2.0, y: 3.0 },
            Coord { x: 2.0, y: 2.0 },
        ];
        assert!(signed_area2(&hole) > 0.0);
        let geom = group_polygon_parts(vec![outer, hole]).unwrap();
        match geom {
            Geometry::Polygon(p) => assert_eq!(p.interiors().len(), 1),
            other => panic!("unexpected: {other:?}"),
        }
    }
}
