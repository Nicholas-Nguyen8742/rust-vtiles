//! Geometry repair — the MVP equivalent of `ST_MakeValid` (TRD §4 rule 3).
//!
//! Full polygon validation (self-intersection repair) requires a computational
//! geometry kernel; the MVP performs the structural repairs that cover the
//! observed source defects (TRD §17):
//! * unclosed rings → closed,
//! * repeated consecutive vertices → deduplicated,
//! * degenerate rings/parts (fewer than 3 distinct points) → dropped,
//! * polygons whose exterior ring is unrecoverable → feature rejected.
//!
//! A `geos`-backed `MakeValid` can be added behind a feature flag post-MVP.

use geo_types::{Geometry, LineString, MultiLineString, MultiPolygon, Polygon};

/// Attempts to repair a geometry. Returns `None` when unrecoverable —
/// callers count these and reject the file if the dataset is dominated by
/// them (TRD §4: "Reject files with unrecoverable geometry errors").
pub fn repair_geometry(geom: Geometry<f64>) -> Option<Geometry<f64>> {
    match geom {
        Geometry::Point(p) => {
            (p.x().is_finite() && p.y().is_finite()).then_some(Geometry::Point(p))
        }
        Geometry::MultiPoint(mp) => {
            let pts: Vec<_> = mp
                .0
                .into_iter()
                .filter(|p| p.x().is_finite() && p.y().is_finite())
                .collect();
            (!pts.is_empty()).then_some(Geometry::MultiPoint(geo_types::MultiPoint(pts)))
        }
        Geometry::LineString(ls) => repair_line(ls).map(Geometry::LineString),
        Geometry::MultiLineString(mls) => {
            let lines: Vec<LineString<f64>> =
                mls.0.into_iter().filter_map(repair_line).collect();
            (!lines.is_empty()).then_some(Geometry::MultiLineString(MultiLineString(lines)))
        }
        Geometry::Polygon(p) => repair_polygon(p).map(Geometry::Polygon),
        Geometry::MultiPolygon(mp) => {
            let polys: Vec<Polygon<f64>> = mp.0.into_iter().filter_map(repair_polygon).collect();
            (!polys.is_empty()).then_some(Geometry::MultiPolygon(MultiPolygon(polys)))
        }
        Geometry::Rect(r) => Some(Geometry::Polygon(r.to_polygon())),
        Geometry::Triangle(t) => Some(Geometry::Polygon(t.to_polygon())),
        // GeometryCollection and unknown variants are rejected by the readers
        // already; treat defensively as unrecoverable here.
        _ => None,
    }
}

/// Deduplicates consecutive vertices and requires at least two distinct ones.
fn repair_line(ls: LineString<f64>) -> Option<LineString<f64>> {
    let mut coords = ls.0;
    coords.dedup();
    coords.retain(|c| c.x.is_finite() && c.y.is_finite());
    (coords.len() >= 2).then_some(LineString::new(coords))
}

/// Closes an open ring and drops it when it cannot hold an area.
fn repair_ring(ls: LineString<f64>) -> Option<LineString<f64>> {
    let mut coords = ls.0;
    coords.retain(|c| c.x.is_finite() && c.y.is_finite());
    coords.dedup();
    if coords.len() < 3 {
        return None;
    }
    if coords.first() != coords.last() {
        coords.push(coords[0]);
    }
    (coords.len() >= 4).then_some(LineString::new(coords))
}

fn repair_polygon(p: Polygon<f64>) -> Option<Polygon<f64>> {
    let exterior = repair_ring(p.exterior().clone())?;
    let interiors: Vec<LineString<f64>> = p
        .interiors()
        .iter()
        .filter_map(|ring| repair_ring(ring.clone()))
        .collect();
    Some(Polygon::new(exterior, interiors))
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::{coord, LineString};

    #[test]
    fn closes_open_ring() {
        let open = LineString::new(vec![
            coord! { x: 0.0, y: 0.0 },
            coord! { x: 1.0, y: 0.0 },
            coord! { x: 1.0, y: 1.0 },
            coord! { x: 0.0, y: 1.0 },
        ]);
        let repaired = repair_ring(open).unwrap();
        assert_eq!(repaired.0.first(), repaired.0.last());
        assert_eq!(repaired.0.len(), 5);
    }

    #[test]
    fn drops_degenerate_ring() {
        let tiny = LineString::new(vec![
            coord! { x: 0.0, y: 0.0 },
            coord! { x: 1.0, y: 1.0 },
        ]);
        assert!(repair_ring(tiny).is_none());
    }

    #[test]
    fn unrecoverable_polygon_is_none() {
        let bad = Polygon::new(
            LineString::new(vec![coord! { x: 0.0, y: 0.0 }, coord! { x: 1.0, y: 1.0 }]),
            vec![],
        );
        assert!(repair_polygon(bad).is_none());
    }

    #[test]
    fn dedupes_repeated_vertices() {
        let ls = LineString::new(vec![
            coord! { x: 0.0, y: 0.0 },
            coord! { x: 0.0, y: 0.0 },
            coord! { x: 1.0, y: 1.0 },
        ]);
        let repaired = repair_line(ls).unwrap();
        assert_eq!(repaired.0.len(), 2);
    }
}
