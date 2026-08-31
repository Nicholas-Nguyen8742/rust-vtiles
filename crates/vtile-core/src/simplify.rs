//! Zoom-dependent geometry simplification.
//!
//! TRD §4: "No aggressive simplification at high zoom levels. For zoom levels
//! 14–16, preserve original parcel geometry where possible. Simplification may
//! be applied only at low zoom levels."
//!
//! Below [`crate::config::TileConfig::simplify_below_zoom`] we apply
//! Douglas–Peucker with an epsilon of roughly half a tile pixel expressed in
//! degrees. Note that a degree-based epsilon is anisotropic away from the
//! equator; this is accepted for MVP and documented in `docs/PRECISION.md`.

use geo::Simplify;
use geo_types::{Geometry, LineString, MultiLineString, MultiPolygon, Polygon};

/// Douglas–Peucker epsilon (degrees) for a zoom level. Returns `0.0` when the
/// zoom must preserve geometry verbatim.
pub fn epsilon_for_zoom(z: u8, simplify_below_zoom: u8, extent: u32) -> f64 {
    if z >= simplify_below_zoom || extent == 0 {
        return 0.0;
    }
    // One tile pixel in degrees of longitude at the equator:
    //   360 / (2^z * extent)
    // Use half a pixel so simplification stays sub-visible.
    let pixel = 360.0 / ((1u64 << z) as f64 * extent as f64);
    pixel * 0.5
}

/// Simplifies a geometry. Returns `None` if simplification collapsed the
/// geometry below its minimum viable shape.
pub fn simplify_geometry(geom: &Geometry<f64>, epsilon: f64) -> Option<Geometry<f64>> {
    if epsilon <= 0.0 {
        return Some(geom.clone());
    }
    match geom {
        Geometry::Point(_) | Geometry::MultiPoint(_) | Geometry::Rect(_) | Geometry::Triangle(_) => {
            Some(geom.clone())
        }
        Geometry::LineString(ls) => {
            let s = ls.simplify(&epsilon);
            (s.0.len() >= 2).then_some(Geometry::LineString(s))
        }
        Geometry::MultiLineString(mls) => {
            let parts: Vec<LineString<f64>> = mls
                .0
                .iter()
                .map(|ls| ls.simplify(&epsilon))
                .filter(|ls| ls.0.len() >= 2)
                .collect();
            (!parts.is_empty()).then_some(Geometry::MultiLineString(MultiLineString(parts)))
        }
        Geometry::Polygon(p) => simplify_polygon(p, epsilon).map(Geometry::Polygon),
        Geometry::MultiPolygon(mp) => {
            let polys: Vec<Polygon<f64>> = mp
                .0
                .iter()
                .filter_map(|p| simplify_polygon(p, epsilon))
                .collect();
            (!polys.is_empty()).then_some(Geometry::MultiPolygon(MultiPolygon(polys)))
        }
        _ => Some(geom.clone()),
    }
}

/// Simplifies a polygon and drops degenerate rings. The polygon is dropped
/// entirely if its exterior ring degenerates.
fn simplify_polygon(poly: &Polygon<f64>, epsilon: f64) -> Option<Polygon<f64>> {
    let exterior = poly.exterior().simplify(&epsilon);
    if !valid_ring(&exterior) {
        return None;
    }
    let interiors: Vec<LineString<f64>> = poly
        .interiors()
        .iter()
        .map(|ring| ring.simplify(&epsilon))
        .filter(valid_ring)
        .collect();
    Some(Polygon::new(exterior, interiors))
}

/// A ring is valid when it has ≥ 4 coordinates (3 distinct + closure).
pub fn valid_ring(ls: &LineString<f64>) -> bool {
    ls.0.len() >= 4
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::coord;

    #[test]
    fn no_simplification_at_high_zoom() {
        assert_eq!(epsilon_for_zoom(14, 14, 4096), 0.0);
        assert_eq!(epsilon_for_zoom(16, 14, 4096), 0.0);
        assert!(epsilon_for_zoom(13, 14, 4096) > 0.0);
    }

    #[test]
    fn epsilon_halves_per_zoom() {
        let e10 = epsilon_for_zoom(10, 14, 4096);
        let e11 = epsilon_for_zoom(11, 14, 4096);
        assert!((e10 / e11 - 2.0).abs() < 1e-9);
    }

    #[test]
    fn simplification_collapses_wiggle() {
        let ls = LineString::new(vec![
            coord! { x: 0.0, y: 0.0 },
            coord! { x: 0.5, y: 0.0000001 },
            coord! { x: 1.0, y: 0.0 },
        ]);
        let geom = Geometry::LineString(ls);
        let out = simplify_geometry(&geom, 0.001).unwrap();
        match out {
            Geometry::LineString(s) => assert_eq!(s.0.len(), 2),
            other => panic!("unexpected geometry: {other:?}"),
        }
    }

    #[test]
    fn zero_epsilon_is_identity() {
        let ls = LineString::new(vec![
            coord! { x: 0.0, y: 0.0 },
            coord! { x: 0.5, y: 0.1 },
            coord! { x: 1.0, y: 0.0 },
        ]);
        let geom = Geometry::LineString(ls.clone());
        match simplify_geometry(&geom, 0.0).unwrap() {
            Geometry::LineString(s) => assert_eq!(s, ls),
            _ => panic!("wrong kind"),
        }
    }
}
