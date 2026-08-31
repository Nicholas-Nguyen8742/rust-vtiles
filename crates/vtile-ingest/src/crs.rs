//! CRS detection and reprojection to EPSG:4326.
//!
//! MVP strategy (TRD §10 "Unsupported CRS: attempt reprojection if safe;
//! otherwise reject"):
//! * EPSG:4326 / WGS84 — identity.
//! * EPSG:4269 (NAD83) — treated as identity for MVP. NAD83 and WGS84 differ
//!   by ~1–2 m, which is below MVT quantization at every supported zoom
//!   (see `docs/PRECISION.md`). The assumption is flagged in metadata.
//! * EPSG:3857 / 900913 (Web Mercator) — exact analytic inverse.
//! * Anything else — rejected with `UnsupportedCrs`.
//!
//! A PROJ-backed transform hook can replace this module post-MVP.

use serde::{Deserialize, Serialize};

use crate::error::{IngestError, IngestResult};

/// Semi-major axis of the WGS84/GRS80 ellipsoid used by EPSG:3857.
const EARTH_RADIUS: f64 = 6_378_137.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CrsKind {
    /// EPSG:4326.
    Wgs84,
    /// EPSG:4269 — treated as WGS84 for MVP (see module docs).
    Nad83,
    /// EPSG:3857 / 900913.
    WebMercator,
    /// Detected but not safely reprojectable in MVP.
    Unsupported,
    /// No CRS information at all.
    Unknown,
}

#[derive(Debug, Clone)]
pub struct CrsInfo {
    pub kind: CrsKind,
    pub epsg: Option<u32>,
    /// Raw `.prj` WKT, when available.
    pub raw: Option<String>,
    /// True when the CRS was missing and WGS84 was assumed by policy.
    pub assumed: bool,
}

impl CrsInfo {
    pub fn label(&self) -> &'static str {
        match self.kind {
            CrsKind::Wgs84 => "EPSG:4326",
            CrsKind::Nad83 => "EPSG:4269 (treated as EPSG:4326)",
            CrsKind::WebMercator => "EPSG:3857",
            CrsKind::Unsupported => "UNSUPPORTED",
            CrsKind::Unknown => "UNKNOWN",
        }
    }
}

/// Detects the CRS from `.prj` WKT content.
///
/// Detection is deliberately conservative: an authority code wins when
/// present; otherwise a handful of common WKT keyword families are matched.
pub fn detect_crs(prj_wkt: Option<&str>) -> CrsInfo {
    let Some(wkt) = prj_wkt else {
        return CrsInfo {
            kind: CrsKind::Unknown,
            epsg: None,
            raw: None,
            assumed: false,
        };
    };

    if let Some(code) = extract_epsg_code(wkt) {
        let kind = match code {
            4326 => CrsKind::Wgs84,
            4269 => CrsKind::Nad83,
            3857 | 900913 | 102100 => CrsKind::WebMercator,
            _ => CrsKind::Unsupported,
        };
        return CrsInfo {
            kind,
            epsg: Some(code),
            raw: Some(wkt.to_string()),
            assumed: false,
        };
    }

    let upper = wkt.to_uppercase();
    let kind = if upper.contains("GCS_WGS_1984") || (upper.contains("WGS_1984") && upper.contains("GEOGCS")) {
        CrsKind::Wgs84
    } else if upper.contains("GCS_NORTH_AMERICAN_1983") || upper.contains("NAD83") {
        CrsKind::Nad83
    } else if upper.contains("WEB_MERCATOR") || upper.contains("MERCATOR_AUXILIARY_SPHERE") {
        CrsKind::WebMercator
    } else {
        CrsKind::Unsupported
    };

    CrsInfo {
        kind,
        epsg: None,
        raw: Some(wkt.to_string()),
        assumed: false,
    }
}

/// Extracts the EPSG authority code from WKT. When multiple AUTHORITY nodes
/// exist (PROJCS wrapping GEOGCS), the LAST one names the root CRS.
fn extract_epsg_code(wkt: &str) -> Option<u32> {
    let upper = wkt.to_uppercase();
    let mut last: Option<u32> = None;
    let mut search_from = 0usize;
    while let Some(rel) = upper[search_from..].find("AUTHORITY") {
        let start = search_from + rel;
        let rest = &wkt[start..];
        // Expect: AUTHORITY["EPSG","4326"] (whitespace tolerated).
        if let Some(code) = parse_authority(rest) {
            last = Some(code);
        }
        search_from = start + "AUTHORITY".len();
    }
    last
}

fn parse_authority(fragment: &str) -> Option<u32> {
    let mut parts = fragment.split('"');
    // fragment: AUTHORITY["EPSG","4326"]...
    let _lead = parts.next()?; // AUTHORITY[
    let name = parts.next()?.trim();
    if !name.eq_ignore_ascii_case("EPSG") {
        return None;
    }
    let _sep = parts.next()?; // ,
    let code = parts.next()?.trim();
    code.parse::<u32>().ok()
}

/// Reprojects a single (x, y) coordinate pair in-place to lon/lat degrees.
///
/// Returns an error for unsupported CRS kinds so callers reject the file
/// (TRD §10) instead of silently misaligning overlays (TRD §18).
pub fn reproject_xy(info: &CrsInfo, x: &mut f64, y: &mut f64) -> IngestResult<()> {
    match info.kind {
        CrsKind::Wgs84 | CrsKind::Nad83 => Ok(()),
        CrsKind::WebMercator => {
            let (lon, lat) = webmercator_to_lonlat(*x, *y);
            *x = lon;
            *y = lat;
            Ok(())
        }
        CrsKind::Unknown => Err(IngestError::UnknownCrs(
            "no .prj provided and no CRS override supplied".into(),
        )),
        CrsKind::Unsupported => Err(IngestError::UnsupportedCrs(format!(
            "CRS {:?} (EPSG {:?}) cannot be safely reprojected in MVP",
            info.kind, info.epsg
        ))),
    }
}

/// Analytic inverse of the spherical Mercator projection (EPSG:3857).
pub fn webmercator_to_lonlat(x: f64, y: f64) -> (f64, f64) {
    let lon = (x / EARTH_RADIUS).to_degrees();
    let lat = (2.0 * (y / EARTH_RADIUS).exp().atan() - std::f64::consts::FRAC_PI_2).to_degrees();
    (lon, lat)
}

/// Forward spherical Mercator (used by tests and future tooling).
pub fn lonlat_to_webmercator(lon: f64, lat: f64) -> (f64, f64) {
    let x = EARTH_RADIUS * lon.to_radians();
    let y = EARTH_RADIUS * (std::f64::consts::FRAC_PI_4 + lat.to_radians() / 2.0).tan().ln();
    (x, y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_last_epsg_authority() {
        let wkt = r#"PROJCS["NAD_1983_StatePlane_New_York_Long_Island_FIPS_3104_Feet",
            GEOGCS["GCS_North_American_1983",AUTHORITY["EPSG","4269"]],
            PROJECTION["Lambert_Conformal_Conic"],
            AUTHORITY["EPSG","2263"]]"#;
        assert_eq!(extract_epsg_code(wkt), Some(2263));
    }

    #[test]
    fn detects_wgs84() {
        let info = detect_crs(Some(r#"GEOGCS["GCS_WGS_1984",AUTHORITY["EPSG","4326"]]"#));
        assert_eq!(info.kind, CrsKind::Wgs84);
    }

    #[test]
    fn missing_prj_is_unknown() {
        let info = detect_crs(None);
        assert_eq!(info.kind, CrsKind::Unknown);
        assert!(reproject_xy(&info, &mut 0.0, &mut 0.0).is_err());
    }

    #[test]
    fn unsupported_code_rejects() {
        let info = detect_crs(Some(r#"PROJCS["x",AUTHORITY["EPSG","2263"]]"#));
        assert_eq!(info.kind, CrsKind::Unsupported);
        assert!(reproject_xy(&info, &mut 0.0, &mut 0.0).is_err());
    }

    #[test]
    fn webmercator_roundtrip() {
        let (x, y) = lonlat_to_webmercator(-73.9855, 40.7580);
        let (lon, lat) = webmercator_to_lonlat(x, y);
        assert!((lon - -73.9855).abs() < 1e-9);
        assert!((lat - 40.7580).abs() < 1e-9);
    }
}
