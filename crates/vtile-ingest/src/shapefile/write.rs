//! Minimal `.shp` / `.shx` / `.dbf` / `.prj` writers for test fixtures.
//!
//! The readers in [`super::shp`] and [`super::dbf`] are hand-rolled, so the
//! fixture writers live here and produce byte layouts that match them
//! exactly (and the ESRI Shapefile Technical Description / dBASE III spec):
//! big-endian record headers, little-endian payloads, 100-byte file headers.
//!
//! Used by `examples/gen_fixtures.rs` (produces the zipped bundles under
//! `tests/fixtures/`) and by the pipeline end-to-end tests (in-memory
//! bundles). Production never writes shapefiles — only reads them.

use std::io::{Cursor, Write};

/// Builds a `.shp` file containing one POLYGON record per ring list.
///
/// Each polygon has a single part. Rings must follow the shapefile winding
/// convention: exterior rings clockwise (negative signed area in a y-up
/// plane), holes counter-clockwise — see `super::shp::group_polygon_parts`.
pub fn write_polygon_shp(polygons: &[Vec<(f64, f64)>]) -> Vec<u8> {
    // Encode record contents first so the header can state the file length.
    let mut records: Vec<Vec<u8>> = Vec::with_capacity(polygons.len());
    for ring in polygons {
        let mut c = Vec::new();
        c.extend_from_slice(&5i32.to_le_bytes()); // shape type POLYGON
        let (xmin, ymin, xmax, ymax) = ring_bbox(ring);
        for v in [xmin, ymin, xmax, ymax] {
            c.extend_from_slice(&v.to_le_bytes());
        }
        c.extend_from_slice(&1i32.to_le_bytes()); // num_parts
        c.extend_from_slice(&(ring.len() as i32).to_le_bytes()); // num_points
        c.extend_from_slice(&0i32.to_le_bytes()); // part offsets[0]
        for (x, y) in ring {
            c.extend_from_slice(&x.to_le_bytes());
            c.extend_from_slice(&y.to_le_bytes());
        }
        records.push(c);
    }

    let total_bytes = 100 + records.iter().map(|r| 8 + r.len()).sum::<usize>();
    let mut out = Vec::with_capacity(total_bytes);
    out.extend_from_slice(&9994i32.to_be_bytes()); // file code
    out.extend_from_slice(&[0u8; 20]); // unused
    out.extend_from_slice(&((total_bytes / 2) as i32).to_be_bytes()); // length in 16-bit words
    out.extend_from_slice(&1000i32.to_le_bytes()); // version
    out.extend_from_slice(&5i32.to_le_bytes()); // header shape type
    let (xmin, ymin, xmax, ymax) = all_bbox(polygons);
    for v in [xmin, ymin, xmax, ymax, 0.0, 0.0, 0.0, 0.0] {
        out.extend_from_slice(&v.to_le_bytes()); // bbox + z/m ranges
    }
    for (i, rec) in records.iter().enumerate() {
        out.extend_from_slice(&((i + 1) as i32).to_be_bytes()); // record number
        out.extend_from_slice(&((rec.len() / 2) as i32).to_be_bytes()); // content words
        out.extend_from_slice(rec);
    }
    debug_assert_eq!(out.len(), total_bytes);
    out
}

/// Builds a minimal well-formed `.shx` header.
///
/// The MVP pipeline never parses `.shx` (the `.shp` is scanned sequentially,
/// see `super::shp`), but bundle validation checks for its presence, so
/// fixtures include a valid 100-byte index header.
pub fn write_shx_header() -> Vec<u8> {
    let mut out = Vec::with_capacity(100);
    out.extend_from_slice(&9994i32.to_be_bytes());
    out.extend_from_slice(&[0u8; 20]);
    out.extend_from_slice(&50i32.to_be_bytes()); // 100 bytes = 50 words
    out.extend_from_slice(&1000i32.to_le_bytes());
    out.extend_from_slice(&5i32.to_le_bytes());
    out.extend_from_slice(&[0u8; 64]);
    out
}

/// A dBASE field definition for [`write_dbf`].
#[derive(Debug, Clone)]
pub struct DbfFieldSpec {
    /// Field name; dBASE III limits this to 10 characters (TRD §17 notes
    /// the attribute-name truncation quirk of shapefiles).
    pub name: String,
    /// dBASE type byte: `b'C'` character, `b'N'` numeric, `b'L'` logical,
    /// `b'D'` date.
    pub ftype: u8,
    pub length: usize,
    pub decimal_count: u8,
}

/// Builds a dBASE III `.dbf` file with the given fields and records.
///
/// Each record is a list of pre-formatted string values, one per field, in
/// field order. Numeric fields are right-aligned, character fields
/// left-aligned, matching real-world dBASE writers.
pub fn write_dbf(fields: &[DbfFieldSpec], records: &[Vec<String>]) -> Vec<u8> {
    let header_size: u16 = (32 + fields.len() * 32 + 1) as u16;
    let record_size: u16 = (1 + fields.iter().map(|f| f.length).sum::<usize>()) as u16;

    let mut out = Vec::new();
    out.push(0x03); // dBASE III without memo
    out.extend_from_slice(&[26, 6, 17]); // YY MM DD (last update)
    out.extend_from_slice(&(records.len() as u32).to_le_bytes());
    out.extend_from_slice(&header_size.to_le_bytes());
    out.extend_from_slice(&record_size.to_le_bytes());
    out.extend_from_slice(&[0u8; 20]); // reserved

    for f in fields {
        let mut name = [0u8; 11];
        for (i, b) in f.name.bytes().take(11).enumerate() {
            name[i] = b;
        }
        out.extend_from_slice(&name);
        out.push(f.ftype);
        out.extend_from_slice(&[0u8; 4]); // reserved
        out.push(f.length as u8);
        out.push(f.decimal_count);
        out.extend_from_slice(&[0u8; 14]); // reserved
    }
    out.push(0x0d); // header terminator

    for rec in records {
        out.push(b' '); // deletion flag: active record
        for (field, value) in fields.iter().zip(rec.iter()) {
            let mut buf = vec![b' '; field.length];
            let bytes = value.as_bytes();
            let n = bytes.len().min(field.length);
            if field.ftype == b'N' || field.ftype == b'F' {
                let start = field.length - n;
                buf[start..start + n].copy_from_slice(&bytes[..n]);
            } else {
                buf[..n].copy_from_slice(&bytes[..n]);
            }
            out.extend_from_slice(&buf);
        }
    }
    out.push(0x1a); // EOF marker
    out
}

/// Standard WGS84 `.prj` WKT, detected as EPSG:4326 by `crate::crs::detect_crs`.
pub const WGS84_PRJ: &str = r#"GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]"#;

/// Packs entries into an in-memory ZIP (Deflate), mirroring a shapefile
/// bundle upload.
pub fn zip_entries(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut buf = Cursor::new(Vec::new());
    {
        let mut zip = zip::ZipWriter::new(&mut buf);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        for (name, content) in entries {
            zip.start_file(*name, options).expect("zip start_file");
            zip.write_all(content).expect("zip write");
        }
        zip.finish().expect("zip finish");
    }
    buf.into_inner()
}

/// Sample parcel geometry used by the shapefile fixtures: three small NYC
/// parcels, one record each. Rings are closed and clockwise (exterior).
pub fn sample_parcel_rings() -> Vec<Vec<(f64, f64)>> {
    fn square(x0: f64, y0: f64, x1: f64, y1: f64) -> Vec<(f64, f64)> {
        // (x0,y0) -> (x0,y1) -> (x1,y1) -> (x1,y0) -> close: clockwise,
        // negative signed area, grouped as an exterior ring by the parser.
        vec![(x0, y0), (x0, y1), (x1, y1), (x1, y0), (x0, y0)]
    }
    vec![
        square(-73.990, 40.750, -73.985, 40.755),
        square(-73.980, 40.740, -73.975, 40.745),
        square(-73.970, 40.760, -73.965, 40.765),
    ]
}

/// CRE-flavored attribute schema matching the sample parcel geometries.
pub fn sample_parcel_dbf() -> (Vec<DbfFieldSpec>, Vec<Vec<String>>) {
    let fields = vec![
        DbfFieldSpec { name: "PARCELID".into(), ftype: b'C', length: 16, decimal_count: 0 },
        DbfFieldSpec { name: "ASSETID".into(), ftype: b'C', length: 16, decimal_count: 0 },
        DbfFieldSpec { name: "MARKET".into(), ftype: b'C', length: 16, decimal_count: 0 },
        DbfFieldSpec { name: "PROPTYPE".into(), ftype: b'C', length: 12, decimal_count: 0 },
    ];
    let records = vec![
        vec![
            "NYC-BB-000001".into(),
            "AST-000123".into(),
            "New York".into(),
            "OFFICE".into(),
        ],
        vec![
            "NYC-BB-000002".into(),
            "AST-000124".into(),
            "New York".into(),
            "RETAIL".into(),
        ],
        vec![
            "NYC-BB-000003".into(),
            "AST-000125".into(),
            "New York".into(),
            "MULTIFAM".into(),
        ],
    ];
    (fields, records)
}

/// Builds a complete shapefile bundle zip from the sample parcels.
///
/// * `include_dbf: false` reproduces the TRD §9 failure example
///   ("Missing required .dbf file." → `MISSING_SHAPEFILE_COMPONENTS`).
/// * `include_prj: false` reproduces the missing-CRS case (`UNKNOWN_CRS`
///   unless the AssumeWgs84 policy is set, TRD §10 / US-04).
pub fn sample_parcel_bundle(include_dbf: bool, include_prj: bool) -> Vec<u8> {
    let shp = write_polygon_shp(&sample_parcel_rings());
    let shx = write_shx_header();
    let (fields, records) = sample_parcel_dbf();
    let dbf = write_dbf(&fields, &records);

    let mut entries: Vec<(String, Vec<u8>)> = vec![
        ("parcels.shp".to_string(), shp),
        ("parcels.shx".to_string(), shx),
    ];
    if include_dbf {
        entries.push(("parcels.dbf".to_string(), dbf));
    }
    if include_prj {
        entries.push(("parcels.prj".to_string(), WGS84_PRJ.as_bytes().to_vec()));
    }
    let refs: Vec<(&str, &[u8])> = entries
        .iter()
        .map(|(n, b)| (n.as_str(), b.as_slice()))
        .collect();
    zip_entries(&refs)
}

fn ring_bbox(ring: &[(f64, f64)]) -> (f64, f64, f64, f64) {
    let mut xmin = f64::INFINITY;
    let mut ymin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    for (x, y) in ring {
        xmin = xmin.min(*x);
        ymin = ymin.min(*y);
        xmax = xmax.max(*x);
        ymax = ymax.max(*y);
    }
    if xmin.is_infinite() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    (xmin, ymin, xmax, ymax)
}

fn all_bbox(polygons: &[Vec<(f64, f64)>]) -> (f64, f64, f64, f64) {
    let mut acc: Option<(f64, f64, f64, f64)> = None;
    for ring in polygons {
        let b = ring_bbox(ring);
        acc = Some(match acc {
            Some((x0, y0, x1, y1)) => (x0.min(b.0), y0.min(b.1), x1.max(b.2), y1.max(b.3)),
            None => b,
        });
    }
    acc.unwrap_or((0.0, 0.0, 0.0, 0.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shapefile::{extract_bundle, read_bundle};

    #[test]
    fn written_bundle_roundtrips_through_readers() {
        let zip = sample_parcel_bundle(true, true);
        let bundle = extract_bundle(&zip).expect("bundle extracts");
        assert!(bundle.prj.is_some());
        assert!(bundle.shx_present);

        let pairs = read_bundle(&bundle).expect("bundle parses");
        assert_eq!(pairs.len(), 3);
        let (geom, props) = &pairs[0];
        assert!(matches!(geom, geo_types::Geometry::Polygon(_)));
        assert_eq!(
            props.get("PARCELID"),
            Some(&serde_json::Value::String("NYC-BB-000001".into()))
        );
        assert_eq!(
            props.get("PROPTYPE"),
            Some(&serde_json::Value::String("OFFICE".into()))
        );
    }

    #[test]
    fn bundle_without_dbf_fails_component_check() {
        let zip = sample_parcel_bundle(false, true);
        let err = extract_bundle(&zip).expect_err("must fail");
        assert_eq!(err.error_code(), "MISSING_SHAPEFILE_COMPONENTS");
    }
}
