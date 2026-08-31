//! Zipped Shapefile bundle handling (TRD §3: `.zip` with `.shp`, `.shx`,
//! `.dbf`, `.prj`).

use std::io::{Cursor, Read};

use crate::error::{IngestError, IngestResult};

use super::shp;
use super::dbf;

/// Maximum uncompressed size of a single bundle member (zip-bomb guard).
const MAX_MEMBER_BYTES: u64 = 4 * 1024 * 1024 * 1024; // 4 GB

/// The four bundle components, read into memory.
#[derive(Debug)]
pub struct ShapefileBundle {
    pub shp: Vec<u8>,
    pub dbf: Vec<u8>,
    pub shx_present: bool,
    /// `.prj` WKT text, when present.
    pub prj: Option<String>,
}

/// Extracts and validates a zipped shapefile bundle.
///
/// Validation per TRD §10: `.shp` and `.dbf` are mandatory; missing `.dbf`
/// yields `INVALID_SHAPEFILE` (see TRD §9 `job.failed` example). A missing
/// `.prj` is allowed here — the CRS policy decides downstream whether to
/// reject or assume WGS84.
///
/// Security: entries with paths escaping the archive root (`..`, absolute)
/// are rejected (TRD §13 / §18 malicious archive mitigation).
pub fn extract_bundle(zip_bytes: &[u8]) -> IngestResult<ShapefileBundle> {
    let mut archive =
        zip::ZipArchive::new(Cursor::new(zip_bytes)).map_err(|e| IngestError::Zip(e.to_string()))?;

    let mut shp: Option<Vec<u8>> = None;
    let mut dbf: Option<Vec<u8>> = None;
    let mut prj: Option<String> = None;
    let mut shx_present = false;

    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .map_err(|e| IngestError::Zip(e.to_string()))?;

        if !file.is_file() {
            continue;
        }
        // `enclosed_name` rejects absolute paths and `..` traversal.
        let Some(safe_name) = file.enclosed_name() else {
            return Err(IngestError::InvalidShapefile(format!(
                "archive entry with unsafe path rejected: {:?}",
                file.name()
            )));
        };
        // Only consider the base name: bundles often nest files in a folder.
        let base = safe_name
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let Some(ext) = base.rsplit('.').next().map(|e| e.to_lowercase()) else {
            continue;
        };
        if file.size() > MAX_MEMBER_BYTES {
            return Err(IngestError::InvalidShapefile(format!(
                "member {base} exceeds the 4 GB member limit"
            )));
        }

        match ext.as_str() {
            "shp" | "dbf" | "prj" => {
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)
                    .map_err(|e| IngestError::Zip(e.to_string()))?;
                match ext.as_str() {
                    "shp" => shp = Some(buf),
                    "dbf" => dbf = Some(buf),
                    "prj" => prj = Some(String::from_utf8_lossy(&buf).into_owned()),
                    _ => unreachable!(),
                }
            }
            "shx" => shx_present = true,
            _ => {}
        }
    }

    // TRD §10 validation table.
    match (&shp, &dbf) {
        (Some(_), Some(_)) => {}
        (None, _) => {
            return Err(IngestError::InvalidShapefile(
                "Missing required .shp file.".into(),
            ))
        }
        (_, None) => {
            return Err(IngestError::InvalidShapefile(
                "Missing required .dbf file.".into(),
            ))
        }
    }

    Ok(ShapefileBundle {
        shp: shp.expect("validated above"),
        dbf: dbf.expect("validated above"),
        shx_present,
        prj,
    })
}

/// Reads a bundle into parsed geometries + attribute records.
///
/// Shapefile has no object ids; record index becomes the feature id so that
/// `Feature.id` survives into MVT (TRD §12 linking fields remain in
/// properties).
pub fn read_bundle(bundle: &ShapefileBundle) -> IngestResult<Vec<(geo_types::Geometry<f64>, serde_json::Map<String, serde_json::Value>)>> {
    let shapes = shp::parse_shp(&bundle.shp)?;
    let table = dbf::parse_dbf(&bundle.dbf)?;

    if shapes.len() != table.records.len() {
        tracing::warn!(
            shapes = shapes.len(),
            records = table.records.len(),
            "shape/record count mismatch; pairing by minimum length"
        );
    }

    let mut out = Vec::with_capacity(shapes.len().min(table.records.len()));
    for (shape, record) in shapes.into_iter().zip(table.records.into_iter()) {
        if let Some(geom) = shp::shp_to_geometry(shape) {
            out.push((geom, record));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zip_with(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut buf = Cursor::new(Vec::new());
        {
            let mut zip = zip::ZipWriter::new(&mut buf);
            let options =
                zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
            for (name, content) in entries {
                zip.start_file(*name, options).unwrap();
                zip.write_all(content).unwrap();
            }
            zip.finish().unwrap();
        }
        buf.into_inner()
    }

    use std::io::Write;

    #[test]
    fn rejects_bundle_missing_dbf() {
        let data = zip_with(&[("a.shp", b"x"), ("a.shx", b"y")]);
        let err = extract_bundle(&data).unwrap_err();
        assert!(matches!(err, IngestError::InvalidShapefile(msg) if msg.contains(".dbf")));
    }

    #[test]
    fn rejects_traversal_paths() {
        let data = zip_with(&[("../evil.shp", b"x"), ("a.dbf", b"y")]);
        assert!(extract_bundle(&data).is_err());
    }

    #[test]
    fn accepts_nested_bundle() {
        let data = zip_with(&[("folder/parcels.shp", b"s"), ("folder/parcels.dbf", b"d"), ("folder/parcels.prj", b"GEOGCS[\"GCS_WGS_1984\"]")]);
        let bundle = extract_bundle(&data).unwrap();
        assert!(bundle.prj.is_some());
        assert!(!bundle.shx_present);
    }
}
