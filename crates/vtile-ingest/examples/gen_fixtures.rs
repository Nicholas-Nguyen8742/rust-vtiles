//! Generates the zipped Shapefile fixtures for the local pipeline and CI.
//!
//! The GeoJSON fixtures under `tests/fixtures/` are checked in as text; the
//! binary Shapefile bundles are produced here from the byte-exact writers in
//! `vtile_ingest::shapefile::write` so nothing hand-assembled drifts from the
//! parser. Run via:
//!
//! ```bash
//! cargo run -p vtile-ingest --example gen_fixtures
//! # or: make fixtures
//! ```
//!
//! Outputs (into `tests/fixtures/`):
//! * `small-parcels.zip` — complete bundle (`.shp/.shx/.dbf/.prj`), happy path
//! * `missing-dbf.zip`   — no `.dbf` → `MISSING_SHAPEFILE_COMPONENTS`
//! * `missing-prj.zip`   — no `.prj` → `UNKNOWN_CRS` (or replay w/ assume-wgs84)

use std::fs;
use std::path::PathBuf;

use vtile_ingest::shapefile::write::sample_parcel_bundle;

fn fixtures_dir() -> PathBuf {
    // crate manifest: <workspace>/crates/vtile-ingest
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("tests")
        .join("fixtures")
}

fn main() {
    let dir = fixtures_dir();
    fs::create_dir_all(&dir).expect("create fixtures dir");

    // small-parcels.zip: complete bundle, WGS84 prj.
    write(
        &dir.join("small-parcels.zip"),
        &sample_parcel_bundle(true, true),
    );

    // missing-dbf.zip: exercises the TRD §9 failure example.
    write(
        &dir.join("missing-dbf.zip"),
        &sample_parcel_bundle(false, true),
    );

    // missing-prj.zip: exercises the unknown-CRS policy path.
    write(
        &dir.join("missing-prj.zip"),
        &sample_parcel_bundle(true, false),
    );

    // Sanity-check one bundle through the real reader before finishing, so a
    // regression in the writers fails loudly here rather than in the tests.
    let bundle = vtile_ingest::shapefile::extract_bundle(&sample_parcel_bundle(true, true))
        .expect("sample bundle must extract");
    let pairs = vtile_ingest::shapefile::read_bundle(&bundle).expect("sample bundle must parse");
    assert_eq!(pairs.len(), 3, "expected 3 sample parcels");

    println!("wrote shapefile fixtures to {}", dir.display());
}

fn write(path: &std::path::Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
    println!("  {} ({} bytes)", path.display(), bytes.len());
}
