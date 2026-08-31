//! End-to-end pipeline tests: sample GeoJSON → normalize → MVT → publish.
//!
//! These exercise the full TRD §10 workflow in-process via `run_job`,
//! against the sample datasets in `examples/data/`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::Utc;

use vtile_core::config::{PropertyPolicy, TileConfig};
use vtile_core::mvt::decode::decode_gzipped_tile;
use vtile_core::model::{
    JobRecord, JobStatus, LayerCategory, LayerMetadataInput, SourceFormat, ZoomRange,
};
use vtile_ingest::normalize::{CrsPolicy, NormalizeOptions};
use vtile_pipeline::events::{EventEmitter, NullEventEmitter, PipelineEvent};
use vtile_pipeline::job::{run_job, JobDeps, JobPaths, RunJobInput};
use vtile_pipeline::store::{FileJobStore, FileLayerCatalog, JobStore, LayerCatalog};
use vtile_pipeline::TileManifest;

const TENANT: &str = "tenant-acme";
const LAYER: &str = "us-parcels-nyc";

fn sample(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join("data")
        .join(name)
}

fn temp_root(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vtile-e2e-{label}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn job_paths(root: &Path) -> JobPaths {
    JobPaths {
        staging_root: root.join("staging").join(TENANT).join("job_test"),
        tiles_root: root.join("tiles").join(TENANT).join(LAYER),
        manifests_root: root.join("manifests").join(TENANT).join(LAYER),
    }
}

fn parcel_job(job_id: &str) -> JobRecord {
    JobRecord {
        job_id: job_id.to_string(),
        tenant_id: TENANT.to_string(),
        layer_id: LAYER.to_string(),
        status: JobStatus::Queued,
        source_format: SourceFormat::GeoJson,
        source_uri: format!("mem://{LAYER}"),
        requested_zoom_range: ZoomRange::new(12, 15),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        error: None,
        outcome: None,
        layer_input: Some(LayerMetadataInput {
            name: Some("NYC Parcels".to_string()),
            description: Some("Sample parcel boundaries".to_string()),
            category: Some(LayerCategory::Parcel),
            tags: vec!["parcel".to_string(), "nyc".to_string()],
        }),
    }
}

fn normalize_opts() -> NormalizeOptions {
    NormalizeOptions {
        max_upload_bytes: 50 * 1024 * 1024,
        crs_policy: CrsPolicy::RequireKnown,
        property_policy: PropertyPolicy::default(),
    }
}

fn tile_config() -> TileConfig {
    TileConfig {
        layer_name: "parcel_boundary".to_string(),
        zoom_range: ZoomRange::new(12, 15),
        ..Default::default()
    }
}

fn collect_pbf(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_pbf(&path, out);
        } else if path.extension().map(|e| e == "pbf").unwrap_or(false) {
            out.push(path);
        }
    }
}

#[test]
fn geojson_parcels_end_to_end() {
    let root = temp_root("parcels");
    let jobs = Arc::new(FileJobStore::new(root.join("jobs")).unwrap());
    let catalog = Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap());
    let deps = JobDeps {
        jobs: jobs.clone(),
        catalog: catalog.clone(),
        events: Arc::new(NullEventEmitter),
    };

    let job = parcel_job("job_e2e_parcels");
    jobs.create(job.clone()).unwrap();
    let paths = job_paths(&root);
    let input = RunJobInput {
        job,
        source_bytes: fs::read(sample("nyc_parcels_sample.geojson")).unwrap(),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths,
    };

    let outcome = run_job(&input, &deps).expect("job should succeed");

    // ── Outcome summary ───────────────────────────────────────────────────
    assert_eq!(outcome.feature_count, 3);
    // All three parcels share one tile at each zoom (12, 13, 14, 15).
    assert_eq!(outcome.tile_count, 4);

    // ── Job record finalized ──────────────────────────────────────────────
    let stored = jobs.get("job_e2e_parcels").unwrap().expect("job stored");
    assert_eq!(stored.status, JobStatus::Completed);
    let summary = stored.outcome.expect("outcome summary attached");
    assert_eq!(summary.feature_count, 3);
    assert_eq!(summary.published_tile_count, 4);
    assert_eq!(summary.tile_version, outcome.tile_version);
    let bbox = summary.bounding_box.to_vec();
    assert_eq!(bbox.len(), 4);
    assert!(bbox[0] < bbox[2] && bbox[1] < bbox[3]);

    // ── Manifest (TRD §6/§14 atomic publish pointer) ─────────────────────
    let manifest_json = fs::read_to_string(input.paths.manifest_path()).unwrap();
    let manifest = TileManifest::from_json(&manifest_json).unwrap();
    assert_eq!(manifest.tenant_id, TENANT);
    assert_eq!(manifest.layer_id, LAYER);
    assert_eq!(manifest.tile_version, outcome.tile_version);
    assert_eq!(manifest.tile_count, 4);
    assert_eq!(manifest.min_zoom, 12);
    assert_eq!(manifest.max_zoom, 15);
    assert_eq!(manifest.bounding_box.to_vec(), bbox);

    // ── Tiles on disk: versioned prefix, valid MVT v2 ────────────────────
    let version_root = input.paths.tiles_root.join(&outcome.tile_version);
    let mut tile_files = Vec::new();
    collect_pbf(&version_root, &mut tile_files);
    assert_eq!(tile_files.len(), 4);

    for file in &tile_files {
        let gz = fs::read(file).unwrap();
        assert!(
            gz.len() < 250_000,
            "tile {} exceeds TRD preferred size",
            file.display()
        );
        let decoded = decode_gzipped_tile(&gz).expect("tile must decode");
        assert_eq!(decoded.layers.len(), 1);
        let layer = &decoded.layers[0];
        assert_eq!(layer.name, "parcel_boundary");
        assert_eq!(layer.version, 2, "MVT version must be 2 (TRD §5)");
        assert_eq!(layer.extent, 4096);
        assert_eq!(layer.feature_count, 3);
        // MVT GeomType POLYGON == 3.
        assert!(layer.geom_types.iter().all(|t| *t == 3));
        // identifiers preserved (TRD §7).
        assert!(layer.keys.iter().any(|k| k == "parcelId"));
        assert!(layer.keys.iter().any(|k| k == "assetId"));
        // PII stripped by the default property denylist (TRD §13).
        assert!(
            !layer.keys.iter().any(|k| k.eq_ignore_ascii_case("ownername")),
            "ownerName must be stripped before publication"
        );
    }

    // ── Catalog entry (TRD §7 layer metadata) ────────────────────────────
    let layer = catalog.get(LAYER).unwrap().expect("layer in catalog");
    assert_eq!(layer.tenant_id, TENANT);
    assert_eq!(layer.feature_count, 3);
    assert_eq!(layer.crs, "EPSG:4326");
    assert!(!layer.assumed_crs);
    assert_eq!(layer.category, LayerCategory::Parcel);
    assert_eq!(layer.min_zoom, 12);
    assert_eq!(layer.max_zoom, 15);
    assert_eq!(layer.tile_version, outcome.tile_version);
    assert!(layer.tags.iter().any(|t| t == "nyc"));
    assert!(layer.published_at.is_some());

    // ── Normalized artifact retained for audit (TRD §6) ──────────────────
    assert!(input.paths.normalized_artifact().exists());
}

#[test]
fn rerunning_completed_job_is_rejected_idempotently() {
    let root = temp_root("idempotency");
    let jobs = Arc::new(FileJobStore::new(root.join("jobs")).unwrap());
    let catalog = Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap());
    let deps = JobDeps {
        jobs: jobs.clone(),
        catalog: catalog.clone(),
        events: Arc::new(NullEventEmitter),
    };

    let job = parcel_job("job_e2e_idem");
    jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job: job.clone(),
        source_bytes: fs::read(sample("nyc_parcels_sample.geojson")).unwrap(),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths: job_paths(&root),
    };
    run_job(&input, &deps).expect("first run succeeds");

    // TRD §14: idempotent job processing using jobId.
    let err = run_job(&input, &deps).expect_err("second run must be rejected");
    assert!(err.to_string().contains("already completed"));
}

/// Captures emitted events for assertions (stands in for EventBridge).
#[derive(Default)]
struct CapturingEmitter {
    events: Mutex<Vec<PipelineEvent>>,
}

impl EventEmitter for CapturingEmitter {
    fn emit(&self, event: PipelineEvent) {
        self.events.lock().unwrap().push(event);
    }
}

#[test]
fn empty_dataset_fails_with_trd_error_code() {
    let root = temp_root("empty");
    let jobs = Arc::new(FileJobStore::new(root.join("jobs")).unwrap());
    let catalog = Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap());
    let emitter = Arc::new(CapturingEmitter::default());
    let deps = JobDeps {
        jobs: jobs.clone(),
        catalog: catalog.clone(),
        events: emitter.clone(),
    };

    let job = parcel_job("job_e2e_empty");
    jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job,
        source_bytes: br#"{"type": "FeatureCollection", "features": []}"#.to_vec(),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths: job_paths(&root),
    };

    let err = run_job(&input, &deps).expect_err("empty dataset must fail");
    // TRD §10: reject with 422 EMPTY_DATASET.
    assert!(err.to_string().contains("empty dataset"), "got: {err}");

    let stored = jobs.get("job_e2e_empty").unwrap().expect("job stored");
    assert_eq!(stored.status, JobStatus::Failed);
    assert!(stored.error.is_some());

    // TRD §9: job.failed event emitted with the classified error code.
    let events = emitter.events.lock().unwrap();
    let failed: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            PipelineEvent::VectorTileJobFailed { error_code, .. } => Some(error_code.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(failed, vec!["EMPTY_DATASET".to_string()]);
}

#[test]
fn point_assets_end_to_end() {
    let root = temp_root("assets");
    let jobs = Arc::new(FileJobStore::new(root.join("jobs")).unwrap());
    let catalog = Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap());
    let deps = JobDeps {
        jobs: jobs.clone(),
        catalog: catalog.clone(),
        events: Arc::new(NullEventEmitter),
    };

    let mut job = parcel_job("job_e2e_assets");
    job.layer_id = "us-assets-nyc".to_string();
    job.layer_input = Some(LayerMetadataInput {
        name: Some("NYC Assets".to_string()),
        description: None,
        category: Some(LayerCategory::AssetPoint),
        tags: vec!["asset".to_string(), "nyc".to_string()],
    });
    jobs.create(job.clone()).unwrap();

    let paths = JobPaths {
        staging_root: root.join("staging").join(TENANT).join("job_assets"),
        tiles_root: root.join("tiles").join(TENANT).join("us-assets-nyc"),
        manifests_root: root.join("manifests").join(TENANT).join("us-assets-nyc"),
    };
    let input = RunJobInput {
        job,
        source_bytes: fs::read(sample("nyc_assets_sample.geojson")).unwrap(),
        tile_config: TileConfig {
            layer_name: "asset_point".to_string(),
            zoom_range: ZoomRange::new(8, 14),
            ..Default::default()
        },
        normalize_opts: normalize_opts(),
        paths: paths.clone(),
    };

    let outcome = run_job(&input, &deps).expect("job should succeed");
    assert_eq!(outcome.feature_count, 4);
    assert!(
        outcome.tile_count >= 7,
        "at least one tile per zoom 8..=14, got {}",
        outcome.tile_count
    );

    let layer = catalog.get("us-assets-nyc").unwrap().expect("layer in catalog");
    assert_eq!(layer.category, LayerCategory::AssetPoint);
    assert_eq!(layer.feature_count, 4);

    // Every tile carries MVT point features.
    let mut tile_files = Vec::new();
    collect_pbf(&paths.tiles_root.join(&outcome.tile_version), &mut tile_files);
    assert_eq!(tile_files.len() as u64, outcome.tile_count);
    for file in &tile_files {
        let decoded = decode_gzipped_tile(&fs::read(file).unwrap()).expect("tile must decode");
        let layer = &decoded.layers[0];
        assert_eq!(layer.name, "asset_point");
        // MVT GeomType POINT == 1.
        assert!(layer.geom_types.iter().all(|t| *t == 1));
        assert!(layer.keys.iter().any(|k| k == "assetId"));
    }
}
