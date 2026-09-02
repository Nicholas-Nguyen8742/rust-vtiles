//! End-to-end pipeline tests: sample GeoJSON → normalize → MVT → publish.
//!
//! These exercise the full TRD §10 workflow in-process via `run_job`,
//! against the sample datasets in `examples/data/` and the fixture library
//! in `tests/fixtures/` (Recommendation 1 US-02), including the
//! validation-failure, quarantine, and replay paths (Recommendation 3).

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
use vtile_ingest::shapefile::write::sample_parcel_bundle;
use vtile_pipeline::events::{EventEmitter, NullEventEmitter, PipelineEvent};
use vtile_pipeline::job::{job_paths_for, run_job, JobDeps, JobPaths, RunJobInput};
use vtile_pipeline::quarantine::{FileQuarantineStore, INPUT_FILE_NAME, REPORT_FILE_NAME};
use vtile_pipeline::recovery::FileDlqStore;
use vtile_pipeline::store::{FileJobStore, FileLayerCatalog, JobStore, LayerCatalog};
use vtile_pipeline::{replay_job, ReplayOptions, ReplayOutcome, TileManifest};

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

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("tests")
        .join("fixtures")
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
        data_dir: root.to_path_buf(),
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
        error_code: None,
        failed_stage: None,
        error_class: None,
        replay_eligible: false,
        idempotency_key: None,
        trace_id: None,
        request_fingerprint: None,
        event_dedupe_fingerprint: None,
        state_version: 1,
        lease_token: None,
        locked_by: None,
        lease_expires_at: None,
        duplicate_event_count: 0,
        requested_tile_version: None,
        replay_audit: None,
        replay_count: 0,
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
        ..Default::default()
    }
}

fn tile_config() -> TileConfig {
    TileConfig {
        layer_name: "parcel_boundary".to_string(),
        zoom_range: ZoomRange::new(12, 15),
        ..Default::default()
    }
}

/// Full local service wiring, including the quarantine store.
fn deps(root: &Path) -> JobDeps {
    JobDeps {
        jobs: Arc::new(FileJobStore::new(root.join("jobs")).unwrap()),
        catalog: Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap()),
        events: Arc::new(NullEventEmitter),
        quarantine: Some(Arc::new(FileQuarantineStore::new(root.join("quarantine")))),
        dlq: Some(Arc::new(FileDlqStore::new(root.join("dlq")))),
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
        quarantine: None,
        dlq: None,
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
    assert!(stored.error_code.is_none());
    assert!(stored.failed_stage.is_none());
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

    // ── latest.json live pointer (Recommendation 2 US-04) ────────────────
    let latest_json = fs::read_to_string(input.paths.latest_path()).unwrap();
    let latest = TileManifest::from_json(&latest_json).unwrap();
    assert_eq!(latest.tile_version, manifest.tile_version);
    assert_eq!(latest.tile_count, manifest.tile_count);

    // ── Tiles on disk: versioned prefix, valid MVT v2 ────────────────────
    let version_root = input.paths.version_root(&outcome.tile_version);
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
        quarantine: None,
        dlq: None,
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

fn failed_event_codes(emitter: &Arc<CapturingEmitter>) -> Vec<String> {
    emitter
        .events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            PipelineEvent::VectorTileJobFailed { error_code, .. } => Some(error_code.clone()),
            _ => None,
        })
        .collect()
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
        quarantine: None,
        dlq: None,
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
    assert_eq!(stored.error_code.as_deref(), Some("EMPTY_DATASET"));
    assert_eq!(stored.failed_stage.as_deref(), Some("NORMALIZING"));

    // TRD §9: job.failed event emitted with the classified error code.
    assert_eq!(failed_event_codes(&emitter), vec!["EMPTY_DATASET".to_string()]);
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
        quarantine: None,
        dlq: None,
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
        data_dir: root.to_path_buf(),
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
    collect_pbf(&paths.version_root(&outcome.tile_version), &mut tile_files);
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

// ────────────────────────────────────────────────────────────────────────────
// Fixture-library tests (Recommendation 1 US-02, Recommendation 2/3 paths)
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn fixture_simple_parcels_end_to_end() {
    let root = temp_root("fx-parcels");
    let d = deps(&root);
    let jobs = d.jobs.clone();
    let catalog = d.catalog.clone();

    let job = parcel_job("job_fx_parcels");
    jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job,
        source_bytes: fs::read(fixture("simple-parcels.geojson")).unwrap(),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths: job_paths_for(&root, TENANT, "job_fx_parcels", LAYER),
    };

    let outcome = run_job(&input, &deps(&root)).expect("fixture job should succeed");
    assert_eq!(outcome.feature_count, 3);
    assert!(outcome.tile_count >= 4, "at least one tile per zoom 12..=15");

    // Tiles decode and carry CRE identifiers without PII.
    let mut tile_files = Vec::new();
    collect_pbf(
        &input.paths.version_root(&outcome.tile_version),
        &mut tile_files,
    );
    assert!(!tile_files.is_empty());
    for file in &tile_files {
        let decoded = decode_gzipped_tile(&fs::read(file).unwrap()).expect("tile must decode");
        let layer = &decoded.layers[0];
        assert_eq!(layer.name, "parcel_boundary");
        assert!(layer.geom_types.iter().all(|t| *t == 3));
        assert!(
            !layer.keys.iter().any(|k| k.eq_ignore_ascii_case("ownername")),
            "ownerName must be stripped before publication"
        );
    }

    // `latest.json` published atomically alongside the manifest.
    let latest = TileManifest::from_json(
        &fs::read_to_string(input.paths.latest_path()).unwrap(),
    )
    .unwrap();
    assert_eq!(latest.tile_version, outcome.tile_version);

    // Completed job has no failure markers.
    let stored = jobs.get("job_fx_parcels").unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Completed);
    assert!(stored.error.is_none());
    assert!(stored.failed_stage.is_none());
    let layer = catalog.get(LAYER).unwrap().unwrap();
    assert_eq!(layer.feature_count, 3);
}

#[test]
fn fixture_invalid_polygon_fails_and_quarantines() {
    let root = temp_root("fx-invalid");
    let d = deps(&root);
    let jobs = d.jobs.clone();
    let emitter = Arc::new(CapturingEmitter::default());

    let job = parcel_job("job_fx_invalid");
    jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job: job.clone(),
        source_bytes: fs::read(fixture("invalid-polygon.geojson")).unwrap(),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths: job_paths_for(&root, TENANT, "job_fx_invalid", LAYER),
    };

    let run_deps = JobDeps {
        jobs: d.jobs.clone(),
        catalog: d.catalog.clone(),
        events: emitter.clone(),
        quarantine: d.quarantine.clone(),
        dlq: d.dlq.clone(),
    };
    let err = run_job(&input, &run_deps).expect_err("invalid polygon must fail");
    assert!(err.to_string().contains("geometry"), "got: {err}");

    // Taxonomy + failed stage persisted on the job record.
    let stored = jobs.get("job_fx_invalid").unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Failed);
    assert_eq!(stored.error_code.as_deref(), Some("GEOMETRY_ERRORS"));
    assert_eq!(stored.failed_stage.as_deref(), Some("NORMALIZING"));
    assert_eq!(failed_event_codes(&emitter), vec!["GEOMETRY_ERRORS".to_string()]);

    // Quarantine (Recommendation 3 US-03): input + error report retained.
    let qdir = root.join("quarantine").join(TENANT).join("job_fx_invalid");
    assert!(qdir.join(INPUT_FILE_NAME).exists(), "input.bin quarantined");
    let report: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(qdir.join(REPORT_FILE_NAME)).unwrap()).unwrap();
    assert_eq!(report["errorCode"], "GEOMETRY_ERRORS");
    assert_eq!(report["failedStage"], "NORMALIZING");
    assert_eq!(report["jobId"], "job_fx_invalid");
    assert_eq!(report["tenantId"], TENANT);
}

#[test]
fn shapefile_bundle_end_to_end() {
    let root = temp_root("fx-shapefile");
    let d = deps(&root);
    let jobs = d.jobs.clone();
    let catalog = d.catalog.clone();

    let mut job = parcel_job("job_fx_shp");
    job.source_format = SourceFormat::Shapefile;
    jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job,
        source_bytes: sample_parcel_bundle(true, true),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths: job_paths_for(&root, TENANT, "job_fx_shp", LAYER),
    };

    let outcome = run_job(&input, &deps(&root)).expect("shapefile job should succeed");
    assert_eq!(outcome.feature_count, 3);
    assert!(outcome.tile_count >= 4);

    // Attributes survived the DBF round-trip into MVT keys.
    let mut tile_files = Vec::new();
    collect_pbf(
        &input.paths.version_root(&outcome.tile_version),
        &mut tile_files,
    );
    assert!(!tile_files.is_empty());
    let mut saw_parcel_id = false;
    for file in &tile_files {
        let decoded = decode_gzipped_tile(&fs::read(file).unwrap()).expect("tile must decode");
        let layer = &decoded.layers[0];
        assert!(layer.geom_types.iter().all(|t| *t == 3));
        if layer.keys.iter().any(|k| k == "PARCELID") {
            saw_parcel_id = true;
        }
    }
    assert!(saw_parcel_id, "PARCELID attribute must reach the tiles");

    let stored = jobs.get("job_fx_shp").unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Completed);
    let layer = catalog.get(LAYER).unwrap().unwrap();
    assert_eq!(layer.source_format, SourceFormat::Shapefile);
    assert!(!layer.assumed_crs, "prj present, CRS detected not assumed");
}

#[test]
fn missing_dbf_fails_with_component_error_and_quarantines() {
    let root = temp_root("fx-missing-dbf");
    let d = deps(&root);
    let jobs = d.jobs.clone();

    let mut job = parcel_job("job_fx_nodb");
    job.source_format = SourceFormat::Shapefile;
    jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job,
        source_bytes: sample_parcel_bundle(false, true),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths: job_paths_for(&root, TENANT, "job_fx_nodb", LAYER),
    };

    let err = run_job(&input, &deps(&root)).expect_err("missing .dbf must fail");
    assert!(err.to_string().contains(".dbf"), "got: {err}");

    // TRD §9 example: "Missing required .dbf file." → machine-readable code.
    let stored = jobs.get("job_fx_nodb").unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Failed);
    assert_eq!(
        stored.error_code.as_deref(),
        Some("MISSING_SHAPEFILE_COMPONENTS")
    );
    assert_eq!(stored.failed_stage.as_deref(), Some("NORMALIZING"));

    let qdir = root.join("quarantine").join(TENANT).join("job_fx_nodb");
    assert!(qdir.join(INPUT_FILE_NAME).exists());
    assert!(qdir.join(REPORT_FILE_NAME).exists());
}

#[test]
fn missing_prj_fails_then_replays_with_assumed_wgs84() {
    let root = temp_root("fx-replay");
    let d = deps(&root);
    let jobs = d.jobs.clone();
    let catalog = d.catalog.clone();
    let job_id = "job_fx_replay";

    let mut job = parcel_job(job_id);
    job.source_format = SourceFormat::Shapefile;
    jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job,
        source_bytes: sample_parcel_bundle(true, false), // no .prj
        tile_config: tile_config(),
        normalize_opts: normalize_opts(), // RequireKnown
        paths: job_paths_for(&root, TENANT, job_id, LAYER),
    };

    // 1. First attempt fails: unknown CRS requires explicit confirmation.
    let err = run_job(&input, &deps(&root)).expect_err("missing .prj must fail");
    assert!(err.to_string().contains("CRS") || err.to_string().contains(".prj"), "got: {err}");
    let stored = jobs.get(job_id).unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Failed);
    assert_eq!(stored.error_code.as_deref(), Some("UNKNOWN_CRS"));
    assert!(root
        .join("quarantine")
        .join(TENANT)
        .join(job_id)
        .join(INPUT_FILE_NAME)
        .exists());

    // 2. Replay with the user's WGS84 confirmation (Recommendation 3 US-03).
    let outcome = match replay_job(
        &deps(&root),
        &root,
        TENANT,
        job_id,
        &ReplayOptions {
            assume_wgs84: true,
            ..Default::default()
        },
    )
    .expect("replay with assume-wgs84 should succeed")
    {
        ReplayOutcome::Executed(outcome) => outcome,
        ReplayOutcome::NoOp { reason } => panic!("expected replay to execute, got no-op: {reason}"),
    };
    assert_eq!(outcome.feature_count, 3);

    let stored = jobs.get(job_id).unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Completed);
    assert!(stored.error.is_none());
    assert!(stored.error_code.is_none());
    assert!(stored.failed_stage.is_none());
    // Sequence 3 US-04: the replay attempt is counted.
    assert_eq!(stored.replay_count, 1);

    // Replay published a fresh tile version and swapped the live pointer.
    let latest = TileManifest::from_json(
        &fs::read_to_string(input.paths.latest_path()).unwrap(),
    )
    .unwrap();
    assert_eq!(latest.tile_version, outcome.tile_version);
    let layer = catalog.get(LAYER).unwrap().unwrap();
    assert!(layer.assumed_crs, "assumed CRS must be flagged in metadata");
}

#[test]
fn replay_rejects_non_failed_and_unknown_jobs() {
    let root = temp_root("fx-replay-guard");
    let d = deps(&root);

    // Unknown job → descriptive error, not a panic.
    let err = replay_job(&deps(&root), &root, TENANT, "job_missing", &ReplayOptions::default())
        .expect_err("unknown job must fail");
    assert!(err.to_string().contains("not found"), "got: {err}");

    // A QUEUED (non-FAILED) job cannot be replayed even if quarantine data
    // exists for its id — Sequence 1 US-05 JOB_ALREADY_ACTIVE.
    let job = parcel_job("job_fx_guard");
    d.jobs.create(job.clone()).unwrap();
    let store = FileQuarantineStore::new(root.join("quarantine"));
    let report = vtile_pipeline::ErrorReport::from_job(&job, "UNKNOWN_CRS", "x", "NORMALIZING");
    store
        .quarantine(&job, b"{}", &report)
        .expect("quarantine write");
    use vtile_pipeline::QuarantineStore;
    let err = replay_job(&deps(&root), &root, TENANT, "job_fx_guard", &ReplayOptions::default())
        .expect_err("queued job must not replay");
    assert!(
        matches!(err, vtile_pipeline::PipelineError::JobAlreadyActive(_)),
        "got: {err}"
    );
}

#[test]
fn feature_count_cap_enforced_before_tiling() {
    let root = temp_root("fx-cap");
    let d = deps(&root);
    let jobs = d.jobs.clone();

    let job = parcel_job("job_fx_cap");
    jobs.create(job.clone()).unwrap();
    let mut opts = normalize_opts();
    opts.max_features = 2; // fixture has 3 features
    let input = RunJobInput {
        job,
        source_bytes: fs::read(fixture("simple-parcels.geojson")).unwrap(),
        tile_config: tile_config(),
        normalize_opts: opts,
        paths: job_paths_for(&root, TENANT, "job_fx_cap", LAYER),
    };

    let err = run_job(&input, &deps(&root)).expect_err("over-cap dataset must fail");
    assert!(err.to_string().contains("exceeds limit"), "got: {err}");
    let stored = jobs.get("job_fx_cap").unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Failed);
    assert_eq!(stored.error_code.as_deref(), Some("FILE_TOO_LARGE"));
}
