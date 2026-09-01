//! Atomic publishing tests (Sequence 2 epic):
//!
//! * US-AP-01 — versioned candidate staging (in-progress output isolated)
//! * US-AP-02 — completeness verification (count, zero-byte, checksum gates)
//! * US-AP-03 — conditional promotion (one winner, previous stays active)
//! * US-AP-05 — rollback (restore, idempotency, invalid targets)
//! * US-AP-06 — publish/rollback audit trail + events

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::Utc;

use vtile_core::config::{PropertyPolicy, TileConfig};
use vtile_core::model::{
    JobRecord, JobStatus, LayerCategory, LayerMetadataInput, SourceFormat, ZoomRange,
};
use vtile_ingest::normalize::{CrsPolicy, NormalizeOptions};
use vtile_pipeline::events::{EventEmitter, NullEventEmitter, PipelineEvent};
use vtile_pipeline::job::{job_paths_for, run_job, JobDeps, JobOutcome, RunJobInput};
use vtile_pipeline::publish::{
    aggregate_checksum, read_candidate_manifest, rollback_layer_version, verify_candidate,
    version_root, AuditAction, FileAuditLog, FileLayerRegistry, PublishMetric, PublishMetrics,
    PublishStatus, TileEntry, PIPELINE_ACTOR,
};
use vtile_pipeline::quarantine::FileQuarantineStore;
use vtile_pipeline::store::{FileJobStore, FileLayerCatalog};
use vtile_pipeline::PipelineError;

const TENANT: &str = "tenant-acme";
const LAYER: &str = "us-parcels-nyc";

fn temp_root(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vtile-pub-{label}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn tiny_parcel_geojson() -> Vec<u8> {
    br#"{
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "id": 1,
          "geometry": {
            "type": "Polygon",
            "coordinates": [[
              [-73.98521, 40.75293], [-73.98432, 40.75302],
              [-73.98441, 40.75368], [-73.98521, 40.75293]
            ]]
          },
          "properties": { "parcelId": "NYC-FX-00001", "market": "New York" }
        }
      ]
    }"#
    .to_vec()
}

fn base_job(job_id: &str) -> JobRecord {
    JobRecord {
        job_id: job_id.to_string(),
        tenant_id: TENANT.to_string(),
        layer_id: LAYER.to_string(),
        status: JobStatus::Queued,
        source_format: SourceFormat::GeoJson,
        source_uri: format!("mem://{LAYER}"),
        requested_zoom_range: ZoomRange::new(12, 14),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        error: None,
        error_code: None,
        failed_stage: None,
        idempotency_key: None,
        request_fingerprint: None,
        event_dedupe_fingerprint: None,
        state_version: 1,
        lease_token: None,
        locked_by: None,
        lease_expires_at: None,
        duplicate_event_count: 0,
        requested_tile_version: None,
        replay_audit: None,
        outcome: None,
        layer_input: Some(LayerMetadataInput {
            name: Some("NYC Parcels".to_string()),
            description: None,
            category: Some(LayerCategory::Parcel),
            tags: vec![],
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
        zoom_range: ZoomRange::new(12, 14),
        ..Default::default()
    }
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

fn deps(root: &Path, emitter: Arc<dyn EventEmitter>) -> JobDeps {
    JobDeps {
        jobs: Arc::new(FileJobStore::new(root.join("jobs")).unwrap()),
        catalog: Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap()),
        events: emitter,
        quarantine: Some(Arc::new(FileQuarantineStore::new(root.join("quarantine")))),
    }
}

/// Runs one happy-path publish for the tiny parcel fixture.
fn run_fixture(root: &Path, job_id: &str) -> JobOutcome {
    let d = deps(root, Arc::new(NullEventEmitter));
    run_fixture_with(root, job_id, &d)
}

fn run_fixture_with(root: &Path, job_id: &str, d: &JobDeps) -> JobOutcome {
    let job = base_job(job_id);
    d.jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job,
        source_bytes: tiny_parcel_geojson(),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths: job_paths_for(root, TENANT, job_id, LAYER),
    };
    run_job(&input, d).expect("fixture publish should succeed")
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

// ── US-AP-01: versioned candidate staging ───────────────────────────────────

#[test]
fn publish_isolates_candidates_and_writes_registry_latest_success_audit() {
    let root = temp_root("isolation");
    let emitter = Arc::new(CapturingEmitter::default());
    let d = deps(&root, emitter.clone());
    let job_id = "job_pub_iso";
    let outcome = run_fixture_with(&root, job_id, &d);
    let paths = job_paths_for(&root, TENANT, job_id, LAYER);

    // Tiles live ONLY under the immutable version path (US-AP-01).
    let top: Vec<String> = fs::read_dir(&paths.tiles_root)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert_eq!(top, vec!["versions".to_string()]);

    let vroot = version_root(&paths.tiles_root, &outcome.tile_version);
    let mut tiles = Vec::new();
    collect_pbf(&vroot, &mut tiles);
    assert!(!tiles.is_empty());

    // Candidate manifest recorded with integrity metadata (US-AP-02).
    let candidate = read_candidate_manifest(&paths.tiles_root, &outcome.tile_version)
        .unwrap()
        .expect("candidate manifest written");
    assert_eq!(candidate.status, "CANDIDATE");
    assert_eq!(candidate.source_job_id, job_id);
    assert_eq!(candidate.checksum_algorithm, "SHA-256");
    assert_eq!(candidate.tile_count, outcome.tile_count);
    assert!(candidate.aggregate_checksum.starts_with("sha256:"));

    // Authoritative registry pointer promoted (US-AP-03).
    let record = FileLayerRegistry::new(&paths.manifests_root)
        .get()
        .unwrap()
        .expect("publication.json written");
    assert_eq!(record.current_tile_version, outcome.tile_version);
    assert_eq!(record.publish_status, PublishStatus::Published);
    assert_eq!(record.published_by, PIPELINE_ACTOR);
    assert!(record.previous_tile_version.is_none());

    // Compatibility pointers + completion marker.
    let latest: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(paths.latest_path()).unwrap()).unwrap();
    assert_eq!(latest["tileVersion"], outcome.tile_version);
    assert!(vroot.join("_SUCCESS").exists());

    // Audit trail + promoted event (US-AP-06).
    let audit = FileAuditLog::new(&paths.manifests_root).entries().unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, AuditAction::Publish);
    assert_eq!(audit[0].source_job_id.as_deref(), Some(job_id));
    assert_eq!(audit[0].to_tile_version, outcome.tile_version);
    assert_eq!(audit[0].actor, PIPELINE_ACTOR);
    let promoted: Vec<_> = emitter
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| matches!(e, PipelineEvent::VectorTileVersionPromoted { .. }))
        .collect();
    assert_eq!(promoted.len(), 1);
}

// ── US-AP-02: completeness verification gates ───────────────────────────────

#[test]
fn verify_candidate_detects_missing_tiles() {
    let root = temp_root("verify-missing");
    let outcome = run_fixture(&root, "job_pub_missing");
    let paths = job_paths_for(&root, TENANT, "job_pub_missing", LAYER);

    verify_candidate(&paths.tiles_root, &outcome.tile_version).expect("valid before tampering");

    // Remove one tile → count mismatch blocks promotion.
    let vroot = version_root(&paths.tiles_root, &outcome.tile_version);
    let mut tiles = Vec::new();
    collect_pbf(&vroot, &mut tiles);
    fs::remove_file(&tiles[0]).unwrap();
    let err = verify_candidate(&paths.tiles_root, &outcome.tile_version).unwrap_err();
    assert!(err.to_string().contains("tile count mismatch"), "got: {err}");
}

#[test]
fn verify_candidate_detects_zero_byte_tiles() {
    let root = temp_root("verify-zero");
    let outcome = run_fixture(&root, "job_pub_zero");
    let paths = job_paths_for(&root, TENANT, "job_pub_zero", LAYER);
    let vroot = version_root(&paths.tiles_root, &outcome.tile_version);
    let mut tiles = Vec::new();
    collect_pbf(&vroot, &mut tiles);

    // Truncate one tile to zero bytes → blocked.
    fs::write(&tiles[0], []).unwrap();
    let err = verify_candidate(&paths.tiles_root, &outcome.tile_version).unwrap_err();
    assert!(err.to_string().contains("zero-byte"), "got: {err}");
}

#[test]
fn verify_candidate_detects_checksum_mismatch() {
    let root = temp_root("verify-corrupt");
    let outcome = run_fixture(&root, "job_pub_corrupt");
    let paths = job_paths_for(&root, TENANT, "job_pub_corrupt", LAYER);
    let vroot = version_root(&paths.tiles_root, &outcome.tile_version);
    let mut tiles = Vec::new();
    collect_pbf(&vroot, &mut tiles);

    // Corrupt (non-zero-length) tile content → aggregate checksum mismatch.
    let mut bytes = fs::read(&tiles[0]).unwrap();
    bytes.extend_from_slice(b"corruption");
    fs::write(&tiles[0], &bytes).unwrap();
    let err = verify_candidate(&paths.tiles_root, &outcome.tile_version).unwrap_err();
    assert!(err.to_string().contains("checksum"), "got: {err}");

    // Classification maps to the publish taxonomy (docs/ERRORS.md).
    let (code, _) = vtile_pipeline::job::error_classification(&err);
    assert_eq!(code, "PUBLISH_VALIDATION_FAILED");
}

#[test]
fn aggregate_checksum_is_stable_and_content_sensitive() {
    let a = TileEntry { rel_path: "12/1/1.pbf".into(), sha256: "sha256:aa".into(), len: 10 };
    let b = TileEntry { rel_path: "13/2/2.pbf".into(), sha256: "sha256:bb".into(), len: 20 };
    let sum_ab = aggregate_checksum(&[a.clone(), b.clone()]);
    // Order-independent (entries are canonicalized by rel_path).
    assert_eq!(sum_ab, aggregate_checksum(&[b.clone(), a.clone()]));
    // Content-sensitive.
    let mut b2 = b.clone();
    b2.len = 21;
    assert_ne!(sum_ab, aggregate_checksum(&[a, b2]));
}

// ── US-AP-03: conditional promotion ─────────────────────────────────────────

#[test]
fn promotion_is_conditional_only_one_winner() {
    let dir = temp_root("promote");
    let registry = FileLayerRegistry::new(&dir);

    let before = PublishMetrics::global().count(PublishMetric::PromotionConflicts);

    // First publish: no previous version.
    let r1 = registry.promote(TENANT, LAYER, None, "v1", "tester").unwrap();
    assert_eq!(r1.current_tile_version, "v1");

    // Second publish conditioned on v1.
    let r2 = registry.promote(TENANT, LAYER, Some("v1"), "v2", "tester").unwrap();
    assert_eq!(r2.current_tile_version, "v2");
    assert_eq!(r2.previous_tile_version.as_deref(), Some("v1"));

    // A stale publisher still expecting v1 loses (concurrent race loser).
    let err = registry
        .promote(TENANT, LAYER, Some("v1"), "v3", "tester")
        .unwrap_err();
    assert!(matches!(err, PipelineError::PromotionConflict(_)), "got: {err}");

    // v2 remains active — the failed promotion changed nothing.
    let current = registry.get().unwrap().unwrap();
    assert_eq!(current.current_tile_version, "v2");
    assert!(PublishMetrics::global().count(PublishMetric::PromotionConflicts) > before);
}

#[test]
fn second_publish_advances_pointer_and_retains_previous_version() {
    let root = temp_root("advance");
    let v1 = run_fixture(&root, "job_adv_1").tile_version;
    let v2 = run_fixture(&root, "job_adv_2").tile_version;
    assert_ne!(v1, v2);

    let paths = job_paths_for(&root, TENANT, "job_adv_2", LAYER);
    let record = FileLayerRegistry::new(&paths.manifests_root).get().unwrap().unwrap();
    assert_eq!(record.current_tile_version, v2);
    assert_eq!(record.previous_tile_version.as_deref(), Some(v1.as_str()));

    // Both version directories retained (rollback targets, TRD §6 90 days).
    assert!(version_root(&paths.tiles_root, &v1).exists());
    assert!(version_root(&paths.tiles_root, &v2).exists());

    // Audit chain: second publish records its predecessor.
    let audit = FileAuditLog::new(&paths.manifests_root).entries().unwrap();
    assert_eq!(audit.len(), 2);
    assert_eq!(audit[1].from_tile_version.as_deref(), Some(v1.as_str()));
    assert_eq!(audit[1].to_tile_version, v2);
}

// ── US-AP-05: rollback ──────────────────────────────────────────────────────

#[test]
fn rollback_restores_previous_version_and_is_audited() {
    let root = temp_root("rollback");
    let emitter = Arc::new(CapturingEmitter::default());
    let d = deps(&root, emitter.clone());
    let v1 = run_fixture_with(&root, "job_rb_1", &d).tile_version;
    let v2 = run_fixture_with(&root, "job_rb_2", &d).tile_version;
    let paths = job_paths_for(&root, TENANT, "job_rb_2", LAYER);

    let record = rollback_layer_version(
        TENANT,
        LAYER,
        &paths.tiles_root,
        &paths.manifests_root,
        &v1,
        "Parcel boundaries misaligned after vendor refresh",
        "sre:oncall",
        emitter.as_ref(),
    )
    .expect("rollback succeeds");

    assert_eq!(record.current_tile_version, v1);
    assert_eq!(record.previous_tile_version.as_deref(), Some(v2.as_str()));
    assert_eq!(record.publish_status, PublishStatus::RolledBack);
    assert!(record
        .rollback_reason
        .as_deref()
        .unwrap()
        .contains("misaligned"));
    assert_eq!(record.rolled_back_by.as_deref(), Some("sre:oncall"));

    // Readers resolve the rolled-back version immediately.
    let latest: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(paths.latest_path()).unwrap()).unwrap();
    assert_eq!(latest["tileVersion"], v1);

    // Audit: PUBLISH, PUBLISH, ROLLBACK with actor + reason.
    let audit = FileAuditLog::new(&paths.manifests_root).entries().unwrap();
    assert_eq!(audit.len(), 3);
    let rb = &audit[2];
    assert_eq!(rb.action, AuditAction::Rollback);
    assert_eq!(rb.from_tile_version.as_deref(), Some(v2.as_str()));
    assert_eq!(rb.to_tile_version, v1);
    assert_eq!(rb.actor, "sre:oncall");

    // Event emitted for observability.
    let rolled_back: Vec<_> = emitter
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| matches!(e, PipelineEvent::VectorTileVersionRolledBack { .. }))
        .collect();
    assert_eq!(rolled_back.len(), 1);
}

#[test]
fn rollback_is_idempotent_and_rejects_invalid_targets() {
    let root = temp_root("rb-guard");
    let v1 = run_fixture(&root, "job_rbg").tile_version;
    let paths = job_paths_for(&root, TENANT, "job_rbg", LAYER);
    let noop = NullEventEmitter;

    // Rolling back to the already-current version is a no-op.
    let before = FileAuditLog::new(&paths.manifests_root).entries().unwrap().len();
    let record = rollback_layer_version(
        TENANT, LAYER, &paths.tiles_root, &paths.manifests_root,
        &v1, "noop", "sre:oncall", &noop,
    )
    .unwrap();
    assert_eq!(record.current_tile_version, v1);
    assert_eq!(record.publish_status, PublishStatus::Published); // unchanged
    assert_eq!(
        FileAuditLog::new(&paths.manifests_root).entries().unwrap().len(),
        before,
        "idempotent rollback must not append audit entries"
    );

    // Unknown target version → rejected.
    let err = rollback_layer_version(
        TENANT, LAYER, &paths.tiles_root, &paths.manifests_root,
        "no-such-version", "x", "sre:oncall", &noop,
    )
    .unwrap_err();
    assert!(matches!(err, PipelineError::RollbackFailed(_)), "got: {err}");
    assert!(err.to_string().contains("not found"), "got: {err}");

    // Layer with no publication at all → rejected.
    let empty = temp_root("rb-empty");
    let err = rollback_layer_version(
        TENANT, LAYER, &empty.join("tiles"), &empty.join("manifests"),
        &v1, "x", "sre:oncall", &noop,
    )
    .unwrap_err();
    assert!(matches!(err, PipelineError::RollbackFailed(_)), "got: {err}");
}
