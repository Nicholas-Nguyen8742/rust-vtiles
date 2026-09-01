//! Idempotency epic (Sequence 1) tests: identity, duplicate suppression,
//! optimistic concurrency, replay guardrails, and telemetry.
//!
//! Covers US-01..US-06 acceptance criteria that are observable in-process
//! (the HTTP layer's header handling is exercised through the same
//! `idempotency::*` functions the routes call).

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;

use vtile_core::model::{
    JobRecord, JobStatus, LayerCategory, LayerMetadataInput, ReplayAudit, SourceFormat, ZoomRange,
};
use vtile_ingest::normalize::{CrsPolicy, NormalizeOptions};
use vtile_core::config::{PropertyPolicy, TileConfig};
use vtile_pipeline::idempotency::{
    classify_ingest_event, event_dedupe_fingerprint, processing_profile_label,
    request_fingerprint, upload_idempotency_key, DedupeRecord, DedupeStore, EventDecision,
    FileDedupeStore, FileOrphanStore, IdempotencyMetrics, Metric, OrphanEvent,
};
use vtile_pipeline::job::{job_paths_for, run_job, JobDeps, RunJobInput};
use vtile_pipeline::quarantine::FileQuarantineStore;
use vtile_pipeline::store::{FileJobStore, FileLayerCatalog, JobStore, LayerCatalog};
use vtile_pipeline::{replay_job, ErrorReport, PipelineError, QuarantineStore, ReplayOptions};
use vtile_pipeline::events::NullEventEmitter;

const TENANT: &str = "tenant-acme";
const LAYER: &str = "us-parcels-nyc";

fn temp_root(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vtile-idem-{label}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn base_job(job_id: &str) -> JobRecord {
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

fn deps(root: &std::path::Path) -> JobDeps {
    JobDeps {
        jobs: Arc::new(FileJobStore::new(root.join("jobs")).unwrap()),
        catalog: Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap()),
        events: Arc::new(NullEventEmitter),
        quarantine: Some(Arc::new(FileQuarantineStore::new(root.join("quarantine")))),
    }
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

// ── US-01: idempotency key + registry ──────────────────────────────────────

#[test]
fn idempotency_keys_are_stable_and_tenant_scoped() {
    let key_a = upload_idempotency_key(TENANT, LAYER, "token-1", "parcel:10-16");
    let key_b = upload_idempotency_key(TENANT, LAYER, "token-1", "parcel:10-16");
    assert_eq!(key_a, key_b, "same inputs must produce the same key");
    assert!(key_a.starts_with("sha256:"));

    // A different tenant or token changes the key (no cross-tenant leakage).
    let other_tenant = upload_idempotency_key("tenant-other", LAYER, "token-1", "parcel:10-16");
    let other_token = upload_idempotency_key(TENANT, LAYER, "token-2", "parcel:10-16");
    assert_ne!(key_a, other_tenant);
    assert_ne!(key_a, other_token);
}

#[test]
fn conditional_create_rejects_duplicate_job_ids() {
    let root = temp_root("create-dup");
    let jobs = FileJobStore::new(root.join("jobs")).unwrap();
    let job = base_job("job_dup");
    jobs.create(job.clone()).unwrap();
    // US-01: attribute_not_exists(jobId) analog.
    let err = jobs.create(job).unwrap_err();
    assert!(matches!(err, PipelineError::JobAlreadyExists(id) if id == "job_dup"));
}

#[test]
fn find_by_idempotency_key_resolves_registered_job() {
    let root = temp_root("find-key");
    let jobs = FileJobStore::new(root.join("jobs")).unwrap();
    let key = upload_idempotency_key(TENANT, LAYER, "token-x", "parcel:10-16");
    let mut job = base_job("job_keyed");
    job.idempotency_key = Some(key.clone());
    job.request_fingerprint = Some("fp-a".to_string());
    jobs.create(job.clone()).unwrap();

    let found = jobs.find_by_idempotency_key(&key).unwrap().expect("resolved");
    assert_eq!(found.job_id, "job_keyed");
    assert!(jobs.find_by_idempotency_key("sha256:unknown").unwrap().is_none());
}

// ── US-02: payload mismatch ────────────────────────────────────────────────

#[test]
fn request_fingerprint_detects_payload_change() {
    let zoom = ZoomRange::new(10, 16);
    let fp_a = request_fingerprint("parcels.geojson", Some("application/geo+json"), SourceFormat::GeoJson, zoom, "parcel:10-16");
    let fp_same = request_fingerprint("parcels.geojson", Some("application/geo+json"), SourceFormat::GeoJson, zoom, "parcel:10-16");
    let fp_diff_file = request_fingerprint("other.geojson", Some("application/geo+json"), SourceFormat::GeoJson, zoom, "parcel:10-16");
    let fp_diff_zoom = request_fingerprint("parcels.geojson", Some("application/geo+json"), SourceFormat::GeoJson, ZoomRange::new(4, 12), "parcel:10-16");
    assert_eq!(fp_a, fp_same);
    assert_ne!(fp_a, fp_diff_file);
    assert_ne!(fp_a, fp_diff_zoom);
}

#[test]
fn processing_profile_label_is_deterministic() {
    let a = processing_profile_label(Some(LayerCategory::Parcel), ZoomRange::new(10, 16));
    let b = processing_profile_label(Some(LayerCategory::Parcel), ZoomRange::new(10, 16));
    let c = processing_profile_label(Some(LayerCategory::AssetPoint), ZoomRange::new(10, 16));
    assert_eq!(a, b);
    assert_ne!(a, c);
}

// ── US-03: duplicate event suppression ─────────────────────────────────────

#[test]
fn classify_ingest_event_decision_table() {
    let fp_seen = true;
    let fp_unseen = false;

    // No job → orphan.
    assert_eq!(classify_ingest_event(None, fp_unseen), EventDecision::Orphan);

    // Pending + unseen fingerprint → start exactly once.
    let mut pending = base_job("j");
    pending.status = JobStatus::UploadPending;
    assert_eq!(
        classify_ingest_event(Some(&pending), fp_unseen),
        EventDecision::StartRun
    );
    // Pending + already-seen fingerprint → suppress.
    assert_eq!(
        classify_ingest_event(Some(&pending), fp_seen),
        EventDecision::DuplicateSuppressed
    );

    // Active statuses → suppress.
    for status in [
        JobStatus::Queued,
        JobStatus::Validating,
        JobStatus::Normalizing,
        JobStatus::Tiling,
        JobStatus::Publishing,
    ] {
        let mut active = base_job("j");
        active.status = status;
        assert_eq!(
            classify_ingest_event(Some(&active), fp_unseen),
            EventDecision::DuplicateSuppressed,
            "status {status:?} must suppress"
        );
    }

    // Terminal statuses → acknowledge without new work.
    for status in [JobStatus::Completed, JobStatus::Failed, JobStatus::Cancelled] {
        let mut terminal = base_job("j");
        terminal.status = status;
        assert_eq!(
            classify_ingest_event(Some(&terminal), fp_unseen),
            EventDecision::TerminalAck,
            "status {status:?} must ack"
        );
    }
}

#[test]
fn event_fingerprint_is_stable() {
    let fp = event_dedupe_fingerprint(TENANT, LAYER, "staging/x/input/a.geojson", "etag1", "job_1");
    let fp2 = event_dedupe_fingerprint(TENANT, LAYER, "staging/x/input/a.geojson", "etag1", "job_1");
    let fp_diff = event_dedupe_fingerprint(TENANT, LAYER, "staging/x/input/a.geojson", "etag2", "job_1");
    assert_eq!(fp, fp2);
    assert_ne!(fp, fp_diff);
}

#[test]
fn dedupe_store_records_and_detects_seen_events() {
    let root = temp_root("dedupe");
    let store = FileDedupeStore::new(root.join("dedupe"));
    let rec = DedupeRecord {
        dedupe_key: "sha256:abc".to_string(),
        job_id: "job_1".to_string(),
        seen_at: Utc::now(),
        source_event_type: "UPLOAD_CONTENT".to_string(),
    };
    assert!(!store.seen("sha256:abc").unwrap());
    store.record(&rec).unwrap();
    assert!(store.seen("sha256:abc").unwrap());
    assert!(!store.seen("sha256:other").unwrap());
}

#[test]
fn orphan_events_are_recorded() {
    let root = temp_root("orphans");
    let store = FileOrphanStore::new(root.join("orphans"));
    let event = OrphanEvent {
        source_event_type: "UPLOAD_CONTENT".to_string(),
        object_key: "staging/unknown-job".to_string(),
        etag: None,
        tenant_id: None,
        reason: "no job record".to_string(),
        detected_at: Utc::now(),
    };
    let path = store.record(&event).unwrap();
    assert!(path.exists());
}

// ── US-04: optimistic state machine + worker lease ─────────────────────────

#[test]
fn state_machine_transition_table() {
    assert!(JobStatus::UploadPending.can_transition_to(JobStatus::Queued));
    assert!(JobStatus::Queued.can_transition_to(JobStatus::Validating));
    assert!(JobStatus::Publishing.can_transition_to(JobStatus::Completed));
    assert!(JobStatus::Failed.can_transition_to(JobStatus::Queued));
    assert!(JobStatus::Completed.can_transition_to(JobStatus::Queued));
    // Illegal edges.
    assert!(!JobStatus::UploadPending.can_transition_to(JobStatus::Completed));
    assert!(!JobStatus::Validating.can_transition_to(JobStatus::Publishing));
    assert!(!JobStatus::Completed.can_transition_to(JobStatus::Failed));
    assert!(!JobStatus::Cancelled.can_transition_to(JobStatus::Queued));
}

#[test]
fn lease_blocks_concurrent_worker_then_allows_takeover_after_expiry() {
    let root = temp_root("lease");
    let jobs = FileJobStore::new(root.join("jobs")).unwrap();
    let job = base_job("job_lease");
    jobs.create(job.clone()).unwrap();

    // First worker acquires the lease.
    let lease_a = jobs.acquire_lease("job_lease", "worker-a", 900).unwrap();
    assert!(!lease_a.lease_token.is_empty());

    // Second worker is rejected while the lease is active (US-04).
    let err = jobs.acquire_lease("job_lease", "worker-b", 900).unwrap_err();
    assert!(matches!(err, PipelineError::LeaseConflict(_)), "got: {err}");

    // Simulate a crash: force the lease into the past, then a takeover is
    // allowed and flagged via the expired counter.
    let before = IdempotencyMetrics::global().count(Metric::LeaseExpiredCount);
    let mut expired = jobs.get("job_lease").unwrap().unwrap();
    expired.lease_expires_at = Some(Utc::now() - chrono::Duration::seconds(10));
    jobs.upsert(expired).unwrap();
    let lease_b = jobs.acquire_lease("job_lease", "worker-b", 900).unwrap();
    assert_ne!(lease_b.lease_token, lease_a.lease_token);
    assert!(IdempotencyMetrics::global().count(Metric::LeaseExpiredCount) > before);
}

#[test]
fn transition_enforces_expected_status_and_bumps_version() {
    let root = temp_root("transition");
    let jobs = FileJobStore::new(root.join("jobs")).unwrap();
    let job = base_job("job_trans");
    jobs.create(job.clone()).unwrap();

    // Legal edge bumps stateVersion.
    let updated = jobs
        .transition("job_trans", None, JobStatus::Queued, JobStatus::Validating)
        .unwrap();
    assert_eq!(updated.status, JobStatus::Validating);
    assert_eq!(updated.state_version, 2);

    // Stale expectation → StateConflict.
    let err = jobs
        .transition("job_trans", None, JobStatus::Queued, JobStatus::Normalizing)
        .unwrap_err();
    assert!(matches!(err, PipelineError::StateConflict(_)), "got: {err}");

    // Illegal edge from the current status → StateConflict.
    let err = jobs
        .transition("job_trans", None, JobStatus::Validating, JobStatus::Completed)
        .unwrap_err();
    assert!(matches!(err, PipelineError::StateConflict(_)), "got: {err}");
}

#[test]
fn transition_rejects_foreign_lease_token() {
    let root = temp_root("lease-token");
    let jobs = FileJobStore::new(root.join("jobs")).unwrap();
    let job = base_job("job_lt");
    jobs.create(job.clone()).unwrap();
    let lease = jobs.acquire_lease("job_lt", "worker-a", 900).unwrap();

    // Correct token proceeds.
    jobs
        .transition("job_lt", Some(&lease.lease_token), JobStatus::Queued, JobStatus::Validating)
        .unwrap();
    // A different worker's token is rejected.
    let err = jobs
        .transition("job_lt", Some("lease_wrong"), JobStatus::Validating, JobStatus::Normalizing)
        .unwrap_err();
    assert!(matches!(err, PipelineError::LeaseConflict(_)), "got: {err}");
}

#[test]
fn run_job_acquires_and_releases_lease_and_bumps_version() {
    let root = temp_root("run-lease");
    let d = deps(&root);
    let jobs = d.jobs.clone();
    let job = base_job("job_run_lease");
    jobs.create(job.clone()).unwrap();

    let input = RunJobInput {
        job,
        source_bytes: tiny_parcel_geojson(),
        tile_config: TileConfig {
            layer_name: "parcel_boundary".to_string(),
            zoom_range: ZoomRange::new(12, 15),
            ..Default::default()
        },
        normalize_opts: NormalizeOptions {
            max_upload_bytes: 50 * 1024 * 1024,
            crs_policy: CrsPolicy::RequireKnown,
            property_policy: PropertyPolicy::default(),
            ..Default::default()
        },
        paths: job_paths_for(&root, TENANT, "job_run_lease", LAYER),
    };
    run_job(&input, &d).expect("run succeeds");

    let stored = jobs.get("job_run_lease").unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Completed);
    // Lease released on completion (US-04).
    assert!(stored.lease_token.is_none());
    assert!(stored.locked_by.is_none());
    // Version history advanced past creation.
    assert!(stored.state_version > 1);
}

// ── US-05: replay guardrails + audit ───────────────────────────────────────

#[test]
fn replay_of_failed_job_records_audit_and_completes() {
    let root = temp_root("replay-audit");
    let d = deps(&root);
    let jobs = d.jobs.clone();
    let store = FileQuarantineStore::new(root.join("quarantine"));

    let mut job = base_job("job_replay_ok");
    job.status = JobStatus::Failed;
    jobs.create(job.clone()).unwrap();
    let report = ErrorReport::from_job(&job, "UNKNOWN_CRS", "x", "NORMALIZING");
    store
        .quarantine(&job, &tiny_parcel_geojson(), &report)
        .unwrap();

    let opts = ReplayOptions {
        assume_wgs84: true,
        requested_by: "sre-user".to_string(),
        reason: "Transient Fargate timeout".to_string(),
        create_new_version: false,
    };
    replay_job(&d, &root, TENANT, "job_replay_ok", &opts).expect("replay succeeds");

    let stored = jobs.get("job_replay_ok").unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Completed);
    let audit: ReplayAudit = stored.replay_audit.expect("audit recorded");
    assert_eq!(audit.requested_by, "sre-user");
    assert_eq!(audit.reason, "Transient Fargate timeout");
    assert!(!audit.create_new_version);
}

#[test]
fn replay_rules_completed_and_active_jobs() {
    let root = temp_root("replay-rules");
    let d = deps(&root);
    let jobs = d.jobs.clone();
    let store = FileQuarantineStore::new(root.join("quarantine"));

    // COMPLETED without createNewVersion → rejected.
    let mut completed = base_job("job_done");
    completed.status = JobStatus::Completed;
    jobs.create(completed.clone()).unwrap();
    let report = ErrorReport::from_job(&completed, "X", "x", "TILING");
    store
        .quarantine(&completed, &tiny_parcel_geojson(), &report)
        .unwrap();
    let err = replay_job(&d, &root, TENANT, "job_done", &ReplayOptions::default()).unwrap_err();
    assert!(err.to_string().contains("createNewVersion"), "got: {err}");

    // Active (Queued) job → JOB_ALREADY_ACTIVE.
    let active = base_job("job_active");
    jobs.create(active.clone()).unwrap();
    let report = ErrorReport::from_job(&active, "X", "x", "NORMALIZING");
    store
        .quarantine(&active, &tiny_parcel_geojson(), &report)
        .unwrap();
    let err = replay_job(&d, &root, TENANT, "job_active", &ReplayOptions::default()).unwrap_err();
    assert!(matches!(err, PipelineError::JobAlreadyActive(_)), "got: {err}");
}

// ── US-06: telemetry ───────────────────────────────────────────────────────

#[test]
fn metrics_snapshot_tracks_counters() {
    let m = IdempotencyMetrics::global();
    let before = m.count(Metric::DuplicateEventsSuppressed);
    m.inc(Metric::DuplicateEventsSuppressed);
    m.inc(Metric::DuplicateEventsSuppressed);
    assert_eq!(m.count(Metric::DuplicateEventsSuppressed), before + 2);

    let snapshot = m.snapshot();
    assert!(snapshot.get("duplicate_events_suppressed").is_some());
    assert!(snapshot.get("lease_acquisition_conflict").is_some());
    assert!(snapshot.get("replay_requested_count").is_some());
    assert!(snapshot.get("orphan_events_detected").is_some());
}
