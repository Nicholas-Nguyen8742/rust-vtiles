//! DLQ and Replay tests (Sequence 3 epic):
//!
//! * US-01 — retry policy + dead-letter capture with full failure context
//! * US-02 — enriched quarantine reports (class, eligibility, remediation)
//! * US-03 — error classification + replay eligibility enforcement
//! * US-04 — bounded manual replays
//! * US-05 — idempotent replay, no-op semantics, DLQ removal on success

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;

use vtile_core::config::{PropertyPolicy, TileConfig};
use vtile_core::model::{
    JobRecord, JobStatus, LayerCategory, LayerMetadataInput, SourceFormat, ZoomRange,
};
use vtile_ingest::normalize::{CrsPolicy, NormalizeOptions};
use vtile_ingest::shapefile::write::sample_parcel_bundle;
use vtile_pipeline::events::{EventEmitter, PipelineEvent};
use vtile_pipeline::job::{job_paths_for, JobDeps, RunJobInput};
use vtile_pipeline::recovery::{
    classify_code, replay_eligible, run_job_with_retries, ErrorClass, FileDlqStore, RetryPolicy,
    DlqStore, MAX_MANUAL_REPLAYS,
};
use vtile_pipeline::store::{FileJobStore, FileLayerCatalog, JobStore, LayerCatalog};
use vtile_pipeline::{
    replay_job, ErrorReport, FileQuarantineStore, Lease, PipelineError, QuarantineStore,
    ReplayOptions, ReplayOutcome,
};

const TENANT: &str = "tenant-acme";
const LAYER: &str = "us-parcels-nyc";

fn temp_root(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vtile-rec-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
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
        requested_zoom_range: ZoomRange::new(12, 14),
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

impl CapturingEmitter {
    fn count_type(&self, event_type: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.event_type() == event_type)
            .count()
    }
}

fn deps(root: &Path, emitter: Arc<CapturingEmitter>) -> JobDeps {
    JobDeps {
        jobs: Arc::new(FileJobStore::new(root.join("jobs")).unwrap()),
        catalog: Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap()),
        events: emitter,
        quarantine: Some(Arc::new(FileQuarantineStore::new(root.join("quarantine")))),
        dlq: Some(Arc::new(FileDlqStore::new(root.join("dlq")))),
    }
}

/// A `JobStore` that fails the first `failures` conditional transitions with
/// a simulated *transient* store error, then delegates — deterministic
/// stand-in for the flaky infrastructure that drives retry/DLQ behavior.
struct FailingJobStore {
    inner: FileJobStore,
    remaining_failures: AtomicUsize,
}

impl FailingJobStore {
    fn new(inner: FileJobStore, failures: usize) -> Self {
        Self {
            inner,
            remaining_failures: AtomicUsize::new(failures),
        }
    }
}

impl JobStore for FailingJobStore {
    fn create(&self, job: JobRecord) -> vtile_pipeline::PipelineResult<()> {
        self.inner.create(job)
    }
    fn update_status(
        &self,
        job_id: &str,
        status: JobStatus,
        error: Option<String>,
    ) -> vtile_pipeline::PipelineResult<()> {
        self.inner.update_status(job_id, status, error)
    }
    fn upsert(&self, job: JobRecord) -> vtile_pipeline::PipelineResult<()> {
        self.inner.upsert(job)
    }
    fn get(&self, job_id: &str) -> vtile_pipeline::PipelineResult<Option<JobRecord>> {
        self.inner.get(job_id)
    }
    fn list(&self) -> vtile_pipeline::PipelineResult<Vec<JobRecord>> {
        self.inner.list()
    }
    fn find_by_idempotency_key(
        &self,
        key: &str,
    ) -> vtile_pipeline::PipelineResult<Option<JobRecord>> {
        self.inner.find_by_idempotency_key(key)
    }
    fn transition(
        &self,
        job_id: &str,
        lease_token: Option<&str>,
        expected: JobStatus,
        next: JobStatus,
    ) -> vtile_pipeline::PipelineResult<JobRecord> {
        let should_fail = self
            .remaining_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1))
            .is_ok();
        if should_fail {
            return Err(PipelineError::Store(
                "simulated transient store failure".to_string(),
            ));
        }
        self.inner.transition(job_id, lease_token, expected, next)
    }
    fn acquire_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        lease_secs: u64,
    ) -> vtile_pipeline::PipelineResult<Lease> {
        self.inner.acquire_lease(job_id, worker_id, lease_secs)
    }
    fn note_duplicate_event(&self, job_id: &str) -> vtile_pipeline::PipelineResult<()> {
        self.inner.note_duplicate_event(job_id)
    }
}

// ── US-03: classification + eligibility ─────────────────────────────────────

#[test]
fn classification_matrix_is_deterministic() {
    assert_eq!(classify_code("PROCESSING_TIMEOUT"), ErrorClass::Transient);
    assert_eq!(classify_code("PROMOTION_CONFLICT"), ErrorClass::Transient);
    assert_eq!(classify_code("INTERNAL_ERROR"), ErrorClass::Transient);
    assert_eq!(
        classify_code("MISSING_SHAPEFILE_COMPONENTS"),
        ErrorClass::PermanentValidation
    );
    assert_eq!(classify_code("EMPTY_DATASET"), ErrorClass::PermanentValidation);
    assert_eq!(classify_code("UNKNOWN_CRS"), ErrorClass::PermanentValidation);
    assert_eq!(classify_code("PIPELINE_ERROR"), ErrorClass::ManualReview);
    assert_eq!(classify_code("SOMETHING_NEW"), ErrorClass::ManualReview);

    // Replay eligibility (server-side enforced): transient + UNKNOWN_CRS
    // (with WGS84 confirmation) replay; everything else does not.
    assert!(replay_eligible(Some("PROCESSING_TIMEOUT")));
    assert!(replay_eligible(Some("UNKNOWN_CRS")));
    assert!(!replay_eligible(Some("EMPTY_DATASET")));
    assert!(!replay_eligible(Some("PIPELINE_ERROR")));
    assert!(!replay_eligible(None));
}

#[test]
fn retry_policy_bounds_and_schedule() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.max_receives, 4);
    assert_eq!(policy.retry_delay_secs, vec![0, 30, 60, 120]);
    assert!(!policy.exhausted(3));
    assert!(policy.exhausted(4));
    assert_eq!(policy.next_delay_secs(0), 0);
    assert_eq!(policy.next_delay_secs(1), 30);
    assert_eq!(policy.next_delay_secs(3), 120);
    assert_eq!(policy.next_delay_secs(99), 120);
}

// ── US-01/US-02: permanent failure → DLQ + enriched quarantine ─────────────

#[test]
fn permanent_validation_failure_is_dead_lettered_without_retry() {
    let root = temp_root("permanent");
    let emitter = Arc::new(CapturingEmitter::default());
    let d = deps(&root, emitter.clone());

    let mut job = base_job("job_perm");
    job.source_format = SourceFormat::Shapefile;
    d.jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job,
        // Missing .dbf → MISSING_SHAPEFILE_COMPONENTS (permanent).
        source_bytes: sample_parcel_bundle(false, true),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths: job_paths_for(&root, TENANT, "job_perm", LAYER),
    };

    let err = run_job_with_retries(&input, &d, &RetryPolicy::default())
        .expect_err("missing .dbf must fail");
    assert!(err.to_string().contains(".dbf"), "got: {err}");

    // No retries for permanent failures.
    assert_eq!(emitter.count_type("vector.tile.job.retry_scheduled"), 0);

    // Job record carries the Sequence 3 taxonomy.
    let stored = d.jobs.get("job_perm").unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Failed);
    assert_eq!(stored.error_code.as_deref(), Some("MISSING_SHAPEFILE_COMPONENTS"));
    assert_eq!(stored.error_class.as_deref(), Some("PERMANENT_VALIDATION"));
    assert!(!stored.replay_eligible);

    // DLQ record captured with full context (US-01).
    let dlq = FileDlqStore::new(root.join("dlq"));
    let record = dlq.get(TENANT, "job_perm").unwrap().expect("dead-lettered");
    assert_eq!(record.error_code, "MISSING_SHAPEFILE_COMPONENTS");
    assert_eq!(record.error_class, ErrorClass::PermanentValidation);
    assert_eq!(record.failed_stage, "NORMALIZING");
    assert_eq!(record.retry_count, 1);
    assert!(!record.replay_eligible);
    assert_eq!(emitter.count_type("vector.tile.job.dead-lettered"), 1);

    // Quarantine report enriched with class/eligibility/remediation (US-02).
    let qdir = root.join("quarantine").join(TENANT).join("job_perm");
    let report: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(qdir.join("error-report.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(report["errorClass"], "PERMANENT_VALIDATION");
    assert_eq!(report["replayEligible"], false);
    assert!(report["remediation"].as_str().unwrap().contains(".shp"));
    assert!(report["quarantineUri"].as_str().unwrap().contains("input.bin"));
    assert_eq!(emitter.count_type("vector.tile.job.quarantined"), 1);
}

// ── US-01: transient failures retry, then succeed or exhaust ────────────────

#[test]
fn transient_failure_retries_then_succeeds() {
    let root = temp_root("transient-ok");
    let emitter = Arc::new(CapturingEmitter::default());
    let jobs = Arc::new(FailingJobStore::new(
        FileJobStore::new(root.join("jobs")).unwrap(),
        1, // first transition fails, then the store recovers
    ));
    let d = JobDeps {
        jobs: jobs.clone(),
        catalog: Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap()),
        events: emitter.clone(),
        quarantine: Some(Arc::new(FileQuarantineStore::new(root.join("quarantine")))),
        dlq: Some(Arc::new(FileDlqStore::new(root.join("dlq")))),
    };

    let job = base_job("job_transient_ok");
    d.jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job,
        source_bytes: tiny_parcel_geojson(),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths: job_paths_for(&root, TENANT, "job_transient_ok", LAYER),
    };

    // Zero-delay test policy keeps the suite fast.
    let policy = RetryPolicy {
        max_receives: 3,
        retry_delay_secs: vec![0, 0],
    };
    let outcome = run_job_with_retries(&input, &d, &policy).expect("retry recovers the job");
    assert_eq!(outcome.feature_count, 1);

    // Exactly one retry was scheduled, nothing was dead-lettered.
    assert_eq!(emitter.count_type("vector.tile.job.retry_scheduled"), 1);
    assert_eq!(emitter.count_type("vector.tile.job.dead-lettered"), 0);
    assert!(FileDlqStore::new(root.join("dlq"))
        .get(TENANT, "job_transient_ok")
        .unwrap()
        .is_none());
}

#[test]
fn transient_failure_exhausts_into_dlq() {
    let root = temp_root("transient-dlq");
    let emitter = Arc::new(CapturingEmitter::default());
    let jobs = Arc::new(FailingJobStore::new(
        FileJobStore::new(root.join("jobs")).unwrap(),
        usize::MAX, // the store never recovers
    ));
    let d = JobDeps {
        jobs: jobs.clone(),
        catalog: Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap()),
        events: emitter.clone(),
        quarantine: Some(Arc::new(FileQuarantineStore::new(root.join("quarantine")))),
        dlq: Some(Arc::new(FileDlqStore::new(root.join("dlq")))),
    };

    let job = base_job("job_transient_dlq");
    d.jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job,
        source_bytes: tiny_parcel_geojson(),
        tile_config: tile_config(),
        normalize_opts: normalize_opts(),
        paths: job_paths_for(&root, TENANT, "job_transient_dlq", LAYER),
    };

    let policy = RetryPolicy {
        max_receives: 2,
        retry_delay_secs: vec![0],
    };
    let err = run_job_with_retries(&input, &d, &policy).expect_err("store never recovers");
    assert!(matches!(err, PipelineError::Store(_)), "got: {err}");

    // One retry was scheduled, then retries exhausted → DLQ.
    assert_eq!(emitter.count_type("vector.tile.job.retry_scheduled"), 1);
    let record = FileDlqStore::new(root.join("dlq"))
        .get(TENANT, "job_transient_dlq")
        .unwrap()
        .expect("dead-lettered after exhaustion");
    assert_eq!(record.error_code, "STORE_ERROR");
    assert_eq!(record.error_class, ErrorClass::Transient);
    assert_eq!(record.retry_count, 2);
    assert!(record.replay_eligible, "transient failures stay replay-eligible");
    assert_eq!(emitter.count_type("vector.tile.job.dead-lettered"), 1);
}

// ── US-03/US-04: replay guardrails ──────────────────────────────────────────

#[test]
fn replay_denied_for_permanent_failure() {
    let root = temp_root("replay-denied");
    let emitter = Arc::new(CapturingEmitter::default());
    let d = deps(&root, emitter.clone());
    let store = FileQuarantineStore::new(root.join("quarantine"));

    let mut job = base_job("job_denied");
    job.status = JobStatus::Failed;
    job.error_code = Some("EMPTY_DATASET".to_string());
    job.error_class = Some("PERMANENT_VALIDATION".to_string());
    d.jobs.create(job.clone()).unwrap();
    let report = ErrorReport::from_job(&job, "EMPTY_DATASET", "empty", "NORMALIZING");
    store
        .quarantine(&job, &tiny_parcel_geojson(), &report)
        .unwrap();

    let err = replay_job(&d, &root, TENANT, "job_denied", &ReplayOptions::default())
        .expect_err("permanent failures must not replay");
    assert!(matches!(err, PipelineError::ReplayNotAllowed(_)), "got: {err}");
    assert!(err.to_string().contains("new upload"), "got: {err}");

    // Denied replays are audited (US-06) and the DLQ entry stays put.
    assert_eq!(emitter.count_type("vector.tile.job.replay.denied"), 1);
}

#[test]
fn replay_unknown_crs_succeeds_and_clears_dlq() {
    let root = temp_root("replay-crs");
    let emitter = Arc::new(CapturingEmitter::default());
    let d = deps(&root, emitter.clone());

    // Fail the original run with UNKNOWN_CRS (missing .prj, RequireKnown).
    let mut job = base_job("job_crs");
    job.source_format = SourceFormat::Shapefile;
    d.jobs.create(job.clone()).unwrap();
    let input = RunJobInput {
        job,
        source_bytes: sample_parcel_bundle(true, false), // no .prj
        tile_config: tile_config(),
        normalize_opts: normalize_opts(), // RequireKnown
        paths: job_paths_for(&root, TENANT, "job_crs", LAYER),
    };
    run_job_with_retries(&input, &d, &RetryPolicy::default())
        .expect_err("missing .prj fails without confirmation");
    let dlq = FileDlqStore::new(root.join("dlq"));
    let record = dlq.get(TENANT, "job_crs").unwrap().expect("dead-lettered");
    assert!(record.replay_eligible, "UNKNOWN_CRS is replay-eligible");

    // Replay with the TRD §10 user confirmation (assume-WGS84).
    let opts = ReplayOptions {
        assume_wgs84: true,
        requested_by: "sre-oncall".to_string(),
        reason: "County export confirmed WGS84".to_string(),
        ..Default::default()
    };
    let outcome =
        replay_job(&d, &root, TENANT, "job_crs", &opts).expect("confirmed replay succeeds");
    assert!(matches!(outcome, ReplayOutcome::Executed(_)));

    // Successful redrive consumes the DLQ entry (US-05) and emits the
    // completion event.
    assert!(dlq.get(TENANT, "job_crs").unwrap().is_none());
    assert_eq!(emitter.count_type("vector.tile.job.replay.completed"), 1);
    let stored = d.jobs.get("job_crs").unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Completed);
    assert_eq!(stored.replay_count, 1);
}

#[test]
fn replay_limit_enforced_after_max_attempts() {
    let root = temp_root("replay-limit");
    let emitter = Arc::new(CapturingEmitter::default());
    let d = deps(&root, emitter.clone());
    let store = FileQuarantineStore::new(root.join("quarantine"));

    let mut job = base_job("job_limited");
    job.status = JobStatus::Failed;
    job.error_code = Some("PROCESSING_TIMEOUT".to_string()); // transient
    job.error_class = Some("TRANSIENT".to_string());
    job.replay_eligible = true;
    job.replay_count = MAX_MANUAL_REPLAYS; // limit exhausted
    d.jobs.create(job.clone()).unwrap();
    let report = ErrorReport::from_job(&job, "PROCESSING_TIMEOUT", "timeout", "TILING");
    store
        .quarantine(&job, &tiny_parcel_geojson(), &report)
        .unwrap();

    let err = replay_job(&d, &root, TENANT, "job_limited", &ReplayOptions::default())
        .expect_err("replay limit must be enforced");
    assert!(matches!(err, PipelineError::ReplayNotAllowed(_)), "got: {err}");
    assert!(err.to_string().contains("maximum"), "got: {err}");
    assert_eq!(emitter.count_type("vector.tile.job.replay.denied"), 1);
}

#[test]
fn completed_replay_is_no_op() {
    let root = temp_root("replay-noop");
    let emitter = Arc::new(CapturingEmitter::default());
    let d = deps(&root, emitter.clone());

    let mut job = base_job("job_noop");
    job.status = JobStatus::Completed;
    d.jobs.create(job.clone()).unwrap();

    let outcome = replay_job(&d, &root, TENANT, "job_noop", &ReplayOptions::default())
        .expect("no-op is not an error");
    match outcome {
        ReplayOutcome::NoOp { reason } => {
            assert!(reason.contains("completed"), "got: {reason}");
        }
        ReplayOutcome::Executed(_) => panic!("completed replay must not re-run"),
    }
    // No events, no state change.
    assert_eq!(emitter.events.lock().unwrap().len(), 0);
    let stored = d.jobs.get("job_noop").unwrap().unwrap();
    assert_eq!(stored.status, JobStatus::Completed);
    assert_eq!(stored.replay_count, 0);
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
