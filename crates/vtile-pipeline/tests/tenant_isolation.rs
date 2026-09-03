//! Pipeline-level tenant isolation tests (Sequence 5 TI-02/TI-05).
//!
//! Worker-side guarantees: a run refuses malformed tenant ids, refuses
//! source URIs carrying another tenant's prefix, refuses traversal layer
//! ids; every emitted event carries the validated tenant; replay never
//! changes tenant identity.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::Utc;

use vtile_core::model::{
    JobRecord, JobStatus, LayerCategory, LayerMetadataInput, SourceFormat, ZoomRange,
};
use vtile_pipeline::events::{EventEmitter, NullEventEmitter, PipelineEvent};
use vtile_pipeline::job::{job_paths_for, run_job, JobDeps, RunJobInput};
use vtile_pipeline::quarantine::FileQuarantineStore;
use vtile_pipeline::store::{FileJobStore, FileLayerCatalog};
use vtile_pipeline::{replay_job, ErrorReport, PipelineError, QuarantineStore, ReplayOptions};

const TENANT: &str = "tenant-acme";
const LAYER: &str = "us-parcels-nyc";

fn temp_root(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vtile-iso-{label}-{}", std::process::id()));
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

fn deps(root: &std::path::Path) -> JobDeps {
    JobDeps {
        jobs: Arc::new(FileJobStore::new(root.join("jobs")).unwrap()),
        catalog: Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap()),
        events: Arc::new(NullEventEmitter),
        quarantine: Some(Arc::new(FileQuarantineStore::new(root.join("quarantine")))),
        dlq: None,
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

fn run_input(job: JobRecord, root: &std::path::Path) -> RunJobInput {
    let paths = job_paths_for(root, &job.tenant_id, &job.job_id, &job.layer_id);
    RunJobInput {
        job,
        source_bytes: tiny_parcel_geojson(),
        tile_config: vtile_core::config::TileConfig {
            layer_name: "parcel_boundary".to_string(),
            zoom_range: ZoomRange::new(12, 14),
            ..Default::default()
        },
        normalize_opts: vtile_ingest::normalize::NormalizeOptions::default(),
        paths,
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

// ── TI-02: worker-side tenant validation ────────────────────────────────────

#[test]
fn run_job_rejects_malformed_tenant_id() {
    let root = temp_root("bad-tenant");
    let d = deps(&root);
    let mut job = base_job("job_bad_tenant");
    job.tenant_id = "EVIL TENANT".to_string(); // invalid pattern
    d.jobs.create(job.clone()).unwrap();

    let err = run_job(&run_input(job, &root), &d).expect_err("malformed tenant must fail");
    assert!(matches!(err, PipelineError::TenantMismatch(_)), "got: {err}");
}

#[test]
fn run_job_rejects_traversal_layer_id() {
    let root = temp_root("traversal-layer");
    let d = deps(&root);
    let mut job = base_job("job_traversal");
    job.layer_id = "../evil".to_string();
    d.jobs.create(job.clone()).unwrap();

    let err = run_job(&run_input(job, &root), &d).expect_err("traversal layer id must fail");
    assert!(matches!(err, PipelineError::TenantMismatch(_)), "got: {err}");
}

#[test]
fn run_job_rejects_source_uri_of_another_tenant() {
    let root = temp_root("cross-tenant-uri");
    let d = deps(&root);
    let mut job = base_job("job_cross_uri");
    // Storage path carries another tenant's prefix → TENANT_MISMATCH rather
    // than reading another tenant's objects.
    job.source_uri =
        "file:///data/staging/tenant-beta/job_cross_uri/input/parcels.zip".to_string();
    d.jobs.create(job.clone()).unwrap();

    let err = run_job(&run_input(job, &root), &d).expect_err("mismatched tenant uri must fail");
    assert!(matches!(err, PipelineError::TenantMismatch(_)), "got: {err}");
    assert!(err.to_string().contains("tenant"), "got: {err}");
}

// ── TI-02: event tenant contract ────────────────────────────────────────────

#[test]
fn every_event_carries_the_validated_tenant() {
    let root = temp_root("event-tenant");
    let mut d = deps(&root);
    let emitter = Arc::new(CapturingEmitter::default());
    d.events = emitter.clone();

    let job = base_job("job_events");
    d.jobs.create(job.clone()).unwrap();
    run_job(&run_input(job, &root), &d).expect("job succeeds");

    let events = emitter.events.lock().unwrap();
    assert!(!events.is_empty(), "a run must emit events");
    for event in events.iter() {
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(
            value["tenantId"], TENANT,
            "every event carries the validated tenant ({})",
            event.event_type()
        );
        assert!(value.get("eventType").is_some());
    }
}

// ── TI-04/TI-05: replay never changes tenant identity ───────────────────────

#[test]
fn replay_preserves_tenant_identity() {
    let root = temp_root("replay-tenant");
    let d = deps(&root);
    let store = FileQuarantineStore::new(root.join("quarantine"));

    // Fail the original run with an eligible error (UNKNOWN_CRS).
    let mut job = base_job("job_replay_tenant");
    job.status = JobStatus::Failed;
    job.error_code = Some("UNKNOWN_CRS".to_string());
    d.jobs.create(job.clone()).unwrap();
    let report = ErrorReport::from_job(&job, "UNKNOWN_CRS", "missing .prj", "NORMALIZING");
    store
        .quarantine(&job, &tiny_parcel_geojson(), &report)
        .unwrap();

    let outcome = replay_job(
        &d,
        &root,
        TENANT,
        "job_replay_tenant",
        &ReplayOptions {
            assume_wgs84: true,
            requested_by: "sre-oncall".to_string(),
            reason: "confirmed WGS84".to_string(),
            ..Default::default()
        },
    )
    .expect("eligible replay succeeds");
    assert!(matches!(
        outcome,
        vtile_pipeline::ReplayOutcome::Executed(_)
    ));

    // Tenant identity is immutable across replay; the replay is audited.
    let stored = d.jobs.get("job_replay_tenant").unwrap().unwrap();
    assert_eq!(stored.tenant_id, TENANT);
    assert_eq!(stored.status, JobStatus::Completed);
    let audit = stored.replay_audit.expect("replay audited");
    assert_eq!(audit.requested_by, "sre-oncall");

    // Cross-tenant replay is refused outright.
    let err = replay_job(
        &d,
        &root,
        "tenant-beta",
        "job_replay_tenant",
        &ReplayOptions::default(),
    )
    .expect_err("cross-tenant replay refused");
    assert!(err.to_string().contains("different tenant"), "got: {err}");
}
