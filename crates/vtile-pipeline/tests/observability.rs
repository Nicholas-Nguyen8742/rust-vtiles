//! Observability tests (Sequence 4 epic):
//!
//! * US-OBS-01 — structured stage-log schema + trace correlation ids
//! * US-OBS-02 — dimensioned metrics registry (counters + histograms)
//! * US-OBS-03 — alert catalog + evaluation
//! * US-OBS-05 — tenant-scoped append-only audit trail
//! * US-OBS-06 — dashboard aggregation + layer freshness/DLQ depth

use std::path::PathBuf;

use chrono::Utc;

use vtile_core::model::{
    Bbox, JobRecord, JobStatus, LayerCategory, LayerMetadataInput, SourceFormat, ZoomRange,
};
use vtile_pipeline::manifest::TileManifest;
use vtile_pipeline::obs::{
    alert_rules, build_dashboard, category_label, dlq_depth, evaluate_alerts, layer_health,
    metric_key, new_span_id, new_trace_id, AlertSeverity, AuditRecord, FileAuditTrail,
    ObsMetrics, StageLog,
};
use vtile_pipeline::store::{FileJobStore, JobStore};

const TENANT: &str = "tenant-acme";
const LAYER: &str = "us-parcels-nyc";

fn temp_root(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vtile-obs-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ── US-OBS-01: correlation + structured log schema ──────────────────────────

#[test]
fn trace_and_span_ids_are_w3c_shaped() {
    let trace = new_trace_id();
    let span = new_span_id();
    assert_eq!(trace.len(), 32, "trace id must be 32 hex chars");
    assert_eq!(span.len(), 16, "span id must be 16 hex chars");
    assert!(trace.chars().all(|c| c.is_ascii_hexdigit()));
    assert!(span.chars().all(|c| c.is_ascii_hexdigit()));
    assert_ne!(new_trace_id(), trace);
}

#[test]
fn stage_log_serializes_camel_case_schema() {
    let mut log = StageLog::new(
        "vector-tile-processor",
        "TILE_GENERATION_COMPLETED",
        TENANT,
        "job_123",
        LAYER,
    );
    log.stage = Some("TILING".to_string());
    log.error_code = None;
    log.feature_count = Some(850_000);
    log.file_bytes = Some(125_000_000);
    log.duration_ms = Some(180_000);
    log.trace_id = Some("abc123".to_string());
    log.span_id = Some("def456".to_string());

    let value = serde_json::to_value(&log).unwrap();
    assert_eq!(value["service"], "vector-tile-processor");
    assert_eq!(value["event"], "TILE_GENERATION_COMPLETED");
    assert_eq!(value["tenantId"], TENANT);
    assert_eq!(value["jobId"], "job_123");
    assert_eq!(value["layerId"], LAYER);
    assert_eq!(value["stage"], "TILING");
    assert_eq!(value["featureCount"], 850_000);
    assert_eq!(value["fileBytes"], 125_000_000);
    assert_eq!(value["durationMs"], 180_000);
    assert_eq!(value["traceId"], "abc123");
    assert_eq!(value["spanId"], "def456");
    // Optional absent fields stay out of the document.
    assert!(value.get("errorCode").is_none());
}

// ── US-OBS-02: dimensioned metrics registry ─────────────────────────────────

#[test]
fn metric_key_is_sorted_and_stable() {
    let a = metric_key(
        "ingest_jobs_failed_total",
        &[("errorCode", "EMPTY_DATASET"), ("tenantId", TENANT)],
    );
    let b = metric_key(
        "ingest_jobs_failed_total",
        &[("tenantId", TENANT), ("errorCode", "EMPTY_DATASET")],
    );
    assert_eq!(a, b, "label order must not change the series key");
    assert!(a.starts_with("ingest_jobs_failed_total{"));
    assert_eq!(metric_key("tile_requests_total", &[]), "tile_requests_total");
}

#[test]
fn counters_accumulate_per_dimension_set() {
    let metrics = ObsMetrics::default();
    let dims_a: [(&str, &str); 1] = [("tenantId", TENANT)];
    let dims_b: [(&str, &str); 1] = [("tenantId", "tenant-other")];
    metrics.inc("ingest_jobs_completed_total", &dims_a);
    metrics.inc("ingest_jobs_completed_total", &dims_a);
    metrics.inc("ingest_jobs_completed_total", &dims_b);
    metrics.add("geospatial_output_bytes", 4096, &dims_a);

    assert_eq!(
        metrics.counter_value(&metric_key("ingest_jobs_completed_total", &dims_a)),
        2
    );
    assert_eq!(
        metrics.counter_value(&metric_key("ingest_jobs_completed_total", &dims_b)),
        1
    );
    assert_eq!(
        metrics.counter_value(&metric_key("geospatial_output_bytes", &dims_a)),
        4096
    );

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot["counters"][metric_key("ingest_jobs_completed_total", &dims_a)],
        2
    );
}

#[test]
fn histograms_track_count_sum_and_percentiles() {
    let metrics = ObsMetrics::default();
    let dims: [(&str, &str); 0] = [];
    for i in 1..=100u64 {
        metrics.observe("ingest_job_duration_seconds", i as f64, &dims);
    }
    let snapshot = metrics.snapshot();
    let hist = &snapshot["histograms"]["ingest_job_duration_seconds"];
    assert_eq!(hist["count"], 100);
    assert_eq!(hist["sum"], 5050.0);
    assert_eq!(hist["max"], 100.0);
    assert_eq!(hist["min"], 1.0);
    assert_eq!(hist["p95"], 95.0);
    assert_eq!(hist["p50"], 50.0);
}

#[test]
fn category_label_is_bounded() {
    assert_eq!(category_label(Some(LayerCategory::Parcel)), "PARCEL");
    assert_eq!(category_label(Some(LayerCategory::FloodRisk)), "FLOOD_RISK");
    assert_eq!(category_label(None), "OTHER");
}

// ── US-OBS-03: alert catalog + evaluation ───────────────────────────────────

#[test]
fn alert_catalog_covers_the_epic_rules() {
    let rules = alert_rules();
    let names: Vec<&str> = rules.iter().map(|r| r.name.as_str()).collect();
    for required in [
        "Tile5xxRateHigh",
        "DlqMessageReceived",
        "JobFailureRateHigh",
        "JobDurationHigh",
        "TileSizeP95High",
        "ReplayFailureRateHigh",
        "TenantAuthorizationFailureSpike",
        "LayerStalenessHigh",
        "RollbackOccurred",
    ] {
        assert!(names.contains(&required), "missing rule: {required}");
    }
    let p1: Vec<&str> = rules
        .iter()
        .filter(|r| r.severity == AlertSeverity::P1)
        .map(|r| r.name.as_str())
        .collect();
    assert!(p1.contains(&"Tile5xxRateHigh"));
    // Every rule links a runbook.
    for rule in &rules {
        assert!(!rule.runbook.is_empty());
    }
}

#[test]
fn alert_evaluation_fires_on_dlq_depth() {
    let firing = serde_json::json!({ "dlq_message_count": 1 });
    let states = evaluate_alerts(&firing);
    let dlq = states
        .iter()
        .find(|s| s.rule.name == "DlqMessageReceived")
        .expect("rule present");
    assert!(dlq.triggered);
    assert_eq!(dlq.current_value, Some(1.0));

    let quiet = serde_json::json!({ "dlq_message_count": 0 });
    let states = evaluate_alerts(&quiet);
    let dlq = states
        .iter()
        .find(|s| s.rule.name == "DlqMessageReceived")
        .unwrap();
    assert!(!dlq.triggered);
}

#[test]
fn alert_evaluation_ratio_rules_sum_dimensions() {
    // 5xx spread across two tenant dimensions: 2/100 = 2% > 1% threshold.
    let snapshot = serde_json::json!({
        "tile_5xx_total{tenantId=tenant-acme}": 1,
        "tile_5xx_total{tenantId=tenant-other}": 1,
        "tile_requests_total{tenantId=tenant-acme}": 60,
        "tile_requests_total{tenantId=tenant-other}": 40,
    });
    let states = evaluate_alerts(&snapshot);
    let rule = states
        .iter()
        .find(|s| s.rule.name == "Tile5xxRateHigh")
        .unwrap();
    assert!(rule.triggered);
    assert!((rule.current_value.unwrap() - 0.02).abs() < 1e-9);
}

#[test]
fn alert_rules_without_data_report_null_not_triggered() {
    let states = evaluate_alerts(&serde_json::json!({}));
    for state in &states {
        assert!(!state.triggered);
        assert!(state.current_value.is_none());
    }
}

// ── US-OBS-05: tenant-scoped audit trail ────────────────────────────────────

fn audit_record(event_type: &str, tenant: &str, layer: Option<&str>) -> AuditRecord {
    AuditRecord {
        event_type: event_type.to_string(),
        event_id: "evt_test".to_string(),
        tenant_id: tenant.to_string(),
        layer_id: layer.map(str::to_string),
        job_id: None,
        tile_version: None,
        actor: Some("tester".to_string()),
        reason: None,
        succeeded: true,
        occurred_at: Utc::now(),
        resource_type: None,
        resource_id: None,
        action: None,
        decision: None,
    }
}

#[test]
fn audit_trail_is_append_only_and_tenant_scoped() {
    let root = temp_root("audit");
    let trail = FileAuditTrail::new(&root);
    trail
        .append(&audit_record("upload.initiated", TENANT, Some(LAYER)))
        .unwrap();
    trail
        .append(&audit_record("upload.completed", TENANT, Some(LAYER)))
        .unwrap();
    trail
        .append(&audit_record("upload.initiated", "tenant-other", None))
        .unwrap();
    // Appends accumulate in one JSONL file.
    let lines = std::fs::read_to_string(trail.path()).unwrap();
    assert_eq!(lines.lines().count(), 3);

    // Tenant-scoped queries never expose other tenants' records.
    let acme = trail.query(Some(TENANT), None, None, 100).unwrap();
    assert_eq!(acme.len(), 2);
    assert!(acme.iter().all(|r| r.tenant_id == TENANT));
    let other = trail.query(Some("tenant-other"), None, None, 100).unwrap();
    assert_eq!(other.len(), 1);

    // Event-type filter.
    let initiated = trail
        .query(Some(TENANT), None, Some("upload.initiated"), 100)
        .unwrap();
    assert_eq!(initiated.len(), 1);

    // Layer filter.
    let by_layer = trail.query(None, Some(LAYER), None, 100).unwrap();
    assert_eq!(by_layer.len(), 2);
}

// ── US-OBS-06: dashboard, layer quality, DLQ depth ──────────────────────────

fn base_job(job_id: &str, status: JobStatus) -> JobRecord {
    JobRecord {
        job_id: job_id.to_string(),
        tenant_id: TENANT.to_string(),
        layer_id: LAYER.to_string(),
        status,
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

fn write_manifest(root: &std::path::Path, generated_secs_ago: i64) {
    let manifest = TileManifest {
        schema_version: 1,
        tenant_id: TENANT.to_string(),
        layer_id: LAYER.to_string(),
        tile_version: "2026-06-17T16-00-00Z-test".to_string(),
        min_zoom: 10,
        max_zoom: 16,
        tile_count: 42,
        total_gzip_bytes: 123_456,
        bounding_box: Bbox::new(-74.2591, 40.4774, -73.7004, 40.9176),
        generated_at: Utc::now() - chrono::Duration::seconds(generated_secs_ago),
        tile_url_template: None,
    };
    let dir = root.join("manifests").join(TENANT).join(LAYER);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("manifest.json"), manifest.to_json().unwrap()).unwrap();
}

#[test]
fn layer_health_reports_freshness() {
    let root = temp_root("freshness");
    write_manifest(&root, 3600);
    let layers = layer_health(&root);
    assert_eq!(layers.len(), 1);
    let layer = &layers[0];
    assert_eq!(layer.layer_id, LAYER);
    assert_eq!(layer.tile_count, 42);
    assert!(layer.staleness_seconds >= 3600);
    assert!(layer.staleness_seconds < 3700);
}

#[test]
fn dlq_depth_counts_tenant_job_files() {
    let root = temp_root("dlq-depth");
    assert_eq!(dlq_depth(&root), 0);
    let dir = root.join("dlq").join(TENANT);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("job_a.json"), "{}").unwrap();
    std::fs::write(dir.join("job_b.json"), "{}").unwrap();
    std::fs::write(dir.join("notes.txt"), "ignore me").unwrap();
    assert_eq!(dlq_depth(&root), 2);
}

#[test]
fn dashboard_aggregates_jobs_layers_and_alerts() {
    let root = temp_root("dashboard");
    let jobs = FileJobStore::new(root.join("jobs")).unwrap();
    jobs.create(base_job("job_ok", JobStatus::Completed)).unwrap();
    jobs.create(base_job("job_bad", JobStatus::Failed)).unwrap();
    jobs.create(base_job("job_run", JobStatus::Tiling)).unwrap();
    write_manifest(&root, 60);

    let dashboard = build_dashboard(&root, &jobs);
    assert_eq!(dashboard["jobs"]["completed"], 1);
    assert_eq!(dashboard["jobs"]["failed"], 1);
    assert_eq!(dashboard["jobs"]["active"], 1);
    assert_eq!(dashboard["jobs"]["total"], 3);
    assert_eq!(dashboard["dlqDepth"], 0);
    let layers = dashboard["layers"].as_array().expect("layers array");
    assert_eq!(layers.len(), 1);
    assert_eq!(layers[0]["layerId"], LAYER);
    // Triggered alerts is an array (contents depend on process-wide metrics).
    assert!(dashboard["triggeredAlerts"].is_array());
    assert!(dashboard["metrics"].is_object());
}
