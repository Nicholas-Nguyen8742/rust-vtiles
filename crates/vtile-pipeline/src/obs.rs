//! Observability and operational trust (Sequence 4 epic).
//!
//! Structured logging with correlation IDs (US-OBS-01), core pipeline
//! metrics with bounded dimensions (US-OBS-02), alert rule catalog + local
//! evaluation (US-OBS-03), trace correlation for failure forensics
//! (US-OBS-04), tenant-scoped audit trail (US-OBS-05), and tile/layer
//! quality telemetry support (US-OBS-06).
//!
//! Local ↔ production mapping:
//! * CloudWatch Logs (JSON) ↔ `tracing` JSON subscriber + [`StageLog`] schema
//! * CloudWatch metrics ↔ [`ObsMetrics`] registry served at `/internal/metrics`
//! * CloudWatch dashboards/alarms ↔ `/internal/dashboard`, `/internal/alerts`
//! * X-Ray ↔ `traceId`/`spanId` correlation fields (W3C-shaped)
//! * CloudTrail / S3 access logs ↔ [`FileAuditTrail`] (append-only)

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use vtile_core::model::{JobStatus, LayerCategory};

use crate::error::PipelineResult;
use crate::idempotency::IdempotencyMetrics;
use crate::job::new_event_id;
use crate::manifest::TileManifest;
use crate::publish::PublishMetrics;
use crate::recovery::RecoveryMetrics;
use crate::store::JobStore;

// ── Correlation (US-OBS-01 / US-OBS-04) ─────────────────────────────────────

/// W3C-shaped trace id (32 hex chars). Created at upload time and propagated
/// through every stage log, event, and span for the job's lifetime.
pub fn new_trace_id() -> String {
    uuid::Uuid::new_v4().as_simple().to_string()
}

/// W3C-shaped span id (16 hex chars) — one per pipeline stage.
pub fn new_span_id() -> String {
    let full = uuid::Uuid::new_v4().as_simple().to_string();
    full[..16].to_string()
}

/// Deployment environment label for the `environment` log/metric dimension.
pub fn environment() -> String {
    std::env::var("VTILE_ENVIRONMENT").unwrap_or_else(|_| "local".to_string())
}

/// Service names for the `service` log field.
pub const SERVICE_API: &str = "vector-tile-api";
pub const SERVICE_PROCESSOR: &str = "vector-tile-processor";

// ── Structured stage logs (US-OBS-01) ───────────────────────────────────────

/// Canonical lifecycle stage names (US-OBS-01 required log stages).
pub mod stage {
    pub const UPLOAD_REQUESTED: &str = "UPLOAD_REQUESTED";
    pub const UPLOAD_COMPLETED: &str = "UPLOAD_COMPLETED";
    pub const JOB_SUBMITTED: &str = "JOB_SUBMITTED";
    pub const VALIDATION_STARTED: &str = "VALIDATION_STARTED";
    pub const VALIDATION_COMPLETED: &str = "VALIDATION_COMPLETED";
    pub const NORMALIZATION_STARTED: &str = "NORMALIZATION_STARTED";
    pub const NORMALIZATION_COMPLETED: &str = "NORMALIZATION_COMPLETED";
    pub const TILE_GENERATION_STARTED: &str = "TILE_GENERATION_STARTED";
    pub const TILE_GENERATION_COMPLETED: &str = "TILE_GENERATION_COMPLETED";
    pub const PUBLISH_STARTED: &str = "PUBLISH_STARTED";
    pub const PUBLISH_COMPLETED: &str = "PUBLISH_COMPLETED";
    pub const JOB_FAILED: &str = "JOB_FAILED";
    pub const JOB_RETRIED: &str = "JOB_RETRIED";
    pub const JOB_SENT_TO_DLQ: &str = "JOB_SENT_TO_DLQ";
}

/// Structured pipeline log line (US-OBS-01 schema). Emitted through
/// `tracing`; the JSON subscriber renders one queryable document per stage,
/// searchable by `jobId`, `tenantId`, `layerId`, and `traceId`.
///
/// SOC2 note: property values never appear here; only operational context
/// (counts, sizes, durations, taxonomy codes).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StageLog {
    pub service: String,
    pub environment: String,
    /// Stage name, e.g. `TILE_GENERATION_COMPLETED`.
    pub event: String,
    pub tenant_id: String,
    pub job_id: String,
    pub layer_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crs: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tile_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
}

impl StageLog {
    pub fn new(
        service: &str,
        event: &str,
        tenant_id: &str,
        job_id: &str,
        layer_id: &str,
    ) -> Self {
        Self {
            service: service.to_string(),
            environment: environment(),
            event: event.to_string(),
            tenant_id: tenant_id.to_string(),
            job_id: job_id.to_string(),
            layer_id: layer_id.to_string(),
            stage: None,
            status: None,
            error_code: None,
            source_format: None,
            crs: None,
            feature_count: None,
            file_bytes: None,
            tile_count: None,
            duration_ms: None,
            trace_id: None,
            span_id: None,
        }
    }
}

/// Emits a stage log with the correlation fields indexed for search.
pub fn emit_stage_log(log: &StageLog) {
    let json = serde_json::to_string(log).unwrap_or_else(|_| "{}".to_string());
    tracing::info!(
        service = %log.service,
        environment = %log.environment,
        stage_event = %log.event,
        tenant_id = %log.tenant_id,
        job_id = %log.job_id,
        layer_id = %log.layer_id,
        stage_log = %json,
        "pipeline stage"
    );
}

/// Bounded label for the `layerCategory` metric dimension.
pub fn category_label(category: Option<LayerCategory>) -> String {
    category
        .map(|c| {
            serde_json::to_string(&c)
                .unwrap_or_else(|_| "\"OTHER\"".to_string())
                .trim_matches('"')
                .to_string()
        })
        .unwrap_or_else(|| "OTHER".to_string())
}

// ── Metrics registry (US-OBS-02) ────────────────────────────────────────────

/// Canonical metric names (US-OBS-02 inventory). Dimensions are bounded:
/// `environment`, `tenantId`, `sourceFormat`, `layerCategory`, `stage`,
/// `errorCode` — never raw file names or asset IDs (cardinality guardrail).
pub mod metric {
    pub const INGEST_UPLOADS_REQUESTED: &str = "ingest_uploads_requested_total";
    pub const INGEST_UPLOADS_COMPLETED: &str = "ingest_uploads_completed_total";
    pub const INGEST_JOBS_SUBMITTED: &str = "ingest_jobs_submitted_total";
    pub const INGEST_JOBS_STARTED: &str = "ingest_jobs_started_total";
    pub const INGEST_JOBS_COMPLETED: &str = "ingest_jobs_completed_total";
    pub const INGEST_JOBS_FAILED: &str = "ingest_jobs_failed_total";
    pub const INGEST_JOB_DURATION_SECONDS: &str = "ingest_job_duration_seconds";
    pub const INGEST_VALIDATION_FAILURES: &str = "ingest_validation_failures_total";
    pub const INGEST_RETRY: &str = "ingest_retry_total";
    pub const INGEST_DLQ_MESSAGES: &str = "ingest_dlq_messages_total";
    pub const GEOSPATIAL_FEATURES_PROCESSED: &str = "geospatial_features_processed_total";
    pub const GEOSPATIAL_TILES_PUBLISHED: &str = "geospatial_tiles_published_total";
    pub const GEOSPATIAL_OUTPUT_BYTES: &str = "geospatial_output_bytes";
    pub const GEOSPATIAL_TILE_SIZE_BYTES: &str = "geospatial_tile_size_bytes";
    pub const LAYERS_PUBLISHED: &str = "layers_published_total";
    pub const LAYER_PUBLISH_DURATION: &str = "layer_publish_duration_seconds";
    pub const LAYER_MAX_TILE_SIZE: &str = "layer_max_tile_size_bytes";
    pub const TILE_REQUESTS: &str = "tile_requests_total";
    pub const TILE_REQUEST_DURATION: &str = "tile_request_duration_seconds";
    pub const TILE_CACHE_HITS: &str = "tile_cache_hits_total";
    pub const TILE_CACHE_MISSES: &str = "tile_cache_misses_total";
    pub const TILE_4XX: &str = "tile_4xx_total";
    pub const TILE_5XX: &str = "tile_5xx_total";
    pub const TILE_EMPTY_RESPONSES: &str = "tile_empty_responses_total";
    pub const TILE_PAYLOAD_BYTES: &str = "tile_payload_bytes";
    pub const TENANT_AUTHORIZATION_DENIED: &str = "tenant_authorization_denied_total";
    pub const CROSS_TENANT_ACCESS_ATTEMPT: &str = "cross_tenant_access_attempt_total";
    pub const REPLAY_OPERATIONS: &str = "replay_operation_total";
}

/// Canonical series key: `name{k1=v1,k2=v2}` with sorted labels.
pub fn metric_key(name: &str, dims: &[(&str, &str)]) -> String {
    if dims.is_empty() {
        return name.to_string();
    }
    let mut labels: Vec<String> = dims.iter().map(|(k, v)| format!("{k}={v}")).collect();
    labels.sort();
    format!("{name}{{{}}}", labels.join(","))
}

/// Bounded histogram state: count/sum/min/max plus a capped sample reservoir
/// for percentile estimates (local mirror of CloudWatch histogram metrics).
#[derive(Debug, Clone)]
struct HistogramState {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    samples: VecDeque<f64>,
}

const MAX_SAMPLES: usize = 4096;

impl HistogramState {
    fn observe(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.samples.push_back(value);
        if self.samples.len() > MAX_SAMPLES {
            let drain = self.samples.len() - MAX_SAMPLES;
            self.samples.drain(..drain);
        }
    }

    fn percentile(&self, p: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let mut sorted: Vec<f64> = self.samples.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn to_json(&self) -> serde_json::Value {
        let avg = if self.count > 0 {
            self.sum / self.count as f64
        } else {
            0.0
        };
        serde_json::json!({
            "count": self.count,
            "sum": self.sum,
            "avg": avg,
            "min": self.min,
            "max": self.max,
            "p50": self.percentile(50.0),
            "p95": self.percentile(95.0),
        })
    }
}

/// Process-wide dimensioned metrics registry (the local analog of CloudWatch
/// custom metrics). Non-blocking for job processing: mutex-guarded maps with
/// bounded histogram reservoirs.
#[derive(Debug, Default)]
pub struct ObsMetrics {
    counters: Mutex<HashMap<String, u64>>,
    histograms: Mutex<HashMap<String, HistogramState>>,
}

impl ObsMetrics {
    pub fn global() -> &'static ObsMetrics {
        static METRICS: OnceLock<ObsMetrics> = OnceLock::new();
        METRICS.get_or_init(ObsMetrics::default)
    }

    pub fn inc(&self, name: &str, dims: &[(&str, &str)]) {
        self.add(name, 1, dims);
    }

    pub fn add(&self, name: &str, value: u64, dims: &[(&str, &str)]) {
        let key = metric_key(name, dims);
        *self
            .counters
            .lock()
            .expect("metrics poisoned")
            .entry(key)
            .or_insert(0) += value;
    }

    pub fn observe(&self, name: &str, value: f64, dims: &[(&str, &str)]) {
        let key = metric_key(name, dims);
        let mut map = self.histograms.lock().expect("metrics poisoned");
        let hist = map.entry(key).or_insert(HistogramState {
            count: 0,
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            samples: VecDeque::new(),
        });
        hist.observe(value);
    }

    pub fn counter_value(&self, key: &str) -> u64 {
        self.counters
            .lock()
            .expect("metrics poisoned")
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    /// `{ "counters": {series: n}, "histograms": {series: {count,sum,avg,min,max,p50,p95}} }`
    pub fn snapshot(&self) -> serde_json::Value {
        let counters = self.counters.lock().expect("metrics poisoned");
        let histograms = self.histograms.lock().expect("metrics poisoned");
        let mut counter_map = serde_json::Map::new();
        let mut sorted_counter_keys: Vec<&String> = counters.keys().collect();
        sorted_counter_keys.sort();
        for key in sorted_counter_keys {
            counter_map.insert(key.clone(), counters[key].into());
        }
        let mut hist_map = serde_json::Map::new();
        let mut sorted_hist_keys: Vec<&String> = histograms.keys().collect();
        sorted_hist_keys.sort();
        for key in sorted_hist_keys {
            hist_map.insert(key.clone(), histograms[key].to_json());
        }
        serde_json::json!({
            "counters": counter_map,
            "histograms": hist_map,
        })
    }
}

/// Unified snapshot of every telemetry family, served at
/// `/internal/metrics`.
pub fn merged_metrics_snapshot() -> serde_json::Value {
    serde_json::json!({
        "idempotency": IdempotencyMetrics::global().snapshot(),
        "publishing": PublishMetrics::global().snapshot(),
        "recovery": RecoveryMetrics::global().snapshot(),
        "pipeline": ObsMetrics::global().snapshot(),
    })
}

/// Flat view used for alert evaluation: family counters, pipeline counters,
/// and histogram stats (`name{dims}.p95`, ...).
pub fn alert_snapshot() -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for snap in [
        IdempotencyMetrics::global().snapshot(),
        PublishMetrics::global().snapshot(),
        RecoveryMetrics::global().snapshot(),
    ] {
        if let Some(obj) = snap.as_object() {
            for (k, v) in obj {
                map.insert(k.clone(), v.clone());
            }
        }
    }
    let obs = ObsMetrics::global().snapshot();
    if let Some(counters) = obs.get("counters").and_then(|v| v.as_object()) {
        for (k, v) in counters {
            map.insert(k.clone(), v.clone());
        }
    }
    if let Some(hists) = obs.get("histograms").and_then(|v| v.as_object()) {
        for (k, h) in hists {
            if let Some(obj) = h.as_object() {
                for stat in ["count", "sum", "avg", "min", "max", "p50", "p95"] {
                    if let Some(v) = obj.get(stat) {
                        map.insert(format!("{k}.{stat}"), v.clone());
                    }
                }
            }
        }
    }
    serde_json::Value::Object(map)
}

// ── Alerts (US-OBS-03) ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    P1,
    P2,
    P3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum AlertCondition {
    /// Fires when the (dimension-summed) metric value exceeds `threshold`.
    /// Metric may reference a histogram stat, e.g. `name.p95`.
    GreaterThan { metric: String, threshold: f64 },
    /// Fires when numerator / sum(denominators) exceeds `threshold`.
    RatioGreaterThan {
        numerator: String,
        denominators: Vec<String>,
        threshold: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertRule {
    pub name: String,
    pub severity: AlertSeverity,
    pub description: String,
    pub condition: AlertCondition,
    pub runbook: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertState {
    pub rule: AlertRule,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_value: Option<f64>,
    pub triggered: bool,
}

/// Alert catalog from the Sequence 3 epic (US-OBS-03) plus the data-quality
/// rules from US-OBS-06. Rules referencing production-only metrics
/// (CloudFront, windowed stalls) evaluate to `currentValue: null` locally.
pub fn alert_rules() -> Vec<AlertRule> {
    vec![
        AlertRule {
            name: "Tile5xxRateHigh".into(),
            severity: AlertSeverity::P1,
            description: "Tile 5xx rate > 1% for 5 minutes".into(),
            condition: AlertCondition::RatioGreaterThan {
                numerator: metric::TILE_5XX.into(),
                denominators: vec![metric::TILE_REQUESTS.into()],
                threshold: 0.01,
            },
            runbook: "docs/OBSERVABILITY.md#tile-delivery-alerts".into(),
        },
        AlertRule {
            name: "OriginFailureRateHigh".into(),
            severity: AlertSeverity::P1,
            description:
                "CloudFront origin failure rate > 1% for 5 minutes (production metric)"
                    .into(),
            condition: AlertCondition::GreaterThan {
                metric: "cloudfront_origin_failure_rate".into(),
                threshold: 0.01,
            },
            runbook: "docs/OBSERVABILITY.md#tile-delivery-alerts".into(),
        },
        AlertRule {
            name: "DlqMessageReceived".into(),
            severity: AlertSeverity::P2,
            description: "DLQ depth > 0 — a job exhausted its retries".into(),
            condition: AlertCondition::GreaterThan {
                metric: "dlq_message_count".into(),
                threshold: 0.0,
            },
            runbook: "docs/RECOVERY.md#replay-workflow-us-04us-05".into(),
        },
        AlertRule {
            name: "JobFailureRateHigh".into(),
            severity: AlertSeverity::P2,
            description: "Job failure rate > 5% over 15 minutes".into(),
            condition: AlertCondition::RatioGreaterThan {
                numerator: metric::INGEST_JOBS_FAILED.into(),
                denominators: vec![
                    metric::INGEST_JOBS_COMPLETED.into(),
                    metric::INGEST_JOBS_FAILED.into(),
                ],
                threshold: 0.05,
            },
            runbook: "docs/ERRORS.md".into(),
        },
        AlertRule {
            name: "NoCompletedJobsWithBacklog".into(),
            severity: AlertSeverity::P2,
            description: "No completed jobs for 30 minutes while queue depth > 0 \
                          (windowed — evaluate against SQS/CloudWatch in production)"
                .into(),
            condition: AlertCondition::GreaterThan {
                metric: "jobs_stalled_with_backlog".into(),
                threshold: 0.0,
            },
            runbook: "docs/OBSERVABILITY.md#pipeline-health-alerts".into(),
        },
        AlertRule {
            name: "JobDurationHigh".into(),
            severity: AlertSeverity::P3,
            description: "Job duration p95 > 2x baseline for 30 minutes".into(),
            condition: AlertCondition::GreaterThan {
                metric: format!("{}.p95", metric::INGEST_JOB_DURATION_SECONDS),
                threshold: 600.0,
            },
            runbook: "docs/OBSERVABILITY.md#pipeline-health-alerts".into(),
        },
        AlertRule {
            name: "TileSizeP95High".into(),
            severity: AlertSeverity::P3,
            description: "Tile size p95 > 500 KB (parcel layers)".into(),
            condition: AlertCondition::GreaterThan {
                metric: format!("{}.p95", metric::GEOSPATIAL_TILE_SIZE_BYTES),
                threshold: 512_000.0,
            },
            runbook: "docs/PUBLISHING.md#candidate-manifest-and-verification-us-ap-02".into(),
        },
        AlertRule {
            name: "ReplayFailureRateHigh".into(),
            severity: AlertSeverity::P2,
            description: "Replay failure rate > 20%".into(),
            condition: AlertCondition::RatioGreaterThan {
                numerator: "replay_failure_count".into(),
                denominators: vec!["replay_requested_count".into()],
                threshold: 0.2,
            },
            runbook: "docs/RECOVERY.md#observability-and-audit-us-06".into(),
        },
        AlertRule {
            name: "TenantAuthorizationFailureSpike".into(),
            severity: AlertSeverity::P2,
            description: "Any cross-tenant authorization denial (security review)".into(),
            condition: AlertCondition::GreaterThan {
                metric: metric::TENANT_AUTHORIZATION_DENIED.into(),
                threshold: 0.0,
            },
            runbook: "docs/OBSERVABILITY.md#tenant-audit".into(),
        },
        AlertRule {
            name: "LayerStalenessHigh".into(),
            severity: AlertSeverity::P2,
            description: "Layer not refreshed within its expected schedule".into(),
            condition: AlertCondition::GreaterThan {
                metric: "layer_staleness_seconds_max".into(),
                threshold: 7 * 24 * 3600.0,
            },
            runbook: "docs/OBSERVABILITY.md#cre-data-quality-alerts".into(),
        },
        AlertRule {
            name: "RollbackOccurred".into(),
            severity: AlertSeverity::P2,
            description: "A layer version rollback occurred".into(),
            condition: AlertCondition::GreaterThan {
                metric: "rollbacks_completed".into(),
                threshold: 0.0,
            },
            runbook: "docs/PUBLISHING.md#rollback-us-ap-05".into(),
        },
    ]
}

/// Resolves a metric reference from a flat alert snapshot. Bare names sum
/// across all dimension variants; `name.stat` references aggregate the
/// histogram stat (max for p95/max, sum otherwise).
fn metric_value(snapshot: &serde_json::Value, metric: &str) -> Option<f64> {
    let (name, stat) = match metric.split_once('.') {
        Some((n, s)) => (n, Some(s)),
        None => (metric, None),
    };
    let map = snapshot.as_object()?;
    let mut acc: Option<f64> = None;
    for (key, value) in map {
        let matches = match stat {
            None => key == name || key.starts_with(&format!("{name}{{")),
            Some(s) => {
                key.starts_with(&format!("{name}{{")) && key.ends_with(&format!(".{s}"))
            }
        };
        if !matches {
            continue;
        }
        if let Some(x) = value.as_f64() {
            acc = Some(match acc {
                None => x,
                Some(a) => {
                    if matches!(stat, Some("p95") | Some("max")) {
                        a.max(x)
                    } else {
                        a + x
                    }
                }
            });
        }
    }
    acc
}

/// Evaluates the full alert catalog against a flat snapshot (see
/// [`alert_snapshot`]). Rules whose metrics have no data yield
/// `currentValue: null` and are not triggered.
pub fn evaluate_alerts(snapshot: &serde_json::Value) -> Vec<AlertState> {
    alert_rules()
        .into_iter()
        .map(|rule| {
            let current = match &rule.condition {
                AlertCondition::GreaterThan { metric, .. } => metric_value(snapshot, metric),
                AlertCondition::RatioGreaterThan {
                    numerator,
                    denominators,
                    ..
                } => {
                    let n = metric_value(snapshot, numerator);
                    let d: f64 = denominators
                        .iter()
                        .filter_map(|m| metric_value(snapshot, m))
                        .sum();
                    match n {
                        Some(n) if d > 0.0 => Some(n / d),
                        _ => None,
                    }
                }
            };
            let triggered = match (&rule.condition, current) {
                (AlertCondition::GreaterThan { threshold, .. }, Some(v)) => v > *threshold,
                (AlertCondition::RatioGreaterThan { threshold, .. }, Some(v)) => v > *threshold,
                _ => false,
            };
            AlertState {
                rule,
                current_value: current,
                triggered,
            }
        })
        .collect()
}

// ── Tenant-scoped audit trail (US-OBS-05) ───────────────────────────────────

/// Audit event catalog (US-OBS-05). `layer.published` and
/// `tile.version.rolled_back` are additionally recorded in the per-layer
/// `manifests/{tenant}/{layer}/audit.jsonl` (Sequence 2); this trail is the
/// cross-cutting, tenant-queryable view.
pub mod audit_event {
    pub const UPLOAD_INITIATED: &str = "upload.initiated";
    pub const UPLOAD_COMPLETED: &str = "upload.completed";
    pub const LAYER_PUBLISHED: &str = "layer.published";
    pub const TILE_VERSION_ROLLED_BACK: &str = "tile.version.rolled_back";
    pub const JOB_REPLAYED: &str = "job.replayed";
    pub const TENANT_ACCESS_DENIED: &str = "tenant.access.denied";
    pub const MANIFEST_UPDATED: &str = "manifest.updated";
}

/// One audit record: who did what to which resource, when, and whether it
/// succeeded (SOC2-aligned).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditRecord {
    pub event_type: String,
    pub event_id: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layer_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tile_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub succeeded: bool,
    pub occurred_at: DateTime<Utc>,
}

/// Append-only JSONL audit trail at `data/audit/audit.jsonl` (local mirror
/// of CloudTrail + S3 access logs).
pub struct FileAuditTrail {
    path: PathBuf,
}

impl FileAuditTrail {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            path: data_dir.join("audit").join("audit.jsonl"),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, record: &AuditRecord) -> PipelineResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(file, "{}", serde_json::to_string(record)?)?;
        Ok(())
    }

    /// Tenant-scoped query — callers pass the tenant they are authorized for
    /// (the API pins authenticated callers to their own tenant).
    pub fn query(
        &self,
        tenant_id: Option<&str>,
        layer_id: Option<&str>,
        event_type: Option<&str>,
        limit: usize,
    ) -> PipelineResult<Vec<AuditRecord>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for line in fs::read_to_string(&self.path)?.lines().rev() {
            if line.trim().is_empty() {
                continue;
            }
            let Ok(record) = serde_json::from_str::<AuditRecord>(line) else {
                continue;
            };
            if let Some(t) = tenant_id {
                if record.tenant_id != t {
                    continue;
                }
            }
            if let Some(l) = layer_id {
                if record.layer_id.as_deref() != Some(l) {
                    continue;
                }
            }
            if let Some(e) = event_type {
                if record.event_type != e {
                    continue;
                }
            }
            out.push(record);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}

/// Best-effort helper for the cross-tenant denial audit path (US-OBS-05):
/// counts the event and appends the record without failing the request.
pub fn record_access_denied(data_dir: &Path, caller_tenant: &str, resource: &str) {
    ObsMetrics::global().inc(
        metric::TENANT_AUTHORIZATION_DENIED,
        &[("tenantId", caller_tenant)],
    );
    ObsMetrics::global().inc(
        metric::CROSS_TENANT_ACCESS_ATTEMPT,
        &[("tenantId", caller_tenant)],
    );
    let record = AuditRecord {
        event_type: audit_event::TENANT_ACCESS_DENIED.to_string(),
        event_id: new_event_id(),
        tenant_id: caller_tenant.to_string(),
        layer_id: None,
        job_id: None,
        tile_version: None,
        actor: Some(format!("tenant:{caller_tenant}")),
        reason: Some(format!("cross-tenant access denied: {resource}")),
        succeeded: false,
        occurred_at: Utc::now(),
    };
    if let Err(e) = FileAuditTrail::new(data_dir).append(&record) {
        tracing::error!(error = %e, "failed to write audit record");
    }
}

// ── Dashboard + layer quality (US-OBS-03 / US-OBS-06) ───────────────────────

/// Per-layer health for the operations dashboard (US-OBS-06 freshness).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LayerHealth {
    pub layer_id: String,
    pub tenant_id: String,
    pub tile_version: String,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub tile_count: u64,
    pub generated_at: DateTime<Utc>,
    pub staleness_seconds: u64,
}

/// Reads every published layer's manifest under `data/manifests/`.
pub fn layer_health(data_dir: &Path) -> Vec<LayerHealth> {
    let mut out = Vec::new();
    let manifests = data_dir.join("manifests");
    let Ok(tenants) = fs::read_dir(&manifests) else {
        return out;
    };
    for tenant in tenants.flatten() {
        if !tenant.path().is_dir() {
            continue;
        }
        let Ok(layers) = fs::read_dir(tenant.path()) else {
            continue;
        };
        for layer in layers.flatten() {
            let manifest_path = layer.path().join("manifest.json");
            let Ok(json) = fs::read_to_string(&manifest_path) else {
                continue;
            };
            let Ok(manifest) = TileManifest::from_json(&json) else {
                continue;
            };
            let staleness = (Utc::now() - manifest.generated_at)
                .num_seconds()
                .max(0) as u64;
            out.push(LayerHealth {
                layer_id: manifest.layer_id,
                tenant_id: manifest.tenant_id,
                tile_version: manifest.tile_version,
                min_zoom: manifest.min_zoom,
                max_zoom: manifest.max_zoom,
                tile_count: manifest.tile_count,
                generated_at: manifest.generated_at,
                staleness_seconds: staleness,
            });
        }
    }
    out.sort_by(|a, b| a.layer_id.cmp(&b.layer_id));
    out
}

/// DLQ depth across tenants (number of dead-lettered jobs).
pub fn dlq_depth(data_dir: &Path) -> u64 {
    let mut depth = 0u64;
    let root = data_dir.join("dlq");
    let Ok(tenants) = fs::read_dir(&root) else {
        return 0;
    };
    for tenant in tenants.flatten() {
        if !tenant.path().is_dir() {
            continue;
        }
        if let Ok(jobs) = fs::read_dir(tenant.path()) {
            depth += jobs
                .flatten()
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|x| x == "json")
                        .unwrap_or(false)
                })
                .count() as u64;
        }
    }
    depth
}

/// Builds the operations dashboard (local analog of the CloudWatch
/// dashboards): job state counts, DLQ depth, layer quality/freshness,
/// triggered alerts, and the full metrics snapshot.
pub fn build_dashboard(data_dir: &Path, jobs: &dyn JobStore) -> serde_json::Value {
    let all = jobs.list().unwrap_or_default();
    let mut upload_pending = 0u64;
    let mut queued = 0u64;
    let mut active = 0u64;
    let mut completed = 0u64;
    let mut failed = 0u64;
    let mut cancelled = 0u64;
    for job in &all {
        match job.status {
            JobStatus::UploadPending => upload_pending += 1,
            JobStatus::Queued => queued += 1,
            JobStatus::Validating
            | JobStatus::Normalizing
            | JobStatus::Tiling
            | JobStatus::Publishing => active += 1,
            JobStatus::Completed => completed += 1,
            JobStatus::Failed => failed += 1,
            JobStatus::Cancelled => cancelled += 1,
        }
    }

    let layers = layer_health(data_dir);
    let depth = dlq_depth(data_dir);
    let snapshot = alert_snapshot();
    let alerts = evaluate_alerts(&snapshot);
    let triggered: Vec<&AlertState> = alerts.iter().filter(|a| a.triggered).collect();

    serde_json::json!({
        "generatedAt": Utc::now(),
        "environment": environment(),
        "jobs": {
            "uploadPending": upload_pending,
            "queued": queued,
            "active": active,
            "completed": completed,
            "failed": failed,
            "cancelled": cancelled,
            "total": all.len() as u64,
        },
        "dlqDepth": depth,
        "layers": layers,
        "triggeredAlerts": triggered,
        "metrics": snapshot,
    })
}
