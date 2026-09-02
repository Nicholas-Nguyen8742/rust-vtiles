//! DLQ and replay (Sequence 3 epic).
//!
//! Recoverable failure path for CRE geospatial ingestion: failures are
//! classified (transient vs. permanent), captured in a dead-letter store
//! with full context, quarantined with remediation guidance, and replayable
//! only when safe — under the original job identity, through the atomic
//! publishing process (Sequence 2).
//!
//! Local ↔ production mapping:
//! * SQS redrive policy (maxReceiveCount 4, backoff 0/30/60/120 s) ↔
//!   [`RetryPolicy`]; each local run/replay is one delivery attempt.
//! * SQS dead-letter queue ↔ [`FileDlqStore`] (`data/dlq/{tenant}/{job}.json`).
//! * EventBridge `vector.tile.job.dead-lettered` ↔ same event via the
//!   pipeline emitter.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use vtile_core::model::JobStatus;

use crate::error::{PipelineError, PipelineResult};
use crate::events::PipelineEvent;
use crate::job::{error_classification, new_event_id, run_job, JobDeps, JobOutcome, RunJobInput};
use crate::obs::{self, metric, stage as obs_stage};

// ── Error classification (US-03) ────────────────────────────────────────────

/// Failure class. Determines replay eligibility and DLQ handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorClass {
    /// Retryable infrastructure failures (timeouts, throttling, I/O).
    Transient,
    /// Source-data problems. Replay is blocked until the data is corrected —
    /// except `UNKNOWN_CRS`, which replays with explicit WGS84 confirmation
    /// (TRD §10 user confirmation).
    PermanentValidation,
    /// Access/configuration problems (production: IAM/KMS).
    PermissionDenied,
    /// Unclassified failures: manual review, never auto-replayed.
    ManualReview,
}

impl ErrorClass {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorClass::Transient => "TRANSIENT",
            ErrorClass::PermanentValidation => "PERMANENT_VALIDATION",
            ErrorClass::PermissionDenied => "PERMISSION_DENIED",
            ErrorClass::ManualReview => "MANUAL_REVIEW",
        }
    }
}

/// Deterministic classification matrix over the error taxonomy
/// (`docs/ERRORS.md`). Normative reference: `docs/RECOVERY.md`.
pub fn classify_code(error_code: &str) -> ErrorClass {
    match error_code {
        // Retryable infrastructure failures.
        "PROCESSING_TIMEOUT"
        | "S3_THROTTLED"
        | "ECS_TASK_TIMEOUT"
        | "TEMPORARY_INTERNAL_ERROR"
        | "INTERNAL_ERROR"
        | "STORE_ERROR"
        | "PROMOTION_CONFLICT" => ErrorClass::Transient,
        // Source-data / validation problems.
        "INVALID_FILE_TYPE"
        | "INVALID_SHAPEFILE"
        | "MISSING_SHAPEFILE_COMPONENTS"
        | "INVALID_GEOJSON"
        | "EMPTY_DATASET"
        | "UNSUPPORTED_CRS"
        | "UNKNOWN_CRS"
        | "GEOMETRY_ERRORS"
        | "ENCODING_ERROR"
        | "PAYLOAD_TOO_LARGE"
        | "FILE_TOO_LARGE"
        | "TILE_SIZE_EXCEEDED"
        | "TILE_GENERATION_FAILED"
        | "PUBLISH_VALIDATION_FAILED" => ErrorClass::PermanentValidation,
        // Everything else (PIPELINE_ERROR, INGEST_FAILED, unknown codes)
        // goes to manual review.
        _ => ErrorClass::ManualReview,
    }
}

/// Replay eligibility, enforced server-side in `replay::start_replay`
/// (US-03). Transient failures replay; permanent failures replay only when
/// the correction is explicit (`UNKNOWN_CRS` + WGS84 confirmation); unknown
/// errors never replay indefinitely.
pub fn replay_eligible(error_code: Option<&str>) -> bool {
    let Some(code) = error_code else {
        return false;
    };
    classify_code(code) == ErrorClass::Transient || code == "UNKNOWN_CRS"
}

/// Operator-facing remediation guidance recorded in failure reports (US-02).
pub fn remediation_for(error_code: &str) -> &'static str {
    match error_code {
        "INVALID_FILE_TYPE" => {
            "Match fileName/content-Type to the declared sourceFormat (.geojson/.json or .zip)."
        }
        "MISSING_SHAPEFILE_COMPONENTS" => {
            "Upload a zipped Shapefile containing .shp, .shx, .dbf, and .prj."
        }
        "INVALID_SHAPEFILE" => "Re-export the Shapefile; verify the bundle opens in a GIS tool.",
        "INVALID_GEOJSON" => {
            "Validate against RFC 7946; split GeometryCollections into typed features."
        }
        "EMPTY_DATASET" => "Provide a non-empty feature collection.",
        "UNSUPPORTED_CRS" => "Reproject the source to EPSG:4326 (WGS84) before upload.",
        "UNKNOWN_CRS" => {
            "Re-upload with a .prj file, or replay with --assume-wgs84 (TRD §10 confirmation)."
        }
        "GEOMETRY_ERRORS" => {
            "Repair the flagged geometries (degenerate rings, non-finite coordinates) and re-upload."
        }
        "ENCODING_ERROR" => "Re-export DBF attributes as UTF-8.",
        "PAYLOAD_TOO_LARGE" => "Split the upload below the size limit.",
        "FILE_TOO_LARGE" => "Split the dataset below the 1,000,000-feature cap.",
        "TILE_SIZE_EXCEEDED" | "TILE_GENERATION_FAILED" | "PUBLISH_VALIDATION_FAILED" => {
            "Inspect the candidate version; adjust the property policy or source density."
        }
        "PROCESSING_TIMEOUT" | "S3_THROTTLED" | "ECS_TASK_TIMEOUT"
        | "TEMPORARY_INTERNAL_ERROR" | "INTERNAL_ERROR" | "STORE_ERROR"
        | "PROMOTION_CONFLICT" => {
            "Transient failure — retry (`vtile replay`); automatic redrive in production."
        }
        _ => "Inspect the failure report; permanent failures need corrected source data, unknown failures need manual review.",
    }
}

// ── Retry policy (US-01) ────────────────────────────────────────────────────

/// Retry/backoff policy — the TRD SQS redrive configuration. Production
/// applies the delays across SQS redeliveries; locally each run/replay is
/// one delivery attempt, and the policy bounds how many attempts count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryPolicy {
    pub max_receives: u64,
    pub retry_delay_secs: Vec<u64>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_receives: 4,
            retry_delay_secs: vec![0, 30, 60, 120],
        }
    }
}

impl RetryPolicy {
    /// True when `retry_count` attempts have been consumed.
    pub fn exhausted(&self, retry_count: u64) -> bool {
        retry_count >= self.max_receives
    }

    /// Delay before the next attempt (flattens at the last configured delay).
    pub fn next_delay_secs(&self, retry_count: u64) -> u64 {
        self.retry_delay_secs
            .get(retry_count as usize)
            .copied()
            .or_else(|| self.retry_delay_secs.last().copied())
            .unwrap_or(0)
    }
}

/// Maximum manual replays per job before a new upload is required (US-04).
pub const MAX_MANUAL_REPLAYS: u64 = 3;

// ── Dead-letter store (US-01) ───────────────────────────────────────────────

/// Dead-lettered job capture — the full failure context needed to triage
/// without log archaeology (US-01 DLQ message attributes).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DlqRecord {
    pub job_id: String,
    pub tenant_id: String,
    pub layer_id: String,
    pub source_uri: String,
    pub error_code: String,
    pub error_class: ErrorClass,
    pub failed_stage: String,
    pub error_message: String,
    /// Delivery attempts consumed so far (runs + replays).
    pub retry_count: u64,
    pub max_receives: u64,
    pub replay_eligible: bool,
    pub failed_at: DateTime<Utc>,
}

/// DLQ persistence port (production: SQS dead-letter queue).
pub trait DlqStore: Send + Sync {
    /// Captures a dead-lettered job (overwrites any prior record for the
    /// same job — the latest failure is the actionable one).
    fn dead_letter(&self, record: &DlqRecord) -> PipelineResult<PathBuf>;
    fn get(&self, tenant_id: &str, job_id: &str) -> PipelineResult<Option<DlqRecord>>;
    /// Removes the DLQ entry (successful replay/redrive).
    fn remove(&self, tenant_id: &str, job_id: &str) -> PipelineResult<()>;
    /// Current DLQ contents for one tenant (depth + triage), newest first.
    fn list_tenant(&self, tenant_id: &str) -> PipelineResult<Vec<DlqRecord>>;
}

/// Filesystem DLQ rooted at `data/dlq/{tenantId}/{jobId}.json`.
pub struct FileDlqStore {
    root: PathBuf,
}

impl FileDlqStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, tenant_id: &str, job_id: &str) -> PathBuf {
        self.root.join(tenant_id).join(format!("{job_id}.json"))
    }
}

impl DlqStore for FileDlqStore {
    fn dead_letter(&self, record: &DlqRecord) -> PipelineResult<PathBuf> {
        let path = self.path_for(&record.tenant_id, &record.job_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Atomic write (tmp + rename) as everywhere else in the local stores.
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_string_pretty(record)?)?;
        fs::rename(&tmp, &path)?;
        Ok(path)
    }

    fn get(&self, tenant_id: &str, job_id: &str) -> PipelineResult<Option<DlqRecord>> {
        let path = self.path_for(tenant_id, job_id);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&fs::read_to_string(path)?)?))
    }

    fn remove(&self, tenant_id: &str, job_id: &str) -> PipelineResult<()> {
        let path = self.path_for(tenant_id, job_id);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    fn list_tenant(&self, tenant_id: &str) -> PipelineResult<Vec<DlqRecord>> {
        let dir = self.root.join(tenant_id);
        let Ok(entries) = fs::read_dir(&dir) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Ok(json) = fs::read_to_string(&path) else {
                continue;
            };
            if let Ok(record) = serde_json::from_str::<DlqRecord>(&json) {
                out.push(record);
            }
        }
        out.sort_by(|a, b| b.failed_at.cmp(&a.failed_at));
        Ok(out)
    }
}

// ── Recovery telemetry (US-06) ──────────────────────────────────────────────

/// Recovery counters merged into `GET /internal/metrics`. (Replay-requested
/// and replay-rejected counts live in `IdempotencyMetrics`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMetric {
    DeadLetteredMessages,
    QuarantinedObjects,
    ReplaySuccess,
    ReplayFailure,
}

impl RecoveryMetric {
    pub const ALL: [RecoveryMetric; 4] = [
        RecoveryMetric::DeadLetteredMessages,
        RecoveryMetric::QuarantinedObjects,
        RecoveryMetric::ReplaySuccess,
        RecoveryMetric::ReplayFailure,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            RecoveryMetric::DeadLetteredMessages => "dlq_message_count",
            RecoveryMetric::QuarantinedObjects => "quarantine_object_count",
            RecoveryMetric::ReplaySuccess => "replay_success_count",
            RecoveryMetric::ReplayFailure => "replay_failure_count",
        }
    }
}

#[derive(Debug, Default)]
pub struct RecoveryMetrics {
    dead_lettered_messages: AtomicU64,
    quarantined_objects: AtomicU64,
    replay_success: AtomicU64,
    replay_failure: AtomicU64,
}

impl RecoveryMetrics {
    pub fn global() -> &'static RecoveryMetrics {
        static METRICS: OnceLock<RecoveryMetrics> = OnceLock::new();
        METRICS.get_or_init(RecoveryMetrics::default)
    }

    pub fn inc(&self, metric: RecoveryMetric) {
        self.counter(metric).fetch_add(1, Ordering::Relaxed);
    }

    pub fn count(&self, metric: RecoveryMetric) -> u64 {
        self.counter(metric).load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for metric in RecoveryMetric::ALL {
            map.insert(metric.as_str().to_string(), self.count(metric).into());
        }
        serde_json::Value::Object(map)
    }

    fn counter(&self, metric: RecoveryMetric) -> &AtomicU64 {
        match metric {
            RecoveryMetric::DeadLetteredMessages => &self.dead_lettered_messages,
            RecoveryMetric::QuarantinedObjects => &self.quarantined_objects,
            RecoveryMetric::ReplaySuccess => &self.replay_success,
            RecoveryMetric::ReplayFailure => &self.replay_failure,
        }
    }
}

// ── Retry orchestration + DLQ capture (US-01) ─────────────────────────────

/// Runs a job under the retry policy: transient failures are retried with
/// backoff (emitting `vector.tile.job.retry_scheduled`), and once retries
/// are exhausted — or the failure is not transient — the job is captured in
/// the dead-letter store with a `vector.tile.job.dead-lettered` event.
///
/// Production mapping: SQS redrive applies the backoff across redeliveries;
/// locally the blocking worker performs the same sequence inline.
pub fn run_job_with_retries(
    input: &RunJobInput,
    deps: &JobDeps,
    policy: &RetryPolicy,
) -> PipelineResult<JobOutcome> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match run_job(input, deps) {
            Ok(outcome) => return Ok(outcome),
            Err(err) => {
                let (code, _message) = error_classification(&err);
                let class = classify_code(&code);
                // Only transient failures earn retries, and only while the
                // policy has attempts left.
                if class == ErrorClass::Transient && (attempt as u64) < policy.max_receives {
                    let delay_secs = policy.next_delay_secs((attempt - 1) as u64);
                    // Reset the job so the next attempt can re-acquire the
                    // lease and re-enter the state machine from QUEUED.
                    if let Ok(Some(mut job)) = deps.jobs.get(&input.job.job_id) {
                        job.status = JobStatus::Queued;
                        job.state_version += 1;
                        job.updated_at = Utc::now();
                        let _ = deps.jobs.upsert(job);
                    }
                    deps.events.emit(PipelineEvent::VectorTileJobRetryScheduled {
                        event_id: new_event_id(),
                        tenant_id: input.job.tenant_id.clone(),
                        job_id: input.job.job_id.clone(),
                        layer_id: input.job.layer_id.clone(),
                        attempt,
                        delay_secs,
                        error_code: code.clone(),
                        occurred_at: Utc::now(),
                    });
                    tracing::warn!(
                        job_id = %input.job.job_id,
                        attempt,
                        delay_secs,
                        error_code = %code,
                        "transient failure; retry scheduled"
                    );
                    // Sequence 4 US-OBS-01/02: retry stage log + metric.
                    obs::ObsMetrics::global().inc(
                        metric::INGEST_RETRY,
                        &[
                            ("tenantId", input.job.tenant_id.as_str()),
                            ("errorCode", code.as_str()),
                        ],
                    );
                    let mut retry_log = obs::StageLog::new(
                        obs::SERVICE_PROCESSOR,
                        obs_stage::JOB_RETRIED,
                        &input.job.tenant_id,
                        &input.job.job_id,
                        &input.job.layer_id,
                    );
                    retry_log.status = Some("RETRYING".to_string());
                    retry_log.error_code = Some(code.clone());
                    retry_log.trace_id = input.job.trace_id.clone();
                    obs::emit_stage_log(&retry_log);
                    if delay_secs > 0 {
                        std::thread::sleep(std::time::Duration::from_secs(delay_secs));
                    }
                    continue;
                }
                dead_letter_failure(deps, &input.job.job_id, &err, attempt);
                return Err(err);
            }
        }
    }
}

/// Captures a failed job in the dead-letter store with full failure context
/// and emits `vector.tile.job.dead-lettered` (Sequence 3 US-01).
///
/// Best-effort: a DLQ write failure is logged but never masks the original
/// job error.
pub fn dead_letter_failure(deps: &JobDeps, job_id: &str, err: &PipelineError, retry_count: u32) {
    let Some(store) = deps.dlq.as_ref() else {
        return;
    };
    let (error_code, error_message) = error_classification(err);
    let class = classify_code(&error_code);
    let Ok(Some(job)) = deps.jobs.get(job_id) else {
        return;
    };
    let record = DlqRecord {
        job_id: job.job_id.clone(),
        tenant_id: job.tenant_id.clone(),
        layer_id: job.layer_id.clone(),
        source_uri: job.source_uri.clone(),
        error_code: error_code.clone(),
        error_class: class,
        failed_stage: job.failed_stage.clone().unwrap_or_default(),
        error_message: error_message.clone(),
        retry_count: retry_count as u64,
        max_receives: RetryPolicy::default().max_receives,
        replay_eligible: replay_eligible(Some(&error_code)),
        failed_at: Utc::now(),
    };
    match store.dead_letter(&record) {
        Ok(path) => {
            RecoveryMetrics::global().inc(RecoveryMetric::DeadLetteredMessages);
            deps.events.emit(PipelineEvent::VectorTileJobDeadLettered {
                event_id: new_event_id(),
                tenant_id: record.tenant_id.clone(),
                job_id: record.job_id.clone(),
                layer_id: record.layer_id.clone(),
                error_code: record.error_code.clone(),
                error_class: class,
                failed_stage: record.failed_stage.clone(),
                retry_count,
                occurred_at: Utc::now(),
            });
            tracing::warn!(
                job_id = %job_id,
                error_code = %error_code,
                error_class = class.as_str(),
                dlq = %path.display(),
                "job dead-lettered"
            );
            // Sequence 4 US-OBS-01/02: DLQ stage log + metric.
            obs::ObsMetrics::global().inc(
                metric::INGEST_DLQ_MESSAGES,
                &[
                    ("tenantId", record.tenant_id.as_str()),
                    ("errorCode", record.error_code.as_str()),
                ],
            );
            let mut dlq_log = obs::StageLog::new(
                obs::SERVICE_PROCESSOR,
                obs_stage::JOB_SENT_TO_DLQ,
                &record.tenant_id,
                &record.job_id,
                &record.layer_id,
            );
            dlq_log.stage = Some(record.failed_stage.clone());
            dlq_log.status = Some("DEAD_LETTERED".to_string());
            dlq_log.error_code = Some(record.error_code.clone());
            dlq_log.trace_id = job.trace_id.clone();
            obs::emit_stage_log(&dlq_log);
        }
        Err(de) => tracing::error!(
            job_id = %job_id,
            error = %de,
            "failed to dead-letter job"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_matrix() {
        assert_eq!(classify_code("PROCESSING_TIMEOUT"), ErrorClass::Transient);
        assert_eq!(classify_code("PROMOTION_CONFLICT"), ErrorClass::Transient);
        assert_eq!(
            classify_code("MISSING_SHAPEFILE_COMPONENTS"),
            ErrorClass::PermanentValidation
        );
        assert_eq!(classify_code("UNKNOWN_CRS"), ErrorClass::PermanentValidation);
        assert_eq!(classify_code("PIPELINE_ERROR"), ErrorClass::ManualReview);
        assert_eq!(classify_code("SOMETHING_NEW"), ErrorClass::ManualReview);
    }

    #[test]
    fn replay_eligibility_rules() {
        assert!(replay_eligible(Some("PROCESSING_TIMEOUT")));
        assert!(replay_eligible(Some("UNKNOWN_CRS")));
        assert!(!replay_eligible(Some("EMPTY_DATASET")));
        assert!(!replay_eligible(Some("PIPELINE_ERROR")));
        assert!(!replay_eligible(None));
    }

    #[test]
    fn retry_policy_bounds() {
        let policy = RetryPolicy::default();
        assert!(!policy.exhausted(3));
        assert!(policy.exhausted(4));
        assert_eq!(policy.next_delay_secs(0), 0);
        assert_eq!(policy.next_delay_secs(1), 30);
        assert_eq!(policy.next_delay_secs(2), 60);
        assert_eq!(policy.next_delay_secs(3), 120);
        assert_eq!(policy.next_delay_secs(99), 120);
    }

    #[test]
    fn dlq_store_roundtrip_and_removal() {
        let dir = std::env::temp_dir().join(format!("vtile-dlq-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let store = FileDlqStore::new(&dir);
        let record = DlqRecord {
            job_id: "job_dlq_1".into(),
            tenant_id: "tenant-acme".into(),
            layer_id: "us-parcels".into(),
            source_uri: "mem://x".into(),
            error_code: "EMPTY_DATASET".into(),
            error_class: ErrorClass::PermanentValidation,
            failed_stage: "NORMALIZING".into(),
            error_message: "empty".into(),
            retry_count: 1,
            max_receives: 4,
            replay_eligible: false,
            failed_at: Utc::now(),
        };
        store.dead_letter(&record).unwrap();
        let listed = store.list_tenant("tenant-acme").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].job_id, "job_dlq_1");
        assert!(store.get("tenant-acme", "job_dlq_1").unwrap().is_some());
        assert!(store.list_tenant("tenant-other").unwrap().is_empty());
        store.remove("tenant-acme", "job_dlq_1").unwrap();
        assert!(store.get("tenant-acme", "job_dlq_1").unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }
}
