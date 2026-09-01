//! Idempotency controls (Sequence 1 epic).
//!
//! Stable job identity, duplicate-event suppression, and telemetry for the
//! ingestion pipeline. Local stand-ins for the production pieces:
//!
//! | Production (TRD §2/§11)        | Local equivalent here               |
//! |--------------------------------|-------------------------------------|
//! | DynamoDB idempotency-key GSI   | `JobStore::find_by_idempotency_key` |
//! | EventBridge/SQS redelivery     | content-PUT re-delivery + `DedupeStore` |
//! | CloudWatch custom metrics      | `IdempotencyMetrics` + `GET /internal/metrics` |
//! | DLQ orphan-event alerting      | `FileOrphanStore` + warn logs       |

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use vtile_core::model::{JobRecord, JobStatus, LayerCategory, SourceFormat, ZoomRange};

use crate::error::PipelineResult;

/// All keys and fingerprints are `sha256:` + lowercase hex.
pub const SHA256_PREFIX: &str = "sha256:";

fn sha256_hex(input: &str) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(input.as_bytes()))
}

fn fingerprint(kind: &str, parts: &[&str]) -> String {
    // NUL-separated so field boundaries are unambiguous.
    let joined = parts.join("\u{0}");
    format!("{SHA256_PREFIX}{}", sha256_hex(&format!("{kind}\u{0}{joined}")))
}

/// Canonical processing-profile label (part of the US-01 key formula). Keeps
/// key computation deterministic regardless of request field ordering.
pub fn processing_profile_label(category: Option<LayerCategory>, zoom: ZoomRange) -> String {
    let cat = match category {
        Some(c) => format!("{c:?}").to_lowercase(),
        None => "other".to_string(),
    };
    format!("{}:{}-{}", cat, zoom.min_zoom, zoom.max_zoom)
}

/// Sequence 1 US-01: canonical upload idempotency key.
///
/// ```text
/// idempotencyKey = SHA-256(tenantId + layerId + clientIdempotencyToken
///                          + requestedProcessingProfile)
/// ```
///
/// Requests without an explicit client token get a server-minted token, so
/// intentional repeat uploads (e.g. a county parcel refresh) receive a new
/// `jobId` unless the client supplies the same token.
pub fn upload_idempotency_key(
    tenant_id: &str,
    layer_id: &str,
    client_token: &str,
    processing_profile: &str,
) -> String {
    fingerprint(
        "idempotency-key",
        &[tenant_id, layer_id, client_token, processing_profile],
    )
}

/// Sequence 1 US-02: fingerprint of the upload *payload*, stored on the job
/// and compared when an idempotency key is reused. A mismatch means the same
/// token was sent with a different request → `IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`.
pub fn request_fingerprint(
    file_name: &str,
    content_type: Option<&str>,
    source_format: SourceFormat,
    zoom: ZoomRange,
    processing_profile: &str,
) -> String {
    let zoom_label = format!("{}-{}", zoom.min_zoom, zoom.max_zoom);
    fingerprint(
        "request",
        &[
            file_name,
            content_type.unwrap_or(""),
            source_format.as_str(),
            &zoom_label,
            processing_profile,
        ],
    )
}

/// Sequence 1 US-03: dedupe fingerprint of the event that starts processing.
///
/// ```text
/// eventDedupeFingerprint = SHA-256(tenantId + layerId + s3ObjectKey
///                                  + s3ETag + jobId)
/// ```
///
/// Locally the "etag" is the SHA-256 of the uploaded bytes (S3 returns an
/// ETag for single PUTs; multipart ETags are not content hashes, so the
/// production normalizer should hash the object there too).
pub fn event_dedupe_fingerprint(
    tenant_id: &str,
    layer_id: &str,
    object_key: &str,
    etag: &str,
    job_id: &str,
) -> String {
    fingerprint("event", &[tenant_id, layer_id, object_key, etag, job_id])
}

// ── US-03: duplicate event suppression ─────────────────────────────────────

/// Decision for an inbound content event (local stand-in for the S3/SQS
/// event boundary).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventDecision {
    /// First event for a pending job — start processing, exactly once.
    StartRun,
    /// The job is already being processed, or this exact event was already
    /// seen — acknowledge, count it, start nothing.
    DuplicateSuppressed,
    /// The job reached a terminal state — acknowledge without new work; a new
    /// run requires an explicit replay.
    TerminalAck,
    /// No job resolves for the event — quarantine and alert, never silently
    /// create an untracked job.
    Orphan,
}

/// Applies the US-03 duplicate-handling rules to an inbound event.
pub fn classify_ingest_event(job: Option<&JobRecord>, fingerprint_seen: bool) -> EventDecision {
    let Some(job) = job else {
        return EventDecision::Orphan;
    };
    if fingerprint_seen {
        return EventDecision::DuplicateSuppressed;
    }
    match job.status {
        JobStatus::UploadPending => EventDecision::StartRun,
        JobStatus::Queued
        | JobStatus::Validating
        | JobStatus::Normalizing
        | JobStatus::Tiling
        | JobStatus::Publishing => EventDecision::DuplicateSuppressed,
        JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled => {
            EventDecision::TerminalAck
        }
    }
}

/// US-03 dedupe record persisted for every accepted event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DedupeRecord {
    pub dedupe_key: String,
    pub job_id: String,
    pub seen_at: DateTime<Utc>,
    pub source_event_type: String,
}

/// Dedupe persistence port. Production maps onto a DynamoDB table keyed by
/// `dedupeKey` with a TTL (fingerprint validity window).
pub trait DedupeStore: Send + Sync {
    fn seen(&self, dedupe_key: &str) -> PipelineResult<bool>;
    fn record(&self, record: &DedupeRecord) -> PipelineResult<()>;
}

/// Filesystem dedupe store rooted at `data/dedupe/`.
pub struct FileDedupeStore {
    root: PathBuf,
}

impl FileDedupeStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, dedupe_key: &str) -> PathBuf {
        // Keys are `sha256:` + hex; filename-safe after the prefix swap.
        self.root
            .join(format!("{}.json", dedupe_key.replace(':', "_")))
    }
}

impl DedupeStore for FileDedupeStore {
    fn seen(&self, dedupe_key: &str) -> PipelineResult<bool> {
        Ok(self.path_for(dedupe_key).exists())
    }

    fn record(&self, record: &DedupeRecord) -> PipelineResult<()> {
        fs::create_dir_all(&self.root)?;
        let path = self.path_for(&record.dedupe_key);
        // Atomic write (tmp + rename) as everywhere else in the local stores.
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_string_pretty(record)?)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }
}

// ── US-01/US-03: orphan events ─────────────────────────────────────────────

/// An event with no resolvable job. Recorded and alerted; never silently
/// turned into an untracked job (US-01 acceptance criterion).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrphanEvent {
    pub source_event_type: String,
    pub object_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub reason: String,
    pub detected_at: DateTime<Utc>,
}

/// Filesystem orphan-event store rooted at `data/orphans/`.
pub struct FileOrphanStore {
    root: PathBuf,
}

impl FileOrphanStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Persists the orphan record; returns the file written.
    pub fn record(&self, event: &OrphanEvent) -> PipelineResult<PathBuf> {
        fs::create_dir_all(&self.root)?;
        let suffix = &sha256_hex(&event.object_key)[..12];
        let path = self.root.join(format!(
            "{}-{suffix}.json",
            event.detected_at.format("%Y%m%dT%H%M%S%3f")
        ));
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_string_pretty(event)?)?;
        fs::rename(&tmp, &path)?;
        Ok(path)
    }
}

// ── US-06: telemetry ───────────────────────────────────────────────────────

/// Metric names from the idempotency epic (US-06), plus `idempotent_replays`
/// (duplicate upload requests resolved to an existing job).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    DuplicateEventsSuppressed,
    IdempotencyKeyConflicts,
    IdempotentReplays,
    OrphanEventsDetected,
    LeaseAcquisitionSuccess,
    LeaseAcquisitionConflict,
    LeaseExpiredCount,
    ReplayRequestedCount,
    ReplayRejectedCount,
    StateTransitionConflict,
}

impl Metric {
    pub const ALL: [Metric; 10] = [
        Metric::DuplicateEventsSuppressed,
        Metric::IdempotencyKeyConflicts,
        Metric::IdempotentReplays,
        Metric::OrphanEventsDetected,
        Metric::LeaseAcquisitionSuccess,
        Metric::LeaseAcquisitionConflict,
        Metric::LeaseExpiredCount,
        Metric::ReplayRequestedCount,
        Metric::ReplayRejectedCount,
        Metric::StateTransitionConflict,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Metric::DuplicateEventsSuppressed => "duplicate_events_suppressed",
            Metric::IdempotencyKeyConflicts => "idempotency_key_conflicts",
            Metric::IdempotentReplays => "idempotent_replays",
            Metric::OrphanEventsDetected => "orphan_events_detected",
            Metric::LeaseAcquisitionSuccess => "lease_acquisition_success",
            Metric::LeaseAcquisitionConflict => "lease_acquisition_conflict",
            Metric::LeaseExpiredCount => "lease_expired_count",
            Metric::ReplayRequestedCount => "replay_requested_count",
            Metric::ReplayRejectedCount => "replay_rejected_count",
            Metric::StateTransitionConflict => "job_state_transition_conflict",
        }
    }
}

/// Process-wide idempotency counters (local analog of CloudWatch custom
/// metrics). Exposed by the API at `GET /internal/metrics`; snapshot deltas
/// are test-friendly under parallel test threads.
#[derive(Debug, Default)]
pub struct IdempotencyMetrics {
    duplicate_events_suppressed: AtomicU64,
    idempotency_key_conflicts: AtomicU64,
    idempotent_replays: AtomicU64,
    orphan_events_detected: AtomicU64,
    lease_acquisition_success: AtomicU64,
    lease_acquisition_conflict: AtomicU64,
    lease_expired_count: AtomicU64,
    replay_requested_count: AtomicU64,
    replay_rejected_count: AtomicU64,
    job_state_transition_conflict: AtomicU64,
}

impl IdempotencyMetrics {
    pub fn global() -> &'static IdempotencyMetrics {
        static METRICS: OnceLock<IdempotencyMetrics> = OnceLock::new();
        METRICS.get_or_init(IdempotencyMetrics::default)
    }

    pub fn inc(&self, metric: Metric) {
        self.counter(metric).fetch_add(1, Ordering::Relaxed);
        tracing::debug!(metric = metric.as_str(), "idempotency metric incremented");
    }

    pub fn count(&self, metric: Metric) -> u64 {
        self.counter(metric).load(Ordering::Relaxed)
    }

    /// JSON snapshot keyed by metric name (served at `/internal/metrics`).
    pub fn snapshot(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for metric in Metric::ALL {
            map.insert(metric.as_str().to_string(), self.count(metric).into());
        }
        serde_json::Value::Object(map)
    }

    fn counter(&self, metric: Metric) -> &AtomicU64 {
        match metric {
            Metric::DuplicateEventsSuppressed => &self.duplicate_events_suppressed,
            Metric::IdempotencyKeyConflicts => &self.idempotency_key_conflicts,
            Metric::IdempotentReplays => &self.idempotent_replays,
            Metric::OrphanEventsDetected => &self.orphan_events_detected,
            Metric::LeaseAcquisitionSuccess => &self.lease_acquisition_success,
            Metric::LeaseAcquisitionConflict => &self.lease_acquisition_conflict,
            Metric::LeaseExpiredCount => &self.lease_expired_count,
            Metric::ReplayRequestedCount => &self.replay_requested_count,
            Metric::ReplayRejectedCount => &self.replay_rejected_count,
            Metric::StateTransitionConflict => &self.job_state_transition_conflict,
        }
    }
}
