//! Pipeline events (TRD §9 event schemas).
//!
//! In production these are published to EventBridge (`source: vis.geo`,
//! detail-type = `eventType`); the MVP implementation logs structured JSON so
//! downstream consumers can be wired without changing call sites.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The three lifecycle events defined in TRD §9, the replay audit event from
/// the idempotency epic (Sequence 1 US-05), and the atomic-publishing events
/// (Sequence 2 US-AP-03/05/06).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "eventType")]
pub enum PipelineEvent {
    #[serde(rename = "vector.tile.job.submitted")]
    VectorTileJobSubmitted {
        event_id: String,
        tenant_id: String,
        job_id: String,
        layer_id: String,
        source_format: String,
        source_uri: String,
        occurred_at: DateTime<Utc>,
    },
    #[serde(rename = "vector.tile.job.completed")]
    VectorTileJobCompleted {
        event_id: String,
        tenant_id: String,
        job_id: String,
        layer_id: String,
        feature_count: u64,
        tile_count: u64,
        min_zoom: u8,
        max_zoom: u8,
        tile_version: String,
        occurred_at: DateTime<Utc>,
    },
    #[serde(rename = "vector.tile.job.failed")]
    VectorTileJobFailed {
        event_id: String,
        tenant_id: String,
        job_id: String,
        error_code: String,
        error_message: String,
        occurred_at: DateTime<Utc>,
    },
    /// Sequence 1 US-05: audit trail for replays — who requested it, why,
    /// and whether a new tile version was explicitly authorized.
    #[serde(rename = "vector.tile.job.replay_requested")]
    VectorTileJobReplayRequested {
        event_id: String,
        tenant_id: String,
        job_id: String,
        layer_id: String,
        requested_by: String,
        reason: String,
        create_new_version: bool,
        occurred_at: DateTime<Utc>,
    },
    /// Sequence 2 US-AP-03/06: a candidate version passed validation and the
    /// authoritative layer pointer moved atomically.
    #[serde(rename = "vector.tile.version.promoted")]
    VectorTileVersionPromoted {
        event_id: String,
        tenant_id: String,
        layer_id: String,
        job_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from_tile_version: Option<String>,
        to_tile_version: String,
        actor: String,
        occurred_at: DateTime<Utc>,
    },
    /// Sequence 2 US-AP-05/06: the authoritative pointer was moved back to a
    /// known-good version without reprocessing.
    #[serde(rename = "vector.tile.version.rolled_back")]
    VectorTileVersionRolledBack {
        event_id: String,
        tenant_id: String,
        layer_id: String,
        from_tile_version: String,
        to_tile_version: String,
        actor: String,
        reason: String,
        occurred_at: DateTime<Utc>,
    },
    /// Sequence 2 US-AP-06: promotion or candidate validation failed; the
    /// previous published version remains active.
    #[serde(rename = "vector.tile.publish.failed")]
    VectorTilePublishFailed {
        event_id: String,
        tenant_id: String,
        layer_id: String,
        job_id: String,
        tile_version: String,
        error_code: String,
        error_message: String,
        occurred_at: DateTime<Utc>,
    },
    /// Sequence 3 US-01: a transient failure will be retried after a backoff
    /// delay.
    #[serde(rename = "vector.tile.job.retry_scheduled")]
    VectorTileJobRetryScheduled {
        event_id: String,
        tenant_id: String,
        job_id: String,
        layer_id: String,
        attempt: u32,
        delay_secs: u64,
        error_code: String,
        occurred_at: DateTime<Utc>,
    },
    /// Sequence 3 US-01/US-06: retries exhausted (or non-retryable failure) —
    /// the job was captured in the dead-letter queue.
    #[serde(rename = "vector.tile.job.dead-lettered")]
    VectorTileJobDeadLettered {
        event_id: String,
        tenant_id: String,
        job_id: String,
        layer_id: String,
        error_code: String,
        error_class: crate::recovery::ErrorClass,
        failed_stage: String,
        retry_count: u32,
        occurred_at: DateTime<Utc>,
    },
    /// Sequence 3 US-02/US-06: a failed upload was quarantined with its
    /// failure report.
    #[serde(rename = "vector.tile.job.quarantined")]
    VectorTileJobQuarantined {
        event_id: String,
        tenant_id: String,
        job_id: String,
        layer_id: String,
        error_code: String,
        quarantine_dir: String,
        occurred_at: DateTime<Utc>,
    },
    /// Sequence 3 US-05: a replay finished successfully.
    #[serde(rename = "vector.tile.job.replay.completed")]
    VectorTileJobReplayCompleted {
        event_id: String,
        tenant_id: String,
        job_id: String,
        layer_id: String,
        tile_version: String,
        occurred_at: DateTime<Utc>,
    },
    /// Sequence 3 US-05/US-06: a replay attempt failed.
    #[serde(rename = "vector.tile.job.replay.failed")]
    VectorTileJobReplayFailed {
        event_id: String,
        tenant_id: String,
        job_id: String,
        layer_id: String,
        error_code: String,
        error_message: String,
        occurred_at: DateTime<Utc>,
    },
    /// Sequence 3 US-03/US-06: a replay request was denied (permanent
    /// failure class, replay limit, or job state).
    #[serde(rename = "vector.tile.job.replay.denied")]
    VectorTileJobReplayDenied {
        event_id: String,
        tenant_id: String,
        job_id: String,
        layer_id: String,
        reason: String,
        occurred_at: DateTime<Utc>,
    },
}

impl PipelineEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            PipelineEvent::VectorTileJobSubmitted { .. } => "vector.tile.job.submitted",
            PipelineEvent::VectorTileJobCompleted { .. } => "vector.tile.job.completed",
            PipelineEvent::VectorTileJobFailed { .. } => "vector.tile.job.failed",
            PipelineEvent::VectorTileJobReplayRequested { .. } => {
                "vector.tile.job.replay_requested"
            }
            PipelineEvent::VectorTileVersionPromoted { .. } => "vector.tile.version.promoted",
            PipelineEvent::VectorTileVersionRolledBack { .. } => {
                "vector.tile.version.rolled_back"
            }
            PipelineEvent::VectorTilePublishFailed { .. } => "vector.tile.publish.failed",
            PipelineEvent::VectorTileJobRetryScheduled { .. } => "vector.tile.job.retry_scheduled",
            PipelineEvent::VectorTileJobDeadLettered { .. } => "vector.tile.job.dead-lettered",
            PipelineEvent::VectorTileJobQuarantined { .. } => "vector.tile.job.quarantined",
            PipelineEvent::VectorTileJobReplayCompleted { .. } => {
                "vector.tile.job.replay.completed"
            }
            PipelineEvent::VectorTileJobReplayFailed { .. } => "vector.tile.job.replay.failed",
            PipelineEvent::VectorTileJobReplayDenied { .. } => "vector.tile.job.replay.denied",
        }
    }
}

/// Where lifecycle events are delivered.
pub trait EventEmitter: Send + Sync {
    fn emit(&self, event: PipelineEvent);
}

/// Structured-log emitter (MVP default).
pub struct LoggingEventEmitter;

impl EventEmitter for LoggingEventEmitter {
    fn emit(&self, event: PipelineEvent) {
        let json = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
        tracing::info!(event_type = event.event_type(), event = %json, "pipeline event");
    }
}

/// No-op emitter for tests.
#[derive(Default)]
pub struct NullEventEmitter;

impl EventEmitter for NullEventEmitter {
    fn emit(&self, _event: PipelineEvent) {}
}
