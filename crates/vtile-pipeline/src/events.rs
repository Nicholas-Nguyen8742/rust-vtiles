//! Pipeline events (TRD §9 event schemas).
//!
//! In production these are published to EventBridge (`source: vis.geo`,
//! detail-type = `eventType`); the MVP implementation logs structured JSON so
//! downstream consumers can be wired without changing call sites.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The three lifecycle events defined in TRD §9, plus the replay audit
/// event added by the idempotency epic (Sequence 1 US-05).
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
