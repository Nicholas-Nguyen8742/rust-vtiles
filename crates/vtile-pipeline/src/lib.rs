//! `vtile-pipeline` — job orchestration (TRD §10 Step Functions states).
//!
//! This crate wires `vtile-ingest` (normalize) and `vtile-core` (tile
//! generation) into an executable job:
//!
//! ```text
//! Validate Upload → Detect Format → Unpack → Validate CRS/Geometry
//! → Normalize → Clean Properties → Generate Tiles → Publish
//! → Write Manifest → Update Catalog → Emit Event
//! ```
//!
//! In production AWS this logic runs in the ECS Fargate tile processor
//! (TRD §11 Decision 2); Step Functions invokes each subcommand of the
//! `vtile` binary. Locally the same code runs against the filesystem.

pub mod error;
pub mod events;
pub mod idempotency;
pub mod job;
pub mod manifest;
pub mod obs;
pub mod publish;
pub mod quarantine;
pub mod recovery;
pub mod replay;
pub mod sink_local;
#[cfg(feature = "aws")]
pub mod sink_s3;
pub mod store;
pub mod tenant;

pub use error::{PipelineError, PipelineResult};
pub use idempotency::{
    classify_ingest_event, event_dedupe_fingerprint, processing_profile_label, request_fingerprint,
    upload_idempotency_key, DedupeRecord, DedupeStore, EventDecision, FileDedupeStore,
    FileOrphanStore, IdempotencyMetrics, Metric, OrphanEvent,
};
pub use job::{new_replay_id, run_job, JobDeps, RunJobInput};
pub use manifest::TileManifest;
pub use obs::{
    alert_rules, build_dashboard, category_label, dlq_depth, emit_stage_log, evaluate_alerts,
    layer_health, merged_metrics_snapshot, new_span_id, new_trace_id, record_access_denied,
    AlertCondition, AlertRule, AlertSeverity, AlertState, AuditRecord, FileAuditTrail,
    LayerHealth, ObsMetrics, StageLog, SERVICE_API, SERVICE_PROCESSOR,
};
pub use publish::{
    aggregate_checksum, promote_layer_version, read_candidate_manifest, rollback_layer_version,
    tile_url_template_for, verify_candidate, write_candidate_manifest, AuditAction,
    CandidateManifest, FileAuditLog, FileLayerRegistry, LayerVersionRecord, PublishAuditRecord,
    PublishMetric, PublishMetrics, PublishStatus, TileEntry, PIPELINE_ACTOR,
};
pub use quarantine::{ErrorReport, FileQuarantineStore, QuarantineStore};
pub use recovery::{
    classify_code, dead_letter_failure, remediation_for, replay_eligible, run_job_with_retries,
    DlqRecord, DlqStore, ErrorClass, FileDlqStore, RecoveryMetric, RecoveryMetrics, RetryPolicy,
    MAX_MANUAL_REPLAYS,
};
pub use replay::{replay_job, ReplayOptions, ReplayOutcome};
pub use store::Lease;
pub use tenant::{
    is_valid_resource_id, is_valid_tenant_id, tenant_alignment_holds, TenantContext,
    TENANT_ID_PATTERN,
};
