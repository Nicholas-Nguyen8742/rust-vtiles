//! DLQ-style replay of quarantined jobs (Recommendation 3 US-03), hardened
//! with the replay guardrails and audit trail from the idempotency epic
//! (Sequence 1 US-05).
//!
//! A replay re-runs a job using its original identity and source bytes — the
//! local equivalent of redriving an SQS dead-letter message.
//!
//! Rules (US-05):
//! * `FAILED` jobs replay (canonical DLQ redrive).
//! * `COMPLETED` jobs replay only with explicit `createNewVersion` intent.
//! * Active jobs are rejected with `JOB_ALREADY_ACTIVE`.
//! * `CANCELLED` jobs never replay.
//!
//! Idempotency (TRD §14 "DLQ replay must not duplicate published tiles"):
//! * `run_job`'s idempotency guard still rejects `COMPLETED`/`CANCELLED`
//!   records — the reset below is persisted first, and replays are the only
//!   sanctioned way back into the workflow.
//! * Each run mints a fresh `tileVersion` under the tenant/layer prefix and
//!   swaps `manifest.json` / `latest.json` atomically, so a replay either
//!   publishes a complete new version or leaves the previous one untouched.
//!
//! Source bytes resolve from the quarantine first; when absent (e.g. a
//! `COMPLETED` re-publish), they fall back to the staged upload, which TRD
//! §6 retains for 30 days.

use std::fs;
use std::path::Path;

use chrono::Utc;
use tracing::info;

use vtile_core::config::TileConfig;
use vtile_core::model::{JobStatus, ReplayAudit};
use vtile_ingest::normalize::{CrsPolicy, NormalizeOptions};

use crate::error::{PipelineError, PipelineResult};
use crate::events::PipelineEvent;
use crate::idempotency::{IdempotencyMetrics, Metric};
use crate::job::{
    default_mvt_layer_name, job_paths_for, new_event_id, run_job, JobDeps, JobOutcome, RunJobInput,
};

/// Options for a replay attempt.
#[derive(Debug, Clone, Default)]
pub struct ReplayOptions {
    /// Assume EPSG:4326 for sources without CRS information. For shapefile
    /// failures with `UNKNOWN_CRS` this flag is the "user confirmation"
    /// required by TRD §10; for other failures it is simply carried through.
    pub assume_wgs84: bool,
    /// Operator identity recorded in the replay audit and the
    /// `vector.tile.job.replay_requested` event (Sequence 1 US-05).
    /// Production enforces role authorization in front of this.
    pub requested_by: String,
    /// Free-form reason recorded in the audit trail ("Transient Fargate
    /// timeout", ...).
    pub reason: String,
    /// Explicit intent to publish a fresh tile version from a `COMPLETED`
    /// job. Required to replay completed jobs; ignored for `FAILED` redrive.
    pub create_new_version: bool,
}

/// Replays a job end to end under its original identity.
///
/// Fails with a descriptive job error when the job record is missing, the
/// tenant does not match, the status is not replayable, or no source bytes
/// can be resolved.
pub fn replay_job(
    deps: &JobDeps,
    data_dir: &Path,
    tenant_id: &str,
    job_id: &str,
    opts: &ReplayOptions,
) -> PipelineResult<JobOutcome> {
    let mut job = deps
        .jobs
        .get(job_id)?
        .ok_or_else(|| PipelineError::Job(format!("job {job_id} not found")))?;
    if job.tenant_id != tenant_id {
        // Tenant isolation (TRD §13): never replay across tenants.
        return Err(PipelineError::Job(format!(
            "job {job_id} belongs to a different tenant"
        )));
    }

    // ── Sequence 1 US-05 replay guardrails ────────────────────────────────
    IdempotencyMetrics::global().inc(Metric::ReplayRequestedCount);
    match job.status {
        // Canonical DLQ redrive.
        JobStatus::Failed => {}
        // Authorized re-publication of a completed layer.
        JobStatus::Completed if opts.create_new_version => {}
        JobStatus::Completed => {
            IdempotencyMetrics::global().inc(Metric::ReplayRejectedCount);
            return Err(PipelineError::Job(format!(
                "job {job_id} is COMPLETED; replay requires explicit createNewVersion intent"
            )));
        }
        JobStatus::Cancelled => {
            IdempotencyMetrics::global().inc(Metric::ReplayRejectedCount);
            return Err(PipelineError::Job(format!(
                "job {job_id} is CANCELLED; cancelled jobs cannot be replayed"
            )));
        }
        active => {
            IdempotencyMetrics::global().inc(Metric::ReplayRejectedCount);
            return Err(PipelineError::JobAlreadyActive(format!(
                "job {job_id} is {} — replay rejected (JOB_ALREADY_ACTIVE)",
                active.as_str()
            )));
        }
    }

    // Source bytes: quarantine first (DLQ redrive), then the staged upload
    // (completed-job re-publish; TRD §6 retains staging for 30 days).
    let quarantined = match deps.quarantine.as_ref() {
        Some(store) => store.load(tenant_id, job_id)?,
        None => None,
    };
    let (source_bytes, source_uri) = match quarantined {
        Some(q) => (q.source_bytes, q.report.source_uri),
        None => {
            let path = job.source_uri.trim_start_matches("file://");
            let bytes = fs::read(path).map_err(|e| {
                PipelineError::Job(format!(
                    "job {job_id} has no quarantined input and staging source {path} is unreadable: {e}"
                ))
            })?;
            (bytes, job.source_uri.clone())
        }
    };

    // Audit + event before re-running: who replayed, and why (US-05).
    let audit = ReplayAudit {
        requested_by: opts.requested_by.clone(),
        reason: opts.reason.clone(),
        create_new_version: opts.create_new_version,
        occurred_at: Utc::now(),
    };
    deps.events.emit(PipelineEvent::VectorTileJobReplayRequested {
        event_id: new_event_id(),
        tenant_id: job.tenant_id.clone(),
        job_id: job.job_id.clone(),
        layer_id: job.layer_id.clone(),
        requested_by: audit.requested_by.clone(),
        reason: audit.reason.clone(),
        create_new_version: audit.create_new_version,
        occurred_at: audit.occurred_at,
    });

    // Clear the failure state and re-enter the workflow at QUEUED.
    job.status = JobStatus::Queued;
    job.error = None;
    job.error_code = None;
    job.failed_stage = None;
    // Release any stale lease left by a crashed run (US-04 takeover).
    job.lease_token = None;
    job.locked_by = None;
    job.lease_expires_at = None;
    job.replay_audit = Some(audit);
    job.state_version += 1;
    job.updated_at = Utc::now();
    deps.jobs.upsert(job.clone())?;

    let layer_id = job.layer_id.clone();
    let category = job.layer_input.as_ref().and_then(|m| m.category);
    let tile_config = TileConfig {
        layer_name: default_mvt_layer_name(category, &layer_id),
        zoom_range: job.requested_zoom_range,
        ..Default::default()
    };
    let normalize_opts = NormalizeOptions {
        crs_policy: if opts.assume_wgs84 {
            CrsPolicy::AssumeWgs84
        } else {
            CrsPolicy::RequireKnown
        },
        ..Default::default()
    };

    info!(
        job_id = %job_id,
        assume_wgs84 = opts.assume_wgs84,
        requested_by = %opts.requested_by,
        create_new_version = opts.create_new_version,
        source_uri = %source_uri,
        "replaying job"
    );

    let input = RunJobInput {
        job,
        source_bytes,
        tile_config,
        normalize_opts,
        paths: job_paths_for(data_dir, tenant_id, job_id, &layer_id),
    };
    run_job(&input, deps)
}
