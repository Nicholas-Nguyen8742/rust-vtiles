//! DLQ-style replay of failed jobs (Recommendation 3 US-03), hardened by the
//! idempotency epic (Sequence 1 US-05) and the DLQ/replay epic (Sequence 3).
//!
//! A replay re-runs a job under its **original identity and source bytes** —
//! the local equivalent of redriving an SQS dead-letter message while
//! preserving the original `jobId`.
//!
//! Guardrails (Sequence 3 US-03/US-04):
//! * `FAILED` jobs replay only when the failure class is replay-eligible
//!   (transient failures, plus `UNKNOWN_CRS` with explicit WGS84
//!   confirmation). Permanent validation failures return `REPLAY_NOT_ALLOWED`
//!   — fix the source and submit a new upload.
//! * `COMPLETED` jobs replay only with explicit `createNewVersion` intent;
//!   otherwise the replay is a no-op (`REPLAY_NO_OP`).
//! * Active jobs are rejected with `JOB_ALREADY_ACTIVE`; `CANCELLED` jobs
//!   never replay.
//! * Manual replays are bounded by `recovery::MAX_MANUAL_REPLAYS`.
//!
//! Idempotency (TRD §14 "DLQ replay must not duplicate published tiles"):
//! * `run_job`'s idempotency guard still rejects `COMPLETED`/`CANCELLED`
//!   records; the reset below is persisted first, and replays are the only
//!   sanctioned way back into the workflow.
//! * Each run mints a fresh `tileVersion` under the tenant/layer prefix and
//!   promotes it through the atomic publish flow (Sequence 2), so a replay
//!   publishes a complete new version or leaves the previous one untouched.
//!
//! Recovery integration (Sequence 3 US-05): a successful replay removes the
//! job's dead-letter record; a failed replay is captured in the DLQ again.

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
    default_mvt_layer_name, error_classification, job_paths_for, new_event_id, run_job, JobDeps,
    JobOutcome, RunJobInput,
};
use crate::recovery::{dead_letter_failure, replay_eligible, RecoveryMetric, RecoveryMetrics, MAX_MANUAL_REPLAYS};

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

/// Result of a replay attempt (Sequence 3 US-05).
#[derive(Debug)]
pub enum ReplayOutcome {
    /// The replay ran to completion and published through the atomic flow.
    Executed(JobOutcome),
    /// The original job already completed successfully — nothing to do
    /// (`REPLAY_NO_OP`). No state was changed.
    NoOp { reason: String },
}

/// Emits `vector.tile.job.replay.denied` and counts the rejection
/// (Sequence 3 US-06 audit events).
fn deny_replay(deps: &JobDeps, tenant_id: &str, job_id: &str, layer_id: &str, reason: &str) {
    IdempotencyMetrics::global().inc(Metric::ReplayRejectedCount);
    deps.events.emit(PipelineEvent::VectorTileJobReplayDenied {
        event_id: new_event_id(),
        tenant_id: tenant_id.to_string(),
        job_id: job_id.to_string(),
        layer_id: layer_id.to_string(),
        reason: reason.to_string(),
        occurred_at: Utc::now(),
    });
}

/// Replays a job end to end under its original identity.
///
/// Fails with a descriptive job error when the job record is missing, the
/// tenant does not match, the failure class is not replay-eligible, the
/// replay limit is exhausted, or the job is still active.
pub fn replay_job(
    deps: &JobDeps,
    data_dir: &Path,
    tenant_id: &str,
    job_id: &str,
    opts: &ReplayOptions,
) -> PipelineResult<ReplayOutcome> {
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

    // ── Replay guardrails (Sequence 1 US-05 + Sequence 3 US-03/US-04) ────
    IdempotencyMetrics::global().inc(Metric::ReplayRequestedCount);
    match job.status {
        // Canonical DLQ redrive.
        JobStatus::Failed => {}
        // Authorized re-publication of a completed layer.
        JobStatus::Completed if opts.create_new_version => {}
        // Sequence 3 US-05: replay of an already-successful job is a no-op.
        JobStatus::Completed => {
            return Ok(ReplayOutcome::NoOp {
                reason: "original job already completed successfully".to_string(),
            });
        }
        JobStatus::Cancelled => {
            let reason = format!("job {job_id} is CANCELLED; cancelled jobs cannot be replayed");
            deny_replay(deps, tenant_id, job_id, &job.layer_id, &reason);
            return Err(PipelineError::ReplayNotAllowed(reason));
        }
        active => {
            let reason = format!(
                "job {job_id} is {} — replay rejected (JOB_ALREADY_ACTIVE)",
                active.as_str()
            );
            deny_replay(deps, tenant_id, job_id, &job.layer_id, &reason);
            return Err(PipelineError::JobAlreadyActive(reason));
        }
    }

    // Sequence 3 US-03: replay eligibility is enforced server-side. Only
    // failures are restricted — completed-job re-publication carries no
    // failure class.
    if job.status == JobStatus::Failed && !replay_eligible(job.error_code.as_deref()) {
        let reason = format!(
            "job {job_id} failed with non-replayable error {:?} ({}); correct the source data and submit a new upload",
            job.error_code.as_deref().unwrap_or("UNKNOWN"),
            job.error_class.as_deref().unwrap_or("UNKNOWN")
        );
        deny_replay(deps, tenant_id, job_id, &job.layer_id, &reason);
        return Err(PipelineError::ReplayNotAllowed(reason));
    }

    // Sequence 3 US-04: bounded manual replays before a new upload is
    // required.
    if job.replay_count >= MAX_MANUAL_REPLAYS {
        let reason = format!(
            "job {job_id} reached the maximum of {MAX_MANUAL_REPLAYS} manual replays; submit a new upload"
        );
        deny_replay(deps, tenant_id, job_id, &job.layer_id, &reason);
        return Err(PipelineError::ReplayNotAllowed(reason));
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

    // Audit + event before re-running: who replayed, and why
    // (Sequence 1 US-05 / Sequence 3 US-04).
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
    job.error_class = None;
    job.replay_eligible = false;
    // Release any stale lease left by a crashed run (Sequence 1 US-04).
    job.lease_token = None;
    job.locked_by = None;
    job.lease_expires_at = None;
    job.replay_audit = Some(audit);
    job.replay_count += 1;
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
        replay_count = job.replay_count,
        source_uri = %source_uri,
        "replaying job"
    );

    let replay_count = job.replay_count;
    let input = RunJobInput {
        job,
        source_bytes,
        tile_config,
        normalize_opts,
        paths: job_paths_for(data_dir, tenant_id, job_id, &layer_id),
    };
    match run_job(&input, deps) {
        Ok(outcome) => {
            // Sequence 3 US-05: successful redrive consumes the DLQ entry.
            if let Some(dlq) = deps.dlq.as_ref() {
                if let Err(e) = dlq.remove(tenant_id, job_id) {
                    tracing::warn!(job_id = %job_id, error = %e, "failed to clear DLQ entry");
                }
            }
            RecoveryMetrics::global().inc(RecoveryMetric::ReplaySuccess);
            deps.events.emit(PipelineEvent::VectorTileJobReplayCompleted {
                event_id: new_event_id(),
                tenant_id: tenant_id.to_string(),
                job_id: job_id.to_string(),
                layer_id: layer_id.clone(),
                tile_version: outcome.tile_version.clone(),
                occurred_at: Utc::now(),
            });
            Ok(ReplayOutcome::Executed(outcome))
        }
        Err(err) => {
            let (code, message) = error_classification(&err);
            RecoveryMetrics::global().inc(RecoveryMetric::ReplayFailure);
            deps.events.emit(PipelineEvent::VectorTileJobReplayFailed {
                event_id: new_event_id(),
                tenant_id: tenant_id.to_string(),
                job_id: job_id.to_string(),
                layer_id: layer_id.clone(),
                error_code: code,
                error_message: message,
                occurred_at: Utc::now(),
            });
            // Failed redrive goes back to the DLQ (US-01); the attempt
            // count includes this replay.
            dead_letter_failure(deps, job_id, &err, replay_count as u32);
            Err(err)
        }
    }
}
