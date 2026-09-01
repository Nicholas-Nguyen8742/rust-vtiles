//! DLQ-style replay of quarantined jobs (Recommendation 3 US-03).
//!
//! A replay re-runs a previously FAILED job using the quarantined source
//! bytes — the local equivalent of redriving an SQS dead-letter message.
//!
//! Idempotency (TRD §14 "DLQ replay must not duplicate published tiles"):
//! * `COMPLETED` jobs are never replayed — the `run_job` idempotency guard
//!   rejects them, and this module only accepts `FAILED` jobs.
//! * Each run mints a fresh `tileVersion` under the tenant/layer prefix and
//!   swaps `manifest.json` / `latest.json` atomically, so a replay either
//!   publishes a complete new version or leaves the previous one untouched.

use std::path::Path;

use chrono::Utc;
use tracing::info;

use vtile_core::config::TileConfig;
use vtile_core::model::JobStatus;
use vtile_ingest::normalize::{CrsPolicy, NormalizeOptions};

use crate::error::{PipelineError, PipelineResult};
use crate::job::{default_mvt_layer_name, job_paths_for, run_job, JobDeps, JobOutcome, RunJobInput};

/// Options for a replay attempt.
#[derive(Debug, Clone, Default)]
pub struct ReplayOptions {
    /// Assume EPSG:4326 for sources without CRS information. For shapefile
    /// failures with `UNKNOWN_CRS` this flag is the "user confirmation"
    /// required by TRD §10; for other failures it is simply carried through.
    pub assume_wgs84: bool,
}

/// Replays a failed, quarantined job end to end.
///
/// Fails with a descriptive job error when the quarantine entry or the job
/// record is missing, or when the job is not in the `FAILED` state.
pub fn replay_job(
    deps: &JobDeps,
    data_dir: &Path,
    tenant_id: &str,
    job_id: &str,
    opts: &ReplayOptions,
) -> PipelineResult<JobOutcome> {
    let quarantine = deps
        .quarantine
        .as_ref()
        .ok_or_else(|| PipelineError::Job("no quarantine store configured".into()))?;
    let quarantined = quarantine
        .load(tenant_id, job_id)?
        .ok_or_else(|| {
            PipelineError::Job(format!(
                "job {job_id} has no quarantined input for tenant {tenant_id}"
            ))
        })?;

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
    if job.status != JobStatus::Failed {
        return Err(PipelineError::Job(format!(
            "only FAILED jobs can be replayed (job {job_id} is {})",
            job.status.as_str()
        )));
    }

    // Clear the failure state and re-enter the workflow at QUEUED.
    job.status = JobStatus::Queued;
    job.error = None;
    job.error_code = None;
    job.failed_stage = None;
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
        source_uri = %quarantined.report.source_uri,
        "replaying quarantined job"
    );

    let input = RunJobInput {
        job,
        source_bytes: quarantined.source_bytes,
        tile_config,
        normalize_opts,
        paths: job_paths_for(data_dir, tenant_id, job_id, &layer_id),
    };
    run_job(&input, deps)
}
