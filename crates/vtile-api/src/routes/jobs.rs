//! `GET /api/v1/jobs/:job_id` (TRD §8.2).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;

use crate::auth;
use crate::dto::JobResponse;
use crate::error::ApiError;
use crate::state::AppState;

/// Returns job status and, once finished, the outcome summary required by
/// TRD §8.2 (feature count, tile count, bbox, completion time).
pub async fn get_job(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<JobResponse>, ApiError> {
    let job = state
        .jobs
        .get(&job_id)?
        .ok_or_else(|| ApiError::not_found("JOB_NOT_FOUND", format!("job {job_id} not found")))?;

    // Tenant isolation (TRD §13 / Sequence 5 TI-01): when token auth is
    // enabled, callers may only observe jobs belonging to their own tenant.
    // Cross-tenant attempts are denied, audited, and counted centrally.
    auth::check_tenant_access(&state, &headers, &job.tenant_id, "JOB", &job_id)?;
    // Sequence 5 TI-06: ALLOW decision audit for control-plane reads.
    vtile_pipeline::obs::record_access_decision(
        &state.data_dir,
        &job.tenant_id,
        auth::authorized_tenant(&state, &headers).as_deref(),
        "JOB",
        &job_id,
        "READ",
        "ALLOW",
        None,
    );

    let outcome = job.outcome.as_ref();
    Ok(Json(JobResponse {
        job_id: job.job_id,
        status: job.status,
        layer_id: job.layer_id,
        feature_count: outcome.map(|o| o.feature_count),
        published_tile_count: outcome.map(|o| o.published_tile_count),
        bounding_box: outcome.map(|o| o.bounding_box.to_vec()),
        completed_at: outcome.map(|o| o.completed_at.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        error: job.error,
        error_code: job.error_code,
        failed_stage: job.failed_stage,
    }))
}
