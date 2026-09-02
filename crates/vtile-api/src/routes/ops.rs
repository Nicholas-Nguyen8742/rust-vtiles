//! Ops routes: layer version rollback (Sequence 2 US-AP-05), authorized job
//! replay, and DLQ inspection (Sequence 3 US-04/US-06).

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use chrono::Utc;

use vtile_core::model::JobStatus;
use vtile_pipeline::obs::{self, metric};
use vtile_pipeline::publish::{rollback_layer_version, LayerVersionRecord};
use vtile_pipeline::recovery::{FileDlqStore, DlqRecord, DlqStore, MAX_MANUAL_REPLAYS};
use vtile_pipeline::{
    new_replay_id, replay_job as run_replay_job, FileQuarantineStore, JobDeps, PipelineError,
    ReplayOptions, ReplayOutcome,
};

use crate::auth;
use crate::dto::{AuditQuery, DlqListQuery, ReplayJobRequest, ReplayJobResponse, RollbackRequest};
use crate::error::ApiError;
use crate::state::AppState;

/// `POST /api/v1/ops/layers/:layer_id/rollback` — repoints the layer to a
/// previously published tile version **without reprocessing** the source
/// dataset (Sequence 2 US-AP-05).
///
/// Governance (US-AP-06): a `reason` is mandatory and recorded with the actor
/// in the append-only audit trail; production restricts this route to
/// operational roles via API Gateway authorization.
///
/// Idempotent: rolling back to the already-current version returns the
/// existing record unchanged.
pub async fn rollback_layer(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(layer_id): Path<String>,
    Json(req): Json<RollbackRequest>,
) -> Result<(StatusCode, Json<LayerVersionRecord>), ApiError> {
    let layer = state
        .catalog
        .get(&layer_id)?
        .ok_or_else(|| {
            ApiError::not_found("LAYER_NOT_FOUND", format!("layer {layer_id} not found"))
        })?;
    // Tenant isolation (TRD §13): cross-tenant rollbacks surface as 404 so
    // layer existence is not leaked.
    if let Some(tenant) = auth::authorized_tenant(&state, &headers) {
        if tenant != layer.tenant_id {
            return Err(ApiError::not_found(
                "LAYER_NOT_FOUND",
                format!("layer {layer_id} not found"),
            ));
        }
    }

    let target = req.target_tile_version.trim().to_string();
    let reason = req.reason.trim().to_string();
    if target.is_empty() {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST",
            "targetTileVersion is required",
        ));
    }
    if reason.is_empty() {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST",
            "reason is required for rollback (auditability, US-AP-06)",
        ));
    }

    // Local actor identity: the authenticated tenant, or a generic operator
    // marker when auth is disabled. Production derives this from IAM/OIDC.
    let actor = auth::authorized_tenant(&state, &headers)
        .map(|t| format!("api:{t}"))
        .unwrap_or_else(|| "api:operator".to_string());

    let tiles_root = state
        .data_dir
        .join("tiles")
        .join(&layer.tenant_id)
        .join(&layer.layer_id);
    let manifests_root = state
        .data_dir
        .join("manifests")
        .join(&layer.tenant_id)
        .join(&layer.layer_id);

    let record = rollback_layer_version(
        &layer.tenant_id,
        &layer.layer_id,
        &tiles_root,
        &manifests_root,
        &target,
        &reason,
        &actor,
        state.events.as_ref(),
    )
    .map_err(|e| match e {
        PipelineError::RollbackFailed(msg) | PipelineError::PublishValidation(msg) => {
            ApiError::unprocessable("ROLLBACK_INVALID_TARGET", msg)
        }
        other => ApiError::from(other),
    })?;

    tracing::info!(
        layer_id = %layer.layer_id,
        target = %target,
        actor = %actor,
        "layer rolled back"
    );
    Ok((StatusCode::OK, Json(record)))
}

/// `POST /api/v1/ops/jobs/:job_id/replay` — authorized replay of a failed
/// job under its original identity (Sequence 3 US-04).
///
/// Governance: `reason` is mandatory and recorded with the actor; production
/// restricts this route to operational roles via RBAC. Response is one of:
/// * `202 REPLAY_ACCEPTED` — replay started asynchronously (`replayId`
///   tracks the attempt), the job runs under its original `jobId`).
/// * `200 REPLAY_NO_OP` — the original job already completed successfully
///   (Sequence 3 US-05).
/// * `409 JOB_ALREADY_ACTIVE` — the job is still being processed.
/// * `422 REPLAY_NOT_ALLOWED` — permanent failure class, replay limit
///   exhausted, or cancelled job.
pub async fn replay_job(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Json(req): Json<ReplayJobRequest>,
) -> Result<(StatusCode, Json<ReplayJobResponse>), ApiError> {
    let job = state
        .jobs
        .get(&job_id)?
        .ok_or_else(|| ApiError::not_found("JOB_NOT_FOUND", format!("job {job_id} not found")))?;
    // Tenant isolation (TRD §13): callers may only replay their own jobs.
    if let Some(tenant) = auth::authorized_tenant(&state, &headers) {
        if tenant != job.tenant_id {
            // Sequence 4 US-OBS-05: cross-tenant replay attempts are audited.
            obs::record_access_denied(&state.data_dir, &tenant, &format!("job {job_id}"));
            return Err(ApiError::forbidden(format!(
                "tenant {tenant} cannot replay job {job_id}"
            )));
        }
    }

    let reason = req.reason.trim().to_string();
    if reason.is_empty() {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST",
            "reason is required for replay (auditability, US-AP-06)",
        ));
    }
    let requested_by = req
        .requested_by
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            auth::authorized_tenant(&state, &headers)
                .map(|t| format!("api:{t}"))
                .or(Some("api:operator".to_string()))
        })
        .unwrap_or_else(|| "api:operator".to_string());

    // Sequence 3 US-05: replay of an already-successful job is a no-op.
    if job.status == JobStatus::Completed {
        return Ok((
            StatusCode::OK,
            Json(ReplayJobResponse {
                original_job_id: job_id,
                replay_id: None,
                status: "REPLAY_NO_OP".to_string(),
                idempotency_key: job.idempotency_key,
                reason: Some("original job already completed successfully".to_string()),
                created_at: Utc::now(),
            }),
        ));
    }
    if job.status == JobStatus::Cancelled {
        return Err(ApiError::unprocessable(
            "REPLAY_NOT_ALLOWED",
            "cancelled jobs cannot be replayed",
        ));
    }
    if job.status != JobStatus::Failed {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "JOB_ALREADY_ACTIVE",
            format!("job {job_id} is {}", job.status.as_str()),
        ));
    }
    // Sequence 3 US-03: eligibility is enforced server-side.
    if !job.replay_eligible {
        return Err(ApiError::unprocessable(
            "REPLAY_NOT_ALLOWED",
            format!(
                "original job failed permanent validation ({:?}); correct the source data and submit a new upload",
                job.error_code
            ),
        ));
    }
    // Sequence 3 US-04: bounded manual replays.
    if job.replay_count >= MAX_MANUAL_REPLAYS {
        return Err(ApiError::unprocessable(
            "REPLAY_NOT_ALLOWED",
            format!(
                "job {job_id} reached the maximum of {MAX_MANUAL_REPLAYS} manual replays; submit a new upload"
            ),
        ));
    }

    let replay_id = new_replay_id();
    // assume-WGS84: request flag wins; otherwise the upload-time tag
    // convention (TRD §10 user confirmation).
    let tagged = job
        .layer_input
        .as_ref()
        .map(|m| m.tags.iter().any(|t| t == "assume-wgs84"))
        .unwrap_or(false);
    let assume_wgs84 = req.assume_crs_wgs84 || tagged;
    let tenant_id = job.tenant_id.clone();
    let replay_job_id = job.job_id.clone();

    // Sequence 4 US-OBS-05: replay audit trail + tenant-isolation metrics.
    obs::ObsMetrics::global().inc(
        metric::REPLAY_OPERATIONS,
        &[("tenantId", tenant_id.as_str())],
    );
    let _ = obs::FileAuditTrail::new(&state.data_dir).append(&obs::AuditRecord {
        event_type: obs::audit_event::JOB_REPLAYED.to_string(),
        event_id: vtile_pipeline::job::new_event_id(),
        tenant_id: tenant_id.clone(),
        layer_id: Some(job.layer_id.clone()),
        job_id: Some(replay_job_id.clone()),
        tile_version: None,
        actor: Some(requested_by.clone()),
        reason: Some(reason.clone()),
        succeeded: true,
        occurred_at: chrono::Utc::now(),
    });

    let deps = JobDeps {
        jobs: state.jobs.clone(),
        catalog: state.catalog.clone(),
        events: state.events.clone(),
        quarantine: Some(Arc::new(FileQuarantineStore::new(
            state.data_dir.join("quarantine"),
        ))),
        dlq: Some(Arc::new(FileDlqStore::new(state.data_dir.join("dlq")))),
    };
    let data_dir = state.data_dir.clone();
    let create_new_version = req.create_new_version;
    let response = ReplayJobResponse {
        original_job_id: job_id.clone(),
        replay_id: Some(replay_id.clone()),
        status: "REPLAY_ACCEPTED".to_string(),
        idempotency_key: job.idempotency_key.clone(),
        reason: None,
        created_at: Utc::now(),
    };

    tokio::task::spawn_blocking(move || {
        let outcome = run_replay_job(
            &deps,
            &data_dir,
            &tenant_id,
            &replay_job_id,
            &ReplayOptions {
                assume_wgs84,
                requested_by,
                reason,
                create_new_version,
            },
        );
        match outcome {
            Ok(ReplayOutcome::Executed(_)) => {}
            Ok(ReplayOutcome::NoOp { reason }) => {
                tracing::info!(job_id = %replay_job_id, reason = %reason, "replay no-op");
            }
            Err(err) => {
                tracing::error!(job_id = %replay_job_id, error = %err, "replay failed");
            }
        }
    });

    Ok((StatusCode::ACCEPTED, Json(response)))
}

/// `GET /api/v1/ops/dlq` — tenant-scoped dead-letter queue inspection
/// (Sequence 3 US-06). When token auth is enabled the caller is pinned to
/// their own tenant regardless of the query string (TRD §13).
pub async fn list_dlq(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<DlqListQuery>,
) -> Result<Json<Vec<DlqRecord>>, ApiError> {
    let store = FileDlqStore::new(state.data_dir.join("dlq"));
    let tenant = auth::authorized_tenant(&state, &headers).or(query.tenant_id);
    let records = match &tenant {
        Some(t) => store.list_tenant(t)?,
        None => {
            let mut all = Vec::new();
            let root = state.data_dir.join("dlq");
            if let Ok(entries) = std::fs::read_dir(&root) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        all.extend(store.list_tenant(&name)?);
                    }
                }
            }
            all.sort_by(|a, b| b.failed_at.cmp(&a.failed_at));
            all
        }
    };
    Ok(Json(records))
}

/// `GET /api/v1/ops/audit` — tenant-scoped audit trail query (Sequence 4
/// US-OBS-05). With token auth enabled the caller is pinned to their own
/// tenant regardless of the query string (TRD §13), so one tenant can never
/// read another's audit history.
pub async fn query_audit(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<AuditQuery>,
) -> Result<Json<Vec<obs::AuditRecord>>, ApiError> {
    let tenant = auth::authorized_tenant(&state, &headers).or(q.tenant_id);
    let trail = obs::FileAuditTrail::new(&state.data_dir);
    let records = trail.query(
        tenant.as_deref(),
        q.layer_id.as_deref(),
        q.event_type.as_deref(),
        q.limit.unwrap_or(200),
    )?;
    Ok(Json(records))
}
