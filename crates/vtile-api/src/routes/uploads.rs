//! Upload/job-creation endpoints (TRD §8.1).

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::Utc;

use vtile_core::config::TileConfig;
use vtile_core::model::{JobRecord, JobStatus, LayerCategory};
use vtile_ingest::normalize::{CrsPolicy, NormalizeOptions};
use vtile_pipeline::events::PipelineEvent;
use vtile_pipeline::job::{new_job_id, run_job, JobPaths, RunJobInput};
use vtile_pipeline::JobDeps;

use crate::dto::{UploadAcceptedResponse, UploadRequest, UploadResponse};
use crate::error::ApiError;
use crate::routes;
use crate::state::AppState;

/// `POST /api/v1/ingest/uploads` — validates the request, creates the job
/// record, and returns the upload URL (TRD §8.1).
///
/// Production: `uploadUrl` is an S3 presigned PUT (TRD §13, 15-minute
/// expiry). Locally it points at this API's content endpoint.
pub async fn create_upload(
    State(state): State<Arc<AppState>>,
    Json(req): Json<UploadRequest>,
) -> Result<(StatusCode, Json<UploadResponse>), ApiError> {
    if req.tenant_id.trim().is_empty() || req.layer_id.trim().is_empty() {
        return Err(ApiError::bad_request(
            "INVALID_REQUEST",
            "tenantId and layerId are required",
        ));
    }
    if !req.source_format.is_supported_in_mvp() {
        return Err(ApiError::unprocessable(
            "UNSUPPORTED_FORMAT",
            format!(
                "{:?} is not supported in MVP (GeoJSON and Shapefile only)",
                req.source_format
            ),
        ));
    }

    let category = req.metadata.as_ref().and_then(|m| m.category);
    let zoom_range = req
        .requested_zoom_range
        .unwrap_or_else(|| category.unwrap_or(LayerCategory::Other).default_zoom_range());

    let job_id = new_job_id();
    let now = Utc::now();
    let staging_root = state
        .data_dir
        .join("staging")
        .join(&req.tenant_id)
        .join(&job_id);
    let job = JobRecord {
        job_id: job_id.clone(),
        tenant_id: req.tenant_id.clone(),
        layer_id: req.layer_id.clone(),
        status: JobStatus::UploadPending,
        source_format: req.source_format,
        source_uri: format!("{}/input/{}", staging_root.display(), req.file_name),
        requested_zoom_range: zoom_range,
        created_at: now,
        updated_at: now,
        error: None,
        outcome: None,
        layer_input: req.metadata.map(|m| vtile_core::model::LayerMetadataInput {
            name: m.name,
            description: m.description,
            category: m.category,
            tags: m.tags,
        }),
    };
    state.jobs.create(job)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(UploadResponse {
            job_id,
            upload_url: format!("/api/v1/ingest/uploads/{job_id}/content"),
            expires_in: state.upload_expires_secs,
            status: JobStatus::UploadPending,
        }),
    ))
}

/// `PUT /api/v1/ingest/uploads/:job_id/content` — receives the upload and
/// starts processing.
///
/// This handler is the local stand-in for the production chain
/// `S3 presigned upload → S3 event → SQS → Step Functions → Fargate`
/// (TRD §2): it persists the payload to staging, emits `job.submitted`, and
/// runs the pipeline in a blocking task.
pub async fn upload_content(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, Json<UploadAcceptedResponse>), ApiError> {
    let job = state
        .jobs
        .get(&job_id)?
        .ok_or_else(|| ApiError::not_found("JOB_NOT_FOUND", format!("job {job_id} not found")))?;
    if job.status.is_terminal() {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "JOB_TERMINAL",
            format!("job {job_id} is already in a terminal state"),
        ));
    }

    // TRD §10: reject oversized payloads with 413.
    let size = body.len() as u64;
    if size > state.max_upload_bytes {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "PAYLOAD_TOO_LARGE",
            format!("{size} bytes exceeds limit of {}", state.max_upload_bytes),
        ));
    }
    if size == 0 {
        return Err(ApiError::unprocessable("EMPTY_DATASET", "upload body is empty"));
    }

    state.jobs.update_status(&job_id, JobStatus::Queued, None)?;
    state.events.emit(PipelineEvent::VectorTileJobSubmitted {
        event_id: vtile_pipeline::job::new_event_id(),
        tenant_id: job.tenant_id.clone(),
        job_id: job.job_id.clone(),
        layer_id: job.layer_id.clone(),
        source_format: job.source_format.as_str().to_string(),
        source_uri: job.source_uri.clone(),
        occurred_at: Utc::now(),
    });

    // Assemble the run.
    let category = job.layer_input.as_ref().and_then(|m| m.category);
    let tile_config = TileConfig {
        layer_name: routes::mvt_layer_name(category, &job.layer_id),
        zoom_range: job.requested_zoom_range,
        ..Default::default()
    };
    let normalize_opts = NormalizeOptions {
        max_upload_bytes: state.max_upload_bytes,
        crs_policy: crs_policy_for(&state, &job_id),
        property_policy: tile_config.property_policy.clone(),
    };
    let paths = JobPaths {
        staging_root: state
            .data_dir
            .join("staging")
            .join(&job.tenant_id)
            .join(&job.job_id),
        tiles_root: state
            .data_dir
            .join("tiles")
            .join(&job.tenant_id)
            .join(&job.layer_id),
        manifests_root: state
            .data_dir
            .join("manifests")
            .join(&job.tenant_id)
            .join(&job.layer_id),
    };

    let deps = JobDeps {
        jobs: state.jobs.clone(),
        catalog: state.catalog.clone(),
        events: state.events.clone(),
    };
    let input = RunJobInput {
        job,
        source_bytes: body.to_vec(),
        tile_config,
        normalize_opts,
        paths,
    };

    // Process off the request path; TRD §14 job start < 30 s.
    tokio::task::spawn_blocking(move || {
        if let Err(err) = run_job(&input, &deps) {
            tracing::error!(job_id = %input.job.job_id, error = %err, "job failed");
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(UploadAcceptedResponse {
            job_id,
            status: JobStatus::Queued,
        }),
    ))
}

/// MVP: the assume-WGS84 choice is a property of the upload request. Because
/// the content PUT carries no JSON body, the flag is looked up from the job's
/// stored layer input tags convention (`assume-wgs84`). Production passes it
/// through SQS message attributes.
fn crs_policy_for(state: &AppState, job_id: &str) -> CrsPolicy {
    let Ok(Some(job)) = state.jobs.get(job_id) else {
        return CrsPolicy::RequireKnown;
    };
    let flagged = job
        .layer_input
        .as_ref()
        .map(|m| m.tags.iter().any(|t| t == "assume-wgs84"))
        .unwrap_or(false);
    if flagged {
        CrsPolicy::AssumeWgs84
    } else {
        CrsPolicy::RequireKnown
    }
}
