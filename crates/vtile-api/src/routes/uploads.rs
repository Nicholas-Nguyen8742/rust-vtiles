//! Upload/job-creation endpoints (TRD §8.1), extended by the idempotency
//! epic (Sequence 1 US-01/US-02/US-03).

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use chrono::Utc;

use vtile_core::config::TileConfig;
use vtile_core::model::{JobRecord, JobStatus, LayerCategory};
use vtile_ingest::normalize::{CrsPolicy, NormalizeOptions};
use vtile_ingest::validate::validate_file_type;
use vtile_pipeline::events::PipelineEvent;
use vtile_pipeline::job::{
    new_idempotency_token, new_job_id, new_tile_version, run_job_with_retries, source_hash,
    JobPaths, RunJobInput,
};
use vtile_pipeline::obs::{self, metric, stage as obs_stage};
use vtile_pipeline::recovery::{FileDlqStore, RetryPolicy};
use vtile_pipeline::{
    classify_ingest_event, event_dedupe_fingerprint, processing_profile_label,
    request_fingerprint, upload_idempotency_key, DedupeRecord, EventDecision, FileDedupeStore,
    FileOrphanStore, FileQuarantineStore, IdempotencyMetrics, JobDeps, Metric, OrphanEvent,
    PipelineError,
};

use crate::dto::{UploadAcceptedResponse, UploadRequest, UploadResponse};
use crate::error::ApiError;
use crate::routes;
use crate::state::AppState;

/// `POST /api/v1/ingest/uploads` — validates the request, creates the job
/// record, and returns the upload URL (TRD §8.1).
///
/// Production: `uploadUrl` is an S3 presigned PUT (TRD §13, 15-minute
/// expiry). Locally it points at this API's content endpoint.
///
/// Idempotency (Sequence 1 US-01/US-02): the optional `Idempotency-Key`
/// header binds repeat requests to one job. Same key + equivalent payload →
/// the existing job is returned (HTTP 200); same key + different payload →
/// `409 IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`. Requests without a token always
/// receive a fresh job (intentional vendor-refresh uploads).
pub async fn create_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
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
    // Recommendation 3 US-01: reject extension/content-type mismatches
    // before creating any job record (`INVALID_FILE_TYPE`).
    validate_file_type(&req.file_name, req.content_type.as_deref(), req.source_format)?;

    let category = req.metadata.as_ref().and_then(|m| m.category);
    let zoom_range = req
        .requested_zoom_range
        .unwrap_or_else(|| category.unwrap_or(LayerCategory::Other).default_zoom_range());

    // The assume-WGS84 DTO flag is normalized into the `assume-wgs84` tag
    // convention so it survives to content-PUT time, where `crs_policy_for`
    // decides the CRS policy.
    let mut layer_input = req.metadata.map(|m| vtile_core::model::LayerMetadataInput {
        name: m.name,
        description: m.description,
        category: m.category,
        tags: m.tags,
    });
    if req.assume_crs_wgs84 {
        let input = layer_input.get_or_insert_with(Default::default);
        if !input.tags.iter().any(|t| t == "assume-wgs84") {
            input.tags.push("assume-wgs84".to_string());
        }
    }

    // ── Sequence 1 US-01/US-02: idempotent job creation ───────────────────
    let client_token = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .unwrap_or_else(new_idempotency_token);
    let profile = req
        .processing_profile
        .clone()
        .unwrap_or_else(|| processing_profile_label(category, zoom_range));
    let idempotency_key = upload_idempotency_key(
        &req.tenant_id,
        &req.layer_id,
        &client_token,
        &profile,
    );
    let payload_fingerprint = request_fingerprint(
        &req.file_name,
        req.content_type.as_deref(),
        req.source_format,
        zoom_range,
        &profile,
    );

    if let Some(existing) = state.jobs.find_by_idempotency_key(&idempotency_key)? {
        return resolve_existing_upload(
            &existing,
            &payload_fingerprint,
            idempotency_key,
            state.upload_expires_secs,
        );
    }

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
        error_code: None,
        failed_stage: None,
        error_class: None,
        replay_eligible: false,
        idempotency_key: Some(idempotency_key.clone()),
        trace_id: Some(obs::new_trace_id()),
        request_fingerprint: Some(payload_fingerprint.clone()),
        event_dedupe_fingerprint: None,
        state_version: 1,
        lease_token: None,
        locked_by: None,
        lease_expires_at: None,
        duplicate_event_count: 0,
        requested_tile_version: Some(new_tile_version()),
        replay_audit: None,
        replay_count: 0,
        outcome: None,
        layer_input,
    };

    // Conditional create (US-01). Losing the race to a concurrent request
    // with the same key converges on the winner's record.
    match state.jobs.create(job.clone()) {
        Ok(()) => {}
        Err(PipelineError::JobAlreadyExists(_)) => {
            if let Some(existing) = state.jobs.find_by_idempotency_key(&idempotency_key)? {
                return resolve_existing_upload(
                    &existing,
                    &payload_fingerprint,
                    idempotency_key,
                    state.upload_expires_secs,
                );
            }
            return Err(ApiError::internal(
                "job registry conflict: duplicate jobId without a matching idempotency key",
            ));
        }
        Err(e) => return Err(e.into()),
    }

    // Sequence 4 US-OBS-01/02/05: upload telemetry + audit trail.
    obs::ObsMetrics::global().inc(
        metric::INGEST_UPLOADS_REQUESTED,
        &[
            ("tenantId", req.tenant_id.as_str()),
            ("sourceFormat", req.source_format.as_str()),
        ],
    );
    let mut requested_log = obs::StageLog::new(
        obs::SERVICE_API,
        obs_stage::UPLOAD_REQUESTED,
        &req.tenant_id,
        &job_id,
        &req.layer_id,
    );
    requested_log.source_format = Some(req.source_format.as_str().to_string());
    requested_log.trace_id = job.trace_id.clone();
    obs::emit_stage_log(&requested_log);
    let _ = obs::FileAuditTrail::new(&state.data_dir).append(&obs::AuditRecord {
        event_type: obs::audit_event::UPLOAD_INITIATED.to_string(),
        event_id: vtile_pipeline::job::new_event_id(),
        tenant_id: req.tenant_id.clone(),
        layer_id: Some(req.layer_id.clone()),
        job_id: Some(job_id.clone()),
        tile_version: None,
        actor: None,
        reason: None,
        succeeded: true,
        occurred_at: Utc::now(),
    });

    tracing::info!(
        job_id = %job_id,
        tenant = %req.tenant_id,
        layer = %req.layer_id,
        idempotency_key = %idempotency_key,
        "upload job created"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(UploadResponse {
            job_id,
            idempotency_key,
            upload_url: format!("/api/v1/ingest/uploads/{job_id}/content"),
            expires_in: state.upload_expires_secs,
            status: JobStatus::UploadPending,
        }),
    ))
}

/// US-01/US-02 duplicate-upload resolution: equivalent payload → return the
/// existing job; different payload → `409 IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`.
fn resolve_existing_upload(
    existing: &JobRecord,
    payload_fingerprint: &str,
    idempotency_key: String,
    expires_secs: u64,
) -> Result<(StatusCode, Json<UploadResponse>), ApiError> {
    if existing.request_fingerprint.as_deref() == Some(payload_fingerprint) {
        IdempotencyMetrics::global().inc(Metric::IdempotentReplays);
        tracing::info!(
            job_id = %existing.job_id,
            idempotency_key = %idempotency_key,
            "duplicate upload request resolved to existing job"
        );
        return Ok((
            StatusCode::OK,
            Json(UploadResponse {
                job_id: existing.job_id.clone(),
                idempotency_key,
                upload_url: format!("/api/v1/ingest/uploads/{}/content", existing.job_id),
                expires_in: expires_secs,
                status: existing.status,
            }),
        ));
    }
    IdempotencyMetrics::global().inc(Metric::IdempotencyKeyConflicts);
    Err(ApiError::new(
        StatusCode::CONFLICT,
        "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH",
        format!(
            "Idempotency-Key was already used with a different payload (job {})",
            existing.job_id
        ),
    ))
}

/// `PUT /api/v1/ingest/uploads/:job_id/content` — receives the upload and
/// starts processing.
///
/// This handler is the local stand-in for the production chain
/// `S3 presigned upload → S3 event → SQS → Step Functions → Fargate`
/// (TRD §2), including the at-least-once redelivery semantics: repeated
/// events are classified and suppressed (Sequence 1 US-03) instead of
/// starting duplicate runs.
pub async fn upload_content(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, Json<UploadAcceptedResponse>), ApiError> {
    let job = match state.jobs.get(&job_id)? {
        Some(job) => job,
        None => {
            // Sequence 1 US-01/US-03: an event with no resolvable job goes
            // to the orphan path (recorded + alerted) — never silently
            // creates an untracked job.
            let orphan = OrphanEvent {
                source_event_type: "UPLOAD_CONTENT".to_string(),
                object_key: format!("staging/{job_id}"),
                etag: None,
                tenant_id: None,
                reason: "content PUT for an unknown jobId".to_string(),
                detected_at: Utc::now(),
            };
            let orphans = FileOrphanStore::new(state.data_dir.join("orphans"));
            if let Ok(path) = orphans.record(&orphan) {
                tracing::warn!(
                    job_id = %job_id,
                    orphan = %path.display(),
                    "orphan upload event recorded"
                );
            }
            IdempotencyMetrics::global().inc(Metric::OrphanEventsDetected);
            return Err(ApiError::not_found(
                "JOB_NOT_FOUND",
                format!("job {job_id} not found"),
            ));
        }
    };

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
        return Err(ApiError::unprocessable(
            "EMPTY_DATASET",
            "upload body is empty",
        ));
    }

    // ── Sequence 1 US-03: duplicate event suppression ─────────────────────
    // The content PUT is the local event; the fingerprint mirrors
    // SHA-256(tenant + layer + objectKey + etag + jobId), with the payload
    // SHA-256 standing in for the S3 ETag.
    let etag = source_hash(&body);
    let object_key = job.source_uri.trim_start_matches("file://").to_string();
    let fingerprint = event_dedupe_fingerprint(
        &job.tenant_id,
        &job.layer_id,
        &object_key,
        &etag,
        &job.job_id,
    );
    let dedupe = FileDedupeStore::new(state.data_dir.join("dedupe"));
    let decision = classify_ingest_event(Some(&job), dedupe.seen(&fingerprint)?);
    match decision {
        EventDecision::Orphan => {
            // Defensive: the job resolved above, so this is unreachable.
            return Err(ApiError::not_found(
                "JOB_NOT_FOUND",
                format!("job {job_id} not found"),
            ));
        }
        EventDecision::DuplicateSuppressed | EventDecision::TerminalAck => {
            IdempotencyMetrics::global().inc(Metric::DuplicateEventsSuppressed);
            state.jobs.note_duplicate_event(&job.job_id)?;
            tracing::info!(
                job_id = %job.job_id,
                status = job.status.as_str(),
                decision = format!("{decision:?}"),
                "duplicate event acknowledged; no new work started"
            );
            return Ok((
                StatusCode::ACCEPTED,
                Json(UploadAcceptedResponse {
                    job_id,
                    status: job.status,
                }),
            ));
        }
        EventDecision::StartRun => {}
    }
    dedupe.record(&DedupeRecord {
        dedupe_key: fingerprint.clone(),
        job_id: job.job_id.clone(),
        seen_at: Utc::now(),
        source_event_type: "UPLOAD_CONTENT".to_string(),
    })?;

    // Bind the event to the job and enqueue (UPLOAD_PENDING → QUEUED).
    let mut queued = job.clone();
    queued.status = JobStatus::Queued;
    queued.event_dedupe_fingerprint = Some(fingerprint);
    queued.state_version += 1;
    queued.updated_at = Utc::now();
    state.jobs.upsert(queued)?;

    state.events.emit(PipelineEvent::VectorTileJobSubmitted {
        event_id: vtile_pipeline::job::new_event_id(),
        tenant_id: job.tenant_id.clone(),
        job_id: job.job_id.clone(),
        layer_id: job.layer_id.clone(),
        source_format: job.source_format.as_str().to_string(),
        source_uri: job.source_uri.clone(),
        occurred_at: Utc::now(),
    });

    // Sequence 4 US-OBS-01/02/05: completion telemetry + audit trail.
    let upload_dims: [(&str, &str); 2] = [
        ("tenantId", job.tenant_id.as_str()),
        ("sourceFormat", job.source_format.as_str()),
    ];
    let obs_metrics = obs::ObsMetrics::global();
    obs_metrics.inc(metric::INGEST_UPLOADS_COMPLETED, &upload_dims);
    obs_metrics.inc(metric::INGEST_JOBS_SUBMITTED, &upload_dims);
    let mut completed_log = obs::StageLog::new(
        obs::SERVICE_API,
        obs_stage::UPLOAD_COMPLETED,
        &job.tenant_id,
        &job.job_id,
        &job.layer_id,
    );
    completed_log.file_bytes = Some(size);
    completed_log.source_format = Some(job.source_format.as_str().to_string());
    completed_log.trace_id = job.trace_id.clone();
    obs::emit_stage_log(&completed_log);
    let mut submitted_log = obs::StageLog::new(
        obs::SERVICE_API,
        obs_stage::JOB_SUBMITTED,
        &job.tenant_id,
        &job.job_id,
        &job.layer_id,
    );
    submitted_log.trace_id = job.trace_id.clone();
    obs::emit_stage_log(&submitted_log);
    let _ = obs::FileAuditTrail::new(&state.data_dir).append(&obs::AuditRecord {
        event_type: obs::audit_event::UPLOAD_COMPLETED.to_string(),
        event_id: vtile_pipeline::job::new_event_id(),
        tenant_id: job.tenant_id.clone(),
        layer_id: Some(job.layer_id.clone()),
        job_id: Some(job.job_id.clone()),
        tile_version: None,
        actor: None,
        reason: None,
        succeeded: true,
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
        ..Default::default()
    };
    let paths = JobPaths {
        data_dir: state.data_dir.clone(),
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
        // Recommendation 3 US-03: failed uploads land in
        // `quarantine/{tenantId}/{jobId}/` for inspection and replay.
        quarantine: Some(Arc::new(FileQuarantineStore::new(
            state.data_dir.join("quarantine"),
        ))),
        // Sequence 3 US-01: dead-letter capture under `dlq/`.
        dlq: Some(Arc::new(FileDlqStore::new(state.data_dir.join("dlq")))),
    };
    let input = RunJobInput {
        job,
        source_bytes: body.to_vec(),
        tile_config,
        normalize_opts,
        paths,
    };

    // Process off the request path; TRD §14 job start < 30 s. Transient
    // failures retry with backoff; exhausted/permanent failures are
    // dead-lettered (Sequence 3 US-01).
    let policy = RetryPolicy::default();
    tokio::task::spawn_blocking(move || {
        if let Err(err) = run_job_with_retries(&input, &deps, &policy) {
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

/// MVP: the assume-WGS84 choice is a property of the upload request
/// (`assumeCrsWgs84`, normalized into the `assume-wgs84` tag by
/// `create_upload`). Because the content PUT carries no JSON body, the flag
/// is looked up from the job's stored layer-input tags. Production passes it
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
