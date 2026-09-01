//! Ops routes: layer version rollback (Sequence 2 US-AP-05).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;

use vtile_pipeline::publish::{rollback_layer_version, LayerVersionRecord};
use vtile_pipeline::PipelineError;

use crate::auth;
use crate::dto::RollbackRequest;
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
