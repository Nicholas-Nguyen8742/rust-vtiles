//! `GET /api/v1/layers` and `GET /api/v1/layers/:layer_id` (TRD §8.3/§8.4).

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;

use vtile_core::model::LayerMetadata;

use crate::auth;
use crate::dto::LayerListQuery;
use crate::error::ApiError;
use crate::state::AppState;

/// Lists catalog entries with optional `tenantId`, `category`, and `market`
/// filters (TRD §8.3). Token-authenticated callers are pinned to their own
/// tenant regardless of the query string (TRD §13 tenant isolation).
pub async fn list_layers(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<LayerListQuery>,
) -> Result<Json<Vec<LayerMetadata>>, ApiError> {
    let tenant = auth::authorized_tenant(&state, &headers).or(query.tenant_id);

    let mut layers = state.catalog.list()?;
    if let Some(tenant) = &tenant {
        layers.retain(|l| l.tenant_id == *tenant);
    }
    if let Some(category) = query.category {
        layers.retain(|l| l.category == category);
    }
    // `market` is a free-text filter over name and tags, matching the TRD
    // §8.3 example (`&market=NYC` matching tags like "nyc").
    if let Some(market) = &query.market {
        let needle = market.to_lowercase();
        layers.retain(|l| {
            l.name.to_lowercase().contains(&needle)
                || l.tags.iter().any(|t| t.to_lowercase().contains(&needle))
        });
    }
    layers.sort_by(|a, b| a.layer_id.cmp(&b.layer_id));
    Ok(Json(layers))
}

/// Fetches one catalog entry (TRD §8.4). Unknown layers and cross-tenant
/// access both surface as `404 LAYER_NOT_FOUND` so layer existence is not
/// leaked across tenants.
pub async fn get_layer(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(layer_id): Path<String>,
) -> Result<Json<LayerMetadata>, ApiError> {
    let layer = state
        .catalog
        .get(&layer_id)?
        .ok_or_else(|| {
            ApiError::not_found("LAYER_NOT_FOUND", format!("layer {layer_id} not found"))
        })?;

    if let Some(tenant) = auth::authorized_tenant(&state, &headers) {
        if tenant != layer.tenant_id {
            return Err(ApiError::not_found(
                "LAYER_NOT_FOUND",
                format!("layer {layer_id} not found"),
            ));
        }
    }
    Ok(Json(layer))
}
