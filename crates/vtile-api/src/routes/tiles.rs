//! Tile serving routes (TRD §8.5 + Sequence 2 US-AP-04 consistent read path).
//!
//! Two read patterns:
//! * `GET /tiles/:tenant/:layer/:z/:x/:y[.pbf]` — stable unversioned URL;
//!   the server resolves the authoritative current version from the layer
//!   registry (`publication.json`), falling back to `manifest.json` for
//!   layers published before the registry existed.
//! * `GET /tiles/:tenant/:layer/versions/:version/:z/:x/:y[.pbf]` — explicit
//!   version URL for pinning, validation, and rollback verification.
//!
//! Tiles are always served from the immutable version path
//! `tiles/{tenant}/{layer}/versions/{version}/...` — a candidate version
//! under generation is never reachable (Sequence 2 US-AP-01).
//!
//! Status-code contract (TRD §8.5):
//! * `200` with gzipped MVT payload, `Content-Type:
//!   application/vnd.mapbox-vector-tile`.
//! * `204 No Content` for empty tiles (never `404`, so map clients don't
//!   treat voids as errors — US-03).
//! * `403` for unauthorized tenant access.
//! * `404` for an unknown/unpublished layer.
//! * `422` for a zoom outside the layer's published range or out-of-grid
//!   coordinates.
//!
//! Production mapping: CloudFront fronts S3 and a Lambda@Edge
//! `OriginResponse` handler supplies the 204-for-missing behavior; the
//! version rewrite maps onto CloudFront Functions resolving the DynamoDB
//! authoritative record (short TTL, invalidation for urgent rollback).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use vtile_core::model::LayerMetadata;
use vtile_pipeline::manifest::TileManifest;
use vtile_pipeline::publish::{version_root, FileLayerRegistry};

use crate::auth;
use crate::error::ApiError;
use crate::state::AppState;

const MVT_CONTENT_TYPE: &str = "application/vnd.mapbox-vector-tile";

struct TileCoord {
    z: u8,
    x: u32,
    y: u32,
}

/// Parses `z/x/y`, tolerating a `.pbf` suffix on `y` (TRD §8.5 shows the
/// URL as `.../{y}.pbf`).
fn parse_tile_coords(z_raw: &str, x_raw: &str, y_raw: &str) -> Result<TileCoord, ApiError> {
    let bad = |which: &str, value: &str| {
        ApiError::bad_request(
            "INVALID_TILE_COORDINATES",
            format!("invalid {which} coordinate: {value:?}"),
        )
    };
    let z: u8 = z_raw.parse().map_err(|_| bad("zoom", z_raw))?;
    let x: u32 = x_raw.parse().map_err(|_| bad("x", x_raw))?;
    let y_str = y_raw.strip_suffix(".pbf").unwrap_or(y_raw);
    let y: u32 = y_str.parse().map_err(|_| bad("y", y_raw))?;
    Ok(TileCoord { z, x, y })
}

/// Tenant authorization + layer lookup + coordinate/range validation shared
/// by both read patterns.
fn resolve_request(
    state: &AppState,
    headers: &HeaderMap,
    tenant_id: &str,
    layer_id: &str,
    z_raw: &str,
    x_raw: &str,
    y_raw: &str,
) -> Result<(LayerMetadata, TileCoord), ApiError> {
    // 1. Tenant authorization (TRD §13): token-authenticated callers are
    //    pinned to their own tenant prefix.
    if let Some(tenant) = auth::authorized_tenant(state, headers) {
        if tenant != tenant_id {
            return Err(ApiError::forbidden(format!(
                "tenant {tenant} cannot access tiles of tenant {tenant_id}"
            )));
        }
    }

    // 2. Layer existence + ownership (TRD §8.5: 404 for invalid layer). The
    //    tenant check doubles as existence-hiding across tenants.
    let layer = state
        .catalog
        .get(layer_id)?
        .filter(|l| l.tenant_id == tenant_id)
        .ok_or_else(|| {
            ApiError::not_found("LAYER_NOT_FOUND", format!("layer {layer_id} not found"))
        })?;

    // 3. Coordinate parsing and range checks (TRD §8.5: 422 for invalid
    //    zoom range).
    let coord = parse_tile_coords(z_raw, x_raw, y_raw)?;
    if coord.z < layer.min_zoom || coord.z > layer.max_zoom {
        return Err(ApiError::unprocessable(
            "ZOOM_OUT_OF_RANGE",
            format!(
                "zoom {} is outside the published range {}..={} for layer {layer_id}",
                coord.z, layer.min_zoom, layer.max_zoom
            ),
        ));
    }
    let axis_count = 1u32.checked_shl(coord.z as u32).ok_or_else(|| {
        ApiError::unprocessable(
            "ZOOM_OUT_OF_RANGE",
            format!("zoom {} exceeds the supported tile grid", coord.z),
        )
    })?;
    if coord.x >= axis_count || coord.y >= axis_count {
        return Err(ApiError::unprocessable(
            "INVALID_TILE_COORDINATES",
            format!(
                "x/y ({}, {}) out of range for zoom {} (valid: 0..={})",
                coord.x,
                coord.y,
                coord.z,
                axis_count - 1
            ),
        ));
    }
    Ok((layer, coord))
}

/// Serves one tile from the immutable version path, or `204` for voids.
async fn serve_tile(
    state: &AppState,
    tenant_id: &str,
    layer_id: &str,
    tile_version: &str,
    coord: &TileCoord,
) -> Result<Response, ApiError> {
    let tiles_root = state
        .data_dir
        .join("tiles")
        .join(tenant_id)
        .join(layer_id);
    let tile_path = version_root(&tiles_root, tile_version)
        .join(format!("{}/{}/{}.pbf", coord.z, coord.x, coord.y));
    let bytes = match tokio::fs::read(&tile_path).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
        Err(e) => {
            return Err(ApiError::internal(format!("reading tile failed: {e}")));
        }
    };

    // Cache policy per US-03: low zooms are static geography (1 day),
    // detail zooms refresh hourly. Production tunes these per layer; urgent
    // rollbacks invalidate the CDN cache (Sequence 2 US-AP-05).
    let cache_control = if coord.z <= 10 {
        HeaderValue::from_static("public, max-age=86400")
    } else {
        HeaderValue::from_static("public, max-age=3600")
    };
    let etag = HeaderValue::from_str(&format!(
        "\"{}/{}/{}/{}\"",
        tile_version, coord.z, coord.x, coord.y
    ))
    .map_err(|e| ApiError::internal(format!("invalid etag: {e}")))?;

    Ok((
        [
            (header::CONTENT_TYPE, HeaderValue::from_static(MVT_CONTENT_TYPE)),
            (header::CONTENT_ENCODING, HeaderValue::from_static("gzip")),
            (header::CACHE_CONTROL, cache_control),
            (header::ETAG, etag),
        ],
        bytes,
    )
        .into_response())
}

pub async fn get_tile(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((tenant_id, layer_id, z_raw, x_raw, y_raw)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
) -> Result<Response, ApiError> {
    let (_layer, coord) = resolve_request(
        &state, &headers, &tenant_id, &layer_id, &z_raw, &x_raw, &y_raw,
    )?;

    // 4. Resolve the live tile version through the authoritative layer
    //    record (Sequence 2 US-AP-03/04); `manifest.json` remains as the
    //    pre-registry fallback.
    let manifests_root = state
        .data_dir
        .join("manifests")
        .join(&tenant_id)
        .join(&layer_id);
    let tile_version = match FileLayerRegistry::new(&manifests_root).get()? {
        Some(record) => record.current_tile_version,
        None => {
            let manifest_path = manifests_root.join("manifest.json");
            let manifest_json =
                tokio::fs::read_to_string(&manifest_path)
                    .await
                    .map_err(|e| {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            ApiError::not_found(
                                "LAYER_NOT_PUBLISHED",
                                format!("layer {layer_id} has no published tiles"),
                            )
                        } else {
                            ApiError::internal(format!("reading manifest failed: {e}"))
                        }
                    })?;
            TileManifest::from_json(&manifest_json)?.tile_version
        }
    };

    // 5. Serve the tile, or 204 for empty/void tiles.
    serve_tile(&state, &tenant_id, &layer_id, &tile_version, &coord).await
}

/// Explicit-version read path (Sequence 2 US-AP-04): serves from the named
/// immutable version regardless of which version is currently promoted —
/// used for pinning, validation, and rollback verification.
pub async fn get_tile_versioned(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((tenant_id, layer_id, version, z_raw, x_raw, y_raw)): Path<(
        String,
        String,
        String,
        String,
        String,
        String,
    )>,
) -> Result<Response, ApiError> {
    let (_layer, coord) = resolve_request(
        &state, &headers, &tenant_id, &layer_id, &z_raw, &x_raw, &y_raw,
    )?;
    serve_tile(&state, &tenant_id, &layer_id, &version, &coord).await
}
