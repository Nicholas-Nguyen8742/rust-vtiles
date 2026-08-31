//! Authentication middleware and CORS policy.
//!
//! MVP: static bearer token check. Production (TRD §13): OAuth2/OIDC via
//! Cognito + API Gateway authorizer; tenant isolation then comes from token
//! claims instead of the trusted `X-Tenant-Id` header used here.

use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method};
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

use crate::error::ApiError;
use crate::state::AppState;

/// Bearer-token gate. Skipped when no token is configured (local dev).
pub async fn require_auth(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(expected) = &state.auth_token else {
        return Ok(next.run(request).await);
    };
    let header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = header.strip_prefix("Bearer ").unwrap_or("");
    if token != expected {
        return Err(ApiError::unauthorized("missing or invalid bearer token"));
    }
    Ok(next.run(request).await)
}

/// Returns the tenant the caller is authorized for, when token auth is
/// enabled. MVP maps one static token to the `X-Tenant-Id` header supplied by
/// the gateway; production derives this from OIDC claims.
pub fn authorized_tenant(state: &AppState, headers: &axum::http::HeaderMap) -> Option<String> {
    if state.auth_token.is_none() {
        return None; // auth disabled → no scoping possible locally
    }
    headers
        .get("X-Tenant-Id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// CORS policy exactly as specified in TRD §13.
pub fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(vec![
            HeaderValue::from_static("https://app.creplatform.com"),
            HeaderValue::from_static("https://analytics.creplatform.com"),
        ])
        .allow_methods([Method::GET, Method::HEAD])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
        .expose_headers([
            header::ETAG,
            header::CONTENT_ENCODING,
        ])
        .max_age(Duration::from_secs(3600))
}
