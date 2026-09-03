//! Authentication middleware, tenant authorization, and CORS policy.
//!
//! MVP: static bearer token; the tenant claim travels in `X-Tenant-Id` and
//! is bound to the authenticated token by the gateway. Production
//! (TRD §13 / Sequence 5 TI-01): OIDC/JWT — tenant identity comes from the
//! token claims (`sub`, `tenantId`, `roles`), never from client-supplied
//! request bodies. Zero-trust rule: every tenant-scoped route resolves
//! Principal → Tenant → Roles → Permissions server-side.

use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

use vtile_pipeline::obs::{self, metric};
use vtile_pipeline::tenant;

use crate::error::ApiError;
use crate::state::AppState;

/// Header carrying the authenticated tenant claim (local analog of the OIDC
/// `tenantId` claim).
pub const TENANT_CLAIM_HEADER: &str = "x-tenant-id";

/// Break-glass reference header (Sequence 5 TI-04). Presence marks a
/// privileged operation; it is always audited and alerted — locally it
/// grants no additional privilege (production uses time-bound elevated IAM
/// roles plus an incident reference).
pub const BREAK_GLASS_HEADER: &str = "x-break-glass-ref";

/// Bearer-token gate. Skipped when no token is configured (local dev).
///
/// When auth is enabled, every tenant-scoped route (`/api/v1/*`, `/tiles/*`)
/// must additionally carry a valid tenant claim (Sequence 5 TI-01/TI-05):
/// missing claim → `401 MISSING_TENANT_CLAIM`; malformed claim (traversal,
/// bad pattern) → `401 INVALID_TENANT_CLAIM`.
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

    // Sequence 5 TI-01: tenant identity is required on tenant-scoped routes
    // and must match the approved tenant pattern (rejects traversal).
    let path = request.uri().path();
    let tenant_scoped = path.starts_with("/api/v1/") || path.starts_with("/tiles/");
    if tenant_scoped {
        let claim = request
            .headers()
            .get(TENANT_CLAIM_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .unwrap_or("");
        if claim.is_empty() {
            return Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "MISSING_TENANT_CLAIM",
                "authenticated requests to tenant-scoped routes must carry the tenant claim header",
            ));
        }
        if !tenant::is_valid_tenant_id(claim) {
            return Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "INVALID_TENANT_CLAIM",
                format!(
                    "tenant claim {claim:?} does not match {}",
                    tenant::TENANT_ID_PATTERN
                ),
            ));
        }
    }

    Ok(next.run(request).await)
}

/// Returns the tenant the caller is authorized for, when token auth is
/// enabled. MVP maps one static token to the `X-Tenant-Id` claim header;
/// production derives this from OIDC claims.
pub fn authorized_tenant(state: &AppState, headers: &axum::http::HeaderMap) -> Option<String> {
    if state.auth_token.is_none() {
        return None; // auth disabled → no scoping possible locally
    }
    headers
        .get(TENANT_CLAIM_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Central tenant-ownership gate (Sequence 5 TI-01). When token auth is
/// enabled, authenticated callers may only touch their own tenant's
/// resources; cross-tenant attempts are denied, audited
/// (`tenant.access.decision` DENY), and counted. Returns `Ok(false)` when no
/// identity is present (auth disabled — local dev).
pub fn check_tenant_access(
    state: &AppState,
    headers: &HeaderMap,
    resource_tenant: &str,
    resource_type: &str,
    resource_id: &str,
) -> Result<bool, ApiError> {
    let Some(caller) = authorized_tenant(state, headers) else {
        return Ok(false);
    };
    if caller == resource_tenant {
        return Ok(true);
    }
    let metrics = obs::ObsMetrics::global();
    metrics.inc(metric::TENANT_AUTHORIZATION_DENIED, &[("tenantId", caller.as_str())]);
    metrics.inc(metric::CROSS_TENANT_ACCESS_ATTEMPT, &[("tenantId", caller.as_str())]);
    obs::record_access_decision(
        &state.data_dir,
        &caller,
        None,
        resource_type,
        resource_id,
        "ACCESS",
        "DENY",
        Some("TENANT_MISMATCH"),
    );
    Err(ApiError::forbidden(format!(
        "tenant {caller} cannot access {resource_type} owned by tenant {resource_tenant}"
    )))
}

/// Extracts the break-glass reference header, if present (Sequence 5 TI-04).
pub fn break_glass_ref(headers: &HeaderMap) -> Option<String> {
    headers
        .get(BREAK_GLASS_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Audits a break-glass operation when the reference header is present.
/// Audit-only locally: privilege grants happen in production IAM.
pub fn audit_break_glass_if_present(
    state: &AppState,
    headers: &HeaderMap,
    tenant_id: &str,
    action: &str,
) {
    if let Some(reference) = break_glass_ref(headers) {
        let principal = authorized_tenant(state, headers);
        obs::record_break_glass(
            &state.data_dir,
            tenant_id,
            principal.as_deref(),
            &reference,
            action,
        );
    }
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
