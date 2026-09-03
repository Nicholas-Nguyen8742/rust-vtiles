//! HTTP-level cross-tenant negative tests (Sequence 5 TI-05).
//!
//! Synthetic tenants `tenant-alpha` / `tenant-beta` exercise the full
//! authorization matrix against the real router (in-process via
//! `ServiceExt::oneshot`): same-tenant allowed, cross-tenant denied,
//! missing/malformed tenant claims rejected, body tenant mismatch rejected.
//! CI must fail on any cross-tenant access success.

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use vtile_api::{build_router, AppState};
use vtile_core::model::{
    Bbox, GeometryKind, JobRecord, JobStatus, LayerCategory, LayerMetadata,
    SecurityClassification, SourceFormat, ZoomRange,
};
use vtile_pipeline::events::NullEventEmitter;
use vtile_pipeline::obs::FileAuditTrail;
use vtile_pipeline::store::{FileJobStore, FileLayerCatalog, JobStore, LayerCatalog};

const TENANT_A: &str = "tenant-alpha";
const TENANT_B: &str = "tenant-beta";
const LAYER_A: &str = "parcels-alpha";
const LAYER_B: &str = "parcels-beta";
const JOB_A: &str = "job-alpha-001";
const JOB_B: &str = "job-beta-001";
const TOKEN: &str = "test-secret";

fn temp_root(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vtile-api-iso-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn layer_meta(tenant: &str, layer_id: &str) -> LayerMetadata {
    LayerMetadata {
        layer_id: layer_id.to_string(),
        tenant_id: tenant.to_string(),
        name: layer_id.to_string(),
        description: None,
        category: LayerCategory::Parcel,
        source_format: SourceFormat::GeoJson,
        crs: "EPSG:4326".to_string(),
        geometry_type: GeometryKind::Polygon,
        feature_count: 3,
        bounding_box: Bbox::new(-74.0, 40.0, -73.0, 41.0),
        min_zoom: 10,
        max_zoom: 16,
        tags: vec![],
        security_classification: SecurityClassification::Internal,
        published_at: None,
        tile_version: "v-test".to_string(),
        tile_url_template: None,
        assumed_crs: false,
    }
}

fn job_record(tenant: &str, job_id: &str, status: JobStatus) -> JobRecord {
    JobRecord {
        job_id: job_id.to_string(),
        tenant_id: tenant.to_string(),
        layer_id: if tenant == TENANT_A { LAYER_A } else { LAYER_B }.to_string(),
        status,
        source_format: SourceFormat::GeoJson,
        source_uri: format!("mem://{tenant}"),
        requested_zoom_range: ZoomRange::new(10, 16),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        error: None,
        error_code: None,
        failed_stage: None,
        error_class: None,
        replay_eligible: false,
        idempotency_key: None,
        trace_id: None,
        request_fingerprint: None,
        event_dedupe_fingerprint: None,
        state_version: 1,
        lease_token: None,
        locked_by: None,
        lease_expires_at: None,
        duplicate_event_count: 0,
        requested_tile_version: None,
        replay_audit: None,
        replay_count: 0,
        outcome: None,
        layer_input: None,
    }
}

struct Ctx {
    root: PathBuf,
    app: axum::Router,
}

/// Builds the router over a seeded data directory. `token: None` disables
/// token auth (local-dev mode); otherwise auth is enabled with `TOKEN`.
fn setup(label: &str, token: Option<&str>) -> Ctx {
    let root = temp_root(label);
    let jobs = Arc::new(FileJobStore::new(root.join("jobs")).unwrap());
    let catalog = Arc::new(FileLayerCatalog::new(root.join("catalog.json")).unwrap());

    catalog.upsert(layer_meta(TENANT_A, LAYER_A)).unwrap();
    catalog.upsert(layer_meta(TENANT_B, LAYER_B)).unwrap();
    jobs.create(job_record(TENANT_A, JOB_A, JobStatus::Failed)).unwrap();
    jobs.create(job_record(TENANT_B, JOB_B, JobStatus::Failed)).unwrap();

    let state = Arc::new(AppState {
        jobs,
        catalog,
        events: Arc::new(NullEventEmitter),
        data_dir: root.clone(),
        auth_token: token.map(str::to_string),
        max_upload_bytes: 50 * 1024 * 1024,
        upload_expires_secs: 900,
    });
    let app = build_router(state);
    Ctx { root, app }
}

async fn send(
    app: axum::Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    tenant_claim: Option<&str>,
    body: Option<(&str, Vec<u8>)>, // (content-type, bytes)
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        builder = builder.header("Authorization", format!("Bearer {t}"));
    }
    if let Some(claim) = tenant_claim {
        builder = builder.header("x-tenant-id", claim);
    }
    let request = match body {
        Some((ct, bytes)) => builder
            .header("Content-Type", ct)
            .body(Body::from(bytes))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    app.oneshot(request).await.unwrap()
}

async fn body_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

// ── Layer catalog access ────────────────────────────────────────────────────

#[tokio::test]
async fn same_tenant_layer_read_allowed() {
    let ctx = setup("layer-allow", Some(TOKEN));
    let response = send(
        ctx.app.clone(),
        Method::GET,
        &format!("/api/v1/layers/{LAYER_A}"),
        Some(TOKEN),
        Some(TENANT_A),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    assert_eq!(body["layerId"], LAYER_A);

    // Sequence 5 TI-06: the ALLOW decision is audited.
    let trail = FileAuditTrail::new(&ctx.root);
    let allows = trail
        .query(Some(TENANT_A), Some(LAYER_A), Some("tenant.access.decision"), 100)
        .unwrap();
    assert!(allows.iter().any(|r| r.decision.as_deref() == Some("ALLOW")));
}

#[tokio::test]
async fn cross_tenant_layer_read_denied() {
    let ctx = setup("layer-deny", Some(TOKEN));
    let response = send(
        ctx.app.clone(),
        Method::GET,
        &format!("/api/v1/layers/{LAYER_A}"),
        Some(TOKEN),
        Some(TENANT_B), // tenant-beta requests tenant-alpha's layer
        None,
    )
    .await;
    // Existence-hiding: cross-tenant reads surface as 404, not 403.
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // The denial is audited under the CALLER's tenant.
    let trail = FileAuditTrail::new(&ctx.root);
    let denials = trail
        .query(Some(TENANT_B), None, Some("tenant.access.decision"), 100)
        .unwrap();
    assert!(denials.iter().any(|r| r.decision.as_deref() == Some("DENY")));
}

#[tokio::test]
async fn layer_listing_is_tenant_scoped() {
    let ctx = setup("layer-list", Some(TOKEN));
    let response = send(
        ctx.app.clone(),
        Method::GET,
        "/api/v1/layers",
        Some(TOKEN),
        Some(TENANT_A),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    let layers = body.as_array().expect("layer list");
    assert_eq!(layers.len(), 1, "tenant-alpha sees exactly its own layer");
    assert_eq!(layers[0]["layerId"], LAYER_A);
}

// ── Job access ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn cross_tenant_job_status_denied() {
    let ctx = setup("job-deny", Some(TOKEN));
    // tenant-alpha requests tenant-beta's job.
    let response = send(
        ctx.app.clone(),
        Method::GET,
        &format!("/api/v1/jobs/{JOB_B}"),
        Some(TOKEN),
        Some(TENANT_A),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // Same-tenant access is allowed.
    let response = send(
        ctx.app.clone(),
        Method::GET,
        &format!("/api/v1/jobs/{JOB_A}"),
        Some(TOKEN),
        Some(TENANT_A),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

// ── Tile delivery ───────────────────────────────────────────────────────────

#[tokio::test]
async fn cross_tenant_tile_request_denied() {
    let ctx = setup("tiles-deny", Some(TOKEN));
    // tenant-beta requests tenant-alpha's tile path.
    let response = send(
        ctx.app.clone(),
        Method::GET,
        &format!("/tiles/{TENANT_A}/{LAYER_A}/12/100/100.pbf"),
        Some(TOKEN),
        Some(TENANT_B),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // Same tenant passes the tenant gate (404 LAYER_NOT_PUBLISHED because no
    // manifest exists — the point is that it is NOT 403).
    let response = send(
        ctx.app.clone(),
        Method::GET,
        &format!("/tiles/{TENANT_A}/{LAYER_A}/12/100/100.pbf"),
        Some(TOKEN),
        Some(TENANT_A),
        None,
    )
    .await;
    assert_ne!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn traversal_layer_id_in_tile_path_rejected() {
    let ctx = setup("tiles-traversal", Some(TOKEN));
    let response = send(
        ctx.app.clone(),
        Method::GET,
        &format!("/tiles/{TENANT_A}/..%2Fevil/12/100/100.pbf"),
        Some(TOKEN),
        Some(TENANT_A),
        None,
    )
    .await;
    assert!(
        response.status() == StatusCode::BAD_REQUEST
            || response.status() == StatusCode::NOT_FOUND,
        "traversal must not resolve, got {}",
        response.status()
    );
}

// ── Replay ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn cross_tenant_replay_denied() {
    let ctx = setup("replay-deny", Some(TOKEN));
    // tenant-beta attempts to replay tenant-alpha's failed job.
    let response = send(
        ctx.app.clone(),
        Method::POST,
        &format!("/api/v1/ops/jobs/{JOB_A}/replay"),
        Some(TOKEN),
        Some(TENANT_B),
        Some(("application/json", br#"{"reason":"test"}"#.to_vec())),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

// ── Authentication and tenant claims (TI-01) ───────────────────────────────

#[tokio::test]
async fn invalid_token_rejected() {
    let ctx = setup("bad-token", Some(TOKEN));
    let response = send(
        ctx.app.clone(),
        Method::GET,
        "/api/v1/layers",
        Some("wrong-token"),
        Some(TENANT_A),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn missing_tenant_claim_rejected() {
    let ctx = setup("no-claim", Some(TOKEN));
    let response = send(
        ctx.app.clone(),
        Method::GET,
        "/api/v1/layers",
        Some(TOKEN),
        None, // no tenant claim
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = body_json(response).await;
    assert_eq!(body["error"]["code"], "MISSING_TENANT_CLAIM");
}

#[tokio::test]
async fn malformed_tenant_claim_rejected() {
    let ctx = setup("bad-claim", Some(TOKEN));
    for bad in ["../evil", "Tenant", "tenant/../x", "ab"] {
        let response = send(
            ctx.app.clone(),
            Method::GET,
            "/api/v1/layers",
            Some(TOKEN),
            Some(bad),
            None,
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "claim {bad:?} must be rejected"
        );
        let body = body_json(response).await;
        assert_eq!(body["error"]["code"], "INVALID_TENANT_CLAIM");
    }
}

// ── Upload tenant binding (TI-01/TI-02) ────────────────────────────────────

#[tokio::test]
async fn upload_body_tenant_mismatch_denied() {
    let ctx = setup("upload-mismatch", Some(TOKEN));
    // Authenticated as tenant-beta, but the body names tenant-alpha.
    let body = serde_json::json!({
        "tenantId": TENANT_A,
        "layerId": LAYER_A,
        "fileName": "parcels.geojson",
        "contentType": "application/geo+json",
        "sourceFormat": "GEOJSON",
    });
    let response = send(
        ctx.app.clone(),
        Method::POST,
        "/api/v1/ingest/uploads",
        Some(TOKEN),
        Some(TENANT_B),
        Some(("application/json", serde_json::to_vec(&body).unwrap())),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn upload_invalid_tenant_id_rejected_in_local_mode() {
    // Auth disabled (local-dev mode): the body tenant id is still validated.
    let ctx = setup("upload-bad-tenant", None);
    let body = serde_json::json!({
        "tenantId": "../evil",
        "layerId": LAYER_A,
        "fileName": "parcels.geojson",
        "contentType": "application/geo+json",
        "sourceFormat": "GEOJSON",
    });
    let response = send(
        ctx.app.clone(),
        Method::POST,
        "/api/v1/ingest/uploads",
        None,
        None,
        Some(("application/json", serde_json::to_vec(&body).unwrap())),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = body_json(response).await;
    assert_eq!(body["error"]["code"], "INVALID_TENANT_ID");
}

#[tokio::test]
async fn upload_traversal_file_name_rejected() {
    let ctx = setup("upload-bad-file", None);
    let body = serde_json::json!({
        "tenantId": TENANT_A,
        "layerId": LAYER_A,
        "fileName": "../../etc/passwd.geojson",
        "contentType": "application/geo+json",
        "sourceFormat": "GEOJSON",
    });
    let response = send(
        ctx.app.clone(),
        Method::POST,
        "/api/v1/ingest/uploads",
        None,
        None,
        Some(("application/json", serde_json::to_vec(&body).unwrap())),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = body_json(response).await;
    assert_eq!(body["error"]["code"], "INVALID_RESOURCE_ID");
}

// ── Upload content tenant binding (TI-03/TI-05) ────────────────────────────

#[tokio::test]
async fn cross_tenant_upload_content_denied() {
    let ctx = setup("content-deny", Some(TOKEN));
    // tenant-beta tries to feed tenant-alpha's job (signed-URL reuse analog).
    let response = send(
        ctx.app.clone(),
        Method::PUT,
        &format!("/api/v1/ingest/uploads/{JOB_A}/content"),
        Some(TOKEN),
        Some(TENANT_B),
        Some((
            "application/geo+json",
            br#"{"type":"FeatureCollection","features":[]}"#.to_vec(),
        )),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

// ── Audit query scoping (TI-06) ─────────────────────────────────────────────

#[tokio::test]
async fn audit_query_is_tenant_scoped() {
    let ctx = setup("audit-scope", Some(TOKEN));
    let trail = FileAuditTrail::new(&ctx.root);
    for (tenant, layer) in [(TENANT_A, LAYER_A), (TENANT_B, LAYER_B)] {
        trail
            .append(&vtile_pipeline::obs::AuditRecord {
                event_type: "tenant.access.decision".to_string(),
                event_id: "evt_test".to_string(),
                tenant_id: tenant.to_string(),
                layer_id: Some(layer.to_string()),
                job_id: None,
                tile_version: None,
                actor: None,
                reason: None,
                succeeded: true,
                occurred_at: chrono::Utc::now(),
                resource_type: None,
                resource_id: None,
                action: None,
                decision: None,
            })
            .unwrap();
    }

    // Authenticated as tenant-alpha: records are pinned to the caller's
    // tenant even when the query asks for tenant-beta.
    let response = send(
        ctx.app.clone(),
        Method::GET,
        &format!("/api/v1/ops/audit?tenantId={TENANT_B}"),
        Some(TOKEN),
        Some(TENANT_A),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    let records = body.as_array().expect("audit records");
    assert!(
        records
            .iter()
            .all(|r| r["tenantId"] == serde_json::Value::String(TENANT_A.to_string())),
        "audit queries are pinned to the caller's tenant"
    );
}
