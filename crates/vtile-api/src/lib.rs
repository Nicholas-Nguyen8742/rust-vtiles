//! `vtile-api` — HTTP API for the vector-tile pipeline (TRD §8).
//!
//! Endpoints:
//! * `POST /api/v1/ingest/uploads` — create job + upload URL (§8.1)
//! * `PUT  /api/v1/ingest/uploads/:job_id/content` — receive upload (local
//!   equivalent of the S3 presigned URL; prod swaps this for S3 direct upload)
//! * `GET  /api/v1/jobs/:job_id` (§8.2)
//! * `GET  /api/v1/layers` (§8.3), `GET /api/v1/layers/:layer_id` (§8.4)
//! * `POST /api/v1/ops/layers/:layer_id/rollback` — atomic version rollback
//!   (Sequence 2 US-AP-05)
//! * `POST /api/v1/ops/jobs/:job_id/replay` — authorized idempotent replay
//!   (Sequence 3 US-04), `GET /api/v1/ops/dlq` — dead-letter inspection
//!   (Sequence 3 US-06)
//! * `GET  /tiles/:tenant/:layer/:z/:x/:y.pbf` (§8.5),
//!   `GET /tiles/:tenant/:layer/versions/:version/:z/:x/:y.pbf` (Sequence 2
//!   US-AP-04 explicit-version read path)
//! * `GET  /healthz`, `GET /internal/metrics` (idempotency + publish telemetry)
//!
//! Production mapping (TRD §2): API Gateway fronts these handlers, uploads go
//! straight to S3 via presigned URLs, and the PUT handler is replaced by the
//! S3 event → SQS → Step Functions chain. The handlers here keep the same
//! request/response contracts so that swap is mechanical.

pub mod auth;
pub mod dto;
pub mod error;
pub mod routes;
pub mod state;

pub use error::ApiError;
pub use state::AppState;

use std::sync::Arc;

use axum::routing::{get, post, put};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

use vtile_ingest::validate::DEFAULT_MAX_UPLOAD_BYTES;

/// Builds the application router with all middleware (TRD §13 CORS, §14
/// payload limit).
pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = crate::auth::cors_layer();

    Router::new()
        .route("/healthz", get(routes::health::healthz))
        .route("/internal/metrics", get(routes::health::metrics))
        .route("/api/v1/ingest/uploads", post(routes::uploads::create_upload))
        .route(
            "/api/v1/ingest/uploads/:job_id/content",
            put(routes::uploads::upload_content),
        )
        .route("/api/v1/jobs/:job_id", get(routes::jobs::get_job))
        .route("/api/v1/layers", get(routes::layers::list_layers))
        .route(
            "/api/v1/layers/:layer_id",
            get(routes::layers::get_layer),
        )
        .route(
            "/api/v1/ops/layers/:layer_id/rollback",
            post(routes::ops::rollback_layer),
        )
        .route(
            "/api/v1/ops/jobs/:job_id/replay",
            post(routes::ops::replay_job),
        )
        .route("/api/v1/ops/dlq", get(routes::ops::list_dlq))
        .route(
            "/tiles/:tenant/:layer/:z/:x/:y",
            get(routes::tiles::get_tile),
        )
        .route(
            "/tiles/:tenant/:layer/versions/:version/:z/:x/:y",
            get(routes::tiles::get_tile_versioned),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_auth,
        ))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        // TRD §10: reject with 413 PAYLOAD_TOO_LARGE above the limit.
        .layer(RequestBodyLimitLayer::new(
            (DEFAULT_MAX_UPLOAD_BYTES + 1024 * 1024) as usize,
        ))
        .with_state(state)
}
