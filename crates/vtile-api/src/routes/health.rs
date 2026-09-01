//! Health and ops endpoints.

use axum::Json;

pub async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// `GET /internal/metrics` — snapshot of the idempotency telemetry
/// (Sequence 1 US-06). Production maps onto CloudWatch custom metrics; see
/// `docs/IDEMPOTENCY.md` for the metric definitions.
pub async fn metrics() -> Json<serde_json::Value> {
    Json(vtile_pipeline::IdempotencyMetrics::global().snapshot())
}
