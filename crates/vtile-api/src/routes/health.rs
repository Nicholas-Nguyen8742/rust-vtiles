//! Health and ops endpoints.

use axum::Json;

pub async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// `GET /internal/metrics` — snapshot of the idempotency, publishing, and
/// recovery telemetry (Sequence 1 US-06 / Sequence 2 US-AP-06 / Sequence 3
/// US-06). Production maps onto CloudWatch custom metrics + dashboards.
pub async fn metrics() -> Json<serde_json::Value> {
    let mut snapshot = vtile_pipeline::IdempotencyMetrics::global().snapshot();
    if let Some(map) = snapshot.as_object_mut() {
        if let Some(publish) = vtile_pipeline::PublishMetrics::global()
            .snapshot()
            .as_object()
            .cloned()
        {
            map.extend(publish);
        }
        if let Some(recovery) = vtile_pipeline::RecoveryMetrics::global()
            .snapshot()
            .as_object()
            .cloned()
        {
            map.extend(recovery);
        }
    }
    Json(snapshot)
}
