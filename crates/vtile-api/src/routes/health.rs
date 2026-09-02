//! Health and ops endpoints.

use std::sync::Arc;

use axum::extract::State;
use axum::Json;

use crate::state::AppState;

pub async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// `GET /internal/metrics` — unified snapshot of every telemetry family:
/// idempotency, publishing, recovery, and the dimensioned pipeline metrics
/// (Sequence 1 US-06 / Sequence 2 US-AP-06 / Sequence 3 US-06 / Sequence 4
/// US-OBS-02). Production maps onto CloudWatch custom metrics + dashboards.
pub async fn metrics() -> Json<serde_json::Value> {
    Json(vtile_pipeline::merged_metrics_snapshot())
}

/// `GET /internal/dashboard` — the local operations dashboard (Sequence 4
/// US-OBS-03): job state counts, DLQ depth, layer quality/freshness,
/// triggered alerts, and the full metrics snapshot. Production equivalent:
/// CloudWatch dashboards.
pub async fn dashboard(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(vtile_pipeline::build_dashboard(
        &state.data_dir,
        state.jobs.as_ref(),
    ))
}

/// `GET /internal/alerts` — evaluates the alert catalog against current
/// telemetry (Sequence 4 US-OBS-03). Rules referencing production-only
/// metrics report `currentValue: null`. Local gauges (DLQ depth, layer
/// staleness) are computed live from the data directory.
pub async fn alerts(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let mut snapshot = vtile_pipeline::obs::alert_snapshot();
    if let Some(map) = snapshot.as_object_mut() {
        let layers = vtile_pipeline::layer_health(&state.data_dir);
        let max_staleness = layers
            .iter()
            .map(|l| l.staleness_seconds)
            .max()
            .unwrap_or(0);
        map.insert(
            "layer_staleness_seconds_max".to_string(),
            serde_json::Value::from(max_staleness),
        );
    }
    let alerts = vtile_pipeline::evaluate_alerts(&snapshot);
    Json(serde_json::json!({
        "environment": vtile_pipeline::obs::environment(),
        "alerts": alerts,
        "snapshot": snapshot,
    }))
}
