//! Request/response DTOs for the TRD §8 API contracts.

use serde::{Deserialize, Serialize};

use vtile_core::model::{JobStatus, LayerCategory, SourceFormat, ZoomRange};

/// `POST /api/v1/ingest/uploads` request (TRD §8.1).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadRequest {
    pub tenant_id: String,
    pub layer_id: String,
    pub file_name: String,
    #[serde(default)]
    pub content_type: Option<String>,
    pub source_format: SourceFormat,
    #[serde(default)]
    pub metadata: Option<LayerMetadataInputDto>,
    /// Optional override of the category-derived zoom range.
    #[serde(default)]
    pub requested_zoom_range: Option<ZoomRange>,
    /// Assume EPSG:4326 when a shapefile has no .prj (US-04).
    #[serde(default)]
    pub assume_crs_wgs84: bool,
    /// Named processing profile (e.g. `parcel_high_zoom`); participates in
    /// the idempotency key (Sequence 1 US-01). Defaults to the
    /// category+zoom-derived label when absent.
    #[serde(default)]
    pub processing_profile: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LayerMetadataInputDto {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub category: Option<LayerCategory>,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// `POST /api/v1/ingest/uploads` response (TRD §8.1).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadResponse {
    pub job_id: String,
    /// Sequence 1 US-01: key under which duplicate upload requests converge
    /// to the same job.
    pub idempotency_key: String,
    pub upload_url: String,
    pub expires_in: u64,
    pub status: JobStatus,
}

/// `PUT .../content` response.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadAcceptedResponse {
    pub job_id: String,
    pub status: JobStatus,
}

/// `GET /api/v1/jobs/{jobId}` response (TRD §8.2).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobResponse {
    pub job_id: String,
    pub status: JobStatus,
    pub layer_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_tile_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounding_box: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Machine-readable failure taxonomy code (docs/ERRORS.md).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    /// Workflow stage where a failed job stopped (e.g. `NORMALIZING`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_stage: Option<String>,
}

/// `GET /api/v1/layers` query parameters (TRD §8.3).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LayerListQuery {
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub category: Option<LayerCategory>,
    /// Free-text filter matched against tags and name (e.g. "NYC").
    #[serde(default)]
    pub market: Option<String>,
}

/// `POST /api/v1/ops/layers/{layerId}/rollback` request (Sequence 2
/// US-AP-05). `reason` is mandatory for SOC2-aligned auditability.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollbackRequest {
    pub target_tile_version: String,
    pub reason: String,
}

/// `POST /api/v1/ops/jobs/{jobId}/replay` request (Sequence 3 US-04).
/// `reason` is mandatory (auditability); `requestedBy` defaults to
/// the authenticated caller when omitted.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayJobRequest {
    pub reason: String,
    #[serde(default)]
    pub requested_by: Option<String>,
    /// Assume EPSG:4326 for `UNKNOWN_CRS` failures (TRD §10 confirmation).
    #[serde(default)]
    pub assume_crs_wgs84: bool,
    /// Replay a `COMPLETED` job as an explicit new-version publish.
    #[serde(default)]
    pub create_new_version: bool,
}

/// `POST /api/v1/ops/jobs/{jobId}/replay` response.
///
/// `status` is `REPLAY_ACCEPTED` (202, replay running under the original
/// jobId) or `REPLAY_NO_OP` (200, original job already completed).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayJobResponse {
    pub original_job_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_id: Option<String>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// `GET /api/v1/ops/dlq` query parameters (Sequence 3 US-06).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DlqListQuery {
    pub tenant_id: Option<String>,
}
