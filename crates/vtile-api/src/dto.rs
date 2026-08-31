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
