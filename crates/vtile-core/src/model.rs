//! Domain models shared across the pipeline (TRD §7 data models).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Source file formats supported by the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceFormat {
    #[serde(rename = "GEOJSON")]
    GeoJson,
    #[serde(rename = "SHAPEFILE")]
    Shapefile,
    /// Post-MVP (TRD §3).
    #[serde(rename = "KML")]
    Kml,
    /// Post-MVP (TRD §3).
    #[serde(rename = "GEOPACKAGE")]
    GeoPackage,
    /// Post-MVP (TRD §3).
    #[serde(rename = "FLATGEOBUF")]
    FlatGeobuf,
}

impl SourceFormat {
    pub fn is_supported_in_mvp(&self) -> bool {
        matches!(self, Self::GeoJson | Self::Shapefile)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GeoJson => "GEOJSON",
            Self::Shapefile => "SHAPEFILE",
            Self::Kml => "KML",
            Self::GeoPackage => "GEOPACKAGE",
            Self::FlatGeobuf => "FLATGEOBUF",
        }
    }
}

/// Job lifecycle states (TRD §7 job record + §10 workflow).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    UploadPending,
    Queued,
    Validating,
    Normalizing,
    Tiling,
    Publishing,
    Completed,
    Failed,
}

impl JobStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

/// layer categories (TRD §1 + §5 zoom strategy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LayerCategory {
    Parcel,
    Zoning,
    FloodRisk,
    Submarket,
    AssetPoint,
    Macro,
    Other,
}

impl LayerCategory {
    /// Recommended zoom range per TRD §5 "Tile Zoom Strategy".
    pub fn default_zoom_range(&self) -> ZoomRange {
        match self {
            Self::Macro => ZoomRange::new(0, 8),
            Self::Submarket => ZoomRange::new(4, 12),
            Self::Parcel => ZoomRange::new(10, 16),
            Self::AssetPoint => ZoomRange::new(4, 16),
            Self::FloodRisk => ZoomRange::new(6, 14),
            // Zoning is not in the TRD table; parcel-adjacent default.
            Self::Zoning => ZoomRange::new(8, 16),
            Self::Other => ZoomRange::new(0, 16),
        }
    }
}

/// Data classification tags required by SOC2 controls (TRD §13).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SecurityClassification {
    Public,
    Internal,
    Confidential,
}

impl Default for SecurityClassification {
    fn default() -> Self {
        Self::Internal
    }
}

/// Inclusive zoom range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ZoomRange {
    pub min_zoom: u8,
    pub max_zoom: u8,
}

impl ZoomRange {
    pub fn new(min_zoom: u8, max_zoom: u8) -> Self {
        Self {
            min_zoom,
            max_zoom: max_zoom.max(min_zoom),
        }
    }

    pub fn contains(&self, z: u8) -> bool {
        z >= self.min_zoom && z <= self.max_zoom
    }

    pub fn iter(&self) -> impl Iterator<Item = u8> {
        self.min_zoom..=self.max_zoom
    }
}

/// Geographic bounding box in EPSG:4326 `[minLon, minLat, maxLon, maxLat]`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<f64>", into = "Vec<f64>")]
pub struct Bbox {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

impl Bbox {
    pub fn new(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Self {
        Self {
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        }
    }

    pub fn union(&self, other: &Bbox) -> Bbox {
        Bbox {
            min_lon: self.min_lon.min(other.min_lon),
            min_lat: self.min_lat.min(other.min_lat),
            max_lon: self.max_lon.max(other.max_lon),
            max_lat: self.max_lat.max(other.max_lat),
        }
    }

    pub fn to_vec(&self) -> Vec<f64> {
        vec![self.min_lon, self.min_lat, self.max_lon, self.max_lat]
    }
}

impl TryFrom<Vec<f64>> for Bbox {
    type Error = String;

    fn try_from(v: Vec<f64>) -> Result<Self, Self::Error> {
        match v[..] {
            [a, b, c, d] => Ok(Bbox::new(a, b, c, d)),
            _ => Err("bbox must have exactly 4 values".to_string()),
        }
    }
}

impl From<Bbox> for Vec<f64> {
    fn from(b: Bbox) -> Vec<f64> {
        b.to_vec()
    }
}

/// Job record (TRD §7). Extended with `outcome` for the status API (§8.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobRecord {
    pub job_id: String,
    pub tenant_id: String,
    pub layer_id: String,
    pub status: JobStatus,
    pub source_format: SourceFormat,
    pub source_uri: String,
    pub requested_zoom_range: ZoomRange,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Populated when the job completes (feature count, tile count, bbox...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<JobOutcomeSummary>,
    /// Layer metadata supplied at upload time (name, category, tags...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layer_input: Option<LayerMetadataInput>,
}

/// Summary attached to a completed job and returned by `GET /jobs/{jobId}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobOutcomeSummary {
    pub feature_count: u64,
    pub published_tile_count: u64,
    pub bounding_box: Bbox,
    pub tile_version: String,
    pub completed_at: DateTime<Utc>,
}

/// User-supplied layer metadata captured at upload time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LayerMetadataInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<LayerCategory>,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Geometry kind summary (TRD §4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GeometryKind {
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    Mixed,
}

impl GeometryKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Point => "POINT",
            Self::MultiPoint => "MULTIPOINT",
            Self::LineString => "LINESTRING",
            Self::MultiLineString => "MULTILINESTRING",
            Self::Polygon => "POLYGON",
            Self::MultiPolygon => "MULTIPOLYGON",
            Self::Mixed => "MIXED",
        }
    }
}

/// Layer catalog record (TRD §7 "Layer Metadata Record").
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LayerMetadata {
    pub layer_id: String,
    pub tenant_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub category: LayerCategory,
    pub source_format: SourceFormat,
    /// Recorded CRS (TRD §14 accuracy: CRS must be recorded in metadata).
    pub crs: String,
    pub geometry_type: GeometryKind,
    pub feature_count: u64,
    pub bounding_box: Bbox,
    pub min_zoom: u8,
    pub max_zoom: u8,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub security_classification: SecurityClassification,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<DateTime<Utc>>,
    /// Current live tile version; tiles live under this version prefix.
    pub tile_version: String,
    /// True when the source CRS was missing and WGS84 was assumed (US-04).
    #[serde(default)]
    pub assumed_crs: bool,
}
