//! End-to-end job runner implementing the TRD §10 workflow states.
//!
//! In AWS, Step Functions invokes the `vtile` binary per state; `run_job`
//! executes the same states in-process for local/dev runs.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use tracing::{info, instrument};

use vtile_core::config::TileConfig;
use vtile_core::model::{
    Bbox, JobOutcomeSummary, JobRecord, JobStatus, LayerCategory, LayerMetadata,
    SecurityClassification,
};
use vtile_core::sink::TileObjectMeta;
use vtile_core::tileset::{generate_tiles, prepare_features, RawFeature, TileStats};
use vtile_ingest::normalize::{
    normalize_source, write_normalized_geojson, NormalizeOptions, SourceFile,
};

use crate::error::{PipelineError, PipelineResult};
use crate::events::{EventEmitter, PipelineEvent};
use crate::manifest::TileManifest;
use crate::quarantine::{ErrorReport, QuarantineStore};
use crate::sink_local::LocalTileSink;
use crate::store::{JobStore, LayerCatalog};

/// Filesystem layout roots for one job (mirrors TRD §6 S3 prefixes).
#[derive(Debug, Clone)]
pub struct JobPaths {
    /// `staging/{tenantId}/{jobId}/`
    pub staging_root: PathBuf,
    /// `tiles/{tenantId}/{layerId}/`
    pub tiles_root: PathBuf,
    /// `manifests/{tenantId}/{layerId}/`
    pub manifests_root: PathBuf,
}

impl JobPaths {
    pub fn normalized_artifact(&self) -> PathBuf {
        self.staging_root.join("normalized.geojson")
    }

    pub fn manifest_path(&self) -> PathBuf {
        self.manifests_root.join("manifest.json")
    }

    /// Stable live pointer written atomically after each publish
    /// (Recommendation 2 US-04); identical content to `manifest.json`.
    pub fn latest_path(&self) -> PathBuf {
        self.manifests_root.join("latest.json")
    }
}

/// Standard local layout for one job (mirrors the TRD §6 S3 prefixes).
pub fn job_paths_for(data_dir: &Path, tenant_id: &str, job_id: &str, layer_id: &str) -> JobPaths {
    JobPaths {
        staging_root: data_dir.join("staging").join(tenant_id).join(job_id),
        tiles_root: data_dir.join("tiles").join(tenant_id).join(layer_id),
        manifests_root: data_dir.join("manifests").join(tenant_id).join(layer_id),
    }
}

/// Shared services for a run.
pub struct JobDeps {
    pub jobs: Arc<dyn JobStore>,
    pub catalog: Arc<dyn LayerCatalog>,
    pub events: Arc<dyn EventEmitter>,
    /// Optional quarantine for failed uploads (Recommendation 3 US-03).
    /// `None` disables quarantine (e.g. unit tests).
    pub quarantine: Option<Arc<dyn QuarantineStore>>,
}

/// Everything needed to run one job.
pub struct RunJobInput {
    pub job: JobRecord,
    pub source_bytes: Vec<u8>,
    pub tile_config: TileConfig,
    pub normalize_opts: NormalizeOptions,
    pub paths: JobPaths,
}

/// Outcome of a successful run.
#[derive(Debug)]
pub struct JobOutcome {
    pub feature_count: u64,
    pub tile_count: u64,
    pub bbox: Bbox,
    pub tile_version: String,
    pub manifest: TileManifest,
    pub stats: TileStats,
    pub warnings: Vec<String>,
}

/// Runs the full workflow: validate → normalize → tile → publish → catalog →
/// event. Job status transitions are persisted through the [`JobStore`].
///
/// Sequence 1 US-04: the run acquires the worker lease first (exactly one
/// active processor per job), checkpoints every stage transition with a
/// conditional update, and releases the lease when the run settles. A lease
/// conflict fails fast without touching job state or emitting a failure
/// event — the winning worker owns the job.
#[instrument(skip_all, fields(job_id = %input.job.job_id, layer = %input.job.layer_id))]
pub fn run_job(input: &RunJobInput, deps: &JobDeps) -> PipelineResult<JobOutcome> {
    let job = &input.job;

    // TRD §14 reliability: idempotent job processing using jobId.
    if let Some(existing) = deps.jobs.get(&job.job_id)? {
        if matches!(
            existing.status,
            JobStatus::Completed | JobStatus::Cancelled
        ) {
            return Err(PipelineError::Job(format!(
                "job {} already completed (idempotency guard)",
                job.job_id
            )));
        }
    }

    // Sequence 1 US-04: worker lease.
    let worker_id = format!("vtile-worker-{}", std::process::id());
    let lease = deps
        .jobs
        .acquire_lease(&job.job_id, &worker_id, WORKER_LEASE_SECS)?;
    info!(
        job_id = %job.job_id,
        worker = %worker_id,
        lease = %lease.lease_token,
        "worker lease acquired"
    );

    // Tracks the workflow stage so failures can report `failedStage`.
    let mut stage = JobStatus::Queued;
    match run_job_inner(input, deps, &mut stage, &lease.lease_token) {
        Ok(outcome) => Ok(outcome),
        Err(err) => {
            let (code, message) = error_classification(&err);
            let failed_stage = stage.as_str().to_string();

            // Persist the failure with taxonomy code + failed stage
            // (Recommendation 3 US-02), releasing the lease (US-04).
            let persisted = match deps.jobs.get(&job.job_id) {
                Ok(Some(mut record)) => {
                    record.status = JobStatus::Failed;
                    record.error = Some(message.clone());
                    record.error_code = Some(code.clone());
                    record.failed_stage = Some(failed_stage.clone());
                    record.state_version += 1;
                    record.lease_token = None;
                    record.locked_by = None;
                    record.lease_expires_at = None;
                    record.updated_at = Utc::now();
                    deps.jobs.upsert(record).is_ok()
                }
                _ => false,
            };
            if !persisted {
                let _ = deps
                    .jobs
                    .update_status(&job.job_id, JobStatus::Failed, Some(message.clone()));
            }

            // Quarantine failed uploads (Recommendation 3 US-03) so SREs can
            // inspect and replay. Only ingest failures carry a source-data
            // problem; tile/store failures are infrastructural.
            if matches!(err, PipelineError::Ingest(_)) {
                if let Some(quarantine) = deps.quarantine.as_ref() {
                    let report = ErrorReport::from_job(job, &code, &message, &failed_stage);
                    match quarantine.quarantine(job, &input.source_bytes, &report) {
                        Ok(dir) => info!(
                            job_id = %job.job_id,
                            quarantine = %dir.display(),
                            "source quarantined for replay"
                        ),
                        Err(qe) => tracing::error!(
                            job_id = %job.job_id,
                            error = %qe,
                            "failed to quarantine source"
                        ),
                    }
                }
            }

            deps.events.emit(PipelineEvent::VectorTileJobFailed {
                event_id: new_event_id(),
                tenant_id: job.tenant_id.clone(),
                job_id: job.job_id.clone(),
                error_code: code,
                error_message: message,
                occurred_at: Utc::now(),
            });
            Err(err)
        }
    }
}

fn run_job_inner(
    input: &RunJobInput,
    deps: &JobDeps,
    stage: &mut JobStatus,
    lease_token: &str,
) -> PipelineResult<JobOutcome> {
    let job = &input.job;
    let paths = &input.paths;
    let mut warnings: Vec<String> = Vec::new();

    // ── 1. Validate upload ────────────────────────────────────────────────
    advance_stage(deps, &job.job_id, lease_token, stage, JobStatus::Validating)?;
    if !job.source_format.is_supported_in_mvp() {
        return Err(PipelineError::Job(format!(
            "source format {:?} not supported in MVP",
            job.source_format
        )));
    }

    // ── 2–7. Detect format → unpack → CRS/geometry validation → normalize ─
    advance_stage(deps, &job.job_id, lease_token, stage, JobStatus::Normalizing)?;
    let source = match job.source_format {
        vtile_core::model::SourceFormat::GeoJson => SourceFile::GeoJson {
            bytes: input.source_bytes.clone(),
        },
        vtile_core::model::SourceFormat::Shapefile => SourceFile::ShapefileZip {
            bytes: input.source_bytes.clone(),
        },
        other => return Err(PipelineError::Job(format!("unsupported format {other:?}"))),
    };
    let dataset = normalize_source(source, &input.normalize_opts)?;
    warnings.extend(dataset.warnings.iter().cloned());
    info!(
        features = dataset.feature_count(),
        rejected = dataset.rejected_features,
        crs = dataset.crs.label(),
        "normalization complete"
    );

    // ── 5. Write normalized artifact ──────────────────────────────────────
    fs::create_dir_all(&paths.staging_root)?;
    let normalized_json = write_normalized_geojson(&dataset);
    fs::write(paths.normalized_artifact(), serde_json::to_string(&normalized_json)?)?;

    let Some(bbox) = dataset.bbox else {
        return Err(PipelineError::Job("normalized dataset has no coordinates".into()));
    };

    // ── 8. Generate vector tiles ──────────────────────────────────────────
    advance_stage(deps, &job.job_id, lease_token, stage, JobStatus::Tiling)?;
    let tile_version = new_tile_version();
    let raw_features: Vec<RawFeature> = dataset
        .features
        .iter()
        .map(|f| RawFeature {
            id: f.id,
            geometry: f.geometry.clone(),
            properties: f.properties.clone(),
        })
        .collect();
    let prepared = prepare_features(raw_features, &input.tile_config);
    if prepared.feature_count() == 0 {
        return Err(PipelineError::Job("no tileable features after preparation".into()));
    }

    let sink_root = paths.tiles_root.join(&tile_version);
    let sink = LocalTileSink::new(&sink_root);
    let meta = TileObjectMeta {
        tenant_id: job.tenant_id.clone(),
        layer_id: job.layer_id.clone(),
        tile_version: tile_version.clone(),
        source_format: job.source_format.as_str().to_string(),
        crs: "EPSG:4326".to_string(),
        min_zoom: input.tile_config.zoom_range.min_zoom,
        max_zoom: input.tile_config.zoom_range.max_zoom,
    };
    let stats = generate_tiles(&prepared, &input.tile_config, &meta, &sink)?;
    info!(
        tiles = stats.tiles_written,
        gzip_bytes = stats.total_gzip_bytes,
        ms = stats.elapsed_ms,
        "tile generation complete"
    );

    // ── 9/10. Publish tiles + write manifest ──────────────────────────────
    advance_stage(deps, &job.job_id, lease_token, stage, JobStatus::Publishing)?;
    let manifest = TileManifest {
        schema_version: crate::manifest::MANIFEST_SCHEMA_VERSION,
        tenant_id: job.tenant_id.clone(),
        layer_id: job.layer_id.clone(),
        tile_version: tile_version.clone(),
        min_zoom: input.tile_config.zoom_range.min_zoom,
        max_zoom: input.tile_config.zoom_range.max_zoom,
        tile_count: stats.tiles_written,
        total_gzip_bytes: stats.total_gzip_bytes,
        bounding_box: bbox,
        generated_at: Utc::now(),
        tile_url_template: None,
    };
    fs::create_dir_all(&paths.manifests_root)?;
    fs::write(paths.manifest_path(), manifest.to_json()?)?;
    // Atomic live pointer (Recommendation 2 US-04): `latest.json` mirrors the
    // manifest and is written via tmp + rename so readers never observe a
    // partial document.
    write_latest_pointer(paths, &manifest)?;

    // ── 11. Update catalog ────────────────────────────────────────────────
    let layer_input = job.layer_input.clone().unwrap_or_default();
    let category = layer_input
        .category
        .unwrap_or(vtile_core::model::LayerCategory::Other);
    let layer_meta = LayerMetadata {
        layer_id: job.layer_id.clone(),
        tenant_id: job.tenant_id.clone(),
        name: layer_input
            .name
            .unwrap_or_else(|| job.layer_id.clone()),
        description: layer_input.description,
        category,
        source_format: job.source_format,
        crs: "EPSG:4326".to_string(),
        geometry_type: prepared.geometry_kind,
        feature_count: prepared.feature_count(),
        bounding_box: bbox,
        min_zoom: input.tile_config.zoom_range.min_zoom,
        max_zoom: input.tile_config.zoom_range.max_zoom,
        tags: layer_input.tags,
        security_classification: SecurityClassification::Internal,
        published_at: Some(Utc::now()),
        tile_version: tile_version.clone(),
        assumed_crs: dataset.crs.assumed,
    };
    deps.catalog.upsert(layer_meta)?;

    // ── 12. Emit completion event + finalize job ──────────────────────────
    deps.events.emit(PipelineEvent::VectorTileJobCompleted {
        event_id: new_event_id(),
        tenant_id: job.tenant_id.clone(),
        job_id: job.job_id.clone(),
        layer_id: job.layer_id.clone(),
        feature_count: prepared.feature_count(),
        tile_count: stats.tiles_written,
        min_zoom: input.tile_config.zoom_range.min_zoom,
        max_zoom: input.tile_config.zoom_range.max_zoom,
        tile_version: tile_version.clone(),
        occurred_at: Utc::now(),
    });

    // Finalize on top of the latest stored record so the version history
    // is intact and the lease is released (Sequence 1 US-04).
    let mut completed = deps
        .jobs
        .get(&job.job_id)?
        .unwrap_or_else(|| job.clone());
    completed.status = JobStatus::Completed;
    completed.updated_at = Utc::now();
    completed.state_version += 1;
    completed.lease_token = None;
    completed.locked_by = None;
    completed.lease_expires_at = None;
    completed.outcome = Some(JobOutcomeSummary {
        feature_count: prepared.feature_count(),
        published_tile_count: stats.tiles_written,
        bounding_box: bbox,
        tile_version: tile_version.clone(),
        completed_at: Utc::now(),
    });
    deps.jobs.upsert(completed)?;

    Ok(JobOutcome {
        feature_count: prepared.feature_count(),
        tile_count: stats.tiles_written,
        bbox,
        tile_version,
        manifest,
        stats,
        warnings,
    })
}

/// Conditional stage checkpoint (Sequence 1 US-04): validates the edge
/// against the state machine, asserts lease ownership, and bumps
/// `stateVersion` — the local analog of a DynamoDB conditional update.
fn advance_stage(
    deps: &JobDeps,
    job_id: &str,
    lease_token: &str,
    stage: &mut JobStatus,
    next: JobStatus,
) -> PipelineResult<()> {
    deps.jobs
        .transition(job_id, Some(lease_token), *stage, next)?;
    *stage = next;
    Ok(())
}

/// Maps errors onto the TRD error-code vocabulary used in `job.failed`
/// events, job records, and API responses (full table in `docs/ERRORS.md`).
pub fn error_classification(err: &PipelineError) -> (String, String) {
    let code = match err {
        PipelineError::Ingest(e) => e.error_code(),
        PipelineError::Tile(e) => match e {
            vtile_core::TileError::SizeExceeded { .. } => "TILE_SIZE_EXCEEDED",
            _ => "TILE_GENERATION_FAILED",
        },
        PipelineError::Store(_) => "STORE_ERROR",
        _ => "PIPELINE_ERROR",
    };
    (code.to_string(), err.to_string())
}

/// Writes `latest.json` alongside `manifest.json` (tmp + rename).
pub fn write_latest_pointer(paths: &JobPaths, manifest: &TileManifest) -> PipelineResult<()> {
    let tmp = paths.latest_path().with_extension("json.tmp");
    fs::write(&tmp, manifest.to_json()?)?;
    fs::rename(&tmp, paths.latest_path())?;
    Ok(())
}

/// TRD layer-naming convention `{source}_{type}` (US-09) for MVT layer
/// names. Single source of truth shared by the CLI, the replay path, and the
/// API (`vtile_api::routes::mvt_layer_name` delegates here).
pub fn default_mvt_layer_name(category: Option<LayerCategory>, layer_id: &str) -> String {
    match category {
        Some(LayerCategory::Parcel) => "parcel_boundary".to_string(),
        Some(LayerCategory::Zoning) => "zoning_district".to_string(),
        Some(LayerCategory::FloodRisk) => "flood_100yr".to_string(),
        Some(LayerCategory::Submarket) => "submarket_area".to_string(),
        Some(LayerCategory::AssetPoint) => "asset_point".to_string(),
        Some(LayerCategory::Macro) => "macro_region".to_string(),
        Some(LayerCategory::Other) | None => format!("{layer_id}_features"),
    }
}

/// Tile version stamp in the TRD example format (`2026-06-17T14-30-00Z`).
pub fn new_tile_version() -> String {
    Utc::now().format("%Y-%m-%dT%H-%M-%SZ").to_string()
}

/// Worker lease TTL (Sequence 1 US-04): matches the TRD Fargate hard limit
/// of 15 minutes; expired leases may be taken over by another worker.
pub const WORKER_LEASE_SECS: u64 = 900;

/// Server-minted client token for upload requests without an explicit
/// `Idempotency-Key` — makes every such request unique, so intentional
/// repeat uploads (e.g. a county parcel refresh) receive a fresh job unless
/// the client supplies the same token (Sequence 1 US-01 CRE rule).
pub fn new_idempotency_token() -> String {
    format!("auto_{}", uuid::Uuid::new_v4().as_simple())
}

pub fn new_event_id() -> String {
    format!("evt_{}", uuid::Uuid::new_v4().as_simple())
}

pub fn new_job_id() -> String {
    format!("job_{}", uuid::Uuid::new_v4().as_simple())
}

/// SHA-256 of a source payload; useful for idempotency keys and the
/// content-hash versioning option in TRD open question 3.
pub fn source_hash(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

/// Best-effort local path helper for tests and the CLI.
pub fn ensure_dir(path: &Path) -> PipelineResult<()> {
    fs::create_dir_all(path)?;
    Ok(())
}
