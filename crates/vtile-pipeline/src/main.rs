//! `vtile` — tile processor CLI.
//!
//! In AWS this binary is the ECS Fargate tile processor entrypoint
//! (TRD §11 Decision 2); Step Functions runs one subcommand per state.
//! Locally it runs the full pipeline against the filesystem.
//!
//! ```bash
//! vtile run --tenant tenant-acme --layer us-parcels-nyc \
//!     --format shapefile --input parcels.zip --data-dir ./data \
//!     --min-zoom 10 --max-zoom 16
//! vtile inspect-tile ./data/tiles/.../12/1206/1538.pbf
//! vtile job-status --data-dir ./data --job-id job_...
//! vtile replay --data-dir ./data --tenant tenant-acme --job-id job_... \
//!     --assume-wgs84 --requested-by sre-user --reason "Transient Fargate timeout"
//! vtile rollback --data-dir ./data --tenant tenant-acme --layer us-parcels-nyc \
//!     --target-version 2026-06-17T14-30-00Z-abcd1234 --reason "Misaligned parcel refresh"
//! vtile dlq list --data-dir ./data [--tenant tenant-acme]
//! ```

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};

use vtile_core::config::TileConfig;
use vtile_core::model::{JobRecord, JobStatus, LayerCategory, SourceFormat, ZoomRange};
use vtile_ingest::normalize::{CrsPolicy, NormalizeOptions};
use vtile_pipeline::events::LoggingEventEmitter;
use vtile_pipeline::job::{
    job_paths_for, new_idempotency_token, new_job_id, new_tile_version, run_job, RunJobInput,
};
use vtile_pipeline::recovery::{run_job_with_retries, FileDlqStore, RetryPolicy};
use vtile_pipeline::store::{FileJobStore, FileLayerCatalog, JobStore};
use vtile_pipeline::{
    processing_profile_label, upload_idempotency_key, FileQuarantineStore, JobDeps, ReplayOptions,
    ReplayOutcome,
};

#[derive(Parser)]
#[command(name = "vtile", about = "Vector tile processor", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the full pipeline for one source file.
    Run {
        /// Tenant identifier (path isolation boundary, TRD §13).
        #[arg(long)]
        tenant: String,
        /// Logical layer id, e.g. `us-parcels-nyc`.
        #[arg(long)]
        layer: String,
        /// Source format.
        #[arg(long, value_enum)]
        format: FormatArg,
        /// Path to the uploaded file (.geojson/.json or .zip shapefile).
        #[arg(long)]
        input: PathBuf,
        /// Local data root (jobs/, catalog, staging/, tiles/, manifests/).
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        /// Minimum zoom to generate.
        #[arg(long)]
        min_zoom: Option<u8>,
        /// Maximum zoom to generate.
        #[arg(long)]
        max_zoom: Option<u8>,
        /// MVT layer name (default: `{layer}_boundary`).
        #[arg(long)]
        layer_name: Option<String>,
        /// Layer category; sets the default zoom range (TRD §5).
        #[arg(long, value_enum)]
        category: Option<CategoryArg>,
        /// Assume EPSG:4326 when a shapefile has no .prj (US-04).
        #[arg(long, default_value_t = false)]
        assume_wgs84: bool,
        /// Skip tile generation; only validate + normalize.
        #[arg(long, default_value_t = false)]
        normalize_only: bool,
    },
    /// Decode a .pbf tile and print a structural summary (tile QA tooling).
    InspectTile {
        /// Path to a tile (.pbf, gzip-compressed or raw).
        path: PathBuf,
    },
    /// Print the status of a job in the production `GET /jobs/{jobId}`
    /// response shape (Recommendation 1 US-03).
    JobStatus {
        /// Local data root (must match the run that created the job).
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        #[arg(long)]
        job_id: String,
    },
    /// Replay a failed, quarantined job (Recommendation 3 US-03: DLQ replay).
    Replay {
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        job_id: String,
        /// Assume EPSG:4326 when the source has no CRS information — the
        /// "user confirmation" TRD §10 requires for missing `.prj`.
        #[arg(long, default_value_t = false)]
        assume_wgs84: bool,
        /// Operator identity recorded in the replay audit (Sequence 1 US-05).
        #[arg(long, default_value = "cli")]
        requested_by: String,
        /// Reason recorded in the replay audit (Sequence 1 US-05).
        #[arg(long, default_value = "")]
        reason: String,
        /// Explicit intent to publish a new tile version from a COMPLETED
        /// job; required to replay completed jobs (Sequence 1 US-05).
        #[arg(long, default_value_t = false)]
        create_new_version: bool,
    },
    /// Roll back a layer to a previously published tile version
    /// (Sequence 2 US-AP-05). Repoints the authoritative version record and
    /// rewrites the manifests — no source reprocessing.
    Rollback {
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        layer: String,
        /// Tile version to restore (must be retained on disk with its
        /// candidate manifest).
        #[arg(long)]
        target_version: String,
        /// Reason recorded in the audit trail (mandatory, US-AP-06).
        #[arg(long)]
        reason: String,
        /// Operator identity recorded in the audit trail.
        #[arg(long, default_value = "cli")]
        requested_by: String,
    },
    /// Inspect the dead-letter queue (Sequence 3 US-01/US-06).
    Dlq {
        #[command(subcommand)]
        command: DlqCommand,
    },
}

#[derive(Subcommand)]
enum DlqCommand {
    /// List dead-lettered jobs, optionally scoped to one tenant.
    List {
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        /// Filter by tenant (omit for all tenants).
        #[arg(long)]
        tenant: Option<String>,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum FormatArg {
    Geojson,
    Shapefile,
}

#[derive(Clone, Copy, ValueEnum)]
enum CategoryArg {
    Parcel,
    Zoning,
    FloodRisk,
    Submarket,
    AssetPoint,
    Macro,
}

impl CategoryArg {
    fn into_category(self) -> LayerCategory {
        match self {
            CategoryArg::Parcel => LayerCategory::Parcel,
            CategoryArg::Zoning => LayerCategory::Zoning,
            CategoryArg::FloodRisk => LayerCategory::FloodRisk,
            CategoryArg::Submarket => LayerCategory::Submarket,
            CategoryArg::AssetPoint => LayerCategory::AssetPoint,
            CategoryArg::Macro => LayerCategory::Macro,
        }
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Run {
            tenant,
            layer,
            format,
            input,
            data_dir,
            min_zoom,
            max_zoom,
            layer_name,
            category,
            assume_wgs84,
            normalize_only,
        } => run(
            tenant,
            layer,
            format,
            input,
            data_dir,
            min_zoom,
            max_zoom,
            layer_name,
            category,
            assume_wgs84,
            normalize_only,
        ),
        Command::InspectTile { path } => inspect_tile(path),
        Command::JobStatus { data_dir, job_id } => job_status(data_dir, job_id),
        Command::Replay {
            data_dir,
            tenant,
            job_id,
            assume_wgs84,
            requested_by,
            reason,
            create_new_version,
        } => replay(
            data_dir,
            tenant,
            job_id,
            assume_wgs84,
            requested_by,
            reason,
            create_new_version,
        ),
        Command::Rollback {
            data_dir,
            tenant,
            layer,
            target_version,
            reason,
            requested_by,
        } => rollback(
            data_dir,
            tenant,
            layer,
            target_version,
            reason,
            requested_by,
        ),
        Command::Dlq { command } => match command {
            DlqCommand::List { data_dir, tenant } => dlq_list(data_dir, tenant),
        },
    }
}

/// Local service wiring shared by `run` and `replay`.
fn build_deps(data_dir: &PathBuf) -> Result<JobDeps> {
    Ok(JobDeps {
        jobs: Arc::new(FileJobStore::new(data_dir.join("jobs"))?),
        // Same catalog path as vtile-api so layers published by the CLI are
        // visible to the API and vice versa (local contract parity).
        catalog: Arc::new(FileLayerCatalog::new(data_dir.join("catalog.json"))?),
        events: Arc::new(LoggingEventEmitter),
        quarantine: Some(Arc::new(FileQuarantineStore::new(
            data_dir.join("quarantine"),
        ))),
        // Sequence 3 US-01: dead-letter capture lives under `dlq/`.
        dlq: Some(Arc::new(FileDlqStore::new(data_dir.join("dlq")))),
    })
}

#[allow(clippy::too_many_arguments)]
fn run(
    tenant: String,
    layer: String,
    format: FormatArg,
    input: PathBuf,
    data_dir: PathBuf,
    min_zoom: Option<u8>,
    max_zoom: Option<u8>,
    layer_name: Option<String>,
    category: Option<CategoryArg>,
    assume_wgs84: bool,
    normalize_only: bool,
) -> Result<()> {
    let source_format = match format {
        FormatArg::Geojson => SourceFormat::GeoJson,
        FormatArg::Shapefile => SourceFormat::Shapefile,
    };
    let category = category.map(CategoryArg::into_category);
    let zoom_range = match (min_zoom, max_zoom) {
        (Some(lo), Some(hi)) => ZoomRange::new(lo, hi),
        _ => category
            .unwrap_or(LayerCategory::Other)
            .default_zoom_range(),
    };

    let source_bytes = fs::read(&input)
        .with_context(|| format!("failed to read input file {}", input.display()))?;

    let job_id = new_job_id();
    let staging_root = data_dir
        .join("staging")
        .join(&tenant)
        .join(&job_id);

    // Persist the raw upload alongside the job (staging/input, TRD §6).
    fs::create_dir_all(staging_root.join("input"))?;
    let input_name = input
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "upload.bin".into());
    fs::write(staging_root.join("input").join(&input_name), &source_bytes)?;

    let now = chrono::Utc::now();
    let job = JobRecord {
        job_id: job_id.clone(),
        tenant_id: tenant.clone(),
        layer_id: layer.clone(),
        status: JobStatus::Queued,
        source_format,
        source_uri: format!("file://{}/input/{}", staging_root.display(), input_name),
        requested_zoom_range: zoom_range,
        created_at: now,
        updated_at: now,
        error: None,
        error_code: None,
        failed_stage: None,
        error_class: None,
        replay_eligible: false,
        // Sequence 1 US-01: CLI runs are intentional one-shot uploads — a
        // server-minted token gives each run its own idempotency key.
        idempotency_key: Some(upload_idempotency_key(
            &tenant,
            &layer,
            &new_idempotency_token(),
            &processing_profile_label(category, zoom_range),
        )),
        trace_id: Some(vtile_pipeline::new_trace_id()),
        request_fingerprint: None,
        event_dedupe_fingerprint: None,
        state_version: 1,
        lease_token: None,
        locked_by: None,
        lease_expires_at: None,
        duplicate_event_count: 0,
        requested_tile_version: Some(new_tile_version()),
        replay_audit: None,
        replay_count: 0,
        outcome: None,
        layer_input: None,
    };

    let mut tile_config = TileConfig {
        layer_name: layer_name.unwrap_or_else(|| format!("{layer}_boundary")),
        zoom_range,
        ..Default::default()
    };
    tile_config
        .validate()
        .map_err(|e| anyhow::anyhow!(e))?;

    let normalize_opts = NormalizeOptions {
        crs_policy: if assume_wgs84 {
            CrsPolicy::AssumeWgs84
        } else {
            CrsPolicy::RequireKnown
        },
        ..Default::default()
    };

    let deps = build_deps(&data_dir)?;
    deps.jobs.create(job.clone())?;

    if normalize_only {
        // Stop after the normalization artifact (Step Functions states 1–7).
        let source = match source_format {
            SourceFormat::GeoJson => {
                vtile_ingest::SourceFile::GeoJson { bytes: source_bytes }
            }
            SourceFormat::Shapefile => {
                vtile_ingest::SourceFile::ShapefileZip { bytes: source_bytes }
            }
            _ => unreachable!(),
        };
        let dataset = vtile_ingest::normalize_source(source, &normalize_opts)?;
        let json = vtile_ingest::write_normalized_geojson(&dataset);
        fs::create_dir_all(&staging_root)?;
        fs::write(
            staging_root.join("normalized.geojson"),
            serde_json::to_string(&json)?,
        )?;
        println!(
            "{}",
            serde_json::json!({
                "jobId": job_id,
                "status": "NORMALIZED",
                "featureCount": dataset.feature_count(),
                "warnings": dataset.warnings,
                "bbox": dataset.bbox.map(|b| b.to_vec()),
            })
        );
        return Ok(());
    }

    let input = RunJobInput {
        job,
        source_bytes,
        tile_config,
        normalize_opts,
        paths: job_paths_for(&data_dir, &tenant, &job_id, &layer),
    };
    let outcome = run_job_with_retries(&input, &deps, &RetryPolicy::default())?;

    println!(
        "{}",
        serde_json::json!({
            "jobId": job_id,
            "status": "COMPLETED",
            "featureCount": outcome.feature_count,
            "tileCount": outcome.tile_count,
            "tileVersion": outcome.tile_version,
            "bbox": outcome.bbox.to_vec(),
            "warnings": outcome.warnings,
            "tilesRoot": input.paths.version_root(&outcome.tile_version),
        })
    );
    Ok(())
}

/// Prints job status in the production `GET /api/v1/jobs/{jobId}` response
/// shape (TRD §8.2), including the failure taxonomy added by Recommendation 3
/// (`errorCode`, `failedStage`).
fn job_status(data_dir: PathBuf, job_id: String) -> Result<()> {
    let jobs = FileJobStore::new(data_dir.join("jobs"))?;
    let job = jobs
        .get(&job_id)?
        .ok_or_else(|| anyhow::anyhow!("job {job_id} not found"))?;
    let outcome = job.outcome.as_ref();
    let response = serde_json::json!({
        "jobId": job.job_id,
        "status": job.status,
        "layerId": job.layer_id,
        "featureCount": outcome.map(|o| o.feature_count),
        "publishedTileCount": outcome.map(|o| o.published_tile_count),
        "boundingBox": outcome.map(|o| o.bounding_box.to_vec()),
        "completedAt": outcome.map(|o| o.completed_at.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        "error": job.error,
        "errorCode": job.error_code,
        "failedStage": job.failed_stage,
        "errorClass": job.error_class,
        "replayEligible": job.replay_eligible,
        "replayCount": job.replay_count,
    });
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}

/// Replays a failed, quarantined job (Recommendation 3 US-03) under the
/// Sequence 1 US-05 guardrails, Sequence 3 eligibility/limit checks, and
/// audit trail.
#[allow(clippy::too_many_arguments)]
fn replay(
    data_dir: PathBuf,
    tenant: String,
    job_id: String,
    assume_wgs84: bool,
    requested_by: String,
    reason: String,
    create_new_version: bool,
) -> Result<()> {
    let deps = build_deps(&data_dir)?;
    let outcome = vtile_pipeline::replay_job(
        &deps,
        &data_dir,
        &tenant,
        &job_id,
        &ReplayOptions {
            assume_wgs84,
            requested_by,
            reason,
            create_new_version,
        },
    )?;
    match outcome {
        ReplayOutcome::Executed(outcome) => {
            println!(
                "{}",
                serde_json::json!({
                    "jobId": job_id,
                    "status": "COMPLETED",
                    "featureCount": outcome.feature_count,
                    "tileCount": outcome.tile_count,
                    "tileVersion": outcome.tile_version,
                    "bbox": outcome.bbox.to_vec(),
                    "warnings": outcome.warnings,
                })
            );
        }
        ReplayOutcome::NoOp { reason } => {
            println!(
                "{}",
                serde_json::json!({
                    "jobId": job_id,
                    "status": "REPLAY_NO_OP",
                    "reason": reason,
                })
            );
        }
    }
    Ok(())
}

/// Lists dead-lettered jobs (Sequence 3 US-01/US-06), optionally scoped to
/// one tenant.
fn dlq_list(data_dir: PathBuf, tenant: Option<String>) -> Result<()> {
    use vtile_pipeline::DlqStore;
    let store = FileDlqStore::new(data_dir.join("dlq"));
    let records = match &tenant {
        Some(t) => store.list_tenant(t)?,
        None => {
            // No tenant filter: walk every tenant directory under dlq/.
            let mut all = Vec::new();
            let root = data_dir.join("dlq");
            if let Ok(entries) = std::fs::read_dir(&root) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        all.extend(store.list_tenant(&name)?);
                    }
                }
            }
            all
        }
    };
    println!("{}", serde_json::to_string_pretty(&records)?);
    Ok(())
}

/// Rolls back a layer to a previously published tile version (Sequence 2
/// US-AP-05) without reprocessing. Emits `vector.tile.version.rolled_back`
/// and appends an audit record (US-AP-06).
#[allow(clippy::too_many_arguments)]
fn rollback(
    data_dir: PathBuf,
    tenant: String,
    layer: String,
    target_version: String,
    reason: String,
    requested_by: String,
) -> Result<()> {
    if reason.trim().is_empty() {
        anyhow::bail!("--reason is required for rollback (auditability)");
    }
    let tiles_root = data_dir.join("tiles").join(&tenant).join(&layer);
    let manifests_root = data_dir.join("manifests").join(&tenant).join(&layer);
    let emitter = LoggingEventEmitter;
    let record = vtile_pipeline::rollback_layer_version(
        &tenant,
        &layer,
        &tiles_root,
        &manifests_root,
        &target_version,
        &reason,
        &requested_by,
        &emitter,
    )?;
    println!("{}", serde_json::to_string_pretty(&record)?);
    Ok(())
}

fn inspect_tile(path: PathBuf) -> Result<()> {
    let bytes = fs::read(&path)
        .with_context(|| format!("failed to read tile {}", path.display()))?;
    // gzip magic: 0x1f 0x8b.
    let decoded = if bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
        vtile_core::mvt::decode::decode_gzipped_tile(&bytes)?
    } else {
        vtile_core::mvt::decode::decode_tile(&bytes)?
    };
    let summary: Vec<serde_json::Value> = decoded
        .layers
        .iter()
        .map(|l| {
            serde_json::json!({
                "name": l.name,
                "version": l.version,
                "extent": l.extent,
                "features": l.feature_count,
                "keys": l.keys,
                "values": l.value_count,
                "geometryCommands": l.geometry_command_count,
                "featureIds": l.feature_ids,
                "geomTypes": l.geom_types,
            })
        })
        .collect();
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
