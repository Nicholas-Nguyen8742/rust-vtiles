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
use vtile_pipeline::job::{new_job_id, run_job, JobPaths, RunJobInput};
use vtile_pipeline::store::{FileJobStore, FileLayerCatalog};
use vtile_pipeline::JobDeps;

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
    }
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
    let tiles_root = data_dir.join("tiles").join(&tenant).join(&layer);
    let manifests_root = data_dir.join("manifests").join(&tenant).join(&layer);

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

    let deps = JobDeps {
        jobs: Arc::new(FileJobStore::new(data_dir.join("jobs"))?),
        catalog: Arc::new(FileLayerCatalog::new(data_dir.join("catalog").join("layers.json"))?),
        events: Arc::new(LoggingEventEmitter),
    };
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
        paths: JobPaths {
            staging_root,
            tiles_root,
            manifests_root,
        },
    };
    let outcome = run_job(&input, &deps)?;

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
            "tilesRoot": input.paths.tiles_root.join(&outcome.tile_version),
        })
    );
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
