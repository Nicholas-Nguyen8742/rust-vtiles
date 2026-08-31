//! `vtile-api` binary — local development server for the TRD §8 API.
//!
//! In production these handlers sit behind API Gateway with S3 presigned
//! uploads (TRD §2); locally the same contracts run against the filesystem
//! so the full upload → process → serve loop can be exercised end to end:
//!
//! ```text
//! vtile-api --data-dir data --port 8080
//! curl -X POST localhost:8080/api/v1/ingest/uploads -d @request.json
//! curl -X PUT  localhost:8080/api/v1/ingest/uploads/{jobId}/content --data-binary @parcels.geojson
//! curl localhost:8080/api/v1/jobs/{jobId}
//! curl localhost:8080/tiles/{tenant}/{layer}/{z}/{x}/{y}.pbf -o tile.pbf
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use vtile_api::state::AppState;
use vtile_api::build_router;
use vtile_ingest::validate::DEFAULT_MAX_UPLOAD_BYTES;
use vtile_pipeline::events::LoggingEventEmitter;
use vtile_pipeline::store::{FileJobStore, FileLayerCatalog};

#[derive(Parser, Debug)]
#[command(
    name = "vtile-api",
    version,
    about = "Vector-tile ingestion API (TRD §8), local filesystem edition"
)]
struct Args {
    /// Data root; `staging/`, `tiles/`, `manifests/`, `jobs/`, `catalog.json`
    /// live underneath (local mirror of the TRD §6 S3 layout).
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,

    /// Interface to bind.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to bind.
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// Static bearer token required on all endpoints. Omit to disable auth
    /// (local development only; production uses OAuth2/OIDC per TRD §13).
    #[arg(long)]
    auth_token: Option<String>,

    /// Max upload size in bytes (TRD §10 validation table).
    #[arg(long, default_value_t = DEFAULT_MAX_UPLOAD_BYTES)]
    max_upload_bytes: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("vtile_api=info,vtile_pipeline=info")),
        )
        .init();

    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir)?;

    let state = Arc::new(AppState {
        jobs: Arc::new(FileJobStore::new(args.data_dir.join("jobs"))?),
        catalog: Arc::new(FileLayerCatalog::new(args.data_dir.join("catalog.json"))?),
        events: Arc::new(LoggingEventEmitter),
        data_dir: args.data_dir,
        auth_token: args.auth_token,
        max_upload_bytes: args.max_upload_bytes,
        // TRD §8.1: presigned URLs expire in 15 minutes.
        upload_expires_secs: 900,
    });

    let router = build_router(state);
    let bind_addr = (args.host.as_str(), args.port);
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(
        host = %args.host,
        port = args.port,
        "vtile-api listening"
    );
    axum::serve(listener, router).await?;
    Ok(())
}
