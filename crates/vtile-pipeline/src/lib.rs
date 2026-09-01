//! `vtile-pipeline` — job orchestration (TRD §10 Step Functions states).
//!
//! This crate wires `vtile-ingest` (normalize) and `vtile-core` (tile
//! generation) into an executable job:
//!
//! ```text
//! Validate Upload → Detect Format → Unpack → Validate CRS/Geometry
//! → Normalize → Clean Properties → Generate Tiles → Publish
//! → Write Manifest → Update Catalog → Emit Event
//! ```
//!
//! In production AWS this logic runs in the ECS Fargate tile processor
//! (TRD §11 Decision 2); Step Functions invokes each subcommand of the
//! `vtile` binary. Locally the same code runs against the filesystem.

pub mod error;
pub mod events;
pub mod job;
pub mod manifest;
pub mod quarantine;
pub mod replay;
pub mod sink_local;
#[cfg(feature = "aws")]
pub mod sink_s3;
pub mod store;

pub use error::{PipelineError, PipelineResult};
pub use job::{run_job, JobDeps, RunJobInput};
pub use manifest::TileManifest;
pub use quarantine::{ErrorReport, FileQuarantineStore, QuarantineStore};
pub use replay::{replay_job, ReplayOptions};
