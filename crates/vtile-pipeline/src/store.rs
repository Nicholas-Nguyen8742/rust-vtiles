//! Job and layer stores.
//!
//! TRD §11 stores these in DynamoDB; this crate defines the port (trait) and
//! ships filesystem implementations so the entire pipeline runs locally.
//! The DynamoDB implementation is a thin adapter over these traits
//! (`jobs` table: PK jobId; `layers` table: PK layerId, GSI tenantId).

use std::fs;
use std::path::{Path, PathBuf};

use vtile_core::model::{JobRecord, JobStatus, LayerMetadata};

use crate::error::{PipelineError, PipelineResult};

/// Job persistence port.
pub trait JobStore: Send + Sync {
    fn create(&self, job: JobRecord) -> PipelineResult<()>;
    fn update_status(
        &self,
        job_id: &str,
        status: JobStatus,
        error: Option<String>,
    ) -> PipelineResult<()>;
    /// Replaces the full record (used when the outcome summary is attached).
    fn upsert(&self, job: JobRecord) -> PipelineResult<()>;
    fn get(&self, job_id: &str) -> PipelineResult<Option<JobRecord>>;
}

/// Layer catalog port (TRD §7 layer metadata).
pub trait LayerCatalog: Send + Sync {
    fn upsert(&self, layer: LayerMetadata) -> PipelineResult<()>;
    fn get(&self, layer_id: &str) -> PipelineResult<Option<LayerMetadata>>;
    fn list(&self) -> PipelineResult<Vec<LayerMetadata>>;
}

/// Writes one JSON file per job under a directory; atomic via tmp+rename.
pub struct FileJobStore {
    dir: PathBuf,
}

impl FileJobStore {
    pub fn new(dir: impl Into<PathBuf>) -> PipelineResult<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path(&self, job_id: &str) -> PathBuf {
        self.dir.join(format!("{job_id}.json"))
    }
}

fn atomic_write_json(path: &Path, value: &impl serde::Serialize) -> PipelineResult<()> {
    let json = serde_json::to_string_pretty(value)?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, json)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

impl JobStore for FileJobStore {
    fn create(&self, job: JobRecord) -> PipelineResult<()> {
        atomic_write_json(&self.path(&job.job_id), &job)
    }

    fn update_status(
        &self,
        job_id: &str,
        status: JobStatus,
        error: Option<String>,
    ) -> PipelineResult<()> {
        let Some(mut job) = self.get(job_id)? else {
            return Err(PipelineError::Store(format!("job {job_id} not found")));
        };
        job.status = status;
        job.error = error;
        job.updated_at = chrono::Utc::now();
        atomic_write_json(&self.path(job_id), &job)
    }

    fn upsert(&self, job: JobRecord) -> PipelineResult<()> {
        atomic_write_json(&self.path(&job.job_id), &job)
    }

    fn get(&self, job_id: &str) -> PipelineResult<Option<JobRecord>> {
        let path = self.path(job_id);
        if !path.exists() {
            return Ok(None);
        }
        let json = fs::read_to_string(path)?;
        Ok(Some(serde_json::from_str(&json)?))
    }
}

/// Single-file catalog (JSON array). Suitable for MVP layer counts; DynamoDB
/// replaces it for the 10K-layer target (TRD §14).
pub struct FileLayerCatalog {
    path: PathBuf,
}

impl FileLayerCatalog {
    pub fn new(path: impl Into<PathBuf>) -> PipelineResult<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self { path })
    }

    fn load(&self) -> PipelineResult<Vec<LayerMetadata>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let json = fs::read_to_string(&self.path)?;
        Ok(serde_json::from_str(&json)?)
    }
}

impl LayerCatalog for FileLayerCatalog {
    fn upsert(&self, layer: LayerMetadata) -> PipelineResult<()> {
        let mut layers = self.load()?;
        if let Some(existing) = layers.iter_mut().find(|l| l.layer_id == layer.layer_id) {
            *existing = layer;
        } else {
            layers.push(layer);
        }
        atomic_write_json(&self.path, &layers)
    }

    fn get(&self, layer_id: &str) -> PipelineResult<Option<LayerMetadata>> {
        Ok(self.load()?.into_iter().find(|l| l.layer_id == layer_id))
    }

    fn list(&self) -> PipelineResult<Vec<LayerMetadata>> {
        self.load()
    }
}
