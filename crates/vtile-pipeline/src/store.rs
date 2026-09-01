//! Job and layer stores.
//!
//! TRD §11 stores these in DynamoDB; this crate defines the port (trait) and
//! ships filesystem implementations so the entire pipeline runs locally.
//! The DynamoDB implementation is a thin adapter over these traits
//! (`jobs` table: PK jobId, GSI on `idempotencyKey`; `layers` table: PK
//! layerId, GSI tenantId).
//!
//! Sequence 1 (idempotency epic) semantics and their DynamoDB analogs:
//! * `create` is conditional — `attribute_not_exists(jobId)`.
//! * `transition` is a conditional update on current status + lease
//!   ownership, bumping `stateVersion` (optimistic concurrency).
//! * `acquire_lease` implements the single-active-worker rule; expired
//!   leases may be taken over (crashed-worker recovery).

use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};

use vtile_core::model::{JobRecord, JobStatus, LayerMetadata};

use crate::error::{PipelineError, PipelineResult};
use crate::idempotency::{IdempotencyMetrics, Metric};

/// Job persistence port.
pub trait JobStore: Send + Sync {
    /// Conditional create (Sequence 1 US-01): fails with
    /// [`PipelineError::JobAlreadyExists`] when a record with the same
    /// `jobId` is already registered.
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

    /// Resolves the job registered under an upload idempotency key
    /// (Sequence 1 US-01). File scan locally; a DynamoDB GSI in production.
    fn find_by_idempotency_key(
        &self,
        idempotency_key: &str,
    ) -> PipelineResult<Option<JobRecord>>;

    /// Conditional state transition (Sequence 1 US-04): the stored status
    /// must equal `expected`, the edge must be legal
    /// (`JobStatus::can_transition_to`), and — when a `lease_token` is
    /// supplied — it must match the active lease. Bumps `stateVersion`.
    fn transition(
        &self,
        job_id: &str,
        lease_token: Option<&str>,
        expected: JobStatus,
        next: JobStatus,
    ) -> PipelineResult<JobRecord>;

    /// Acquires the worker lease when it is absent or expired
    /// (Sequence 1 US-04). Rejects runnable-state violations and active
    /// foreign leases.
    fn acquire_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        lease_secs: u64,
    ) -> PipelineResult<Lease>;

    /// Records an acknowledged duplicate event (Sequence 1 US-03).
    fn note_duplicate_event(&self, job_id: &str) -> PipelineResult<()>;
}

/// An active worker lease (Sequence 1 US-04).
#[derive(Debug, Clone)]
pub struct Lease {
    pub lease_token: String,
    pub worker_id: String,
    pub lease_expires_at: DateTime<Utc>,
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
        let path = self.path(&job.job_id);
        if path.exists() {
            return Err(PipelineError::JobAlreadyExists(job.job_id));
        }
        atomic_write_json(&path, &job)
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
        job.state_version += 1;
        job.updated_at = Utc::now();
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

    fn find_by_idempotency_key(
        &self,
        idempotency_key: &str,
    ) -> PipelineResult<Option<JobRecord>> {
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return Ok(None);
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Ok(json) = fs::read_to_string(&path) else {
                continue;
            };
            let Ok(job) = serde_json::from_str::<JobRecord>(&json) else {
                continue;
            };
            if job.idempotency_key.as_deref() == Some(idempotency_key) {
                return Ok(Some(job));
            }
        }
        Ok(None)
    }

    fn transition(
        &self,
        job_id: &str,
        lease_token: Option<&str>,
        expected: JobStatus,
        next: JobStatus,
    ) -> PipelineResult<JobRecord> {
        let Some(mut job) = self.get(job_id)? else {
            return Err(PipelineError::Store(format!("job {job_id} not found")));
        };
        if let (Some(expected_token), Some(stored_token)) =
            (lease_token, job.lease_token.as_deref())
        {
            if expected_token != stored_token {
                IdempotencyMetrics::global().inc(Metric::LeaseAcquisitionConflict);
                return Err(PipelineError::LeaseConflict(format!(
                    "job {job_id}: lease owned by {:?}, not by token {expected_token}",
                    job.locked_by
                )));
            }
        }
        if job.status != expected || !expected.can_transition_to(next) {
            IdempotencyMetrics::global().inc(Metric::StateTransitionConflict);
            return Err(PipelineError::StateConflict(format!(
                "job {job_id}: transition {} -> {} rejected (status is {})",
                expected.as_str(),
                next.as_str(),
                job.status.as_str()
            )));
        }
        job.status = next;
        job.state_version += 1;
        job.updated_at = Utc::now();
        atomic_write_json(&self.path(job_id), &job)?;
        Ok(job)
    }

    fn acquire_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        lease_secs: u64,
    ) -> PipelineResult<Lease> {
        let Some(mut job) = self.get(job_id)? else {
            return Err(PipelineError::Store(format!("job {job_id} not found")));
        };
        if !job.status.is_runnable() {
            return Err(PipelineError::Job(format!(
                "job {job_id} is not runnable (status {})",
                job.status.as_str()
            )));
        }
        if let Some(expires) = job.lease_expires_at {
            if job.lease_token.is_some() && expires > Utc::now() {
                IdempotencyMetrics::global().inc(Metric::LeaseAcquisitionConflict);
                return Err(PipelineError::LeaseConflict(format!(
                    "job {job_id} is locked by {:?} until {expires}",
                    job.locked_by
                )));
            }
            // Lease lapsed: crashed-worker takeover (Sequence 1 US-04).
            IdempotencyMetrics::global().inc(Metric::LeaseExpiredCount);
        }
        let lease = Lease {
            lease_token: format!("lease_{}", uuid::Uuid::new_v4().as_simple()),
            worker_id: worker_id.to_string(),
            lease_expires_at: Utc::now() + Duration::seconds(lease_secs as i64),
        };
        job.lease_token = Some(lease.lease_token.clone());
        job.locked_by = Some(lease.worker_id.clone());
        job.lease_expires_at = Some(lease.lease_expires_at);
        job.state_version += 1;
        job.updated_at = Utc::now();
        atomic_write_json(&self.path(job_id), &job)?;
        IdempotencyMetrics::global().inc(Metric::LeaseAcquisitionSuccess);
        Ok(lease)
    }

    fn note_duplicate_event(&self, job_id: &str) -> PipelineResult<()> {
        let Some(mut job) = self.get(job_id)? else {
            return Err(PipelineError::Store(format!("job {job_id} not found")));
        };
        job.duplicate_event_count += 1;
        job.updated_at = Utc::now();
        atomic_write_json(&self.path(job_id), &job)
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
