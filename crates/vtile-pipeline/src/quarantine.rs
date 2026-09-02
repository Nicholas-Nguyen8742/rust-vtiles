//! Quarantine for failed uploads (Recommendation 3 US-03).
//!
//! When a job fails during validation or normalization, the raw source bytes
//! are retained alongside a machine-readable error report so operators can
//! inspect the exact payload that failed and replay it (`vtile replay`)
//! without asking the client to re-upload. Layout mirrors the TRD §6 prefix
//! convention:
//!
//! ```text
//! quarantine/{tenantId}/{jobId}/
//!   input.bin          — the original upload bytes
//!   error-report.json  — ErrorReport (code, message, failed stage, ...)
//! ```
//!
//! Only *ingest* failures are quarantined: they are the ones where the
//! source data itself is the problem. Tile/store failures are
//! infrastructural and replaying the same bytes adds nothing.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use vtile_core::model::{JobRecord, SourceFormat, ZoomRange};

use crate::error::{PipelineError, PipelineResult};

/// File name of the quarantined source payload.
pub const INPUT_FILE_NAME: &str = "input.bin";
/// File name of the error report.
pub const REPORT_FILE_NAME: &str = "error-report.json";

/// Machine-readable failure report stored next to the quarantined input
/// (Recommendation 3 US-02/US-03 + Sequence 3 US-02: the report carries the
/// classified error, replay eligibility, and remediation guidance so source
/// problems can be diagnosed without log archaeology).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorReport {
    pub job_id: String,
    pub tenant_id: String,
    pub layer_id: String,
    pub source_format: SourceFormat,
    pub source_uri: String,
    pub requested_zoom_range: ZoomRange,
    /// Taxonomy code, e.g. `MISSING_SHAPEFILE_COMPONENTS` (docs/ERRORS.md).
    pub error_code: String,
    pub error_message: String,
    /// Workflow stage where the job stopped, e.g. `NORMALIZING`.
    pub failed_stage: String,
    /// Sequence 3 US-03: `TRANSIENT` / `PERMANENT_VALIDATION` /
    /// `PERMISSION_DENIED` / `MANUAL_REVIEW` (docs/RECOVERY.md).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,
    /// Sequence 3 US-03: whether `vtile replay` is allowed for this failure.
    #[serde(default)]
    pub replay_eligible: bool,
    /// Sequence 3 US-02: operator-facing remediation guidance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remediation: Option<String>,
    /// Location of the quarantined source bytes (Sequence 3 US-02
    /// `quarantineUri`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantine_uri: Option<String>,
    pub quarantined_at: DateTime<Utc>,
}

impl ErrorReport {
    pub fn from_job(
        job: &JobRecord,
        error_code: &str,
        error_message: &str,
        failed_stage: &str,
    ) -> Self {
        Self {
            job_id: job.job_id.clone(),
            tenant_id: job.tenant_id.clone(),
            layer_id: job.layer_id.clone(),
            source_format: job.source_format,
            source_uri: job.source_uri.clone(),
            requested_zoom_range: job.requested_zoom_range,
            error_code: error_code.to_string(),
            error_message: error_message.to_string(),
            failed_stage: failed_stage.to_string(),
            error_class: Some(crate::recovery::classify_code(error_code).as_str().to_string()),
            replay_eligible: crate::recovery::replay_eligible(Some(error_code)),
            remediation: Some(crate::recovery::remediation_for(error_code).to_string()),
            quarantine_uri: None,
            quarantined_at: Utc::now(),
        }
    }
}

/// A quarantined job: error report plus the raw source bytes.
#[derive(Debug)]
pub struct QuarantinedJob {
    pub report: ErrorReport,
    pub source_bytes: Vec<u8>,
}

/// Quarantine persistence port.
///
/// Production maps onto the `quarantine/` S3 prefix with the same
/// object layout; the filesystem implementation below is the local/dev
/// equivalent (TRD §11 keeps every AWS dependency behind a trait).
pub trait QuarantineStore: Send + Sync {
    /// Persists the failed upload and its error report; returns the
    /// quarantine directory/prefix.
    fn quarantine(
        &self,
        job: &JobRecord,
        source_bytes: &[u8],
        report: &ErrorReport,
    ) -> PipelineResult<PathBuf>;

    /// Loads a quarantined job, if present.
    fn load(&self, tenant_id: &str, job_id: &str) -> PipelineResult<Option<QuarantinedJob>>;
}

/// Filesystem quarantine rooted at `quarantine/` under the data dir.
pub struct FileQuarantineStore {
    root: PathBuf,
}

impl FileQuarantineStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn dir_for(&self, tenant_id: &str, job_id: &str) -> PathBuf {
        self.root.join(tenant_id).join(job_id)
    }
}

impl QuarantineStore for FileQuarantineStore {
    fn quarantine(
        &self,
        job: &JobRecord,
        source_bytes: &[u8],
        report: &ErrorReport,
    ) -> PipelineResult<PathBuf> {
        let dir = self.dir_for(&job.tenant_id, &job.job_id);
        fs::create_dir_all(&dir)?;
        fs::write(dir.join(INPUT_FILE_NAME), source_bytes)?;
        // Enrich the report with the quarantine location (Sequence 3 US-02
        // `quarantineUri`), then write atomically (tmp + rename) so readers
        // never observe a partial JSON document.
        let mut report = report.clone();
        report.quarantine_uri = Some(dir.join(INPUT_FILE_NAME).display().to_string());
        let report_path = dir.join(REPORT_FILE_NAME);
        let tmp = report_path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_string_pretty(&report)?)?;
        fs::rename(&tmp, report_path)?;
        Ok(dir)
    }

    fn load(&self, tenant_id: &str, job_id: &str) -> PipelineResult<Option<QuarantinedJob>> {
        let dir = self.dir_for(tenant_id, job_id);
        let input_path = dir.join(INPUT_FILE_NAME);
        let report_path = dir.join(REPORT_FILE_NAME);
        if !input_path.exists() || !report_path.exists() {
            return Ok(None);
        }
        let source_bytes = fs::read(input_path)?;
        let report: ErrorReport = serde_json::from_str(&fs::read_to_string(report_path)?)
            .map_err(|e| PipelineError::Job(format!("corrupt quarantine report: {e}")))?;
        Ok(Some(QuarantinedJob {
            report,
            source_bytes,
        }))
    }
}
