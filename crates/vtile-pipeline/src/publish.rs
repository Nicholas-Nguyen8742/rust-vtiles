//! Atomic publishing (Sequence 2 epic).
//!
//! **Key architectural rule:** the live tile path is never the direct write
//! target of tile generation.
//!
//! ```text
//! generate candidate version (versions/{tileVersion}/, status=CANDIDATE)
//!   → validate completeness (count, zero-byte, aggregate SHA-256)
//!   → promote authoritative version pointer (conditional update)
//!   → clients resolve the current version
//!   → serve tiles from the immutable version path
//! ```
//!
//! CRE users therefore always see either the previous known-good layer
//! version or the new fully published one — never a partial tileset.
//!
//! Local ↔ production mapping:
//! * versioned S3 output paths ↔ `tiles/{tenant}/{layer}/versions/{v}/`
//! * DynamoDB authoritative layer record ↔ `publication.json` next to the
//!   manifest (conditional promote = read-check-write on `currentTileVersion`)
//! * candidate manifest + `_SUCCESS` marker ↔ same layout, filesystem
//! * audit trail (DynamoDB/data lake) ↔ append-only `audit.jsonl`

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use vtile_core::model::Bbox;

use crate::error::{PipelineError, PipelineResult};
use crate::events::{EventEmitter, PipelineEvent};
use crate::job::new_event_id;
use crate::manifest::{TileManifest, MANIFEST_SCHEMA_VERSION};

/// Candidate manifest schema version (US-AP-02).
pub const CANDIDATE_MANIFEST_SCHEMA_VERSION: u32 = 1;
/// Subdirectory inside a version root holding publish metadata.
pub const MANIFEST_SUBDIR: &str = "_manifest";
/// Candidate manifest file name.
pub const CANDIDATE_FILE: &str = "candidate.json";
/// Optional completion marker written after a candidate passes validation.
/// It is never the sole source of publication truth (US-AP-02).
pub const SUCCESS_MARKER: &str = "_SUCCESS";
/// Default publish actor for pipeline-driven promotions (US-AP-06).
pub const PIPELINE_ACTOR: &str = "pipeline:vtile-publisher";

// ── Paths ───────────────────────────────────────────────────────────────────

/// Immutable version directory under a layer's tile root:
/// `tiles/{tenantId}/{layerId}/versions/{tileVersion}/` (US-AP-01).
pub fn version_root(tiles_root: &Path, tile_version: &str) -> PathBuf {
    tiles_root.join("versions").join(tile_version)
}

/// Candidate manifest path for a version.
pub fn candidate_path(tiles_root: &Path, tile_version: &str) -> PathBuf {
    version_root(tiles_root, tile_version)
        .join(MANIFEST_SUBDIR)
        .join(CANDIDATE_FILE)
}

/// Client URL template for a layer's versions (US-AP-04 read pattern).
pub fn tile_url_template_for(tenant_id: &str, layer_id: &str) -> String {
    format!("/tiles/{tenant_id}/{layer_id}/versions/{{tileVersion}}/{{z}}/{{x}}/{{y}}.pbf")
}

// ── Integrity entries + aggregate checksum (US-AP-02) ───────────────────────

/// One tile object as recorded during generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TileEntry {
    /// Path relative to the version root, e.g. `14/4824/6157.pbf`.
    pub rel_path: String,
    /// SHA-256 of the gzipped tile bytes (`sha256:` + hex).
    pub sha256: String,
    /// Gzipped size in bytes.
    pub len: u64,
}

/// Deterministic aggregate checksum over a tile set: SHA-256 of the sorted,
/// canonicalized per-tile records. Re-derived from disk during validation.
pub fn aggregate_checksum(entries: &[TileEntry]) -> String {
    let mut sorted: Vec<&TileEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    for entry in sorted {
        hasher.update(entry.rel_path.as_bytes());
        hasher.update(b"\0");
        hasher.update(entry.sha256.as_bytes());
        hasher.update(b"\0");
        hasher.update(entry.len.to_le_bytes());
        hasher.update(b"\n");
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

// ── Candidate manifest (US-AP-01/02) ────────────────────────────────────────

/// Candidate publish manifest written at the end of tile generation,
/// **before** the version can be promoted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateManifest {
    pub schema_version: u32,
    pub tenant_id: String,
    pub layer_id: String,
    pub tile_version: String,
    /// `CANDIDATE` until promoted; the authoritative pointer lives in the
    /// layer registry, not here.
    pub status: String,
    pub source_job_id: String,
    pub source_format: String,
    pub crs: String,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub feature_count: u64,
    pub tile_count: u64,
    pub total_gzip_bytes: u64,
    pub bounding_box: Bbox,
    pub generated_at: DateTime<Utc>,
    pub checksum_algorithm: String,
    /// SHA-256 over the canonicalized tile entries; validation re-derives it
    /// from disk and refuses promotion on mismatch.
    pub aggregate_checksum: String,
    /// Tile root relative to the data dir, e.g.
    /// `tiles/tenant_acme/nyc_parcels/versions/2026-06-17T16-00-00Z/`.
    pub tile_root: String,
}

/// Writes `versions/{v}/_manifest/candidate.json` atomically (tmp + rename).
pub fn write_candidate_manifest(
    tiles_root: &Path,
    candidate: &CandidateManifest,
) -> PipelineResult<PathBuf> {
    let path = candidate_path(tiles_root, &candidate.tile_version);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_string_pretty(candidate)?)?;
    fs::rename(&tmp, &path)?;
    Ok(path)
}

/// Reads the candidate manifest for a version, if present.
pub fn read_candidate_manifest(
    tiles_root: &Path,
    tile_version: &str,
) -> PipelineResult<Option<CandidateManifest>> {
    let path = candidate_path(tiles_root, tile_version);
    if !path.exists() {
        return Ok(None);
    }
    let manifest = serde_json::from_str(&fs::read_to_string(&path)?)
        .map_err(|e| PipelineError::PublishValidation(format!("malformed candidate manifest: {e}")))?;
    Ok(Some(manifest))
}

/// Completeness verification gate (US-AP-02). Walks the version directory and
/// refuses to pass when:
/// * the candidate manifest is missing or malformed,
/// * the on-disk tile count differs from the manifest,
/// * any `.pbf` is zero-byte,
/// * any expected zoom level has no tiles,
/// * the re-derived aggregate checksum differs from the manifest.
pub fn verify_candidate(tiles_root: &Path, tile_version: &str) -> PipelineResult<CandidateManifest> {
    let manifest = read_candidate_manifest(tiles_root, tile_version)?
        .ok_or_else(|| {
            PipelineError::PublishValidation(format!(
                "candidate manifest missing for version {tile_version}"
            ))
        })?;

    let vroot = version_root(tiles_root, tile_version);
    let mut entries: Vec<TileEntry> = Vec::new();
    walk_tiles(&vroot, &vroot, &mut entries)?;

    // Zero-byte tiles are never intentional: empty regions are absent tiles,
    // and the encoder always produces a non-trivial gzip stream.
    if let Some(zero) = entries.iter().find(|e| e.len == 0) {
        return Err(PipelineError::PublishValidation(format!(
            "zero-byte tile detected: {}",
            zero.rel_path
        )));
    }

    if entries.len() as u64 != manifest.tile_count {
        return Err(PipelineError::PublishValidation(format!(
            "tile count mismatch for version {tile_version}: manifest expects {}, found {} on disk",
            manifest.tile_count,
            entries.len()
        )));
    }

    for z in manifest.min_zoom..=manifest.max_zoom {
        let prefix = format!("{z}/");
        if !entries.iter().any(|e| e.rel_path.starts_with(&prefix)) {
            return Err(PipelineError::PublishValidation(format!(
                "zoom level {z} has no tiles in version {tile_version}"
            )));
        }
    }

    let derived = aggregate_checksum(&entries);
    if derived != manifest.aggregate_checksum {
        return Err(PipelineError::PublishValidation(format!(
            "aggregate checksum mismatch for version {tile_version}: manifest {}, derived {derived}",
            manifest.aggregate_checksum
        )));
    }

    Ok(manifest)
}

/// Recursively collects `.pbf` entries (skipping `_manifest/`), hashing each
/// file. Relative paths use `/` separators for canonical checksums.
fn walk_tiles(root: &Path, dir: &Path, out: &mut Vec<TileEntry>) -> PipelineResult<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|n| n.to_str()) == Some(MANIFEST_SUBDIR) {
                continue;
            }
            walk_tiles(root, &path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("pbf") {
            let bytes = fs::read(&path)?;
            use sha2::Digest;
            let digest = hex::encode(sha2::Sha256::digest(&bytes));
            let rel_path = path
                .strip_prefix(root)
                .map_err(|e| PipelineError::PublishValidation(e.to_string()))?
                .to_string_lossy()
                .replace('\\', "/");
            out.push(TileEntry {
                rel_path,
                sha256: format!("sha256:{digest}"),
                len: bytes.len() as u64,
            });
        }
    }
    Ok(())
}

// ── Authoritative layer version registry (US-AP-03) ─────────────────────────

/// Publication status of the authoritative layer record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublishStatus {
    Published,
    RolledBack,
}

/// Authoritative layer version record — the local analog of the DynamoDB
/// record that owns `currentTileVersion`. Stored as `publication.json` next
/// to `manifest.json`/`latest.json` in `manifests/{tenant}/{layer}/`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LayerVersionRecord {
    pub layer_id: String,
    pub tenant_id: String,
    pub current_tile_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_tile_version: Option<String>,
    pub publish_status: PublishStatus,
    pub updated_at: DateTime<Utc>,
    pub published_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolled_back_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolled_back_by: Option<String>,
}

/// Layer-scoped registry rooted at the layer's manifests directory.
pub struct FileLayerRegistry {
    dir: PathBuf,
}

impl FileLayerRegistry {
    pub fn new(layer_manifests_dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: layer_manifests_dir.into(),
        }
    }

    fn path(&self) -> PathBuf {
        self.dir.join("publication.json")
    }

    pub fn get(&self) -> PipelineResult<Option<LayerVersionRecord>> {
        let path = self.path();
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&fs::read_to_string(path)?)?))
    }

    /// Conditional promotion (US-AP-03): succeeds only when the stored
    /// `currentTileVersion` equals `expected_previous` (`None` for a layer's
    /// first publish). Losing a concurrent promotion race yields
    /// [`PipelineError::PromotionConflict`] and leaves the previous version
    /// active — the local analog of a DynamoDB `ConditionExpression`.
    pub fn promote(
        &self,
        tenant_id: &str,
        layer_id: &str,
        expected_previous: Option<&str>,
        new_version: &str,
        actor: &str,
    ) -> PipelineResult<LayerVersionRecord> {
        let existing = self.get()?;
        let actual_previous = existing.as_ref().map(|r| r.current_tile_version.as_str());
        if actual_previous != expected_previous {
            PublishMetrics::global().inc(PublishMetric::PromotionConflicts);
            return Err(PipelineError::PromotionConflict(format!(
                "layer {layer_id}: current version is {actual_previous:?}, expected {expected_previous:?}"
            )));
        }
        let record = LayerVersionRecord {
            layer_id: layer_id.to_string(),
            tenant_id: tenant_id.to_string(),
            current_tile_version: new_version.to_string(),
            previous_tile_version: existing.map(|r| r.current_tile_version),
            publish_status: PublishStatus::Published,
            updated_at: Utc::now(),
            published_by: actor.to_string(),
            rollback_reason: None,
            rolled_back_at: None,
            rolled_back_by: None,
        };
        self.write(&record)?;
        Ok(record)
    }

    /// Rollback pointer update (US-AP-05): repoints `currentTileVersion` to a
    /// previously published version without regeneration. Idempotent — a
    /// rollback to the current version returns the record unchanged.
    pub fn rollback(
        &self,
        target_version: &str,
        reason: &str,
        actor: &str,
    ) -> PipelineResult<LayerVersionRecord> {
        let Some(mut record) = self.get()? else {
            return Err(PipelineError::RollbackFailed(
                "layer has no published version".to_string(),
            ));
        };
        if record.current_tile_version == target_version {
            return Ok(record); // idempotent no-op
        }
        record.previous_tile_version = Some(record.current_tile_version.clone());
        record.current_tile_version = target_version.to_string();
        record.publish_status = PublishStatus::RolledBack;
        record.rollback_reason = Some(reason.to_string());
        record.rolled_back_at = Some(Utc::now());
        record.rolled_back_by = Some(actor.to_string());
        record.updated_at = Utc::now();
        self.write(&record)?;
        Ok(record)
    }

    fn write(&self, record: &LayerVersionRecord) -> PipelineResult<()> {
        fs::create_dir_all(&self.dir)?;
        let path = self.path();
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_string_pretty(record)?)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }
}

// ── Publish audit trail (US-AP-06) ──────────────────────────────────────────

/// Audited publish action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuditAction {
    Publish,
    Rollback,
}

/// Immutable append-only audit record (SOC2-aligned traceability: every
/// version can be traced back to its originating job and actor).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishAuditRecord {
    pub audit_id: String,
    pub tenant_id: String,
    pub layer_id: String,
    pub action: AuditAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_tile_version: Option<String>,
    pub to_tile_version: String,
    pub actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

/// Append-only JSONL audit log in the layer's manifests directory.
pub struct FileAuditLog {
    dir: PathBuf,
}

impl FileAuditLog {
    pub fn new(layer_manifests_dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: layer_manifests_dir.into(),
        }
    }

    fn path(&self) -> PathBuf {
        self.dir.join("audit.jsonl")
    }

    pub fn append(&self, record: &PublishAuditRecord) -> PipelineResult<()> {
        fs::create_dir_all(&self.dir)?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path())?;
        writeln!(file, "{}", serde_json::to_string(record)?)?;
        Ok(())
    }

    pub fn entries(&self) -> PipelineResult<Vec<PublishAuditRecord>> {
        let path = self.path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for line in fs::read_to_string(path)?.lines() {
            if line.trim().is_empty() {
                continue;
            }
            out.push(serde_json::from_str(line)?);
        }
        Ok(out)
    }
}

fn new_audit_id() -> String {
    format!("audit_{}", uuid::Uuid::new_v4().as_simple())
}

// ── Orchestration: promote + rollback ───────────────────────────────────────

/// Builds the compatibility `TileManifest` served to readers from a
/// validated candidate.
pub fn tile_manifest_from_candidate(candidate: &CandidateManifest) -> TileManifest {
    TileManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        tenant_id: candidate.tenant_id.clone(),
        layer_id: candidate.layer_id.clone(),
        tile_version: candidate.tile_version.clone(),
        min_zoom: candidate.min_zoom,
        max_zoom: candidate.max_zoom,
        tile_count: candidate.tile_count,
        total_gzip_bytes: candidate.total_gzip_bytes,
        bounding_box: candidate.bounding_box,
        generated_at: candidate.generated_at,
        tile_url_template: Some(tile_url_template_for(
            &candidate.tenant_id,
            &candidate.layer_id,
        )),
    }
}

/// Writes `manifest.json` + `latest.json` (atomic) for the promoted/rolled-back
/// version.
fn write_live_pointers(manifests_root: &Path, manifest: &TileManifest) -> PipelineResult<()> {
    fs::create_dir_all(manifests_root)?;
    let json = manifest.to_json()?;
    // manifest.json: plain write (the registry record is the authority).
    fs::write(manifests_root.join("manifest.json"), &json)?;
    // latest.json: atomic swap so readers never observe a partial document.
    let latest = manifests_root.join("latest.json");
    let tmp = latest.with_extension("json.tmp");
    fs::write(&tmp, &json)?;
    fs::rename(&tmp, latest)?;
    Ok(())
}

/// Atomic promotion (Sequence 2 US-AP-02/03/06):
/// 1. verify the candidate version on disk (completeness gate),
/// 2. conditionally move the authoritative layer pointer,
/// 3. rewrite the compatibility manifests, drop the `_SUCCESS` marker,
/// 4. append the audit record and emit `vector.tile.version.promoted`.
///
/// Any failure leaves the previous published version active.
pub fn promote_layer_version(
    tiles_root: &Path,
    manifests_root: &Path,
    candidate: &CandidateManifest,
    expected_previous: Option<&str>,
    actor: &str,
    events: &dyn EventEmitter,
) -> PipelineResult<LayerVersionRecord> {
    // 1. Completeness gate (US-AP-02).
    verify_candidate(tiles_root, &candidate.tile_version)?;

    // 2. Authoritative conditional promotion (US-AP-03).
    let registry = FileLayerRegistry::new(manifests_root);
    let record = registry.promote(
        &candidate.tenant_id,
        &candidate.layer_id,
        expected_previous,
        &candidate.tile_version,
        actor,
    )?;

    // 3. Compatibility pointers + completion marker.
    write_live_pointers(manifests_root, &tile_manifest_from_candidate(candidate))?;
    fs::write(
        version_root(tiles_root, &candidate.tile_version).join(SUCCESS_MARKER),
        record.updated_at.to_rfc3339(),
    )?;

    // 4. Audit + observability (US-AP-06).
    FileAuditLog::new(manifests_root).append(&PublishAuditRecord {
        audit_id: new_audit_id(),
        tenant_id: candidate.tenant_id.clone(),
        layer_id: candidate.layer_id.clone(),
        action: AuditAction::Publish,
        from_tile_version: expected_previous.map(str::to_string),
        to_tile_version: candidate.tile_version.clone(),
        actor: actor.to_string(),
        source_job_id: Some(candidate.source_job_id.clone()),
        reason: None,
        occurred_at: Utc::now(),
    })?;
    events.emit(PipelineEvent::VectorTileVersionPromoted {
        event_id: new_event_id(),
        tenant_id: candidate.tenant_id.clone(),
        layer_id: candidate.layer_id.clone(),
        job_id: candidate.source_job_id.clone(),
        from_tile_version: expected_previous.map(str::to_string),
        to_tile_version: candidate.tile_version.clone(),
        actor: actor.to_string(),
        occurred_at: Utc::now(),
    });
    PublishMetrics::global().inc(PublishMetric::VersionsPublished);
    Ok(record)
}

/// Rollback (Sequence 2 US-AP-05): repoint the authoritative layer record —
/// and the compatibility manifests — to a previously published version. No
/// reprocessing occurs. Idempotent: rolling back to the current version is a
/// no-op returning the existing record.
pub fn rollback_layer_version(
    tenant_id: &str,
    layer_id: &str,
    tiles_root: &Path,
    manifests_root: &Path,
    target_version: &str,
    reason: &str,
    actor: &str,
    events: &dyn EventEmitter,
) -> PipelineResult<LayerVersionRecord> {
    let registry = FileLayerRegistry::new(manifests_root);
    let current = registry.get()?.ok_or_else(|| {
        PipelineError::RollbackFailed(format!("layer {layer_id} has no published version"))
    })?;
    if current.current_tile_version == target_version {
        return Ok(current); // idempotent
    }

    // The target must be a known-good, retained version: it needs its
    // candidate manifest (the zoom/bbox/checksum source of truth).
    let target_manifest = read_candidate_manifest(tiles_root, target_version)?
        .ok_or_else(|| {
            PipelineError::RollbackFailed(format!(
                "target version {target_version} not found under {}",
                version_root(tiles_root, target_version).display()
            ))
        })?;

    let record = registry.rollback(target_version, reason, actor)?;
    write_live_pointers(manifests_root, &tile_manifest_from_candidate(&target_manifest))?;

    FileAuditLog::new(manifests_root).append(&PublishAuditRecord {
        audit_id: new_audit_id(),
        tenant_id: tenant_id.to_string(),
        layer_id: layer_id.to_string(),
        action: AuditAction::Rollback,
        from_tile_version: Some(current.current_tile_version.clone()),
        to_tile_version: target_version.to_string(),
        actor: actor.to_string(),
        source_job_id: None,
        reason: Some(reason.to_string()),
        occurred_at: Utc::now(),
    })?;
    events.emit(PipelineEvent::VectorTileVersionRolledBack {
        event_id: new_event_id(),
        tenant_id: tenant_id.to_string(),
        layer_id: layer_id.to_string(),
        from_tile_version: current.current_tile_version.clone(),
        to_tile_version: target_version.to_string(),
        actor: actor.to_string(),
        reason: reason.to_string(),
        occurred_at: Utc::now(),
    });
    PublishMetrics::global().inc(PublishMetric::RollbacksCompleted);
    Ok(record)
}

// ── Telemetry (US-AP-06) ────────────────────────────────────────────────────

/// Publish-lifecycle counters, merged into `GET /internal/metrics`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishMetric {
    VersionsPublished,
    PromotionConflicts,
    PublishValidationFailures,
    RollbacksCompleted,
}

impl PublishMetric {
    pub const ALL: [PublishMetric; 4] = [
        PublishMetric::VersionsPublished,
        PublishMetric::PromotionConflicts,
        PublishMetric::PublishValidationFailures,
        PublishMetric::RollbacksCompleted,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            PublishMetric::VersionsPublished => "versions_published",
            PublishMetric::PromotionConflicts => "promotion_conflicts",
            PublishMetric::PublishValidationFailures => "publish_validation_failures",
            PublishMetric::RollbacksCompleted => "rollbacks_completed",
        }
    }
}

#[derive(Debug, Default)]
pub struct PublishMetrics {
    versions_published: AtomicU64,
    promotion_conflicts: AtomicU64,
    publish_validation_failures: AtomicU64,
    rollbacks_completed: AtomicU64,
}

impl PublishMetrics {
    pub fn global() -> &'static PublishMetrics {
        static METRICS: OnceLock<PublishMetrics> = OnceLock::new();
        METRICS.get_or_init(PublishMetrics::default)
    }

    pub fn inc(&self, metric: PublishMetric) {
        self.counter(metric).fetch_add(1, Ordering::Relaxed);
    }

    pub fn count(&self, metric: PublishMetric) -> u64 {
        self.counter(metric).load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for metric in PublishMetric::ALL {
            map.insert(metric.as_str().to_string(), self.count(metric).into());
        }
        serde_json::Value::Object(map)
    }

    fn counter(&self, metric: PublishMetric) -> &AtomicU64 {
        match metric {
            PublishMetric::VersionsPublished => &self.versions_published,
            PublishMetric::PromotionConflicts => &self.promotion_conflicts,
            PublishMetric::PublishValidationFailures => &self.publish_validation_failures,
            PublishMetric::RollbacksCompleted => &self.rollbacks_completed,
        }
    }
}
