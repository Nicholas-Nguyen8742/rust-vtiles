//! Local filesystem tile sink: `{root}/{z}/{x}/{y}.pbf`.
//!
//! Used for dev/staging runs and tests; mirrors the S3 key layout of TRD §6
//! (`tiles/{tenantId}/{layerId}/versions/{version}/{z}/{x}/{y}.pbf`) with the
//! tenant/layer/version prefix supplied as `root`.
//!
//! Sequence 2 US-AP-02: while writing, the sink records a per-tile entry
//! (relative path, SHA-256, size) so the candidate manifest can carry an
//! aggregate checksum and completeness verification can re-derive it from
//! disk.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use vtile_core::error::{Result as TileResult, TileError};
use vtile_core::sink::{TileObjectMeta, TileSink};
use vtile_core::tilemath::TileId;

use crate::publish::TileEntry;

#[derive(Debug)]
pub struct LocalTileSink {
    root: PathBuf,
    /// Per-tile integrity entries accumulated during generation (US-AP-02).
    entries: Mutex<Vec<TileEntry>>,
}

impl LocalTileSink {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            entries: Mutex::new(Vec::new()),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Snapshot of the integrity entries for every tile written so far.
    pub fn entries(&self) -> Vec<TileEntry> {
        self.entries.lock().expect("sink poisoned").clone()
    }
}

/// Canonical on-disk path for a tile under a root prefix.
pub fn tile_path(root: &Path, tile: &TileId) -> PathBuf {
    root.join(format!("{}/{}/{}.pbf", tile.z, tile.x, tile.y))
}

impl TileSink for LocalTileSink {
    fn write_tile(&self, tile: &TileId, gzipped: &[u8], _meta: &TileObjectMeta) -> TileResult<()> {
        let path = tile_path(&self.root, tile);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, gzipped).map_err(TileError::Io)?;
        // Integrity entry for the candidate manifest (US-AP-01/02).
        use sha2::Digest;
        let digest = hex::encode(sha2::Sha256::digest(gzipped));
        self.entries.lock().expect("sink poisoned").push(TileEntry {
            rel_path: format!("{}/{}/{}.pbf", tile.z, tile.x, tile.y),
            sha256: format!("sha256:{digest}"),
            len: gzipped.len() as u64,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_zxy_layout_and_records_entries() {
        let dir = std::env::temp_dir().join(format!("vtile-test-{}", std::process::id()));
        let sink = LocalTileSink::new(&dir);
        let tile = TileId::new(10, 301, 384);
        sink.write_tile(&tile, b"data", &TileObjectMeta::default())
            .unwrap();
        assert_eq!(tile_path(&dir, &tile), dir.join("10/301/384.pbf"));
        assert_eq!(fs::read(tile_path(&dir, &tile)).unwrap(), b"data");
        let entries = sink.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].rel_path, "10/301/384.pbf");
        assert_eq!(entries[0].len, 4);
        assert!(entries[0].sha256.starts_with("sha256:"));
        let _ = fs::remove_dir_all(&dir);
    }
}
