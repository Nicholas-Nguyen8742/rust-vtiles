//! Local filesystem tile sink: `{root}/{z}/{x}/{y}.pbf`.
//!
//! Used for dev/staging runs and tests; mirrors the S3 key layout of TRD §6
//! (`tiles/{tenantId}/{layerId}/{version}/{z}/{x}/{y}.pbf`) with the
//! tenant/layer/version prefix supplied as `root`.

use std::fs;
use std::path::{Path, PathBuf};

use vtile_core::error::{Result as TileResult, TileError};
use vtile_core::sink::{TileObjectMeta, TileSink};
use vtile_core::tilemath::TileId;

#[derive(Debug)]
pub struct LocalTileSink {
    root: PathBuf,
}

impl LocalTileSink {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_zxy_layout() {
        let dir = std::env::temp_dir().join(format!("vtile-test-{}", std::process::id()));
        let sink = LocalTileSink::new(&dir);
        let tile = TileId::new(10, 301, 384);
        sink.write_tile(&tile, b"data", &TileObjectMeta::default())
            .unwrap();
        assert_eq!(
            tile_path(&dir, &tile),
            dir.join("10/301/384.pbf")
        );
        assert_eq!(fs::read(tile_path(&dir, &tile)).unwrap(), b"data");
        let _ = fs::remove_dir_all(&dir);
    }
}
