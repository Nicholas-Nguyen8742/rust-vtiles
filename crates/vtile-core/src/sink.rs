//! Tile output sinks.
//!
//! The engine streams tiles into a [`TileSink`] instead of buffering them in
//! memory: a 1M-feature dataset can produce 100k+ tiles, so backpressure and
//! constant memory matter (TRD §18 "Large Shapefiles exceed processing
//! memory").
//!
//! Implementations:
//! * [`MemoryTileSink`] — tests and small jobs.
//! * Local filesystem and S3 sinks live in `vtile-pipeline`.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::Result;
use crate::tilemath::TileId;

/// Object metadata written alongside each published tile (TRD §6).
#[derive(Debug, Clone, Default)]
pub struct TileObjectMeta {
    pub tenant_id: String,
    pub layer_id: String,
    pub tile_version: String,
    pub source_format: String,
    pub crs: String,
    pub min_zoom: u8,
    pub max_zoom: u8,
}

/// Receives generated tiles. Implementations must be safe to share across
/// worker threads (`generate_tiles` uses rayon).
pub trait TileSink: Sync {
    /// Writes one gzip-compressed MVT tile.
    fn write_tile(&self, tile: &TileId, gzipped: &[u8], meta: &TileObjectMeta) -> Result<()>;

    /// Called once after all tiles were written. Default is a no-op.
    fn finish(&self) -> Result<()> {
        Ok(())
    }
}

/// In-memory sink for tests and very small layers.
#[derive(Debug, Default)]
pub struct MemoryTileSink {
    pub tiles: Mutex<HashMap<TileId, Vec<u8>>>,
}

impl MemoryTileSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.tiles.lock().expect("sink poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, tile: &TileId) -> Option<Vec<u8>> {
        self.tiles.lock().expect("sink poisoned").get(tile).cloned()
    }
}

impl TileSink for MemoryTileSink {
    fn write_tile(&self, tile: &TileId, gzipped: &[u8], _meta: &TileObjectMeta) -> Result<()> {
        self.tiles
            .lock()
            .expect("sink poisoned")
            .insert(*tile, gzipped.to_vec());
        Ok(())
    }
}
