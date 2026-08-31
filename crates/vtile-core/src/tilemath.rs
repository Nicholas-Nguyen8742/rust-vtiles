//! Web Mercator (EPSG:3857) tile mathematics.
//!
//! Implements the slippy-map / Mapbox XYZ scheme used by the pipeline
//! (TRD §5: "Tile coordinate scheme: Web Mercator XYZ").
//!
//! World space is normalized to the unit square `[0,1] x [0,1]` where
//! `(0,0)` is the north-west corner of the world and `(1,1)` the south-east
//! corner (y grows southward, matching tile row ordering).

use serde::{Deserialize, Serialize};

/// Maximum latitude representable in Web Mercator: `atan(sinh(π))` in degrees.
pub const MAX_MERCATOR_LAT: f64 = 85.051_129_017_834_63;

/// A position in normalized world space (both axes in `[0, 1]`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorldPos {
    /// 0.0 at longitude -180, 1.0 at longitude +180.
    pub x: f64,
    /// 0.0 at ~85.05°N, 1.0 at ~85.05°S (Mercator-projected).
    pub y: f64,
}

impl WorldPos {
    pub fn new(x: f64, y: f64) -> Self {
        Self {
            x: x.clamp(0.0, 1.0),
            y: y.clamp(0.0, 1.0),
        }
    }
}

/// Converts a WGS84 lon/lat pair (EPSG:4326, degrees) into world space.
///
/// Latitudes beyond the Mercator limit are clamped (TRD: features outside the
/// valid range are flagged upstream; clamping here keeps tiling total).
pub fn lonlat_to_world(lon: f64, lat: f64) -> WorldPos {
    let x = (lon + 180.0) / 360.0;
    let lat = lat.clamp(-MAX_MERCATOR_LAT, MAX_MERCATOR_LAT);
    let lat_rad = lat.to_radians();
    // Spherical Mercator northing, normalized to [0, 1].
    let y = (1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / std::f64::consts::PI) / 2.0;
    WorldPos::new(x, y)
}

/// Inverse of [`lonlat_to_world`] (used for tile bbox reporting).
pub fn world_to_lonlat(world: &WorldPos) -> (f64, f64) {
    let lon = world.x * 360.0 - 180.0;
    let n = std::f64::consts::PI * (1.0 - 2.0 * world.y);
    let lat = (n.sinh().atan()).to_degrees();
    (lon, lat)
}

/// A tile address in the XYZ scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TileId {
    pub z: u8,
    pub x: u32,
    pub y: u32,
}

impl TileId {
    pub fn new(z: u8, x: u32, y: u32) -> Self {
        Self { z, x, y }
    }

    /// Number of tiles per axis at this zoom.
    pub fn axis_count(&self) -> u32 {
        1u32 << self.z
    }

    /// Canonical `z/x/y.pbf` object name fragment.
    pub fn path(&self) -> String {
        format!("{}/{}/{}.pbf", self.z, self.x, self.y)
    }
}

/// Returns the world-space bounding box of a tile.
///
/// `min` is the north-west corner, `max` the south-east corner
/// (remember y grows southward in world space).
pub fn tile_world_bounds(tile: &TileId) -> (WorldPos, WorldPos) {
    let n = (1u64 << tile.z) as f64;
    let size = 1.0 / n;
    (
        WorldPos::new(tile.x as f64 * size, tile.y as f64 * size),
        WorldPos::new((tile.x as f64 + 1.0) * size, (tile.y as f64 + 1.0) * size),
    )
}

/// Transforms world-space positions into integer tile-local coordinates
/// for a specific tile, extent and buffer.
///
/// Tile-local space: `(0,0)` is the tile's north-west corner, `(extent,extent)`
/// its south-east corner. Coordinates may fall outside `[0, extent]` for
/// buffered geometry; the MVT spec explicitly allows this.
#[derive(Debug, Clone, Copy)]
pub struct TileTransform {
    pub tile: TileId,
    pub extent: u32,
    /// Buffer in extent units (e.g. 256 with extent 4096 = 1/16 tile).
    pub buffer: u32,
    n: f64,
}

impl TileTransform {
    pub fn new(tile: TileId, extent: u32, buffer: u32) -> Self {
        Self {
            tile,
            extent,
            buffer,
            n: (1u64 << tile.z) as f64,
        }
    }

    /// Converts a world position to tile-local integer coordinates.
    ///
    /// The computation stays in `f64` until the final rounding step so that no
    /// intermediate truncation occurs (TRD §14 "no silent coordinate
    /// truncation"). Relative coordinates are bounded by ±(extent+buffer), so
    /// `i32` cannot overflow for any zoom ≤ 30.
    pub fn to_tile_xy(&self, world: &WorldPos) -> (i32, i32) {
        let extent = self.extent as f64;
        let fx = (world.x * self.n - self.tile.x as f64) * extent;
        let fy = (world.y * self.n - self.tile.y as f64) * extent;
        (fx.round() as i32, fy.round() as i32)
    }

    /// True if the position lies inside the tile expanded by the buffer.
    pub fn contains_buffered(&self, world: &WorldPos) -> bool {
        let (x, y) = self.to_tile_xy(world);
        let b = self.buffer as i32;
        x >= -b && x <= self.extent as i32 + b && y >= -b && y <= self.extent as i32 + b
    }
}

/// Computes the inclusive range of tile indices whose extents intersect a
/// world-space bbox expanded by `buffer` (extent units).
///
/// Returns `None` when the bbox falls entirely outside the grid after
/// clamping (should not happen for clamped world coordinates, but guards
/// against degenerate inputs).
pub fn tile_range_for_world_bbox(
    min: &WorldPos,
    max: &WorldPos,
    z: u8,
    buffer: u32,
    extent: u32,
) -> Option<(u32, u32, u32, u32)> {
    let n = (1u64 << z) as f64;
    let max_index = (1u64 << z) as i64 - 1;
    // Buffer expressed in tile units (buffer / extent of one tile).
    let buf_tiles = buffer as f64 / extent as f64;

    let x0 = (min.x * n - buf_tiles).floor() as i64;
    let x1 = (max.x * n + buf_tiles).floor() as i64;
    let y0 = (min.y * n - buf_tiles).floor() as i64;
    let y1 = (max.y * n + buf_tiles).floor() as i64;

    let x0 = x0.clamp(0, max_index) as u32;
    let x1 = x1.clamp(0, max_index) as u32;
    let y0 = y0.clamp(0, max_index) as u32;
    let y1 = y1.clamp(0, max_index) as u32;

    if x1 < x0 || y1 < y0 {
        None
    } else {
        Some((x0, x1, y0, y1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn world_origin_maps_to_center() {
        let w = lonlat_to_world(0.0, 0.0);
        assert!((w.x - 0.5).abs() < 1e-12);
        assert!((w.y - 0.5).abs() < 1e-12);
    }

    #[test]
    fn world_corners() {
        let nw = lonlat_to_world(-180.0, 85.051_129);
        assert!(nw.x.abs() < 1e-9);
        assert!(nw.y.abs() < 1e-6);
        let se = lonlat_to_world(180.0, -85.051_129);
        assert!((se.x - 1.0).abs() < 1e-9);
        assert!((se.y - 1.0).abs() < 1e-6);
    }

    #[test]
    fn lonlat_roundtrip() {
        for &(lon, lat) in &[(-73.9855, 40.7580), (2.3522, 48.8566), (139.6917, 35.6895)] {
            let w = lonlat_to_world(lon, lat);
            let (lon2, lat2) = world_to_lonlat(&w);
            assert!((lon2 - lon).abs() < 1e-9, "lon mismatch for ({lon}, {lat})");
            assert!((lat2 - lat).abs() < 1e-9, "lat mismatch for ({lon}, {lat})");
        }
    }

    #[test]
    fn tile_addressing_at_zoom_1() {
        // NYC is in the north-west quadrant of the world.
        let w = lonlat_to_world(-73.9855, 40.7580);
        let n = 2.0_f64;
        assert_eq!((w.x * n).floor() as u32, 0);
        assert_eq!((w.y * n).floor() as u32, 0);
    }

    #[test]
    fn transform_maps_tile_nw_corner_to_origin() {
        let tile = TileId::new(10, 301, 384);
        let (nw, _) = tile_world_bounds(&tile);
        let t = TileTransform::new(tile, 4096, 256);
        let (x, y) = t.to_tile_xy(&nw);
        assert_eq!((x, y), (0, 0));
    }

    #[test]
    fn transform_maps_tile_se_corner_to_extent() {
        let tile = TileId::new(10, 301, 384);
        let (_, se) = tile_world_bounds(&tile);
        let t = TileTransform::new(tile, 4096, 256);
        let (x, y) = t.to_tile_xy(&se);
        assert!((x - 4096).abs() <= 1);
        assert!((y - 4096).abs() <= 1);
    }

    #[test]
    fn range_includes_buffer_neighbours() {
        // A point exactly on the tile border must include the neighbour tile
        // because of the buffer.
        let w = WorldPos::new(0.5, 0.5);
        let (x0, x1, y0, y1) = tile_range_for_world_bbox(&w, &w, 1, 256, 4096).unwrap();
        // At z=1 the point sits on the corner shared by all four tiles.
        assert_eq!((x0, x1, y0, y1), (0, 1, 0, 1));
    }

    #[test]
    fn range_clamps_to_grid() {
        let min = WorldPos::new(0.0, 0.0);
        let max = WorldPos::new(1.0, 1.0);
        let (x0, x1, y0, y1) = tile_range_for_world_bbox(&min, &max, 2, 256, 4096).unwrap();
        assert_eq!((x0, x1, y0, y1), (0, 3, 0, 3));
    }
}
