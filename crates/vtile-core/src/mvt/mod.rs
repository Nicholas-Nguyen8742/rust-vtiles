//! Mapbox Vector Tile v2 encoding.
//!
//! * [`pbf`] — minimal protobuf wire-format writer.
//! * [`MvtValue`] — typed feature attribute value.
//! * [`MvtLayer`] — layer builder with key/value interning.
//! * [`GeometryCommands`] — MVT command-sequence encoder.
//! * [`decode`] — small decoder used for tests and QA tooling.

pub mod decode;
pub mod pbf;

use std::collections::HashMap;

use pbf::PbfWriter;

/// MVT spec v2: layers must declare version 2.
pub const MVT_VERSION: u64 = 2;

/// Geometry kinds (proto enum `Tile.GeomType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum GeomKind {
    Unknown = 0,
    Point = 1,
    LineString = 2,
    Polygon = 3,
}

/// MVT geometry command ids.
pub const CMD_MOVE_TO: u32 = 1;
pub const CMD_LINE_TO: u32 = 2;
pub const CMD_CLOSE_PATH: u32 = 7;

#[inline]
fn command_int(id: u32, count: u32) -> u32 {
    (id & 0x7) | (count << 3)
}

#[inline]
fn zigzag_param(v: i32) -> u32 {
    PbfWriter::zigzag32(v)
}

/// Builds an MVT command sequence (`Feature.geometry`).
///
/// The cursor starts at `(0, 0)` per feature and all parameters are
/// delta-encoded, exactly as required by the spec.
#[derive(Debug, Default)]
pub struct GeometryCommands {
    cmds: Vec<u32>,
    cursor: (i32, i32),
}

impl GeometryCommands {
    pub fn new() -> Self {
        Self::default()
    }

    fn push_param(&mut self, to: (i32, i32)) {
        let dx = to.0 - self.cursor.0;
        let dy = to.1 - self.cursor.1;
        self.cursor = to;
        self.cmds.push(zigzag_param(dx));
        self.cmds.push(zigzag_param(dy));
    }

    /// One `MoveTo` command covering `points` (count ≥ 1). A multi-point
    /// geometry is a single `MoveTo` with count > 1.
    pub fn move_to(&mut self, points: &[(i32, i32)]) {
        if points.is_empty() {
            return;
        }
        self.cmds.push(command_int(CMD_MOVE_TO, points.len() as u32));
        for &p in points {
            self.push_param(p);
        }
    }

    /// One `LineTo` command covering `points`.
    pub fn line_to(&mut self, points: &[(i32, i32)]) {
        if points.is_empty() {
            return;
        }
        self.cmds.push(command_int(CMD_LINE_TO, points.len() as u32));
        for &p in points {
            self.push_param(p);
        }
    }

    pub fn close_path(&mut self) {
        self.cmds.push(command_int(CMD_CLOSE_PATH, 1));
    }

    pub fn into_commands(self) -> Vec<u32> {
        self.cmds
    }

    pub fn is_empty(&self) -> bool {
        self.cmds.is_empty()
    }
}

/// Encodes a point or multipoint geometry.
pub fn encode_points(points: &[(i32, i32)]) -> Vec<u32> {
    let mut g = GeometryCommands::new();
    g.move_to(points);
    g.into_commands()
}

/// Encodes (multi)linestrings. Parts with fewer than 2 points are skipped.
pub fn encode_lines(lines: &[Vec<(i32, i32)>]) -> Vec<u32> {
    let mut g = GeometryCommands::new();
    for line in lines {
        if line.len() < 2 {
            continue;
        }
        g.move_to(&line[..1]);
        g.line_to(&line[1..]);
    }
    g.into_commands()
}

/// A polygon ring ready for encoding: tile coordinates WITHOUT the repeated
/// closing vertex, plus whether it is an exterior ring.
#[derive(Debug, Clone)]
pub struct Ring {
    pub points: Vec<(i32, i32)>,
    pub exterior: bool,
}

/// Twice the signed area of a ring (shoelace formula) in tile coordinates.
///
/// Tile space has y growing downward, so a ring that is clockwise on screen
/// (the MVT exterior convention) yields a POSITIVE value.
pub fn signed_area2(points: &[(i32, i32)]) -> i64 {
    let n = points.len();
    let mut area: i64 = 0;
    for i in 0..n {
        let (x1, y1) = points[i];
        let (x2, y2) = points[(i + 1) % n];
        area = area.wrapping_add(i64::from(x1) * i64::from(y2) - i64::from(x2) * i64::from(y1));
    }
    area
}

/// Encodes polygon rings, enforcing the MVT winding convention:
/// exterior rings must have positive signed area, interior (hole) rings
/// negative, in tile coordinates (y-down). Degenerate rings (< 3 distinct
/// points or zero area) are dropped.
pub fn encode_rings(rings: &[Ring]) -> Vec<u32> {
    let mut g = GeometryCommands::new();
    for ring in rings {
        if ring.points.len() < 3 {
            continue;
        }
        let area = signed_area2(&ring.points);
        if area == 0 {
            continue;
        }
        let positive = area > 0;
        if positive == ring.exterior {
            g.move_to(&ring.points[..1]);
            g.line_to(&ring.points[1..]);
        } else {
            // Wrong orientation for its role: reverse the ring.
            let mut pts = ring.points.clone();
            pts.reverse();
            g.move_to(&pts[..1]);
            g.line_to(&pts[1..]);
        }
        g.close_path();
    }
    g.into_commands()
}

/// Typed feature attribute values supported by MVT.
#[derive(Debug, Clone, PartialEq)]
pub enum MvtValue {
    String(String),
    Double(f64),
    Int(i64),
    Uint(u64),
    Bool(bool),
}

/// Dedup key for interned values (`f64` is not `Hash`, so compare bits).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ValueKey {
    String(String),
    DoubleBits(u64),
    Int(i64),
    Uint(u64),
    Bool(bool),
}

impl MvtValue {
    fn key(&self) -> ValueKey {
        match self {
            MvtValue::String(s) => ValueKey::String(s.clone()),
            MvtValue::Double(d) => ValueKey::DoubleBits(d.to_bits()),
            MvtValue::Int(i) => ValueKey::Int(*i),
            MvtValue::Uint(u) => ValueKey::Uint(*u),
            MvtValue::Bool(b) => ValueKey::Bool(*b),
        }
    }

    /// Serializes into `Tile.Value` fields.
    fn encode(&self, w: &mut PbfWriter) {
        match self {
            MvtValue::String(s) => w.string_field(1, s),
            MvtValue::Double(d) => w.double_field(3, *d),
            MvtValue::Int(i) => w.int64_field(4, *i),
            MvtValue::Uint(u) => w.varint_field(5, *u),
            MvtValue::Bool(b) => w.bool_field(7, *b),
        }
    }

    /// Rough serialized size, used for the per-feature payload budget.
    pub fn size_estimate(&self) -> usize {
        match self {
            MvtValue::String(s) => s.len(),
            MvtValue::Double(_) => 8,
            MvtValue::Int(_) | MvtValue::Uint(_) => 10,
            MvtValue::Bool(_) => 1,
        }
    }
}

/// A fully-encoded feature ready for layer assembly.
#[derive(Debug, Clone)]
pub struct EncodedFeature {
    pub id: Option<u64>,
    pub kind: GeomKind,
    pub geometry: Vec<u32>,
    /// Pairs of already-interned (key_index, value_index).
    pub tags: Vec<u32>,
}

/// Layer builder with key/value interning per the MVT spec.
///
/// Keys and values are deduplicated per layer; features reference them by
/// index through the `tags` array.
#[derive(Debug)]
pub struct MvtLayer {
    name: String,
    extent: u32,
    keys: Vec<String>,
    key_index: HashMap<String, u32>,
    values: Vec<MvtValue>,
    value_index: HashMap<ValueKey, u32>,
    features: Vec<EncodedFeature>,
}

impl MvtLayer {
    pub fn new(name: impl Into<String>, extent: u32) -> Self {
        Self {
            name: name.into(),
            extent,
            keys: Vec::new(),
            key_index: HashMap::new(),
            values: Vec::new(),
            value_index: HashMap::new(),
            features: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn feature_count(&self) -> usize {
        self.features.len()
    }

    pub fn is_empty(&self) -> bool {
        self.features.is_empty()
    }

    fn intern_key(&mut self, key: &str) -> u32 {
        if let Some(&idx) = self.key_index.get(key) {
            return idx;
        }
        let idx = self.keys.len() as u32;
        self.keys.push(key.to_string());
        self.key_index.insert(key.to_string(), idx);
        idx
    }

    fn intern_value(&mut self, value: &MvtValue) -> u32 {
        let key = value.key();
        if let Some(&idx) = self.value_index.get(&key) {
            return idx;
        }
        let idx = self.values.len() as u32;
        self.values.push(value.clone());
        self.value_index.insert(key, idx);
        idx
    }

    /// Adds a feature, interning its properties into keys/values.
    pub fn add_feature(
        &mut self,
        id: Option<u64>,
        kind: GeomKind,
        geometry: Vec<u32>,
        properties: &[(String, MvtValue)],
    ) {
        if geometry.is_empty() {
            return;
        }
        let mut tags = Vec::with_capacity(properties.len() * 2);
        for (k, v) in properties {
            let ki = self.intern_key(k);
            let vi = self.intern_value(v);
            tags.push(ki);
            tags.push(vi);
        }
        self.features.push(EncodedFeature {
            id,
            kind,
            geometry,
            tags,
        });
    }

    /// Estimated uncompressed size (for cheap pre-checks before encoding).
    pub fn approximate_bytes(&self) -> usize {
        let feat_bytes: usize = self
            .features
            .iter()
            .map(|f| f.geometry.len() * 4 + f.tags.len() * 2 + 16)
            .sum();
        let key_bytes: usize = self.keys.iter().map(|k| k.len() + 4).sum();
        let val_bytes: usize = self.values.iter().map(|v| v.size_estimate() + 4).sum();
        feat_bytes + key_bytes + val_bytes + self.name.len() + 32
    }

    /// Serializes the layer as a complete `Tile` message (single layer per
    /// tile in MVP; compositing is post-MVP per TRD §1 scope).
    pub fn to_tile_bytes(&self) -> Vec<u8> {
        let mut tile = PbfWriter::with_capacity(self.approximate_bytes());
        tile.message_field(3, |layer| self.encode_layer(layer));
        tile.into_bytes()
    }

    fn encode_layer(&self, layer: &mut PbfWriter) {
        // required string name = 1;
        layer.string_field(1, &self.name);
        // repeated Feature features = 2;
        for f in &self.features {
            layer.message_field(2, |feat| {
                if let Some(id) = f.id {
                    feat.varint_field(1, id);
                }
                if !f.tags.is_empty() {
                    feat.packed_varint_field(2, &f.tags);
                }
                feat.varint_field(3, f.kind as u64);
                feat.packed_varint_field(4, &f.geometry);
            });
        }
        // repeated string keys = 3;
        for k in &self.keys {
            layer.string_field(3, k);
        }
        // repeated Value values = 4;
        for v in &self.values {
            layer.message_field(4, |val| v.encode(val));
        }
        // optional uint32 extent = 5;
        layer.varint_field(5, u64::from(self.extent));
        // required uint32 version = 15;
        layer.varint_field(15, MVT_VERSION);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use decode::decode_tile;

    #[test]
    fn point_geometry_command_sequence() {
        // MoveTo(1) at (25, 17): command int = (1 & 7) | (1 << 3) = 9,
        // params zigzag(25)=50, zigzag(17)=34.
        let cmds = encode_points(&[(25, 17)]);
        assert_eq!(cmds, vec![9, 50, 34]);
    }

    #[test]
    fn linestring_command_sequence() {
        // From the MVT spec example: M(2 2) L(10 10)
        let cmds = encode_lines(&[vec![(2, 2), (10, 10)]]);
        // MoveTo 9, params 4,4 ; LineTo (1<<3)|2=10, params zigzag(8)=16,16
        assert_eq!(cmds, vec![9, 4, 4, 18, 16, 16]);
    }

    #[test]
    fn polygon_ring_is_closed_and_wound() {
        // Square given counter-clockwise in y-down space (negative area):
        // encoder must reverse it so the exterior ring has positive area.
        let ring = Ring {
            points: vec![(0, 0), (0, 10), (10, 10), (10, 0)],
            exterior: true,
        };
        let area_before = signed_area2(&ring.points);
        assert!(area_before < 0);
        let cmds = encode_rings(&[ring]);
        // MoveTo(1)=9 ; LineTo(2)=(2<<3)|2=18 ; ClosePath=15
        assert_eq!(cmds.len(), 1 + 2 + 1 + 4 + 1);
        assert_eq!(*cmds.last().unwrap(), 15);
    }

    #[test]
    fn degenerate_rings_dropped() {
        let ring = Ring {
            points: vec![(0, 0), (5, 5)],
            exterior: true,
        };
        assert!(encode_rings(&[ring]).is_empty());
        let collinear = Ring {
            points: vec![(0, 0), (5, 0), (10, 0)],
            exterior: true,
        };
        assert!(encode_rings(&[collinear]).is_empty());
    }

    #[test]
    fn layer_roundtrips_through_decoder() {
        let mut layer = MvtLayer::new("parcel_boundary", 4096);
        layer.add_feature(
            Some(42),
            GeomKind::Point,
            encode_points(&[(100, 200)]),
            &[
                ("parcelId".to_string(), MvtValue::String("NYC-1".into())),
                ("far".to_string(), MvtValue::Double(10.5)),
                ("floors".to_string(), MvtValue::Int(12)),
                ("active".to_string(), MvtValue::Bool(true)),
            ],
        );
        layer.add_feature(
            None,
            GeomKind::Point,
            encode_points(&[(300, 400)]),
            &[("parcelId".to_string(), MvtValue::String("NYC-2".into()))],
        );

        let bytes = layer.to_tile_bytes();
        let decoded = decode_tile(&bytes).expect("tile should decode");
        assert_eq!(decoded.layers.len(), 1);
        let l = &decoded.layers[0];
        assert_eq!(l.name, "parcel_boundary");
        assert_eq!(l.version, 2);
        assert_eq!(l.extent, 4096);
        assert_eq!(l.feature_count, 2);
        assert_eq!(l.keys, vec!["parcelId", "far", "floors", "active"]);
        // 4 unique values: "NYC-1", 10.5, 12, true, "NYC-2" => 5
        assert_eq!(l.value_count, 5);
        assert_eq!(l.feature_ids, vec![Some(42), None]);
        assert_eq!(l.geom_types, vec![1, 1]);
    }

    #[test]
    fn values_are_deduplicated() {
        let mut layer = MvtLayer::new("l", 4096);
        for i in 0..3 {
            layer.add_feature(
                Some(i),
                GeomKind::Point,
                encode_points(&[(i as i32, 0)]),
                &[("market".to_string(), MvtValue::String("NYC".into()))],
            );
        }
        let decoded = decode_tile(&layer.to_tile_bytes()).unwrap();
        assert_eq!(decoded.layers[0].value_count, 1);
        assert_eq!(decoded.layers[0].keys.len(), 1);
    }
}
