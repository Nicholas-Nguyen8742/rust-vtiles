//! Minimal Protocol Buffers wire-format writer scoped to the MVT v2 schema.
//!
//! MVT tiles are `vector_tile.Tile` messages defined by the Mapbox Vector Tile
//! specification v2 (proto2 syntax). The schema is tiny and frozen, so we
//! encode the wire format directly instead of generating code:
//!
//! ```proto
//! message Tile {
//!   message Value {
//!     optional string string_value = 1;
//!     optional float  float_value  = 2;
//!     optional double double_value = 3;
//!     optional int64  int_value    = 4;
//!     optional uint64 uint_value   = 5;
//!     optional sint64 sint_value   = 6;
//!     optional bool   bool_value   = 7;
//!   }
//!   message Feature {
//!     optional uint64 id       = 1;
//!     repeated uint32 tags     = 2 [packed = true];
//!     optional GeomType type   = 3;
//!     repeated uint32 geometry = 4 [packed = true];
//!   }
//!   message Layer {
//!     required uint32 version  = 15;
//!     required string name     = 1;
//!     repeated Feature features = 2;
//!     repeated string keys     = 3;
//!     repeated Value values    = 4;
//!     optional uint32 extent   = 5;
//!   }
//!   repeated Layer layers = 3;
//! }
//! ```

/// Wire type 0: base-128 varint.
pub const WIRE_VARINT: u8 = 0;
/// Wire type 1: fixed 64-bit.
pub const WIRE_64BIT: u8 = 1;
/// Wire type 2: length-delimited.
pub const WIRE_LEN: u8 = 2;
/// Wire type 5: fixed 32-bit.
pub const WIRE_32BIT: u8 = 5;

/// Append-only protobuf buffer writer.
#[derive(Debug, Default)]
pub struct PbfWriter {
    buf: Vec<u8>,
}

impl PbfWriter {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Writes an unsigned base-128 varint.
    #[inline]
    pub fn write_varint(&mut self, mut v: u64) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                self.buf.push(byte);
                return;
            }
            self.buf.push(byte | 0x80);
        }
    }

    /// ZigZag encoding for signed 32-bit values (sint32).
    #[inline]
    pub fn zigzag32(v: i32) -> u32 {
        ((v << 1) ^ (v >> 31)) as u32
    }

    /// ZigZag encoding for signed 64-bit values (sint64).
    #[inline]
    pub fn zigzag64(v: i64) -> u64 {
        ((v << 1) ^ (v >> 63)) as u64
    }

    /// Writes a field key `(field_number << 3) | wire_type`.
    #[inline]
    pub fn write_tag(&mut self, field: u32, wire: u8) {
        self.write_varint(((field << 3) | wire as u32) as u64);
    }

    pub fn varint_field(&mut self, field: u32, v: u64) {
        self.write_tag(field, WIRE_VARINT);
        self.write_varint(v);
    }

    pub fn bool_field(&mut self, field: u32, v: bool) {
        self.varint_field(field, v as u64);
    }

    /// `sint64` field (ZigZag varint).
    pub fn sint_field(&mut self, field: u32, v: i64) {
        self.varint_field(field, Self::zigzag64(v));
    }

    /// `int64` field. Negative values encode as 64-bit two's complement,
    /// matching proto2 semantics used by the MVT `int_value` field.
    pub fn int64_field(&mut self, field: u32, v: i64) {
        self.varint_field(field, v as u64);
    }

    pub fn double_field(&mut self, field: u32, v: f64) {
        self.write_tag(field, WIRE_64BIT);
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn float_field(&mut self, field: u32, v: f32) {
        self.write_tag(field, WIRE_32BIT);
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn bytes_field(&mut self, field: u32, data: &[u8]) {
        self.write_tag(field, WIRE_LEN);
        self.write_varint(data.len() as u64);
        self.buf.extend_from_slice(data);
    }

    pub fn string_field(&mut self, field: u32, s: &str) {
        self.bytes_field(field, s.as_bytes());
    }

    /// Packed repeated varint field (used for `Feature.tags` and
    /// `Feature.geometry`).
    pub fn packed_varint_field(&mut self, field: u32, values: &[u32]) {
        let mut tmp = PbfWriter::with_capacity(values.len() * 2);
        for v in values {
            tmp.write_varint(u64::from(*v));
        }
        self.bytes_field(field, tmp.as_bytes());
    }

    /// Writes a nested message field: the closure fills a scratch buffer that
    /// is then written length-prefixed.
    pub fn message_field(&mut self, field: u32, build: impl FnOnce(&mut PbfWriter)) {
        let mut tmp = PbfWriter::new();
        build(&mut tmp);
        self.bytes_field(field, tmp.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_encoding_matches_known_vectors() {
        let mut w = PbfWriter::new();
        w.write_varint(0);
        w.write_varint(1);
        w.write_varint(127);
        w.write_varint(128);
        w.write_varint(300);
        assert_eq!(w.into_bytes(), vec![0x00, 0x01, 0x7f, 0x80, 0x01, 0xac, 0x02]);
    }

    #[test]
    fn zigzag_known_vectors() {
        assert_eq!(PbfWriter::zigzag32(0), 0);
        assert_eq!(PbfWriter::zigzag32(-1), 1);
        assert_eq!(PbfWriter::zigzag32(1), 2);
        assert_eq!(PbfWriter::zigzag32(-2), 3);
        assert_eq!(PbfWriter::zigzag64(-1), 1);
    }

    #[test]
    fn tag_layout() {
        // Field 1, wire type 2 => key byte 0x0a (the classic "name" field).
        let mut w = PbfWriter::new();
        w.string_field(1, "");
        assert_eq!(w.into_bytes(), vec![0x0a, 0x00]);
    }
}
