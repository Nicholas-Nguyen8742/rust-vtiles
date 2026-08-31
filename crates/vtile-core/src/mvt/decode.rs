//! Minimal MVT tile decoder.
//!
//! This is intentionally read-only and structural: it walks the protobuf wire
//! format and extracts layer/feature metadata without fully interpreting
//! geometry command streams. It powers unit tests and the QA tooling required
//! by the TRD release criteria ("Visual tile QA completed at zoom 10/12/14/16").

use crate::error::{Result, TileError};

use super::pbf::{WIRE_32BIT, WIRE_64BIT, WIRE_LEN, WIRE_VARINT};

/// Cursor over a byte slice for wire-format parsing.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn read_varint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        loop {
            if self.pos >= self.data.len() {
                return Err(TileError::Encoding("varint runs past end of buffer".into()));
            }
            let byte = self.data[self.pos];
            self.pos += 1;
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 64 {
                return Err(TileError::Encoding("varint longer than 64 bits".into()));
            }
        }
    }

    fn read_tag(&mut self) -> Result<(u32, u8)> {
        let key = self.read_varint()?;
        Ok(((key >> 3) as u32, (key & 0x7) as u8))
    }

    fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_varint()? as usize;
        if self.remaining() < len {
            return Err(TileError::Encoding(
                "length-delimited field exceeds buffer".into(),
            ));
        }
        let out = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(out)
    }

    fn skip(&mut self, wire: u8) -> Result<()> {
        match wire {
            WIRE_VARINT => {
                self.read_varint()?;
            }
            WIRE_64BIT => {
                if self.remaining() < 8 {
                    return Err(TileError::Encoding("truncated fixed64".into()));
                }
                self.pos += 8;
            }
            WIRE_LEN => {
                self.read_bytes()?;
            }
            WIRE_32BIT => {
                if self.remaining() < 4 {
                    return Err(TileError::Encoding("truncated fixed32".into()));
                }
                self.pos += 4;
            }
            other => {
                return Err(TileError::Encoding(format!("unsupported wire type {other}")));
            }
        }
        Ok(())
    }
}

/// A decoded layer summary.
#[derive(Debug, Default, Clone)]
pub struct DecodedLayer {
    pub name: String,
    pub version: u32,
    pub extent: u32,
    pub feature_count: usize,
    pub keys: Vec<String>,
    pub value_count: usize,
    pub feature_ids: Vec<Option<u64>>,
    /// Raw enum values of `Feature.type` per feature.
    pub geom_types: Vec<u32>,
    /// Total number of geometry command integers across features.
    pub geometry_command_count: usize,
}

/// A decoded tile summary.
#[derive(Debug, Default)]
pub struct DecodedTile {
    pub layers: Vec<DecodedLayer>,
}

fn decode_feature(data: &[u8], layer: &mut DecodedLayer) -> Result<()> {
    let mut r = Reader::new(data);
    let mut id: Option<u64> = None;
    let mut geom_type: u32 = 0;
    let mut cmd_count = 0usize;
    while r.remaining() > 0 {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_VARINT) => id = Some(r.read_varint()?),
            (3, WIRE_VARINT) => geom_type = r.read_varint()? as u32,
            (4, WIRE_LEN) => {
                let packed = r.read_bytes()?;
                let mut pr = Reader::new(packed);
                while pr.remaining() > 0 {
                    pr.read_varint()?;
                    cmd_count += 1;
                }
            }
            (_, w) => r.skip(w)?,
        }
    }
    layer.feature_count += 1;
    layer.feature_ids.push(id);
    layer.geom_types.push(geom_type);
    layer.geometry_command_count += cmd_count;
    Ok(())
}

fn decode_layer(data: &[u8]) -> Result<DecodedLayer> {
    let mut layer = DecodedLayer::default();
    let mut r = Reader::new(data);
    while r.remaining() > 0 {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_LEN) => {
                layer.name = String::from_utf8_lossy(r.read_bytes()?).into_owned();
            }
            (2, WIRE_LEN) => {
                let feature = r.read_bytes()?;
                decode_feature(feature, &mut layer)?;
            }
            (3, WIRE_LEN) => {
                layer.keys.push(String::from_utf8_lossy(r.read_bytes()?).into_owned());
            }
            (4, WIRE_LEN) => {
                r.read_bytes()?;
                layer.value_count += 1;
            }
            (5, WIRE_VARINT) => layer.extent = r.read_varint()? as u32,
            (15, WIRE_VARINT) => layer.version = r.read_varint()? as u32,
            (_, w) => r.skip(w)?,
        }
    }
    Ok(layer)
}

/// Decodes a tile's structure. `data` must be the raw MVT protobuf (not gzip).
pub fn decode_tile(data: &[u8]) -> Result<DecodedTile> {
    let mut tile = DecodedTile::default();
    let mut r = Reader::new(data);
    while r.remaining() > 0 {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (3, WIRE_LEN) => {
                let layer = r.read_bytes()?;
                tile.layers.push(decode_layer(layer)?);
            }
            (_, w) => r.skip(w)?,
        }
    }
    Ok(tile)
}

/// Decodes a gzip-compressed tile.
pub fn decode_gzipped_tile(gz: &[u8]) -> Result<DecodedTile> {
    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(gz);
    let mut raw = Vec::new();
    decoder
        .read_to_end(&mut raw)
        .map_err(|e| TileError::Encoding(format!("gzip decode failed: {e}")))?;
    decode_tile(&raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_garbage_gracefully() {
        // 0xff 0xff ... is a varint that never terminates.
        assert!(decode_tile(&[0xff, 0xff, 0xff]).is_err());
    }
}
