//! Minimal dBASE III/IV `.dbf` reader for shapefile attributes.
//!
//! Reference: dBASE .DBF file structure. Supports the field types found in
//! county assessor data: C (character), N/F (numeric), L (logical),
//! D (date). Other types degrade to strings.
//!
//! Encoding policy (TRD §4 rule 4): UTF-8 first, ISO-8859-1 fallback.

use serde_json::{Map, Value};

use crate::error::{IngestError, IngestResult};

#[derive(Debug, Clone)]
pub struct DbfField {
    pub name: String,
    pub ftype: u8,
    pub length: usize,
    pub decimal_count: u8,
}

#[derive(Debug)]
pub struct DbfTable {
    pub fields: Vec<DbfField>,
    /// One property object per record, field order preserved.
    pub records: Vec<Map<String, Value>>,
}

/// Parses a `.dbf` file into a schema + records.
pub fn parse_dbf(data: &[u8]) -> IngestResult<DbfTable> {
    if data.len() < 32 {
        return Err(IngestError::InvalidShapefile("dbf too small".into()));
    }
    let num_records = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let header_size = u16::from_le_bytes([data[8], data[9]]) as usize;
    let record_size = u16::from_le_bytes([data[10], data[11]]) as usize;

    if header_size < 33 || record_size == 0 || header_size > data.len() {
        return Err(IngestError::InvalidShapefile("corrupt dbf header".into()));
    }

    // Field descriptors: 32 bytes each, terminated by 0x0D.
    let mut fields = Vec::new();
    let mut pos = 32usize;
    while pos + 32 <= header_size - 1 {
        if data[pos] == 0x0d {
            break;
        }
        let name_end = (pos..pos + 11).find(|&i| data[i] == 0).unwrap_or(pos + 11);
        let name = decode_dbf_string(&data[pos..name_end])
            .trim()
            .to_string();
        let ftype = data[pos + 11];
        let length = data[pos + 16] as usize;
        let decimal_count = data[pos + 17];
        if !name.is_empty() && length > 0 {
            fields.push(DbfField {
                name,
                ftype,
                length,
                decimal_count,
            });
        }
        pos += 32;
    }

    let expected_min = header_size + num_records * record_size;
    if data.len() < expected_min {
        return Err(IngestError::InvalidShapefile(format!(
            "dbf truncated: need {expected_min} bytes, have {}",
            data.len()
        )));
    }

    let mut records = Vec::with_capacity(num_records.min(10_000_000));
    for i in 0..num_records {
        let start = header_size + i * record_size;
        let rec = &data[start..start + record_size];
        if rec.is_empty() {
            break;
        }
        // Deletion flag: '*' marks a record deleted by dBASE editors; skip.
        if rec[0] == b'*' {
            continue;
        }
        let mut map = Map::new();
        let mut offset = 1usize;
        for field in &fields {
            if offset + field.length > rec.len() {
                break;
            }
            let raw = &rec[offset..offset + field.length];
            offset += field.length;
            map.insert(field.name.clone(), decode_field(field, raw));
        }
        records.push(map);
    }

    Ok(DbfTable { fields, records })
}

fn decode_field(field: &DbfField, raw: &[u8]) -> Value {
    match field.ftype {
        b'C' | b'M' | b'T' => Value::String(decode_dbf_string(raw).trim().to_string()),
        b'N' | b'F' => {
            let text = decode_dbf_string(raw).trim().to_string();
            if text.is_empty() {
                return Value::Null;
            }
            if field.decimal_count == 0 && !text.contains('.') {
                if let Ok(i) = text.parse::<i64>() {
                    return Value::from(i);
                }
            }
            match text.parse::<f64>() {
                Ok(f) if f.is_finite() => {
                    serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null)
                }
                _ => Value::String(text),
            }
        }
        b'L' => match raw.first() {
            Some(b'T') | Some(b't') | Some(b'Y') | Some(b'y') | Some(b'1') => Value::Bool(true),
            Some(b'F') | Some(b'f') | Some(b'N') | Some(b'n') | Some(b'0') => Value::Bool(false),
            _ => Value::Null,
        },
        // Dates kept as raw YYYYMMDD strings; converting to ISO-8601 would
        // alter source values (TRD §14 accuracy rule).
        _ => Value::String(decode_dbf_string(raw).trim().to_string()),
    }
}

/// UTF-8 with ISO-8859-1 fallback (TRD §4 rule 4). Latin-1 maps 1:1 onto the
/// first 256 Unicode code points, so this conversion is lossless.
pub fn decode_dbf_string(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => bytes.iter().map(|&b| b as char).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_dbf() -> Vec<u8> {
        // Two fields: NAME (C,8), VALUE (N,6,2); two records.
        let mut data = Vec::new();
        let header_size: u16 = 32 + 2 * 32 + 1; // terminator
        let record_size: u16 = 1 + 8 + 6;
        data.push(0x03); // dBASE III
        data.extend_from_slice(&[26, 6, 17]); // date
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&header_size.to_le_bytes());
        data.extend_from_slice(&record_size.to_le_bytes());
        data.extend_from_slice(&[0u8; 20]); // reserved

        // Field 1: NAME C 8
        data.extend_from_slice(b"NAME\0\0\0\0\0\0\0");
        data.push(b'C');
        data.extend_from_slice(&[0u8; 4]);
        data.push(8);
        data.push(0);
        data.extend_from_slice(&[0u8; 14]);
        // Field 2: VALUE N 6.2
        data.extend_from_slice(b"VALUE\0\0\0\0\0\0");
        data.push(b'N');
        data.extend_from_slice(&[0u8; 4]);
        data.push(6);
        data.push(2);
        data.extend_from_slice(&[0u8; 14]);
        data.push(0x0d); // header terminator

        // Record 1
        data.push(b' ');
        data.extend_from_slice(b"EMPIRE  ");
        data.extend_from_slice(b" 10.50");
        // Record 2
        data.push(b' ');
        data.extend_from_slice(b"FLATIRON");
        data.extend_from_slice(b"     7");

        data.push(0x1a);
        data
    }

    #[test]
    fn parses_fields_and_records() {
        let table = parse_dbf(&build_dbf()).unwrap();
        assert_eq!(table.fields.len(), 2);
        assert_eq!(table.fields[0].name, "NAME");
        assert_eq!(table.fields[1].ftype, b'N');
        assert_eq!(table.records.len(), 2);

        let r0 = &table.records[0];
        assert_eq!(r0["NAME"], Value::String("EMPIRE".into()));
        assert_eq!(r0["VALUE"], serde_json::json!(10.5));

        let r1 = &table.records[1];
        // decimal_count == 0 branch is only for fields declared 0; VALUE has
        // decimal_count 2 so integral text becomes float 7.0.
        assert_eq!(r1["VALUE"], serde_json::json!(7.0));
    }

    #[test]
    fn latin1_fallback() {
        // 0xE9 is é in Latin-1 but invalid UTF-8 on its own.
        let s = decode_dbf_string(&[b'C', b'a', b'f', 0xe9]);
        assert_eq!(s, "Café");
    }
}
