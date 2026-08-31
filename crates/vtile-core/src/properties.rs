//! Property handling: JSON attributes → MVT values, with policy enforcement
//! (TRD §4 normalization rule 5 and §18 attribute-bloat mitigation).

use crate::config::PropertyPolicy;
use crate::mvt::MvtValue;
use serde_json::Value;

/// Property attribute mode used during tile-size mitigation (TRD §5 order:
/// drop low-value attributes before touching geometry).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrMode {
    /// Keep every allowed property.
    Full,
    /// Keep only core identifiers (assetId, parcelId, ...).
    Core,
    /// Drop all properties (geometry only).
    None,
}

/// Converts a cleaned JSON property map into an ordered list of MVT values,
/// applying the policy caps.
///
/// Keys are sorted for deterministic output (identical inputs always produce
/// byte-identical tiles, which keeps S3 ETags stable across reruns).
pub fn json_to_mvt_properties(
    props: &serde_json::Map<String, Value>,
    policy: &PropertyPolicy,
    mode: AttrMode,
) -> Vec<(String, MvtValue)> {
    let mut out: Vec<(String, MvtValue)> = Vec::new();
    let mut payload_bytes = 0usize;

    let mut entries: Vec<(&String, &Value)> = props.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    for (key, value) in entries {
        if out.len() >= policy.max_fields_per_feature {
            break;
        }
        match mode {
            AttrMode::Full => {
                if !policy.is_allowed(key) {
                    continue;
                }
            }
            AttrMode::Core => {
                if !policy.is_core(key) {
                    continue;
                }
            }
            AttrMode::None => break,
        }
        let Some(mv) = json_value_to_mvt(value) else {
            continue;
        };
        payload_bytes += key.len() + mv.size_estimate();
        if payload_bytes > policy.max_property_bytes_per_feature {
            break;
        }
        out.push((key.clone(), mv));
    }
    out
}

/// Maps a JSON value onto the MVT value types.
///
/// * integers → `Int`/`Uint`, other numbers → `Double`
/// * strings/bools pass through
/// * `null` is dropped
/// * arrays/objects are serialized as JSON strings (keeps the attribute
///   instead of silently losing it; TRD §14: "must not alter source attribute
///   values unless configured")
pub fn json_value_to_mvt(value: &Value) -> Option<MvtValue> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(MvtValue::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(MvtValue::Int(i))
            } else if let Some(u) = n.as_u64() {
                Some(MvtValue::Uint(u))
            } else {
                n.as_f64().map(MvtValue::Double)
            }
        }
        Value::String(s) => Some(MvtValue::String(s.clone())),
        Value::Array(_) | Value::Object(_) => {
            Some(MvtValue::String(serde_json::to_string(value).ok()?))
        }
    }
}

/// Estimates the serialized size of a JSON property map (used for validation
/// reports before tiling).
pub fn property_payload_bytes(props: &serde_json::Map<String, Value>) -> usize {
    props
        .iter()
        .map(|(k, v)| k.len() + format!("{v}").len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(value: serde_json::Value) -> serde_json::Map<String, Value> {
        match value {
            Value::Object(m) => m,
            _ => panic!("expected object"),
        }
    }

    #[test]
    fn converts_number_types() {
        assert_eq!(json_value_to_mvt(&json!(5)), Some(MvtValue::Int(5)));
        assert_eq!(json_value_to_mvt(&json!(10.5)), Some(MvtValue::Double(10.5)));
        assert_eq!(json_value_to_mvt(&json!(true)), Some(MvtValue::Bool(true)));
        assert_eq!(json_value_to_mvt(&json!(null)), None);
        assert_eq!(
            json_value_to_mvt(&json!("x")),
            Some(MvtValue::String("x".into()))
        );
    }

    #[test]
    fn strips_pii_and_sorts_keys() {
        let policy = PropertyPolicy::default();
        let props = map(json!({
            "ownerName": "Jane Doe",
            "parcelId": "NYC-1",
            "market": "New York",
        }));
        let out = json_to_mvt_properties(&props, &policy, AttrMode::Full);
        let keys: Vec<&str> = out.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["market", "parcelId"]);
    }

    #[test]
    fn core_mode_keeps_only_core_fields() {
        let policy = PropertyPolicy::default();
        let props = map(json!({
            "parcelId": "NYC-1",
            "market": "New York",
            "zoning": "C4",
        }));
        let out = json_to_mvt_properties(&props, &policy, AttrMode::Core);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, "parcelId");
        let none = json_to_mvt_properties(&props, &policy, AttrMode::None);
        assert!(none.is_empty());
    }

    #[test]
    fn payload_budget_enforced() {
        let mut policy = PropertyPolicy::default();
        policy.max_property_bytes_per_feature = 16;
        policy.max_fields_per_feature = 100;
        let props = map(json!({
            "a": "0123456789",
            "b": "0123456789",
            "c": "0123456789",
        }));
        let out = json_to_mvt_properties(&props, &policy, AttrMode::Full);
        // "a"(1 key + 10 value = 11) fits; adding "b" exceeds 16.
        assert_eq!(out.len(), 1);
    }
}
