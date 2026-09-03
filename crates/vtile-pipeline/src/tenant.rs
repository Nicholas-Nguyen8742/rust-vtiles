//! Tenant isolation primitives (Sequence 5 epic).
//!
//! Zero-trust, defense-in-depth: tenant identity comes from the
//! authenticated principal (never client-supplied bodies), every resource is
//! tenant-scoped, every access path is authorized + logged + tested.

use serde::{Deserialize, Serialize};

/// Tenant identity resolved from the authenticated principal and propagated
/// through jobs, events, workers, and audit records (Sequence 5 TI-02).
///
/// Production: resolved from the OIDC/JWT claims
/// (`sub`, `tenantId`, `roles`). Local: resolved from the static token +
/// tenant claim header (`vtile_api::auth`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantContext {
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_principal: Option<String>,
    #[serde(default)]
    pub roles: Vec<String>,
}

/// Approved tenant id shape (Sequence 5 TI-02):
/// `^[a-z0-9-_]{3,64}$`. Implemented without a regex dependency; the
/// constant documents the production validation pattern.
pub const TENANT_ID_PATTERN: &str = "^[a-z0-9-_]{3,64}$";

/// Tenant id validation: 3–64 chars of `[a-z0-9-_]`. Rejects empty ids,
/// uppercase/mixed case, whitespace, path traversal (`../`), and anything
/// outside the approved pattern.
pub fn is_valid_tenant_id(id: &str) -> bool {
    let len = id.len();
    (3..=64).contains(&len)
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

/// Resource ids (layerId, jobId, fileName) must not carry path separators,
/// traversal sequences, or control characters — they are interpolated into
/// storage paths (Sequence 5 TI-02/TI-05 path traversal negative tests).
pub fn is_valid_resource_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && !id.contains('/')
        && !id.contains('\\')
        && !id.contains("..")
        && !id.contains('\0')
        && !id.chars().any(char::is_control)
}

/// Defense-in-depth alignment check (Sequence 5 TI-02): the tenant embedded
/// in a storage path / source URI must equal the job's tenant. Only applied
/// to path-like URIs carrying the staging layout marker (`/input/`); opaque
/// test URIs (`mem://…`) are exempt.
pub fn tenant_alignment_holds(job_tenant: &str, path_or_uri: &str) -> bool {
    if !path_or_uri.contains("/input/") {
        return true;
    }
    path_or_uri.contains(&format!("/{job_tenant}/"))
        || path_or_uri.starts_with(&format!("{job_tenant}/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_validation_matrix() {
        // Valid.
        assert!(is_valid_tenant_id("tenant-acme"));
        assert!(is_valid_tenant_id("tenant_acme"));
        assert!(is_valid_tenant_id("tenant-alpha"));
        assert!(is_valid_tenant_id("abc"));
        assert!(is_valid_tenant_id(&"a".repeat(64)));
        // Invalid.
        assert!(!is_valid_tenant_id(""));
        assert!(!is_valid_tenant_id("ab")); // too short
        assert!(!is_valid_tenant_id(&"a".repeat(65))); // too long
        assert!(!is_valid_tenant_id("Tenant-Acme")); // uppercase
        assert!(!is_valid_tenant_id("tenant acme")); // whitespace
        assert!(!is_valid_tenant_id("../evil")); // traversal
        assert!(!is_valid_tenant_id("tenant/acme")); // separator
        assert!(!is_valid_tenant_id("tenant.acme")); // unapproved char
    }

    #[test]
    fn resource_id_validation_rejects_traversal() {
        assert!(is_valid_resource_id("us-parcels-nyc"));
        assert!(is_valid_resource_id("job_01J9XYZ"));
        assert!(is_valid_resource_id("parcels.geojson"));
        assert!(!is_valid_resource_id(""));
        assert!(!is_valid_resource_id("../evil"));
        assert!(!is_valid_resource_id("a/b"));
        assert!(!is_valid_resource_id("a\\b"));
        assert!(!is_valid_resource_id(".."));
        assert!(!is_valid_resource_id("a\0b"));
        assert!(!is_valid_resource_id(&"x".repeat(129)));
    }

    #[test]
    fn tenant_alignment_detects_mismatched_paths() {
        // Aligned staging path.
        assert!(tenant_alignment_holds(
            "tenant-acme",
            "file:///data/staging/tenant-acme/job_1/input/parcels.zip"
        ));
        // Mismatched tenant in path.
        assert!(!tenant_alignment_holds(
            "tenant-acme",
            "file:///data/staging/tenant-evil/job_1/input/parcels.zip"
        ));
        // Opaque test URIs are exempt.
        assert!(tenant_alignment_holds("tenant-acme", "mem://us-parcels-nyc"));
    }

    #[test]
    fn tenant_context_serializes_camel_case() {
        let ctx = TenantContext {
            tenant_id: "tenant-acme".into(),
            user_id: Some("user_123".into()),
            service_principal: Some("vector-tile-worker".into()),
            roles: vec!["CRE_DATA_ENGINEER".into()],
        };
        let value = serde_json::to_value(&ctx).unwrap();
        assert_eq!(value["tenantId"], "tenant-acme");
        assert_eq!(value["userId"], "user_123");
        assert_eq!(value["servicePrincipal"], "vector-tile-worker");
    }
}
