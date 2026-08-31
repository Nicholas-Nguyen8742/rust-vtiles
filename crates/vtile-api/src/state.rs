//! Shared application state.

use std::path::PathBuf;
use std::sync::Arc;

use vtile_pipeline::events::EventEmitter;
use vtile_pipeline::store::{JobStore, LayerCatalog};

/// Everything handlers need. Cheap to clone (all pointers).
#[derive(Clone)]
pub struct AppState {
    pub jobs: Arc<dyn JobStore>,
    pub catalog: Arc<dyn LayerCatalog>,
    pub events: Arc<dyn EventEmitter>,
    /// Local data root: `staging/`, `tiles/`, `manifests/` underneath.
    pub data_dir: PathBuf,
    /// Static bearer token for MVP auth; `None` disables the check.
    /// Production: replaced by API Gateway + Cognito OIDC (TRD §13).
    pub auth_token: Option<String>,
    /// Max upload size in bytes (TRD §10 validation table).
    pub max_upload_bytes: u64,
    /// Presigned URL lifetime reported to clients (TRD §8.1: 900 s).
    pub upload_expires_secs: u64,
}
