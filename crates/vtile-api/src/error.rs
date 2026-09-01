//! API error type with the TRD §8/§10 status-code mapping.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn bad_request(code: &str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "FORBIDDEN", message)
    }

    pub fn not_found(code: &str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    pub fn unprocessable(code: &str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, code, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "error": {
                "code": self.code,
                "message": self.message,
            }
        });
        (self.status, Json(body)).into_response()
    }
}

/// Maps pipeline errors onto TRD HTTP contracts.
impl From<vtile_pipeline::PipelineError> for ApiError {
    fn from(err: vtile_pipeline::PipelineError) -> Self {
        use vtile_pipeline::PipelineError as E;
        match &err {
            E::Ingest(ingest) => ingest_error_to_api(ingest),
            other => {
                let (code, message) = vtile_pipeline::job::error_classification(other);
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, code, message)
            }
        }
    }
}

impl From<vtile_ingest::IngestError> for ApiError {
    fn from(err: vtile_ingest::IngestError) -> Self {
        ingest_error_to_api(&err)
    }
}

fn ingest_error_to_api(err: &vtile_ingest::IngestError) -> ApiError {
    use vtile_ingest::IngestError as I;
    // Single source of truth for the code (docs/ERRORS.md); only the HTTP
    // status is decided here.
    let code = err.error_code();
    let status = match err {
        I::PayloadTooLarge { .. } | I::DatasetTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        I::Io(_) | I::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    };
    ApiError::new(status, code, err.to_string())
}
