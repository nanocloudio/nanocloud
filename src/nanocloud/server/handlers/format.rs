use super::error::ApiError;
use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OutputFormat {
    Json,
    Yaml,
}

impl OutputFormat {
    pub fn negotiate(format: Option<&str>, headers: &HeaderMap) -> Self {
        if let Some(explicit) = format {
            match explicit.to_ascii_lowercase().as_str() {
                "yaml" | "yml" => return OutputFormat::Yaml,
                "json" => return OutputFormat::Json,
                _ => {}
            }
        }

        if let Some(accept) = headers
            .get(header::ACCEPT)
            .and_then(|value| value.to_str().ok())
        {
            for candidate in accept.split(',').map(|s| s.trim().to_ascii_lowercase()) {
                if candidate.contains("yaml") {
                    return OutputFormat::Yaml;
                }
            }
        }

        OutputFormat::Json
    }
}

pub fn respond_with<T>(value: T, format: OutputFormat) -> Result<Response, ApiError>
where
    T: Serialize,
{
    match format {
        OutputFormat::Json => Ok(Json(value).into_response()),
        OutputFormat::Yaml => {
            let body = serde_yaml::to_string(&value)
                .map_err(|err| ApiError::internal_error(err.into()))?;
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/yaml")
                .body(Body::from(body))
                .map_err(|err| ApiError::internal_error(err.into()))
        }
    }
}
