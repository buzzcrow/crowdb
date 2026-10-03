// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Native Access protocol proxy. Endpoints are configured launch inputs, never request URLs.

use std::time::Duration;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::{any, get};
use axum::{Json, Router};
use crowdb_console_shared::config::{ServerEntry, ServiceType};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::{err_400, err_502, ErrorBody};
use crate::state::AppState;

mod credentials;
mod inspection;
mod signing;

const BODY_LIMIT: usize = 16 * 1024 * 1024;
type ApiError = (StatusCode, Json<ErrorBody>);

pub(crate) fn read_router() -> Router<AppState> {
    Router::new()
        .route("/api/access/connections", get(connections))
        .route("/api/access/s3-inspect/locations", get(inspection::locations))
        .route("/api/access/:protocol", any(proxy))
        .route("/api/access/:protocol/", any(proxy))
        .route("/api/access/:protocol/*path", any(proxy))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
}

fn protocol_environment(protocol: &str) -> Option<&'static str> {
    match protocol {
        "s3" => Some("CROWDB_S3_PUBLIC_URI"),
        "iceberg" => Some("CROWDB_ICEBERG_PUBLIC_URI"),
        _ => None,
    }
}

fn validate_origin(origin: &str) -> Result<String, ApiError> {
    let url = reqwest::Url::parse(origin).map_err(|_| err_400("Access origin is invalid"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(err_400(
            "Access endpoint must be an HTTP origin without credentials or path",
        ));
    }
    Ok(url.origin().ascii_serialization())
}

fn origin(state: &AppState, protocol: &str) -> Result<Option<String>, ApiError> {
    let environment = protocol_environment(protocol).ok_or_else(|| err_400("Unknown Access protocol"))?;
    if let Ok(value) = std::env::var(environment) {
        return validate_origin(&value).map(Some);
    }
    if state.managed_mode {
        return Ok(None);
    }
    if let Some(value) = crate::services::access::origin(state, protocol) {
        return validate_origin(&value).map(Some);
    }
    let config = state.config.read().unwrap();
    config
        .servers
        .iter()
        .find(|server| server.id == format!("console-access-{protocol}"))
        .map(|server| validate_origin(&server.url))
        .transpose()
}

pub(crate) async fn connections(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let iceberg = origin(&state, "iceberg")?;
    let reader = crate::services::access::reader(&state, iceberg.as_deref()).map_err(err_502)?;
    Ok(Json(json!({"iceberg":iceberg,"s3":origin(&state,"s3")?,
        "iceberg_ready":reader.is_some() && iceberg.is_some(),
        "configurable":!state.managed_mode,"max_request_bytes":BODY_LIMIT})))
}

#[derive(Deserialize)]
pub(crate) struct Connection {
    protocol: String,
    origin: String,
}

pub(crate) async fn configure(
    State(state): State<AppState>,
    Json(connection): Json<Connection>,
) -> Result<Json<Value>, ApiError> {
    if connection.protocol == "iceberg" {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: "Iceberg is bound to this Console deployment".into(),
            }),
        ));
    }
    let environment =
        protocol_environment(&connection.protocol).ok_or_else(|| err_400("Unknown Access protocol"))?;
    if state.managed_mode || std::env::var_os(environment).is_some() {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: "Access endpoint is fixed by the deployment environment".into(),
            }),
        ));
    }
    let origin = validate_origin(&connection.origin)?;
    let id = format!("console-access-{}", connection.protocol);
    {
        let mut config = state.config.write().unwrap();
        if let Some(entry) = config.servers.iter_mut().find(|entry| entry.id == id) {
            entry.url.clone_from(&origin);
        } else {
            let mut entry = ServerEntry::new(id, origin);
            entry.service_type = ServiceType::AccessServer;
            entry.auto_start = false;
            config.servers.push(entry);
        }
    }
    state.persist().map_err(|error| err_502(error.to_string()))?;
    connections(State(state)).await
}

fn request_headers(source: &HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for name in ["content-type", "range", "if-match", "if-none-match"] {
        if let Some(value) = source.get(name) {
            headers.insert(name, value.clone());
        }
    }
    headers
}

pub(crate) async fn proxy(
    State(state): State<AppState>,
    Path(parameters): Path<Vec<String>>,
    request: Request,
) -> Result<Response, ApiError> {
    let protocol = &parameters[0];
    let target = origin(&state, protocol)?.ok_or_else(|| err_502("Access endpoint is not configured"))?;
    let prefix = format!("/api/access/{protocol}");
    let path = request.uri().path().strip_prefix(&prefix).unwrap_or("");
    if protocol == "iceberg"
        && !(path == "/v1/config" || path.starts_with("/v1/namespaces") || path == "/v1/tables/rename")
    {
        return Err(err_400("Unsupported Iceberg Catalog operation"));
    }
    if !matches!(
        request.method().as_str(),
        "GET" | "HEAD" | "POST" | "PUT" | "DELETE"
    ) {
        return Err(err_400("Unsupported Access method"));
    }
    let query = request
        .uri()
        .query()
        .map_or(String::new(), |query| format!("?{query}"));
    let url = format!("{target}{}{query}", if path.is_empty() { "/" } else { path });
    let mut headers = request_headers(request.headers());
    if protocol == "iceberg" {
        let token = if matches!(request.method().as_str(), "GET" | "HEAD") {
            crate::services::access::reader(&state, Some(&target))
                .map_err(err_502)?
                .ok_or_else(|| {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(ErrorBody {
                            error: "Cluster Catalog reader is not configured on the Console server".into(),
                        }),
                    )
                })?
        } else {
            credentials::writer(&state).map_err(err_502)?
        };
        headers.insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .map_err(|_| err_502("Invalid cluster Catalog credential"))?,
        );
    }
    let method = request.method().clone();
    let body = axum::body::to_bytes(request.into_body(), BODY_LIMIT)
        .await
        .map_err(|_| err_400("Request exceeds 16 MiB; use multipart upload"))?;
    let url = reqwest::Url::parse(&url).map_err(|_| err_400("Invalid Access URL"))?;
    if protocol == "s3" {
        let credentials = credentials::s3(&state, &target).map_err(err_502)?;
        signing::sign(&credentials, &method, &url, &body, &mut headers).map_err(err_502)?;
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(|error| err_502(error.to_string()))?;
    let upstream = client
        .request(method, url)
        .headers(headers)
        .body(body)
        .send()
        .await
        .map_err(|_| err_502("Access request failed; refresh resource state before retrying a mutation"))?;
    let mut response = Response::builder().status(upstream.status());
    for name in [
        "content-type",
        "content-length",
        "content-range",
        "accept-ranges",
        "etag",
        "last-modified",
        "x-amz-request-id",
        "www-authenticate",
    ] {
        if let Some(value) = upstream.headers().get(name) {
            response = response.header(name, value);
        }
    }
    response
        .body(Body::from_stream(upstream.bytes_stream()))
        .map_err(|error| err_502(error.to_string()))
}
