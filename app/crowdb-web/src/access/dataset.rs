// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fixed-origin Dataset control proxy with bounded metadata and private credentials.

use crate::{
    error::{err_400, err_403, err_502},
    state::AppState,
};
use axum::{body::Body, extract::Request, http::Method, response::Response};
use std::time::Duration;

const PREFIX: &str = "/api/access/dataset";
const API: &str = "/api/dataset/v2";
const METADATA_LIMIT: usize = 1024 * 1024;

/// Exact matrix and Split preview POSTs observe immutable Snapshot content.
pub(crate) fn observes(method: &Method, path: &str) -> bool {
    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return true;
    }
    let Some(path) = path.strip_prefix(PREFIX).and_then(|path| path.strip_prefix(API)) else {
        return false;
    };
    let parts: Vec<_> = path.split('/').collect();
    *method == Method::POST
        && matches!(parts.as_slice(),
        ["", "namespaces", "dataset-ns", "datasets", dataset, "snapshots", snapshot, "matrix" | "split-preview"]
        if !dataset.is_empty() && snapshot.len() == 32 && snapshot.bytes().all(|b| b.is_ascii_hexdigit()))
}

pub(super) async fn proxy(state: AppState, request: Request) -> Result<Response, super::ApiError> {
    let seconds = if observes(request.method(), request.uri().path()) {
        6
    } else {
        60
    };
    tokio::time::timeout(Duration::from_secs(seconds), forward(state, request))
        .await
        .map_err(|_| err_502("Dataset reply unavailable; retry the saved complete request"))?
}

async fn forward(state: AppState, request: Request) -> Result<Response, super::ApiError> {
    let path = request
        .uri()
        .path()
        .strip_prefix(PREFIX)
        .ok_or_else(|| err_400("Invalid Dataset path"))?;
    if !(path == format!("{API}/capabilities")
        || path.starts_with(&format!("{API}/namespaces/dataset-ns/datasets")))
    {
        return Err(err_400("Unsupported Dataset control version or namespace"));
    }
    let observe = observes(request.method(), request.uri().path());
    if state.readonly && !observe {
        return Err(err_403("Workspace is readonly"));
    }
    if !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::POST | Method::PUT | Method::DELETE
    ) {
        return Err(err_400("Unsupported Dataset method"));
    }
    let origin =
        super::origin(&state, "dataset")?.ok_or_else(|| err_502("Dataset endpoint is not configured"))?;
    let token = super::credentials::dataset(&state).map_err(err_502)?;
    let query = request.uri().query().map_or(String::new(), |q| format!("?{q}"));
    let target = format!("{origin}{path}{query}");
    let target = reqwest::Url::parse(&target).map_err(|_| err_400("Invalid Dataset URL"))?;
    if target.path() != path {
        return Err(err_400("Dataset path normalization is not permitted"));
    }
    let method = request.method().clone();
    let headers = super::request_headers(request.headers());
    let limit = if method == Method::PUT
        && matches!(path.strip_prefix(API).map(|path| path.split('/').collect::<Vec<_>>()).as_deref(),
            Some(["", "namespaces", "dataset-ns", "datasets", dataset, "snapshots", snapshot])
                if !dataset.is_empty() && snapshot.len() == 32 && snapshot.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        8 * METADATA_LIMIT
    } else {
        4 * METADATA_LIMIT
    };
    let body = axum::body::to_bytes(request.into_body(), limit)
        .await
        .map_err(|_| err_400("Dataset request exceeds its body limit"))?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(if observe { 6 } else { 60 }))
        .build()
        .map_err(|_| err_502("Dataset transport could not start"))?;
    let mut upstream = client
        .request(method, target)
        .headers(headers)
        .bearer_auth(token)
        .body(body)
        .send()
        .await
        .map_err(|_| err_502("Dataset reply unavailable; retry the saved complete request"))?;
    let mut response = Response::builder().status(upstream.status());
    for name in ["content-type", "x-request-id", "www-authenticate"] {
        if let Some(value) = upstream.headers().get(name) {
            response = response.header(name, value);
        }
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = upstream
        .chunk()
        .await
        .map_err(|_| err_502("Dataset response interrupted; retry the saved complete request"))?
    {
        if bytes.len().saturating_add(chunk.len()) > METADATA_LIMIT {
            return Err(err_502("Dataset metadata response exceeds 1 MiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    response
        .body(Body::from(bytes))
        .map_err(|_| err_502("Invalid Dataset response"))
}
