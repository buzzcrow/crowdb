// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Liveness probe for the console web binary.
//!
//! The pre-A12 `/api/cluster/snapshot` aggregator that lived here has
//! been retired; the SPA composes the same
//! information from the per-resource endpoints under `/api/racks/...`,
//! `/api/nodes/...`, and `/api/stores/...`, all of which read from the
//! monitor cache.

pub async fn healthz() -> &'static str {
    "ok"
}

pub async fn mode(
    axum::extract::State(state): axum::extract::State<crate::state::AppState>,
) -> axum::Json<serde_json::Value> {
    let mode = match state.web_mode {
        Some(crowdb_console_shared::config::web::WebMode::Docker) => "docker",
        Some(crowdb_console_shared::config::web::WebMode::BareMetal) => "bare-metal-pending",
        None if state.node_monitor_url.is_some() => "node",
        None => "legacy",
    };
    axum::Json(serde_json::json!({"mode": mode}))
}

pub async fn managed_api_unavailable() -> axum::http::StatusCode {
    axum::http::StatusCode::SERVICE_UNAVAILABLE
}

pub(crate) async fn require_editable(
    axum::extract::State(state): axum::extract::State<crate::state::AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let observe = matches!(
        *request.method(),
        axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS
    );
    if state.web_mode == Some(crowdb_console_shared::config::web::WebMode::Docker) && !observe {
        return Err(axum::http::StatusCode::FORBIDDEN);
    }
    Ok(next.run(request).await)
}

/// Container deployments expose disk observation but never disk management.
pub(crate) async fn require_disk_management(
    axum::extract::State(state): axum::extract::State<crate::state::AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    if state.web_mode == Some(crowdb_console_shared::config::web::WebMode::Docker) {
        return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(next.run(request).await)
}
