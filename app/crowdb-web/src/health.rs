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
        None => "legacy",
    };
    axum::Json(serde_json::json!({"mode": mode}))
}

pub async fn managed_api_unavailable() -> axum::http::StatusCode {
    axum::http::StatusCode::SERVICE_UNAVAILABLE
}
