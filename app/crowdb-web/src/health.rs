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

pub async fn managed_authority(
    axum::extract::State(state): axum::extract::State<crate::state::AppState>,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    let group0_reachable = if state.authority_seeds.is_empty() {
        false
    } else {
        let timeout = std::time::Duration::from_millis(state.authority_timeout_ms);
        let kv = state.kv_client().await;
        let result = tokio::time::timeout(timeout, async {
            kv.refresh_topology().await?;
            let sysmd = crowdb_kv_client::CrowdbSysmdClient::from_shared(kv);
            sysmd.list_racks().await?;
            sysmd.list_stores().await?;
            Ok::<(), crowdb_kv_client::Error>(())
        })
        .await;
        if let Err(error) = &result {
            tracing::debug!(%error, "Group 0 authority probe timed out");
        } else if let Ok(Err(error)) = &result {
            tracing::debug!(%error, "Group 0 authority probe failed");
        }
        matches!(result, Ok(Ok(())))
    };
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(serde_json::json!({
            "source": "group0",
            "available": false,
            "group0_reachable": group0_reachable,
            "api_ready": false
        })),
    )
}

pub async fn managed_api_unavailable() -> axum::http::StatusCode {
    axum::http::StatusCode::SERVICE_UNAVAILABLE
}
