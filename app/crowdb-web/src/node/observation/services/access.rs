// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact Access readiness diagnostics even while the process is not ready.

use crate::state::AppState;
use crowdb_protocol::mgmt::node::NodeServiceIntent;
use std::time::Duration;

pub(super) async fn read(state: &AppState, intent: &NodeServiceIntent) -> Result<String, String> {
    let invalid = || "Access current intent has no valid local health listener".to_owned();
    let config: toml::Value = toml::from_str(&intent.configuration).map_err(|_| invalid())?;
    let address = config
        .get("health")
        .and_then(|health| health.get("listen"))
        .and_then(toml::Value::as_str)
        .ok_or_else(invalid)?;
    let origin =
        super::super::protocols::internal_origin(state, intent.node_id, address).ok_or_else(invalid)?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(1))
        .build()
        .map_err(|_| invalid())?;
    let mut response = client
        .get(format!("{origin}/_crowdb/health/ready"))
        .send()
        .await
        .map_err(|_| "Access readiness is unreachable or exceeded its one-second deadline".to_owned())?;
    let status = response.status();
    if status.as_u16() != 200 && status.as_u16() != 503 {
        return Err(format!("Access readiness returned HTTP {status}"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "Access readiness body is unavailable")?
    {
        if bytes.len() + chunk.len() > 65_536 {
            return Err("Access readiness body exceeds 65536 bytes".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let body: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| "Access readiness body is invalid JSON")?;
    if body["ready"].as_bool() != Some(status.as_u16() == 200) {
        return Err("Access readiness HTTP status and ready flag disagree".into());
    }
    let fields = [
        "listener",
        "metadata",
        "chunks",
        "native_pool",
        "cleanup",
        "authentication",
    ];
    let flags = fields
        .into_iter()
        .map(|name| {
            let value = body[name]
                .as_str()
                .filter(|value| ["ready", "busy", "unavailable"].contains(value))
                .ok_or_else(|| format!("Access readiness has no valid {name} state"))?;
            Ok(format!("{name}={value}"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(format!(
        "Access readiness HTTP {status}: {}; details: {}; cleanup backlog: {}/{}",
        flags.join(", "),
        body["details"],
        body["cleanup_backlog"],
        body["cleanup_backlog_limit"]
    ))
}
