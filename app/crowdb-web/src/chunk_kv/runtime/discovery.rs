// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact-owner discovery from cluster configuration or one bounded registry record.

use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_console_shared::config::ServiceType;
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::{common::InstanceValue, key::InstanceKey};

use super::super::Failure;
use crate::{
    error::{err_404, err_409, err_502},
    state::AppState,
};

pub(super) async fn origin(state: &AppState, instance_id: u64, endpoint: &str) -> Result<String, Failure> {
    let configured = state
        .config
        .read()
        .unwrap()
        .servers
        .iter()
        .find(|server| {
            server.service_type == ServiceType::ChunkKv && server.rpc_url.as_deref() == Some(endpoint)
        })
        .map(|server| server.url.clone());
    if let Some(origin) = configured {
        return Ok(origin);
    }
    let key = InstanceKey {
        service: "chunk-kv".into(),
        instance_id,
    }
    .to_path();
    let client = state.kv_client().await;
    let GetOutcome::Found { value, .. } = client
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .map_err(|error| err_502(error.to_string()))?
    else {
        return Err(err_404(
            "Owner management endpoint is not registered in this cluster",
        ));
    };
    if value.len() > 256 * 1024 {
        return Err(err_502("Owner registration exceeds 256 KiB inspection budget"));
    }
    let record: InstanceValue = serde_json::from_slice(&value).map_err(|error| err_502(error.to_string()))?;
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    if record.instance_id != instance_id
        || record.rpc_endpoint != endpoint
        || now.abs_diff(record.last_heartbeat_ms) > 15_000
    {
        return Err(err_409(
            "Owner registration changed or expired; refresh the catalog",
        ));
    }
    record
        .extra
        .and_then(|extra| extra.chunk_kv)
        .and_then(|extra| extra.http_endpoint)
        .ok_or_else(|| err_404("Owner has not advertised a management endpoint"))
}
