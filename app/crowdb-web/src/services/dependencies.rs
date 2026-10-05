// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::state::AppState;

pub(crate) async fn group0_ready(state: &AppState) -> bool {
    if state.service_operations.load().contains("cluster/init") {
        return false;
    }
    let sysmd = crowdb_kv_client::CrowdbSysmdClient::from_shared(state.kv_client().await);
    tokio::time::timeout(std::time::Duration::from_secs(2), sysmd.get_group(0, 0))
        .await
        .is_ok_and(|result| result.is_ok_and(|group| group.is_some()))
}
use axum::{extract::State, Json};
use crowdb_console_shared::config::ServiceType;
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use serde_json::{json, Value};
use std::collections::HashSet;

pub(super) async fn authority(State(state): State<AppState>) -> Json<Value> {
    Json(json!({"ready": group0_ready(&state).await}))
}

pub(super) async fn storage(State(state): State<AppState>) -> Json<Value> {
    let reason = tokio::time::timeout(std::time::Duration::from_secs(2), storage_wait_reason(&state))
        .await
        .unwrap_or_else(|_| Some("Waiting: DiskIO ownership probe timed out".into()));
    Json(json!({"ready": reason.is_none(), "reason": reason}))
}

pub(super) async fn storage_wait_reason(state: &AppState) -> Option<String> {
    let Ok(selected) = super::plans::selected_nodes(state, "diskio") else {
        return Some("Waiting: DiskIO service plans could not be read".into());
    };
    let servers = state.config.read().unwrap().servers.clone();
    let diskio: Vec<_> = servers
        .iter()
        .filter(|server| server.service_type == ServiceType::Diskio)
        .collect();
    if selected
        .iter()
        .any(|node| !diskio.iter().any(|server| server.node_id == Some(*node)))
    {
        return Some("Waiting: deploy selected DiskIO services before starting ChunkDB".into());
    }
    let client = state.kv_client().await;
    let registry = ServiceRegistryClient::from_shared(client.clone());
    let Ok(instances) = registry.read_all_diskio_instances().await else {
        return Some("Waiting: live DiskIO ownership registry is unavailable".into());
    };
    let hardware = HardwareClient::from_shared(client);
    let Ok(groups) = hardware.list_disk_groups().await else {
        return Some("Waiting: disk-group hardware is unavailable".into());
    };
    if diskio.is_empty() || groups.is_empty() {
        return Some("Waiting: configure disk groups and publish live DiskIO ownership".into());
    }
    let mut owned = HashSet::new();
    for server in diskio {
        let instance = instances.iter().find(|(_, value)| {
            server.rpc_url.as_deref().is_some_and(|endpoint| {
                rpc_address(endpoint).is_some_and(|address| Some(address) == rpc_address(&value.rpc_endpoint))
            })
        });
        let ownership = instance
            .and_then(|(_, value)| value.extra.as_ref())
            .and_then(|extra| extra.diskdb.as_ref());
        let Some(ownership) = ownership.filter(|value| !value.owned_dg_ids.is_empty()) else {
            return Some(format!(
                "Waiting: {} must publish live disk-group ownership",
                server.id
            ));
        };
        owned.extend(ownership.owned_dg_ids.iter().copied());
    }
    if groups.iter().any(|group| !owned.contains(&group.dg_id)) {
        return Some("Waiting: every configured disk group needs live DiskIO ownership".into());
    }
    None
}

fn rpc_address(endpoint: &str) -> Option<(String, u16)> {
    let url = reqwest::Url::parse(&if endpoint.contains("://") {
        endpoint.to_owned()
    } else {
        format!("tcp://{endpoint}")
    })
    .ok()?;
    Some((url.host_str()?.to_owned(), url.port()?))
}
