// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Merge newly provisioned disks without replacing concurrent service updates.

use axum::{http::StatusCode, Json};
use crowdb_console_shared::config::{DiskEntry, DiskGroupEntry};
use crowdb_paxos_kv_client::HardwareClient;

use crate::{
    error::{err_409, err_502, map_config_err, map_persist_err, ErrorBody},
    state::AppState,
};

pub(crate) fn validate_metadata_only(
    state: &AppState,
    paths: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<(), (StatusCode, Json<ErrorBody>)> {
    if state.node_monitor_url.is_some() && paths.into_iter().any(|path| !path.as_ref().is_empty()) {
        return Err(err_409(
            "Physical media require Create File Disk or Join Candidates and owner verification",
        ));
    }
    Ok(())
}

pub(crate) async fn persist_added(
    state: &AppState,
    entries: &[DiskEntry],
) -> Result<(), (StatusCode, Json<ErrorBody>)> {
    let hardware = HardwareClient::from_shared(state.kv_client().await);
    let mut groups = std::collections::BTreeMap::new();
    for entry in entries {
        let key = (entry.rack_id, entry.node_id, entry.disk_group_id);
        if groups.contains_key(&key) {
            continue;
        }
        let group = hardware
            .get_disk_group(key.0, key.1, key.2)
            .await
            .map_err(|error| err_502(error.to_string()))?
            .ok_or_else(|| err_502("Published disk parent changed; refresh to reconcile"))?;
        groups.insert(
            key,
            DiskGroupEntry {
                rack_id: key.0,
                node_id: key.1,
                id: key.2,
                name: group.value.name,
            },
        );
    }
    {
        let mut config = state.config.write().unwrap();
        let mut updated = config.clone();
        for group in groups.into_values() {
            if !updated
                .disk_groups
                .iter()
                .any(|existing| existing.node_id == group.node_id && existing.id == group.id)
            {
                updated.disk_groups.push(group);
            }
        }
        for entry in entries {
            if let Some(existing) = updated
                .disks
                .iter_mut()
                .find(|existing| existing.disk_id == entry.disk_id)
            {
                // Group 0 already confirmed this geometry. Its projection may
                // use canonical media names instead of the request's aliases.
                *existing = entry.clone();
                continue;
            }
            updated.add_disk(entry.clone()).map_err(map_config_err)?;
        }
        *config = updated;
    }
    state.persist().map_err(map_persist_err)
}
