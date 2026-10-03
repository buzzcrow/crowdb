// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Least-loaded immutable disk-group owner selection.

use crowdb_protocol::sysdata::DiskdbOwnerEntry;

#[must_use]
pub fn pick_least_loaded_instance(instance_ids: &[u64], owners: &[DiskdbOwnerEntry]) -> Option<u64> {
    if instance_ids.is_empty() {
        return None;
    }
    let mut counts = std::collections::HashMap::<u64, usize>::new();
    for owner in owners {
        *counts.entry(owner.instance_id).or_default() += 1;
    }
    instance_ids
        .iter()
        .map(|id| (*id, counts.get(id).copied().unwrap_or(0)))
        .min_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)))
        .map(|(id, _)| id)
}

/// Ensure newly assigned groups have a data-plane destination before publishing ownership.
/// Existing bindings are authoritative and are never changed here.
///
/// # Errors
/// Returns an error when no ordinary data group exists or Group 0 cannot persist the binding.
pub async fn ensure_data_binding(
    hw: &crowdb_kv_client::HardwareClient,
    rack_id: u64,
    node_id: u64,
    dg_id: u64,
) -> Result<(), String> {
    use crowdb_protocol::key::{BindMapKey, TextKey};
    if hw
        .get_bind(rack_id, node_id, dg_id)
        .await
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Ok(());
    }
    let sysmd = crowdb_kv_client::CrowdbSysmdClient::from_shared(hw.shared_kv());
    let mut stores = sysmd.list_stores().await.map_err(|e| e.to_string())?;
    stores.sort_by_key(|store| store.store_id);
    let mut destination = None;
    for store in stores {
        if let Some(group) = sysmd
            .list_groups_in_store(store.store_id)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|group| group.group_id != 0)
            .min_by_key(|group| group.group_id)
        {
            destination = Some((store.store_id, group.group_id));
            break;
        }
    }
    let (store_id, group_id) =
        destination.ok_or_else(|| "Create an ordinary data group before adding a DiskGroup".to_owned())?;
    let key = BindMapKey {
        rack_id,
        node_id,
        disk_group_id: dg_id,
    }
    .to_path();
    let value = serde_json::to_vec(&crowdb_protocol::common::BindMapValue { store_id, group_id })
        .map_err(|e| e.to_string())?;
    match hw.kv().put_cas(0, 0, key.as_bytes(), &value, 0).await {
        Ok(_) => Ok(()),
        Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::CasBusy) => hw
            .get_bind(rack_id, node_id, dg_id)
            .await
            .map_err(|e| e.to_string())?
            .map(|_| ())
            .ok_or_else(|| "DiskGroup binding changed concurrently; retry creation".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}
