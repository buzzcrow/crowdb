// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded `DiskGroup` provisioning with reconciliation after unknown outcomes.
use crate::{
    error::{err_409, err_500, err_502, map_config_err, map_persist_err, ErrorBody},
    state::AppState,
};
use axum::{http::StatusCode, Json};
use crowdb_console_shared::{
    cluster::{DiskGroupId, NodeId, RackId},
    config::DiskGroupEntry,
    ops,
};
type Failure = (StatusCode, Json<ErrorBody>);

pub(crate) async fn create(
    state: AppState,
    node_id: NodeId,
    id: DiskGroupId,
    name: String,
) -> Result<(StatusCode, Json<DiskGroupEntry>), Failure> {
    let operation = crate::services::Operation::claim(&state, vec![format!("disk-group/{node_id}/{id}")])?;
    let work = tokio::spawn(async move {
        let _operation = operation;
        provision(state, node_id, id, name).await
    });
    // Dropping JoinHandle detaches authorized work; it cannot cancel between
    // authoritative publication and its local launch/configuration update.
    tokio::time::timeout(std::time::Duration::from_secs(8), work).await
        .map_err(|_| err_502("DiskGroup creation outcome unknown after 8 seconds. Refresh the Node and reconcile this ID before retrying."))?
        .map_err(|error| err_500(format!("DiskGroup provisioning task failed: {error}")))?
}

async fn provision(
    state: AppState,
    node_id: NodeId,
    id: DiskGroupId,
    name: String,
) -> Result<(StatusCode, Json<DiskGroupEntry>), Failure> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let existing = ctx
        .config()
        .disk_groups
        .iter()
        .find(|entry| entry.node_id == node_id && entry.id == id)
        .cloned();
    let entry = if let Some(entry) = existing {
        if entry.name != name {
            return Err(err_409("DiskGroup ID already has a different name"));
        }
        entry
    } else {
        ops::hardware::add_disk_group(&ctx, node_id, id, &name)
            .await
            .map_err(map_config_err)?
    };
    state.commit_op_context(&ctx).map_err(map_persist_err)?;

    let hw = crate::mgmt::build_hardware_client(&state)
        .await
        .ok_or_else(|| err_502("no group-0 endpoint; disk-group owner cannot be assigned"))?;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        auto_assign_owner(&hw, entry.rack_id, node_id, entry.id),
    )
    .await
    .map_err(|_| err_502("DiskGroup exists; owner assignment outcome unknown. Refresh before retrying."))?
    .map_err(|error| err_502(format!("DiskGroup exists; auto-assign owner: {error}")))?;

    Ok((StatusCode::CREATED, Json(entry)))
}

async fn auto_assign_owner(
    hw: &crowdb_kv_client::HardwareClient,
    rack_id: RackId,
    node_id: NodeId,
    dg_id: DiskGroupId,
) -> Result<(), String> {
    crate::owner_assignment::ensure_data_binding(hw, rack_id, node_id, dg_id).await?;
    let svc = crowdb_kv_client::ServiceRegistryClient::from_shared(hw.shared_kv());
    let instances = svc
        .read_all_diskdb_instances()
        .await
        .map_err(|e| format!("read_all_diskdb_instances: {e}"))?;
    if instances.is_empty() {
        return Err("DiskGroup binding exists; deploy a registered DiskDB service, then reconcile creation to assign its owner".into());
    }
    let owners = hw.list_owners().await.map_err(|e| format!("list_owners: {e}"))?;
    let instance_ids: Vec<u64> = instances.iter().map(|(id, _)| *id).collect();
    let instance_id = owners
        .iter()
        .find(|owner| owner.rack_id == rack_id && owner.node_id == node_id && owner.dg_id == dg_id)
        .map(|owner| owner.instance_id)
        .or_else(|| crate::owner_assignment::pick_least_loaded_instance(&instance_ids, &owners))
        .ok_or_else(|| "no eligible diskdb instance".to_string())?;
    // Lease = 1 hour from now (the diskdb keepalive will refresh it).
    #[allow(clippy::cast_possible_truncation)]
    let lease_expiry_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
        + 3_600_000;
    let disk_group = hw
        .get_disk_group(rack_id, node_id, dg_id)
        .await
        .map_err(|e| format!("get_disk_group: {e}"))?
        .ok_or_else(|| format!("disk-group {dg_id} missing from group 0"))?;
    hw.add_disk_group_with_owner(
        rack_id,
        node_id,
        dg_id,
        &disk_group.value,
        instance_id,
        lease_expiry_ms,
    )
    .await
    .map_err(|e| format!("add_disk_group_with_owner: {e}"))?;
    tracing::info!(dg_id, instance_id, "auto-assign: assigned DG to diskdb instance");
    Ok(())
}
