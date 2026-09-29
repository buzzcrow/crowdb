// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Rack-fenced disk-group and disk mutations in Group 0.

use crowdb_kv_client::{BatchOp, Error as KvError, GetOutcome, ReadMode};
use crowdb_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskValue};
use crowdb_protocol::key::{DiskGroupKey, DiskKey, NodeKey, RackKey, TextKey};
use crowdb_protocol::DiskIdExt;

use crate::config::{DiskEntry, DiskGroupEntry};
use crate::error::{Error, Result};
use crate::ops::hardware::{authority, validate_disk_input, AddDiskInput};
use crate::ops::OpContext;

const RETRIES: u64 = 10;

async fn rack_revision(ctx: &OpContext, rack_id: u64) -> Result<(String, RackValue, u64)> {
    let path = RackKey { rack_id }.to_path();
    let (value, revision) = required::<RackValue>(ctx, &path, "rack", rack_id.to_string()).await?;
    Ok((path, value, revision))
}

async fn required<T: serde::de::DeserializeOwned>(
    ctx: &OpContext,
    path: &str,
    kind: &str,
    id: String,
) -> Result<(T, u64)> {
    match ctx
        .kv()
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await?
    {
        GetOutcome::Found { value, revision, .. } => Ok((
            serde_json::from_slice(&value).map_err(|error| Error::Config(error.to_string()))?,
            revision,
        )),
        GetOutcome::NotFound => Err(Error::NotFound {
            kind: kind.into(),
            id,
        }),
    }
}

async fn optional<T: serde::de::DeserializeOwned>(ctx: &OpContext, path: &str) -> Result<Option<T>> {
    match ctx
        .kv()
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await?
    {
        GetOutcome::Found { value, .. } => Ok(Some(
            serde_json::from_slice(&value).map_err(|error| Error::Config(error.to_string()))?,
        )),
        GetOutcome::NotFound => Ok(None),
    }
}

fn put<T: serde::Serialize>(path: &str, value: &T) -> Result<BatchOp> {
    let encoded = serde_json::to_vec(value).map_err(|error| Error::Config(error.to_string()))?;
    Ok(BatchOp::Put {
        key: path.as_bytes().to_vec().into(),
        value: encoded.into(),
    })
}

fn delete(path: &str) -> BatchOp {
    BatchOp::Delete {
        key: path.as_bytes().to_vec().into(),
    }
}

async fn fenced(
    ctx: &OpContext,
    rack_path: &str,
    rack: &RackValue,
    revision: u64,
    mut ops: Vec<BatchOp>,
) -> Result<bool> {
    ops.insert(0, put(rack_path, rack)?);
    match ctx
        .kv()
        .batch_write_cas(0, 0, &ops, rack_path.as_bytes(), revision)
        .await
    {
        Ok(_) => Ok(true),
        Err(KvError::CasFailed { .. } | KvError::CasBusy | KvError::OutcomeUnknown) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn pause(attempt: u64) {
    tokio::time::sleep(std::time::Duration::from_millis((attempt + 1) * 5)).await;
}

fn node_location(nodes: &[(u64, u64, NodeValue)], node_id: u64) -> Result<u64> {
    nodes
        .iter()
        .find_map(|(rack, id, _)| (*id == node_id).then_some(*rack))
        .ok_or_else(|| Error::NotFound {
            kind: "node".into(),
            id: node_id.to_string(),
        })
}

/// Create a disk group and update its node membership in one confirmed write.
///
/// # Errors
/// Returns an authority error, a missing node, or a conflicting group.
pub async fn add_disk_group_to_group0(
    ctx: &OpContext,
    node_id: u64,
    dg_id: u64,
    name: &str,
) -> Result<DiskGroupEntry> {
    authority::ready(ctx).await?;
    let rack_id = node_location(&ctx.sysmd().list_nodes().await?, node_id)?;
    let entry = DiskGroupEntry {
        id: dg_id,
        rack_id,
        node_id,
        name: name.into(),
    };
    let node_path = NodeKey { rack_id, node_id }.to_path();
    let group_path = DiskGroupKey {
        rack_id,
        node_id,
        disk_group_id: dg_id,
    }
    .to_path();
    for attempt in 0..RETRIES {
        let (rack_path, rack, revision) = rack_revision(ctx, rack_id).await?;
        let (mut node, _) = required::<NodeValue>(ctx, &node_path, "node", node_id.to_string()).await?;
        let current = optional::<DiskGroupValue>(ctx, &group_path).await?;
        if let Some(group) = current {
            if group.name != name || !node.disk_group_ids.contains(&dg_id) {
                return Err(Error::Conflict {
                    kind: "disk_group".into(),
                    id: dg_id.to_string(),
                });
            }
            return Ok(entry);
        }
        if node.disk_group_ids.contains(&dg_id) {
            return Err(Error::Conflict {
                kind: "disk_group membership".into(),
                id: dg_id.to_string(),
            });
        }
        node.disk_group_ids.push(dg_id);
        node.disk_group_ids.sort_unstable();
        node.last_used_dg_id = node.last_used_dg_id.max(dg_id);
        let group = DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: Vec::new(),
            name: name.into(),
        };
        if fenced(
            ctx,
            &rack_path,
            &rack,
            revision,
            vec![put(&node_path, &node)?, put(&group_path, &group)?],
        )
        .await?
        {
            return Ok(entry);
        }
        pause(attempt).await;
    }
    Err(KvError::OutcomeUnknown.into())
}

/// Return confirmed disk groups on one node.
///
/// # Errors
/// Returns an authority error or a missing node.
pub async fn list_disk_groups_from_group0(ctx: &OpContext, node_id: u64) -> Result<Vec<DiskGroupEntry>> {
    authority::ready(ctx).await?;
    let rack_id = node_location(&ctx.sysmd().list_nodes().await?, node_id)?;
    let mut groups: Vec<_> = ctx
        .sysmd()
        .list_disk_groups_on_node(rack_id, node_id)
        .await?
        .into_iter()
        .map(|group| DiskGroupEntry {
            id: group.dg_id,
            rack_id,
            node_id,
            name: group.value.name,
        })
        .collect();
    groups.sort_unstable_by_key(|group| group.id);
    Ok(groups)
}

/// Delete an empty, unowned disk group and remove node membership atomically.
///
/// # Errors
/// Returns an authority error, missing record, or child/assignment conflict.
pub async fn remove_disk_group_from_group0(ctx: &OpContext, node_id: u64, dg_id: u64) -> Result<()> {
    authority::ready(ctx).await?;
    let rack_id = node_location(&ctx.sysmd().list_nodes().await?, node_id)?;
    let node_path = NodeKey { rack_id, node_id }.to_path();
    let group_path = DiskGroupKey {
        rack_id,
        node_id,
        disk_group_id: dg_id,
    }
    .to_path();
    let mut uncertain = false;
    for attempt in 0..RETRIES {
        let (rack_path, rack, revision) = rack_revision(ctx, rack_id).await?;
        let (mut node, _) = required::<NodeValue>(ctx, &node_path, "node", node_id.to_string()).await?;
        let group = optional::<DiskGroupValue>(ctx, &group_path).await?;
        if group.is_none() && uncertain && !node.disk_group_ids.contains(&dg_id) {
            return Ok(());
        }
        let group = group.ok_or_else(|| Error::NotFound {
            kind: "disk_group".into(),
            id: dg_id.to_string(),
        })?;
        if !group.disk_ids.is_empty()
            || !ctx
                .sysmd()
                .list_disks_in_group(rack_id, node_id, dg_id)
                .await?
                .is_empty()
            || ctx.sysmd().get_owner(rack_id, node_id, dg_id).await?.is_some()
            || ctx.sysmd().get_bind(rack_id, node_id, dg_id).await?.is_some()
        {
            return Err(Error::Conflict {
                kind: "disk_group with children or assignment".into(),
                id: dg_id.to_string(),
            });
        }
        if !node.disk_group_ids.contains(&dg_id) {
            return Err(Error::Conflict {
                kind: "disk_group membership".into(),
                id: dg_id.to_string(),
            });
        }
        node.disk_group_ids.retain(|id| *id != dg_id);
        if fenced(
            ctx,
            &rack_path,
            &rack,
            revision,
            vec![put(&node_path, &node)?, delete(&group_path)],
        )
        .await?
        {
            return Ok(());
        }
        uncertain = true;
        pause(attempt).await;
    }
    Err(KvError::OutcomeUnknown.into())
}

/// Add a validated disk and update its group membership in one confirmed write.
///
/// # Errors
/// Returns a validation, authority, missing group, or conflicting disk error.
pub async fn add_disk_to_group0(
    ctx: &OpContext,
    node_id: u64,
    dg_id: u64,
    input: &AddDiskInput,
) -> Result<DiskEntry> {
    authority::ready(ctx).await?;
    let rack_id = node_location(&ctx.sysmd().list_nodes().await?, node_id)?;
    let (entry, disk_id, value) = validate_disk_input(input, dg_id, rack_id, node_id)?;
    let group_path = DiskGroupKey {
        rack_id,
        node_id,
        disk_group_id: dg_id,
    }
    .to_path();
    let disk_path = DiskKey {
        rack_id,
        node_id,
        disk_group_id: dg_id,
        disk_id,
    }
    .to_path();
    for attempt in 0..RETRIES {
        let (rack_path, rack, revision) = rack_revision(ctx, rack_id).await?;
        let (mut group, _) =
            required::<DiskGroupValue>(ctx, &group_path, "disk_group", dg_id.to_string()).await?;
        if let Some(actual) = optional::<DiskValue>(ctx, &disk_path).await? {
            if actual == value && group.disk_ids.contains(&disk_id) {
                return Ok(entry);
            }
            return Err(Error::Conflict {
                kind: "disk".into(),
                id: input.disk_id.clone(),
            });
        }
        if group.disk_ids.contains(&disk_id) {
            return Err(Error::Conflict {
                kind: "disk membership".into(),
                id: input.disk_id.clone(),
            });
        }
        group.disk_ids.push(disk_id);
        group.disk_ids.sort_unstable_by_key(|id| (id.high, id.low));
        if fenced(
            ctx,
            &rack_path,
            &rack,
            revision,
            vec![put(&group_path, &group)?, put(&disk_path, &value)?],
        )
        .await?
        {
            return Ok(entry);
        }
        pause(attempt).await;
    }
    Err(KvError::OutcomeUnknown.into())
}

/// Return confirmed disks on one node and disk group.
///
/// # Errors
/// Returns an authority error or a missing node.
pub async fn list_disks_from_group0(ctx: &OpContext, node_id: u64, dg_id: u64) -> Result<Vec<DiskEntry>> {
    authority::ready(ctx).await?;
    let rack_id = node_location(&ctx.sysmd().list_nodes().await?, node_id)?;
    let mut disks = Vec::new();
    for (disk_id, value) in ctx.sysmd().list_disks_in_group(rack_id, node_id, dg_id).await? {
        disks.push(disk_entry(rack_id, node_id, dg_id, disk_id, &value));
    }
    disks.sort_unstable_by(|a, b| a.disk_id.cmp(&b.disk_id));
    Ok(disks)
}

/// Remove a disk and its group membership in one confirmed write.
///
/// # Errors
/// Returns an authority error, missing disk, or membership conflict.
pub async fn remove_disk_from_group0(
    ctx: &OpContext,
    node_id: u64,
    dg_id: u64,
    disk_id: &str,
) -> Result<DiskEntry> {
    authority::ready(ctx).await?;
    let rack_id = node_location(&ctx.sysmd().list_nodes().await?, node_id)?;
    let id = DiskId::from_display_string(disk_id).map_err(|message| Error::Validation {
        field: "disk_id".into(),
        message,
    })?;
    let group_path = DiskGroupKey {
        rack_id,
        node_id,
        disk_group_id: dg_id,
    }
    .to_path();
    let disk_path = DiskKey {
        rack_id,
        node_id,
        disk_group_id: dg_id,
        disk_id: id,
    }
    .to_path();
    let mut removed = None;
    for attempt in 0..RETRIES {
        let (rack_path, rack, revision) = rack_revision(ctx, rack_id).await?;
        let (mut group, _) =
            required::<DiskGroupValue>(ctx, &group_path, "disk_group", dg_id.to_string()).await?;
        let current = optional::<DiskValue>(ctx, &disk_path).await?;
        if current.is_none() && !group.disk_ids.contains(&id) {
            return removed.ok_or_else(|| Error::NotFound {
                kind: "disk".into(),
                id: disk_id.into(),
            });
        }
        let value = current.ok_or_else(|| Error::Conflict {
            kind: "disk membership".into(),
            id: disk_id.into(),
        })?;
        if !group.disk_ids.contains(&id) {
            return Err(Error::Conflict {
                kind: "disk membership".into(),
                id: disk_id.into(),
            });
        }
        let entry = disk_entry(rack_id, node_id, dg_id, id, &value);
        group.disk_ids.retain(|candidate| *candidate != id);
        if fenced(
            ctx,
            &rack_path,
            &rack,
            revision,
            vec![put(&group_path, &group)?, delete(&disk_path)],
        )
        .await?
        {
            return Ok(entry);
        }
        removed = Some(entry);
        pause(attempt).await;
    }
    Err(KvError::OutcomeUnknown.into())
}

fn disk_entry(rack_id: u64, node_id: u64, dg_id: u64, disk_id: DiskId, value: &DiskValue) -> DiskEntry {
    DiskEntry {
        disk_id: disk_id.to_display_string(),
        rack_id,
        node_id,
        disk_group_id: dg_id,
        disk_type: match crowdb_protocol::diskdb::rpc::DiskType::try_from(value.disk_type) {
            Ok(kind) => format!("{kind:?}"),
            Err(()) => value.disk_type.to_string(),
        },
        capacity_bytes: value
            .capacity_units
            .saturating_mul(u64::from(value.unit_size_bytes)),
        zone_size_bytes: value
            .zone_size_units
            .saturating_mul(u64::from(value.unit_size_bytes)),
        unit_size_bytes: value.unit_size_bytes,
        device_path: value.device_path.clone(),
    }
}
