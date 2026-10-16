// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Verify bound authority before inspecting an explicitly retained disk.
use super::{durable, Result};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
use crowdb_protocol::{
    common::HwStatus,
    disk_storage::DiskStorageRecord,
    key::DiskKey,
    mgmt::node::{
        storage::{CheckRetainedDisk, RetainedDiskCondition, RetainedMedium},
        NodeBinding,
    },
    DiskIdExt,
};
use std::path::Path;

pub(super) async fn check(root: &Path, request: CheckRetainedDisk) -> Result<RetainedMedium> {
    let binding: NodeBinding = durable::read(&root.join("node-binding.json"))?
        .ok_or("retained check is waiting for Group 0 binding")?;
    if request.version != 1
        || binding.node_id != request.node_id
        || binding.bootstrap.cluster_id != request.cluster_id
    {
        return Err("retained check belongs to another schema, node or cluster".into());
    }
    let disk_id = crowdb_protocol::common::DiskId::from_display_string(&request.disk_id)?;
    if disk_id.to_display_string() != request.disk_id {
        return Err("retained disk ID is not canonical".into());
    }
    let key = DiskKey {
        rack_id: request.rack_id,
        node_id: request.node_id,
        disk_group_id: request.disk_group_id,
        disk_id,
    };
    let hardware = HardwareClient::new(CrowdbKvClient::new(ClientConfig::new(binding.management_seeds)));
    let nodes = hardware.list_nodes().await?;
    let locations: Vec<_> = nodes.iter().filter(|(_, id, _)| *id == request.node_id).collect();
    if locations.len() != 1 || locations[0].0 != request.rack_id {
        return Err("retained node placement is absent or ambiguous".into());
    }
    let record = hardware
        .disk_storage(&key)
        .await?
        .ok_or("retained backing is absent")?;
    let membership = hardware.get_disk_with_revision(&key).await?;
    validate(
        &hardware,
        &key,
        &record,
        membership.as_ref().map(|(_, revision)| *revision),
        &request.condition,
    )
    .await?;
    let inputs = std::env::var("CROWDB_DISK_INPUTS").unwrap_or_else(|_| "[]".into());
    let observation =
        super::super::storage::retained::check(root, &serde_json::from_str::<Vec<_>>(&inputs)?, &record)?;
    if hardware.disk_storage(&key).await?.as_ref() != Some(&record)
        || hardware.get_disk_with_revision(&key).await? != membership
    {
        return Err("retained authority changed during the medium check".into());
    }
    Ok(observation)
}

async fn validate(
    hw: &HardwareClient,
    key: &DiskKey,
    record: &DiskStorageRecord,
    revision: Option<u64>,
    condition: &RetainedDiskCondition,
) -> Result<()> {
    match condition {
        RetainedDiskCondition::Removed { operation_id } => {
            let removal = hw
                .disk_removal_receipt(key)
                .await?
                .filter(|receipt| receipt.operation_id == *operation_id)
                .ok_or("retained removal operation changed")?;
            let mut expected = removal.disk;
            expected.removed = record.removed;
            if expected != *record {
                return Err("retained removal geometry changed".into());
            }
            if record.removed && revision.is_some() {
                return Err("removed medium still has membership".into());
            }
            if !record.removed && hw.disk_rejoin_receipt(key).await?.is_none() {
                return Err("retained medium was not explicitly rejoined".into());
            }
        }
        RetainedDiskCondition::Offline { revision: expected } => {
            let text = expected;
            let expected = text.parse::<u64>()?;
            if expected.to_string() != *text {
                return Err("retained disk revision is not canonical".into());
            }
            let current = hw
                .get_disk_with_revision(key)
                .await?
                .ok_or("retained disk membership is absent")?;
            if record.removed
                || expected == 0
                || revision != Some(expected)
                || current.0.status != HwStatus::Offline as i32
            {
                return Err("retained check requires the exact Offline revision".into());
            }
        }
    }
    Ok(())
}
