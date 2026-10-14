// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish a durably prepared file through the existing Group-0 authority.

use std::path::Path;

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    disk_storage::DiskStorageRecord,
    diskdb::rpc::DiskValue,
    mgmt::node::{
        storage::{DiskBackend, FilePreparation, FilePreparationPhase, PrepareDiskFile},
        NodeBinding,
    },
};

use super::{durable, Result};
use crate::node::storage::file_preparation as files;

pub(super) fn authority(root: &Path, request: &PrepareDiskFile) -> Result<HardwareClient> {
    let binding: NodeBinding = durable::read(&root.join("node-binding.json"))?
        .ok_or("file admission is waiting for Group 0 binding")?;
    if binding.node_id != request.node_id || binding.bootstrap.cluster_id != request.cluster_id {
        return Err("file admission belongs to another node or cluster".into());
    }
    Ok(HardwareClient::new(CrowdbKvClient::new(ClientConfig::new(
        binding.management_seeds,
    ))))
}

pub(super) async fn validate_placement(root: &Path, request: &PrepareDiskFile) -> Result<()> {
    let Some(placement) = &request.admission else {
        return Ok(());
    };
    let hardware = authority(root, request)?;
    let node = hardware
        .get_node(placement.rack_id, request.node_id)
        .await?
        .ok_or("file admission node is absent from Group 0")?;
    if !node.disk_group_ids.contains(&placement.disk_group_id)
        || hardware
            .get_disk_group(placement.rack_id, request.node_id, placement.disk_group_id)
            .await?
            .is_none()
    {
        return Err("file admission requires an existing DiskGroup on this node".into());
    }
    Ok(())
}

pub(super) async fn publish(root: &Path, state: &FilePreparation) -> Result<()> {
    if state.request.admission.is_none() {
        return Ok(());
    }
    let record = record(state)?;
    files::outcome(
        root,
        &state.request.operation_id,
        FilePreparationPhase::Publishing,
        None,
    )?;
    authority(root, &state.request)?
        .admit_disk_storage(&record)
        .await?;
    files::outcome(
        root,
        &state.request.operation_id,
        FilePreparationPhase::WaitingForDiskIO,
        None,
    )?;
    super::file_activation::activate(root, state, &record).await?;
    Ok(())
}

fn record(state: &FilePreparation) -> Result<DiskStorageRecord> {
    let request = &state.request;
    let placement = request
        .admission
        .as_ref()
        .ok_or("file admission placement missing")?;
    if !state.preparation_complete {
        return Err("file admission requires completed preparation".into());
    }
    let (high, low) = uuid::Uuid::parse_str(&request.operation_id)?.as_u64_pair();
    let unit = u64::from(placement.unit_size_bytes);
    let record = DiskStorageRecord {
        version: 1,
        operation_id: request.operation_id.clone(),
        rack_id: placement.rack_id,
        node_id: request.node_id,
        disk_group_id: placement.disk_group_id,
        disk_id: DiskId { high, low },
        backend: DiskBackend::File,
        backing_identity: state
            .backing_identity
            .clone()
            .ok_or("prepared file backing identity missing")?,
        media_type: placement.media_type,
        sparse: request.sparse,
        geometry: DiskValue {
            disk_type: placement.media_type.unwrap_or_default(),
            capacity_units: request.capacity_bytes / unit,
            zone_size_units: placement.zone_size_bytes / unit,
            unit_size_bytes: placement.unit_size_bytes,
            zone_count: u32::try_from(request.capacity_bytes / placement.zone_size_bytes)?,
            status: HwStatus::Init as i32,
            device_path: state.path.clone(),
        },
        removed: false,
    };
    record.validate()?;
    Ok(record)
}
