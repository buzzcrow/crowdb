// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish a durably prepared file through the existing Group-0 authority.

use std::path::Path;

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    disk_storage::{DiskStorageRecord, PhysicalBacking},
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
    hardware
        .validate_disk_group_storage(
            placement.rack_id,
            request.node_id,
            placement.disk_group_id,
            placement.media_type,
            placement.unit_size_bytes,
        )
        .await?;
    Ok(())
}

pub(super) async fn publish(root: &Path, state: &FilePreparation) -> Result<()> {
    if state.request.admission.is_none() {
        return Ok(());
    }
    let hardware = authority(root, &state.request)?;
    let record = verified_record(&hardware, state).await?;
    files::outcome(
        root,
        &state.request.operation_id,
        FilePreparationPhase::Publishing,
        None,
    )?;
    hardware.admit_disk_storage(&record).await?;
    files::outcome(
        root,
        &state.request.operation_id,
        FilePreparationPhase::WaitingForDiskIO,
        None,
    )?;
    super::file_activation::activate(root, state, &record).await?;
    Ok(())
}

async fn verified_record(hardware: &HardwareClient, state: &FilePreparation) -> Result<DiskStorageRecord> {
    let mut record = record(state)?;
    let key = crowdb_protocol::key::DiskKey {
        rack_id: record.rack_id,
        node_id: record.node_id,
        disk_group_id: record.disk_group_id,
        disk_id: record.disk_id,
    };
    if hardware
        .disk_storage(&key)
        .await?
        .is_some_and(|existing| existing.version == 1)
    {
        return Ok(record);
    }
    let (registry, _) = crowdb_console_shared::deployment::registry::read(hardware.kv())
        .await?
        .ok_or("managed file is waiting for node host authority")?;
    let node = registry
        .nodes
        .iter()
        .find(|node| {
            node.node_id == record.node_id
                && node.rack_id == record.rack_id
                && node.confirmed
                && !node.cancelled
        })
        .ok_or("managed file is waiting for confirmed node host")?;
    record.version = 2;
    record.physical_backing = Some(PhysicalBacking {
        physical_host_id: node.physical_host_id.clone(),
        resource_identity: record.backing_identity.clone(),
        capacity_bytes: state.request.capacity_bytes,
    });
    record.validate()?;
    Ok(record)
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
        physical_backing: None,
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

#[cfg(feature = "test-util")]
/// # Errors
/// Runs production Group-0 publication and `DiskIO` availability reconciliation.
pub async fn publish_file_for_tests(root: &Path, operation: &str) -> Result<FilePreparation> {
    let state = files::status(root, operation)?;
    publish(root, &state).await?;
    Ok(files::status(root, operation)?)
}
