// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A read through `DiskIO` verifies availability before initial Online publication.

use std::{path::Path, time::Duration};

use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, OperationOptions, SegmentTarget};
use crowdb_protocol::{
    disk_storage::DiskStorageRecord,
    mgmt::node::{
        storage::{FilePreparation, FilePreparationPhase},
        NodeBinding,
    },
};

use super::{durable, Result};
use crate::node::storage::file_preparation as files;

pub(super) async fn activate(root: &Path, state: &FilePreparation, record: &DiskStorageRecord) -> Result<()> {
    let hardware = super::file_admission::authority(root, &state.request)?;
    let actual = hardware
        .get_disk(
            record.rack_id,
            record.node_id,
            record.disk_group_id,
            &record.disk_id,
        )
        .await?
        .ok_or("disk activation membership missing")?;
    if !matches!(
        crowdb_protocol::common::HwStatus::try_from(actual.status),
        Ok(crowdb_protocol::common::HwStatus::Init | crowdb_protocol::common::HwStatus::Up)
    ) {
        files::outcome(
            root,
            &state.request.operation_id,
            FilePreparationPhase::Failed,
            Some("Disk health or Offline exclusion requires explicit validated recovery".into()),
        )?;
        return Ok(());
    }
    let binding: NodeBinding =
        durable::read(&root.join("node-binding.json"))?.ok_or("disk activation binding missing")?;
    let result = tokio::time::timeout(Duration::from_secs(3), probe(record, binding.management_seeds)).await;
    let error = match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("Waiting for DiskIO: {error}")),
        Err(_) => Some("Waiting for DiskIO: availability probe timed out".into()),
    };
    if let Some(error) = error {
        files::outcome(
            root,
            &state.request.operation_id,
            FilePreparationPhase::WaitingForDiskIO,
            Some(error),
        )?;
        return Ok(());
    }
    hardware.activate_disk_storage(record).await?;
    files::outcome(
        root,
        &state.request.operation_id,
        FilePreparationPhase::Registered,
        None,
    )?;
    Ok(())
}

async fn probe(record: &DiskStorageRecord, management_seeds: Vec<String>) -> Result<()> {
    let client = DiskioClient::connect_disk(
        DiskioClientConfig {
            management_seeds,
            normal_connections_per_endpoint: 1,
            priority_connections_per_endpoint: 1,
            rpc_workers: 1,
            max_pending_calls: 8,
            send_queue_capacity: 16,
            retry_attempts: 1,
            default_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        crowdb_protocol::key::DiskKey {
            rack_id: record.rack_id,
            node_id: record.node_id,
            disk_group_id: record.disk_group_id,
            disk_id: record.disk_id,
        },
    )
    .await?;
    let target = SegmentTarget::new(
        DiskId::new(record.disk_id.high, record.disk_id.low),
        0,
        0,
        1,
        4096,
    )?;
    let bytes = client
        .read(target, 0, 4096, OperationOptions::within(Duration::from_secs(1)))
        .await?;
    if bytes.len() != 4096 {
        return Err("disk availability probe returned a short read".into());
    }
    Ok(())
}
