// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Verify before Init; reconcile publication before any repeated blank check.

use super::{authorize, journal, Result};
use crate::node::storage::blank;
use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, OperationOptions, SegmentTarget};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
use crowdb_protocol::{
    common::HwStatus,
    key::DiskKey,
    mgmt::node::storage::{SuppliedPreparation, SuppliedPreparationPhase},
};
use std::{path::Path, time::Duration};

pub(super) async fn run(root: &Path, id: &str) -> Result<()> {
    run_with_inputs(root, id, journal::inputs()?).await
}

pub(super) async fn run_with_inputs(
    root: &Path,
    id: &str,
    inputs: Vec<crowdb_protocol::mgmt::node::storage::DiskInput>,
) -> Result<()> {
    let mut state = journal::status(root, id)?;
    let binding = authorize(root, &state.request).await?;
    let retirement = super::retirement(root, &binding);
    let hardware = HardwareClient::new(CrowdbKvClient::new(ClientConfig::new(
        binding.management_seeds.clone(),
    )));
    let record = &state.request.record;
    let key = DiskKey {
        rack_id: record.rack_id,
        node_id: record.node_id,
        disk_group_id: record.disk_group_id,
        disk_id: record.disk_id,
    };
    hardware
        .validate_disk_group_storage(
            record.rack_id,
            record.node_id,
            record.disk_group_id,
            record.media_type,
            record.geometry.unit_size_bytes,
        )
        .await?;
    hardware.reserve_disk_resource(record).await?;
    match hardware.disk_storage(&key).await? {
        Some(retained) if retained == *record => {}
        Some(_) => {
            return Err("supplied medium conflicts with retained backing; use explicit recovery".into())
        }
        None => {
            if hardware.get_disk_with_revision(&key).await?.is_some() {
                return Err("supplied medium has legacy membership; explicit migration is required".into());
            }
            let mut medium = verify_medium(root, &state, inputs, &retirement).await?;
            // Keep the claimed descriptor through the authority write. DiskIO
            // acquires its own descriptor only after this object is dropped.
            authorize(root, &state.request).await?;
            medium.unchanged()?;
            state.phase = SuppliedPreparationPhase::Publishing;
            journal::save(root, &state)?;
            hardware.admit_disk_storage(record).await?;
            drop(medium);
        }
    }
    let actual = hardware
        .get_disk_with_revision(&key)
        .await?
        .ok_or("supplied membership missing")?
        .0;
    if actual.status == HwStatus::Up as i32 {
        hardware.activate_disk_storage(record).await?;
        state.phase = SuppliedPreparationPhase::Registered;
        state.error = None;
        return Ok(journal::save(root, &state)?);
    }
    if actual.status != HwStatus::Init as i32 {
        return Err("supplied membership is excluded; explicit validated Online recovery is required".into());
    }
    state.phase = SuppliedPreparationPhase::WaitingForDiskio;
    state.verified_bytes = 0;
    journal::save(root, &state)?;
    match verify_diskio(root, &mut state, binding.management_seeds, &retirement).await {
        Ok(()) => {}
        Err(error)
            if error
                .downcast_ref::<crowdb_diskio_client::DiskioError>()
                .is_some_and(|error| {
                    error.is_retryable_read()
                        || matches!(error, crowdb_diskio_client::DiskioError::TopologyUnavailable(_))
                }) =>
        {
            state.error = Some(error.to_string().chars().take(128).collect());
            return Ok(journal::save(root, &state)?);
        }
        Err(error) => return Err(error),
    }
    authorize(root, &state.request).await?;
    hardware.activate_disk_storage(&state.request.record).await?;
    state.phase = SuppliedPreparationPhase::Registered;
    state.error = None;
    Ok(journal::save(root, &state)?)
}

async fn verify_diskio(
    root: &Path,
    state: &mut SuppliedPreparation,
    seeds: Vec<String>,
    retirement: &Path,
) -> Result<()> {
    let record = state.request.record.clone();
    let client = DiskioClient::connect_disk(
        DiskioClientConfig {
            management_seeds: seeds,
            normal_connections_per_endpoint: 1,
            priority_connections_per_endpoint: 1,
            rpc_workers: 1,
            max_pending_calls: 8,
            send_queue_capacity: 16,
            retry_attempts: 1,
            default_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        DiskKey {
            rack_id: record.rack_id,
            node_id: record.node_id,
            disk_group_id: record.disk_group_id,
            disk_id: record.disk_id,
        },
    )
    .await?;
    let unit = record.geometry.unit_size_bytes;
    let mut last = 0;
    for zone in 0..record.geometry.zone_count {
        for offset in 0..record.geometry.zone_size_units {
            if retirement.exists() {
                return Err("supplied provisioning retired by explicit cleanup".into());
            }
            let target = SegmentTarget::new(
                DiskId::new(record.disk_id.high, record.disk_id.low),
                zone,
                offset,
                1,
                unit,
            )?;
            let bytes = client
                .read(target, 0, unit, OperationOptions::within(Duration::from_secs(1)))
                .await?;
            if bytes.len() != unit as usize || bytes.iter().any(|byte| *byte != 0) {
                return Err("DiskIO usable-byte verification failed; medium is not blank".into());
            }
            state.verified_bytes += u64::from(unit);
            if state.verified_bytes - last >= 64 * 1024 * 1024 {
                journal::save(root, state)?;
                last = state.verified_bytes;
            }
        }
    }
    Ok(())
}

async fn verify_medium(
    root: &Path,
    state: &SuppliedPreparation,
    inputs: Vec<crowdb_protocol::mgmt::node::storage::DiskInput>,
    retirement: &Path,
) -> Result<blank::VerifiedMedium> {
    let execution_root = root.to_path_buf();
    let retired = retirement.to_path_buf();
    let mut verifying = state.clone();
    tokio::task::spawn_blocking(move || -> Result<_> {
        verifying.phase = SuppliedPreparationPhase::Verifying;
        verifying.error = None;
        verifying.verified_bytes = 0;
        journal::save(&execution_root, &verifying)?;
        let mut last = 0;
        let medium = blank::verify(
            &execution_root,
            &inputs,
            &verifying.request.record.clone(),
            |count| {
                if retired.exists() {
                    return Err(std::io::Error::other(
                        "supplied provisioning retired by explicit cleanup",
                    ));
                }
                if count == 0 || count - last >= 64 * 1024 * 1024 {
                    verifying.verified_bytes = count;
                    journal::save(&execution_root, &verifying)?;
                    last = count;
                }
                Ok(())
            },
        )?;
        Ok(medium)
    })
    .await?
}
