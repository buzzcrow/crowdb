// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Seal each selected medium's placement before dispatching monitor-owned work.

use super::{
    journal::{self, AcceptedSetup},
    storage, Result,
};
use crate::DeploymentProfile;
use crowdb_kv_client::{CrowdbKvClient, HardwareClient};
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    disk_storage::{DiskStorageRecord, PhysicalBacking},
    diskdb::rpc::DiskValue,
    mgmt::node::{
        setup::{NodeSetupPhase, SetupDiskStatus, SetupStepPhase, SetupStorage},
        storage::{PrepareSuppliedDisk, SuppliedPreparationPhase},
        NodeBinding, NodeControl,
    },
    DiskIdExt,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub(super) async fn advance(
    profile: &DeploymentProfile,
    state: &mut AcceptedSetup,
    binding: &NodeBinding,
    kv: &Arc<CrowdbKvClient>,
) -> Result<bool> {
    if state.supplied.is_empty() {
        prepare(profile, state, binding, kv).await?;
    }
    let root = &profile.paths.data_root;
    let mut steps = Vec::new();
    let mut ready = true;
    for accepted in state.supplied.clone() {
        let record = &accepted.request.record;
        storage::ensure_group(kv, binding, record.disk_group_id, Some(accepted.binding)).await?;
        let path = crate::node::storage::supplied::path(root, &record.operation_id)?;
        let current = if path.try_exists()? {
            Some(crate::node::storage::supplied::status(
                root,
                &record.operation_id,
            )?)
        } else {
            None
        };
        if current
            .as_ref()
            .is_some_and(|current| current.request != accepted.request)
        {
            return Err("supplied preparation differs from sealed node setup".into());
        }
        let retry = state.retry_disks.remove(&record.operation_id);
        let mut phase = match current.as_ref().map(|current| current.phase) {
            Some(SuppliedPreparationPhase::Registered) => SetupStepPhase::Ready,
            Some(SuppliedPreparationPhase::Failed) if !retry => SetupStepPhase::Failed,
            _ => SetupStepPhase::Preparing,
        };
        if current.is_none() || retry {
            // Consume explicit retry before dispatch; a lost reply reconciles the
            // same operation, never creates an unbounded automatic failure loop.
            journal::save(root, state)?;
            super::super::super::super::control_node(
                &profile.paths.run_root.join("node-control.sock"),
                &NodeControl::PrepareSuppliedDisk {
                    request: accepted.request.clone(),
                },
            )
            .await?;
            phase = SetupStepPhase::Preparing;
        }
        ready &= phase == SetupStepPhase::Ready;
        steps.push(SetupDiskStatus {
            path: record.geometry.device_path.clone(),
            phase,
            detail: current
                .and_then(|current| current.error)
                .map(|error| journal::bounded(&error, 128)),
            operation_id: record.operation_id.clone(),
            disk_id: Some(record.disk_id.to_display_string()),
        });
    }
    state.status.disks = steps;
    if state
        .status
        .disks
        .iter()
        .any(|step| step.phase == SetupStepPhase::Failed)
    {
        state.status.phase = NodeSetupPhase::Failed;
        return Err("supplied disk preparation failed; inspect each disk and explicitly retry".into());
    }
    Ok(ready)
}

async fn prepare(
    profile: &DeploymentProfile,
    state: &mut AcceptedSetup,
    binding: &NodeBinding,
    kv: &Arc<CrowdbKvClient>,
) -> Result<()> {
    let SetupStorage::Supplied { disks } = &state.request.choices.storage else {
        return Err("supplied setup choices missing".into());
    };
    if state.supplied_observations.len() != disks.len() {
        return Err(
            "accepted setup lacks sealed media lineage; explicit migration/review is required".into(),
        );
    }
    let (registry, _) = crowdb_console_shared::deployment::registry::read(kv)
        .await?
        .ok_or("node host authority missing")?;
    let node = registry
        .nodes
        .iter()
        .find(|node| node.node_id == binding.node_id && node.confirmed && !node.cancelled)
        .ok_or("confirmed node host missing")?;
    let hardware = HardwareClient::from_shared(Arc::clone(kv));
    let mut groups: BTreeMap<Option<i32>, (u64, crowdb_protocol::common::BindMapValue)> = BTreeMap::new();
    let mut excluded = BTreeSet::new();
    let mut accepted = Vec::new();
    for medium in disks {
        let observed = state
            .supplied_observations
            .iter()
            .find(|disk| disk.path == medium.path)
            .ok_or("sealed media observation missing")?;
        let resource = observed
            .resource_identity
            .clone()
            .ok_or("sealed media lineage missing")?;
        let (group, metadata) = if let Some(group) = groups.get(&medium.media_type) {
            group.clone()
        } else {
            let group = storage::select_group(
                &hardware,
                kv,
                node.rack_id,
                binding,
                medium.media_type,
                &excluded,
                false,
            )
            .await?;
            excluded.insert(group.0);
            groups.insert(medium.media_type, group.clone());
            group
        };
        let unit = 1024 * 1024;
        let quantum = 64 * unit;
        let zone = (medium.capacity_bytes.min(32 * 1024 * 1024 * 1024) / quantum) * quantum;
        if zone < 64 * 1024 * 1024 {
            return Err("supplied medium is smaller than the minimum 64 MiB zone".into());
        }
        let zones = medium.capacity_bytes / zone;
        let operation = uuid::Uuid::new_v4();
        let (high, low) = operation.as_u64_pair();
        let request = PrepareSuppliedDisk {
            cluster_id: binding.bootstrap.cluster_id.clone(),
            initialize: medium.initialize,
            record: DiskStorageRecord {
                version: 2,
                operation_id: operation.to_string(),
                rack_id: node.rack_id,
                node_id: binding.node_id,
                disk_group_id: group,
                disk_id: DiskId { high, low },
                backend: medium.backend,
                backing_identity: medium.backing_identity.clone(),
                media_type: medium.media_type,
                sparse: medium.backend == crowdb_protocol::mgmt::node::storage::DiskBackend::File
                    && observed
                        .allocated_bytes
                        .is_some_and(|bytes| bytes < medium.capacity_bytes),
                removed: false,
                physical_backing: Some(PhysicalBacking {
                    physical_host_id: node.physical_host_id.clone(),
                    resource_identity: resource,
                    capacity_bytes: medium.capacity_bytes,
                }),
                geometry: DiskValue {
                    disk_type: medium.media_type.unwrap_or_default(),
                    capacity_units: (zone / unit) * zones,
                    zone_size_units: zone / unit,
                    unit_size_bytes: u32::try_from(unit)?,
                    zone_count: u32::try_from(zones)?,
                    status: HwStatus::Init as i32,
                    device_path: medium.path.clone(),
                },
            },
        };
        request.validate()?;
        accepted.push(super::super::journal::AcceptedSupplied {
            request,
            binding: metadata,
        });
    }
    state.supplied = accepted;
    journal::save(&profile.paths.data_root, state)
}
