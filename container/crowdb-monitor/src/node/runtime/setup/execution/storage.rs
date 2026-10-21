// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Default files retain their operation, placement and geometry across restarts.

use super::{journal, AcceptedSetup, Result};
use crate::DeploymentProfile;
use crowdb_kv_client::{CrowdbKvClient, HardwareClient};
use crowdb_protocol::mgmt::node::{
    setup::{SetupDiskStatus, SetupStepPhase, SetupStorage},
    storage::{DiskInput, FileDiskAdmission, FilePreparationPhase, PrepareDiskFile},
    NodeBinding, NodeControl,
};

pub(super) async fn advance(
    profile: &DeploymentProfile,
    state: &mut AcceptedSetup,
    binding: &NodeBinding,
    kv: &std::sync::Arc<CrowdbKvClient>,
) -> Result<bool> {
    let (capacity_bytes, reserve_bytes, sparse) = match &state.request.choices.storage {
        SetupStorage::Automatic {
            capacity_bytes,
            reserve_bytes,
            sparse,
        } => (*capacity_bytes, *reserve_bytes, *sparse),
        SetupStorage::Diskless => return Ok(true),
        SetupStorage::Supplied { .. } => return super::supplied::advance(profile, state, binding, kv).await,
    };
    let root = &profile.paths.data_root;
    if state.file.is_none() {
        let inputs: Vec<DiskInput> =
            serde_json::from_str(&std::env::var("CROWDB_DISK_INPUTS").unwrap_or_else(|_| "[]".into()))?;
        let inventory = crate::node::discover_disks(root, &inputs)?;
        if !inputs.is_empty() || !inventory.disks.is_empty() {
            return Err("existing or supplied media prevents automatic fallback file creation".into());
        }
        let hardware = HardwareClient::from_shared(std::sync::Arc::clone(kv));
        let rack = hardware
            .list_nodes()
            .await?
            .into_iter()
            .find(|(_, id, _)| *id == binding.node_id)
            .ok_or("node hardware is not published")?
            .0;
        let (id, data_binding) = select_group(
            &hardware,
            kv,
            rack,
            binding,
            None,
            &std::collections::BTreeSet::new(),
            true,
        )
        .await?;
        state.storage_binding = Some(data_binding);
        state.file = Some(PrepareDiskFile {
            operation_id: uuid::Uuid::new_v4().to_string(),
            cluster_id: binding.bootstrap.cluster_id.clone(),
            node_id: binding.node_id,
            capacity_bytes,
            reserve_bytes,
            sparse,
            admission: Some(FileDiskAdmission {
                rack_id: rack,
                disk_group_id: id,
                unit_size_bytes: 1024 * 1024,
                zone_size_bytes: capacity_bytes.min(32 * 1024 * 1024 * 1024),
                media_type: None,
            }),
        });
        journal::save(root, state)?;
    }
    let request = state.file.clone().ok_or("default file request missing")?;
    let placement = request
        .admission
        .as_ref()
        .ok_or("default file placement missing")?;
    ensure_group(
        kv,
        binding,
        placement.disk_group_id,
        state.storage_binding.clone(),
    )
    .await?;
    advance_file(profile, state, request).await
}

async fn advance_file(
    profile: &DeploymentProfile,
    state: &mut AcceptedSetup,
    request: PrepareDiskFile,
) -> Result<bool> {
    let root = &profile.paths.data_root;
    let status = crate::node::storage::file_preparation::status(root, &request.operation_id);
    let retry = state.retry_disks.remove(&request.operation_id);
    let ready = status
        .as_ref()
        .is_ok_and(|status| status.phase == FilePreparationPhase::Registered);
    state.status.disks = vec![disk_status(profile, &request, status.as_ref().ok(), ready)?];
    if !ready {
        if !retry
            && status
                .as_ref()
                .is_ok_and(|status| status.phase == FilePreparationPhase::Failed)
        {
            state.status.disks[0].phase = SetupStepPhase::Failed;
            return Err("default file preparation failed; explicit retry required".into());
        }
        // Persist consumption before dispatch. Reconcile the same operation on
        // a lost reply; a terminal failure needs another explicit retry.
        journal::save(root, state)?;
        super::super::super::control_node(
            &profile.paths.run_root.join("node-control.sock"),
            &NodeControl::PrepareDiskFile { request },
        )
        .await?;
    }
    Ok(ready)
}

pub(super) async fn ensure_group(
    kv: &std::sync::Arc<CrowdbKvClient>,
    binding: &NodeBinding,
    group_id: u64,
    data_binding: Option<crowdb_protocol::common::BindMapValue>,
) -> Result<()> {
    let ctx = crowdb_console_shared::ops::OpContext::with_shared_client_preserving_hint(
        std::sync::Arc::clone(kv),
        &binding.management_seeds,
        crowdb_console_shared::ConsoleConfig::default(),
    );
    let groups =
        crowdb_console_shared::ops::hardware::list_disk_groups_from_group0(&ctx, binding.node_id).await?;
    let metadata =
        crowdb_console_shared::deployment::preparation::metadata_binding(&ctx, &binding.bootstrap).await?;
    if !data_binding
        .as_ref()
        .is_some_and(|data| data.store_id == metadata.store_id && data.group_id == metadata.group_id)
    {
        return Err("managed disk metadata differs from accepted default binding".into());
    }
    if let Some(group) = groups.iter().find(|group| group.id == group_id) {
        let current = ctx
            .sysmd()
            .get_bind(group.rack_id, binding.node_id, group_id)
            .await?;
        if !current
            .as_ref()
            .zip(data_binding.as_ref())
            .is_some_and(|(actual, expected)| {
                actual.store_id == expected.store_id && actual.group_id == expected.group_id
            })
        {
            return Err("managed disk group metadata binding changed; inspect placement before retry".into());
        }
        return Ok(());
    }
    crowdb_console_shared::ops::hardware::add_disk_group_bound_to_group0(
        &ctx,
        binding.node_id,
        group_id,
        "Managed storage",
        data_binding,
    )
    .await?;
    Ok(())
}

pub(super) async fn select_group(
    hardware: &HardwareClient,
    kv: &std::sync::Arc<CrowdbKvClient>,
    rack: u64,
    binding: &NodeBinding,
    media_type: Option<i32>,
    excluded: &std::collections::BTreeSet<u64>,
    automatic: bool,
) -> Result<(u64, crowdb_protocol::common::BindMapValue)> {
    let node = binding.node_id;
    let ctx = crowdb_console_shared::ops::OpContext::with_shared_client_preserving_hint(
        std::sync::Arc::clone(kv),
        &binding.management_seeds,
        crowdb_console_shared::ConsoleConfig::default(),
    );
    let accepted =
        crowdb_console_shared::deployment::preparation::storage_binding(&ctx, &binding.bootstrap, rack, node)
            .await?;
    let mut groups = hardware.list_disk_groups_on_node(rack, node).await?;
    groups.sort_unstable_by_key(|group| group.dg_id);
    for group in groups {
        if excluded.contains(&group.dg_id) || automatic && group.dg_id != accepted.disk_group_id {
            continue;
        }
        match hardware
            .validate_disk_group_storage(rack, node, group.dg_id, media_type, 1024 * 1024)
            .await
        {
            Ok(()) => {}
            Err(crowdb_kv_client::Error::Server(reason))
                if reason == "DiskGroup media class or allocation unit is incompatible with this disk" =>
            {
                if automatic {
                    return Err("reserved disk group has incompatible media geometry".into());
                }
                continue;
            }
            Err(error) => return Err(error.into()),
        }
        if let Some(binding) = hardware.get_bind(rack, node, group.dg_id).await? {
            if binding.store_id == accepted.store_id && binding.group_id == accepted.group_id {
                return Ok((
                    group.dg_id,
                    crowdb_protocol::common::BindMapValue {
                        store_id: binding.store_id,
                        group_id: binding.group_id,
                    },
                ));
            } else if automatic {
                return Err("reserved disk group has a conflicting metadata binding".into());
            }
        } else if automatic {
            return Err("reserved disk group has no accepted metadata binding".into());
        }
    }
    let id = if automatic {
        accepted.disk_group_id
    } else {
        hardware.allocate_disk_group_id(rack, node).await?
    };
    Ok((
        id,
        crowdb_protocol::common::BindMapValue {
            store_id: accepted.store_id,
            group_id: accepted.group_id,
        },
    ))
}

fn disk_status(
    profile: &DeploymentProfile,
    request: &PrepareDiskFile,
    status: Option<&crowdb_protocol::mgmt::node::storage::FilePreparation>,
    ready: bool,
) -> Result<SetupDiskStatus> {
    Ok(SetupDiskStatus {
        path: profile
            .paths
            .data_root
            .join("disks")
            .join(format!("managed-{}.img", request.operation_id))
            .to_string_lossy()
            .into_owned(),
        phase: if ready {
            SetupStepPhase::Ready
        } else {
            SetupStepPhase::Preparing
        },
        detail: status
            .and_then(|status| status.error.as_ref())
            .map(|text| journal::bounded(text, 128)),
        operation_id: request.operation_id.clone(),
        disk_id: Some({
            use crowdb_protocol::DiskIdExt;
            let (high, low) = uuid::Uuid::parse_str(&request.operation_id)?.as_u64_pair();
            crowdb_protocol::common::DiskId { high, low }.to_display_string()
        }),
    })
}
