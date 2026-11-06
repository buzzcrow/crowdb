// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Ordered storage diagnostics from authoritative records; never repairs state.

use super::super::fact;
use crate::deployment::preparation::DefaultStorageBinding;
use crowdb_paxos_kv_client::DiskRecord;
use crowdb_protocol::{
    common::{HwStatus, InstanceValue},
    mgmt::cluster::{DependencyKind, DependencyObservation, DependencyState},
    sysdata::{DiskGroupEntry, DiskdbOwnerEntry, KVGroupBindEntry},
};

type Read<'a, T> = Result<&'a [T], &'a crowdb_paxos_kv_client::Error>;
type Check = Result<String, (&'static str, String)>;

pub(super) fn observation(id: &str, result: Check) -> DependencyObservation {
    let mut observation = fact(
        id,
        DependencyKind::Storage,
        if result.is_ok() {
            DependencyState::Ready
        } else if result
            .as_ref()
            .is_err_and(|(code, _)| code.starts_with("storage-waiting-for-"))
        {
            DependencyState::Waiting
        } else {
            DependencyState::Unknown
        },
        result.as_ref().err().map_or("storage-ready", |(code, _)| *code),
    );
    observation.detail = match result {
        Ok(detail) | Err((_, detail)) => detail,
    };
    if let Some(reason) = &mut observation.reason {
        reason.detail.clone_from(&observation.detail);
    }
    observation
}

pub(super) fn disks(
    expected: &DefaultStorageBinding,
    groups: Read<'_, DiskGroupEntry>,
    disks: Read<'_, DiskRecord>,
    binds: Read<'_, KVGroupBindEntry>,
) -> Check {
    let groups = groups.map_err(|_| {
        (
            "storage-groups-unavailable",
            "Group-0 disk-group read failed".into(),
        )
    })?;
    let group = groups
        .iter()
        .find(|group| (group.rack_id, group.node_id, group.dg_id) == location(expected))
        .ok_or_else(|| {
            (
                "storage-group-missing",
                "Accepted disk group is not registered in Group 0".into(),
            )
        })?;
    if group.value.status != HwStatus::Up as i32 {
        return Err((
            "storage-group-not-up",
            format!("Disk-group status is {}; expected Up", group.value.status),
        ));
    }
    if group.value.disk_ids.is_empty() {
        return Err(("storage-group-empty", "Disk group has no registered disks".into()));
    }
    let disks = disks.map_err(|_| ("storage-disks-unavailable", "Group-0 disk read failed".into()))?;
    for id in &group.value.disk_ids {
        let disk = disks
            .iter()
            .find(|disk| {
                (disk.rack_id, disk.node_id, disk.disk_group_id) == location(expected) && disk.disk_id == *id
            })
            .ok_or_else(|| {
                (
                    "storage-disk-missing",
                    format!("Disk {id:?} is absent from its accepted disk group"),
                )
            })?;
        if disk.value.status != HwStatus::Up as i32 {
            return Err((
                "storage-disk-not-up",
                format!("Disk {id:?} status is {}; expected Up", disk.value.status),
            ));
        }
        if disk.value.capacity_units == 0 {
            return Err((
                "storage-disk-capacity-invalid",
                format!("Disk {id:?} has zero capacity units"),
            ));
        }
    }
    let binds = binds.map_err(|_| {
        (
            "storage-bindings-unavailable",
            "Group-0 metadata-binding read failed".into(),
        )
    })?;
    if !binds.iter().any(|bind| {
        (bind.rack_id, bind.node_id, bind.dg_id) == location(expected)
            && (bind.store_id, bind.group_id) == (expected.store_id, expected.group_id)
    }) {
        return Err((
            "storage-metadata-binding-missing",
            format!(
                "Disk group is not bound to accepted store {}/group {}",
                expected.store_id, expected.group_id
            ),
        ));
    }
    Ok(format!("Group 0: disk group Up; all {} member disks Up with nonzero capacity; metadata binding store {}/group {} matches accepted placement", group.value.disk_ids.len(), expected.store_id, expected.group_id))
}

pub(super) fn owner(
    expected: &DefaultStorageBinding,
    owners: Read<'_, DiskdbOwnerEntry>,
    instances: Read<'_, (u64, InstanceValue)>,
    now: u64,
) -> Check {
    let owners = owners.map_err(|_| ("storage-owners-unavailable", "Group-0 owner read failed".into()))?;
    let owner = owners
        .iter()
        .find(|owner| (owner.rack_id, owner.node_id, owner.dg_id) == location(expected))
        .ok_or_else(|| ("storage-owner-missing", "Disk group has no assigned owner".into()))?;
    if owner.lease_expiry_ms <= now {
        return Err((
            "storage-owner-lease-expired",
            format!(
                "Owner {} lease expired at {}",
                owner.instance_id, owner.lease_expiry_ms
            ),
        ));
    }
    let instances = instances.map_err(|_| {
        (
            "storage-instances-unavailable",
            "Group-0 live DiskDB registry read failed".into(),
        )
    })?;
    let instance = instances
        .iter()
        .find(|(id, _)| *id == owner.instance_id)
        .ok_or_else(|| {
            (
                "storage-owner-not-live",
                format!("Owner {} has no live DiskDB registration", owner.instance_id),
            )
        })?;
    if !instance
        .1
        .extra
        .as_ref()
        .and_then(|extra| extra.diskdb.as_ref())
        .is_some_and(|extra| extra.owned_dg_ids.contains(&expected.disk_group_id))
    {
        return Err((
            "storage-owner-group-not-loaded",
            format!(
                "Owner {} has not reported disk group {} loaded",
                owner.instance_id, expected.disk_group_id
            ),
        ));
    }
    Ok(format!("DiskDB instance {} is live and reports disk group {} loaded; owner lease expires at {} ({} ms remaining)", owner.instance_id, expected.disk_group_id, owner.lease_expiry_ms, owner.lease_expiry_ms - now))
}

pub(super) fn space(
    expected: &DefaultStorageBinding,
    owners: Read<'_, DiskdbOwnerEntry>,
    instances: Read<'_, (u64, InstanceValue)>,
    now: u64,
) -> Check {
    let owners = owners.map_err(|_| ("storage-owners-unavailable", "Group-0 owner read failed".into()))?;
    let owner = owners
        .iter()
        .find(|owner| (owner.rack_id, owner.node_id, owner.dg_id) == location(expected))
        .ok_or_else(|| ("storage-owner-missing", "Disk group has no assigned owner".into()))?;
    let instances = instances.map_err(|_| {
        (
            "storage-space-unavailable",
            "Group-0 current-owner space summary read failed".into(),
        )
    })?;
    let usage = instances
        .iter()
        .find(|(id, _)| *id == owner.instance_id)
        .and_then(|(_, instance)| instance.extra.as_ref())
        .and_then(|extra| extra.diskdb.as_ref())
        .and_then(|extra| {
            extra
                .group_usages
                .iter()
                .find(|usage| usage.disk_group_id == expected.disk_group_id)
        })
        .ok_or_else(|| {
            (
                "storage-space-missing",
                format!(
                    "Current owner {} has not reported space usage for disk group {}",
                    owner.instance_id, expected.disk_group_id
                ),
            )
        })?;
    if !usage.usage_known {
        return Err((
            "storage-space-unknown",
            "Disk-group space usage is not yet known".into(),
        ));
    }
    if !now
        .checked_sub(usage.sampled_at_ms)
        .is_some_and(|age| age <= 15_000)
    {
        return Err((
            "storage-space-not-current",
            format!(
                "Space sample timestamp {} is stale or in the future relative to {now}",
                usage.sampled_at_ms
            ),
        ));
    }
    if usage.allocatable_disk_count == 0 {
        return Err((
            "storage-no-allocatable-disks",
            "Disk group has no allocatable disks".into(),
        ));
    }
    if usage.allocatable_free_bytes == 0 {
        return Err((
            "storage-space-exhausted",
            "Disk group has zero allocatable free bytes".into(),
        ));
    }
    Ok(format!(
        "Space sample at {} ({} ms old): {} allocatable disks, {} free bytes, {} capacity bytes",
        usage.sampled_at_ms,
        now - usage.sampled_at_ms,
        usage.allocatable_disk_count,
        usage.allocatable_free_bytes,
        usage.allocatable_capacity_bytes
    ))
}

fn location(expected: &DefaultStorageBinding) -> (u64, u64, u64) {
    (expected.rack_id, expected.node_id, expected.disk_group_id)
}
