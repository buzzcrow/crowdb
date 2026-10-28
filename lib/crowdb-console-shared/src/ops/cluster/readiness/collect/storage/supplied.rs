// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resolve every sealed supplied medium through its actual Group-0 placement.

use crate::deployment::preparation::DefaultStorageBinding;
use crowdb_paxos_kv_client::DiskRecord;
use crowdb_protocol::{
    common::DiskId,
    mgmt::node::{
        observation::NodeRuntimeObservation,
        setup::{SetupStepPhase, SetupStorage, SetupStorageMode},
    },
    sysdata::DiskGroupEntry,
    DiskIdExt,
};
use std::collections::{BTreeMap, HashSet};

type Failure = (&'static str, String);

pub(super) fn bindings(
    expected: &DefaultStorageBinding,
    node: &NodeRuntimeObservation,
    disks: Result<&[DiskRecord], &crowdb_paxos_kv_client::Error>,
    groups: Result<&[DiskGroupEntry], &crowdb_paxos_kv_client::Error>,
) -> Result<Vec<DefaultStorageBinding>, Failure> {
    let SetupStorage::Supplied { disks: media } = &node.policy.setup.storage else {
        unreachable!("caller selected supplied storage")
    };
    let setup = node
        .setup
        .as_ref()
        .filter(|setup| {
            setup.discovery_id == node.discovery_id
                && setup.node_id == Some(expected.node_id)
                && setup.cluster_id.as_deref()
                    == node
                        .binding
                        .as_ref()
                        .map(|binding| binding.bootstrap.cluster_id.as_str())
                && setup.storage_mode == Some(SetupStorageMode::Supplied)
                && uuid::Uuid::parse_str(&setup.operation_id).is_ok()
                && setup.disks.len() == media.len()
                && !media.is_empty()
        })
        .ok_or_else(|| {
            failure(
                "storage-supplied-intent-missing",
                "Current sealed supplied-media setup is unavailable",
            )
        })?;
    let disks = disks.map_err(|_| failure("storage-disks-unavailable", "Group-0 disk read failed"))?;
    let groups =
        groups.map_err(|_| failure("storage-groups-unavailable", "Group-0 disk-group read failed"))?;
    let mut seen = HashSet::new();
    let mut selected = BTreeMap::new();
    for medium in media {
        let status = setup
            .disks
            .iter()
            .find(|disk| disk.path == medium.path && disk.phase == SetupStepPhase::Ready)
            .ok_or_else(|| {
                failure(
                    "storage-supplied-medium-pending",
                    "Accepted supplied medium is not registered",
                )
            })?;
        let id = status
            .disk_id
            .as_deref()
            .and_then(|value| DiskId::from_display_string(value).ok())
            .filter(|id| seen.insert(*id))
            .ok_or_else(|| {
                failure(
                    "storage-supplied-identity-invalid",
                    "Sealed supplied disk identity is missing or duplicated",
                )
            })?;
        let locations: Vec<_> = disks.iter().filter(|disk| disk.disk_id == id).collect();
        let [disk] = locations.as_slice() else {
            return Err(failure(
                "storage-supplied-disk-missing",
                "Accepted supplied disk has no unique Group-0 placement",
            ));
        };
        if disk.rack_id != expected.rack_id || disk.node_id != expected.node_id {
            return Err(failure(
                "storage-supplied-placement-conflict",
                "Accepted supplied disk belongs to a different node",
            ));
        }
        if !groups.iter().any(|group| {
            group.rack_id == expected.rack_id
                && group.node_id == expected.node_id
                && group.dg_id == disk.disk_group_id
                && group.value.disk_ids.contains(&id)
        }) {
            return Err(failure(
                "storage-supplied-membership-conflict",
                "Accepted supplied disk is absent from its registered group membership",
            ));
        }
        selected.insert(
            disk.disk_group_id,
            DefaultStorageBinding {
                disk_group_id: disk.disk_group_id,
                ..expected.clone()
            },
        );
    }
    Ok(selected.into_values().collect())
}

fn failure(code: &'static str, detail: &str) -> Failure {
    (code, detail.into())
}
