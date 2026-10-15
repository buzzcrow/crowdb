// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One immutable admission intent; every mutable step remains monitor-owned.

use super::super::{durable, Result};
use crowdb_protocol::mgmt::node::{
    setup::{
        NodeSetupPhase, NodeSetupRequest, NodeSetupStatus, SetupServiceStatus, SetupStepPhase, SetupStorage,
    },
    storage::DiskInput,
    NodeBinding,
};
use serde::{Deserialize, Serialize};
use std::path::Path;

mod supplied;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AcceptedSetup {
    pub(super) request: NodeSetupRequest,
    pub(super) status: NodeSetupStatus,
    pub(super) binding: Option<NodeBinding>,
    pub(super) file: Option<crowdb_protocol::mgmt::node::storage::PrepareDiskFile>,
    #[serde(default)]
    pub(super) storage_binding: Option<crowdb_protocol::common::BindMapValue>,
    #[serde(default)]
    pub(super) supplied_observations: Vec<crowdb_protocol::mgmt::node::storage::DiskObservation>,
    #[serde(default)]
    pub(super) supplied: Vec<AcceptedSupplied>,
    #[serde(default)]
    pub(super) retry_supplied: std::collections::BTreeSet<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AcceptedSupplied {
    pub(super) request: crowdb_protocol::mgmt::node::storage::PrepareSuppliedDisk,
    pub(super) binding: crowdb_protocol::common::BindMapValue,
}

pub(super) fn read(root: &Path) -> Result<Option<AcceptedSetup>> {
    let current: Option<AcceptedSetup> = durable::read(&root.join("node-setup.json"))?;
    if let Some(current) = &current {
        current.request.validate()?;
        supplied::validate(current)?;
        validate_status(&current.status)?;
        for identity in [&current.request.discovery_id, &current.request.operation_id] {
            let parsed = uuid::Uuid::parse_str(identity)?;
            if parsed.is_nil() || parsed.to_string() != *identity {
                return Err("stored node setup identity is not canonical".into());
            }
        }
        if current.status.version != 1
            || current.status.discovery_id != current.request.discovery_id
            || current.status.operation_id != current.request.operation_id
        {
            return Err("stored node setup identity differs".into());
        }
    }
    Ok(current)
}

pub(super) fn save(root: &Path, state: &AcceptedSetup) -> Result<()> {
    supplied::validate(state)?;
    validate_status(&state.status)?;
    durable::write(&root.join("node-setup.json"), state)?;
    Ok(())
}

fn validate_status(status: &NodeSetupStatus) -> Result<()> {
    if serde_json::to_vec(status)?.len() > 60 * 1024 {
        return Err("node setup progress exceeds its control reply bound".into());
    }
    Ok(())
}

pub(super) fn bounded(text: &str, maximum: usize) -> String {
    let end = text
        .char_indices()
        .map(|(index, character)| index + character.len_utf8())
        .take_while(|end| *end <= maximum)
        .last()
        .unwrap_or(0);
    text[..end].to_owned()
}

pub(super) fn accept(root: &Path, request: NodeSetupRequest, running: bool) -> Result<NodeSetupStatus> {
    let inputs = if matches!(&request.choices.storage, SetupStorage::Supplied { .. }) {
        crate::node::storage::supplied::inputs()?
    } else {
        Vec::new()
    };
    accept_with_inputs(root, request, running, &inputs)
}

pub(super) fn accept_with_inputs(
    root: &Path,
    request: NodeSetupRequest,
    running: bool,
    inputs: &[DiskInput],
) -> Result<NodeSetupStatus> {
    request.validate()?;
    if root
        .join(format!(
            "node-setup-retired-{}.json",
            uuid::Uuid::parse_str(&request.operation_id)?
        ))
        .exists()
    {
        return Err("node setup operation was retired by explicit cleanup".into());
    }
    let identity = crate::node::NodeIdentity::load_or_create(root)?;
    for text in [&request.discovery_id, &request.operation_id] {
        let id = uuid::Uuid::parse_str(text)?;
        if id.is_nil() || id.to_string() != *text {
            return Err("node setup requires canonical identities".into());
        }
    }
    if identity.uuid().to_string() != request.discovery_id
        || !crate::node::trust::authorized_operation(root, &request.operation_id)?
    {
        return Err("node setup requires the target UUID and authenticated admission operation".into());
    }
    if let Some(mut current) = read(root)? {
        if current.request == request {
            return replay(root, &mut current, running);
        }
        retire_cancelled(root, &current, running)?;
    }
    let supplied_observations = validate_supplied(root, &request, inputs)?;
    let status = NodeSetupStatus {
        version: 1,
        discovery_id: request.discovery_id.clone(),
        operation_id: request.operation_id.clone(),
        phase: NodeSetupPhase::Accepted,
        storage_mode: Some(request.choices.storage.mode()),
        node_id: None,
        cluster_id: None,
        disks: Vec::new(),
        services: request
            .choices
            .services
            .iter()
            .map(|service| SetupServiceStatus {
                kind: service.kind,
                phase: if service.enabled {
                    SetupStepPhase::Pending
                } else {
                    SetupStepPhase::Disabled
                },
                detail: None,
                operation_id: None,
            })
            .collect(),
        error: None,
    };
    save(
        root,
        &AcceptedSetup {
            request,
            status: status.clone(),
            binding: None,
            file: None,
            storage_binding: None,
            supplied_observations,
            supplied: Vec::new(),
            retry_supplied: std::collections::BTreeSet::new(),
        },
    )?;
    Ok(status)
}

fn replay(root: &Path, current: &mut AcceptedSetup, running: bool) -> Result<NodeSetupStatus> {
    if current.status.phase == NodeSetupPhase::Failed && !running {
        current.status.phase = NodeSetupPhase::Accepted;
        current.status.error = None;
        for service in &mut current.status.services {
            if service.phase == SetupStepPhase::Failed {
                service.phase = SetupStepPhase::Pending;
                service.detail = None;
            }
        }
        for disk in &mut current.status.disks {
            if disk.phase == SetupStepPhase::Failed {
                current.retry_supplied.insert(disk.operation_id.clone());
                disk.phase = SetupStepPhase::Pending;
                disk.detail = None;
            }
        }
        save(root, current)?;
    }
    Ok(current.status.clone())
}

fn retire_cancelled(root: &Path, current: &AcceptedSetup, running: bool) -> Result<()> {
    if running
        || current.binding.is_some()
        || current.file.is_some()
        || !current.supplied.is_empty()
        || current
            .status
            .services
            .iter()
            .any(|service| service.operation_id.is_some())
        || crate::node::trust::authorized_operation(root, &current.request.operation_id)?
    {
        return Err("node setup inputs differ from accepted intent".into());
    }
    std::fs::rename(
        root.join("node-setup.json"),
        root.join(format!(
            "cancelled-node-setup-{}.json",
            current.request.operation_id
        )),
    )?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

fn validate_supplied(
    root: &Path,
    request: &NodeSetupRequest,
    inputs: &[DiskInput],
) -> Result<Vec<crowdb_protocol::mgmt::node::storage::DiskObservation>> {
    let SetupStorage::Supplied { disks } = &request.choices.storage else {
        return Ok(Vec::new());
    };
    let inventory = crate::node::discover_disks(root, inputs)?;
    let mut observations = Vec::new();
    for selected in disks {
        let actual = inventory
            .disks
            .iter()
            .find(|disk| disk.path == selected.path)
            .ok_or("selected disk is no longer discovered")?;
        if actual.error.is_some()
            || !actual.supplied
            || actual.backend != Some(selected.backend)
            || actual.backing_identity.as_ref() != Some(&selected.backing_identity)
            || actual.capacity_bytes != Some(selected.capacity_bytes)
            || actual.resource_identity.is_none()
            || selected.capacity_bytes < 64 * 1024 * 1024
        {
            return Err("selected disk observation changed; refresh before admission".into());
        }
        observations.push(actual.clone());
    }
    Ok(observations)
}
