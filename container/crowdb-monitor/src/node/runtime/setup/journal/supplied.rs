// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reject changed or oversized sealed media plans before dispatching effects.

use super::{AcceptedSetup, Result, SetupStorage};
use std::collections::BTreeSet;

pub(super) fn validate(state: &AcceptedSetup) -> Result<()> {
    if state.supplied.len() > 64 || state.supplied_observations.len() > 64 || state.retry_supplied.len() > 64
    {
        return Err("sealed supplied plan exceeds bound".into());
    }
    if state.supplied.is_empty() {
        return Ok(());
    }
    let SetupStorage::Supplied { disks } = &state.request.choices.storage else {
        return Err("sealed media plan differs from storage mode".into());
    };
    if state.supplied.len() != disks.len() || state.supplied_observations.len() != disks.len() {
        return Err("sealed media plan does not cover exactly the selected disks".into());
    }
    let binding = state
        .binding
        .as_ref()
        .ok_or("sealed media plan has no node binding")?;
    let mut paths = BTreeSet::new();
    let mut operations = BTreeSet::new();
    for accepted in &state.supplied {
        accepted.request.validate()?;
        let record = &accepted.request.record;
        let operation = uuid::Uuid::parse_str(&record.operation_id)?;
        let (high, low) = operation.as_u64_pair();
        let selected = disks
            .iter()
            .find(|disk| disk.path == record.geometry.device_path)
            .ok_or("sealed medium was not selected")?;
        let observation = state
            .supplied_observations
            .iter()
            .find(|disk| disk.path == selected.path)
            .ok_or("sealed medium observation missing")?;
        let physical = record
            .physical_backing
            .as_ref()
            .ok_or("sealed medium physical identity missing")?;
        if operation.is_nil()
            || operation.to_string() != record.operation_id
            || (record.disk_id.high, record.disk_id.low) != (high, low)
            || !paths.insert(&record.geometry.device_path)
            || !operations.insert(&record.operation_id)
            || record.node_id != binding.node_id
            || accepted.request.cluster_id != binding.bootstrap.cluster_id
            || accepted.binding.group_id == 0
            || selected.backend != record.backend
            || selected.backing_identity != record.backing_identity
            || selected.capacity_bytes != physical.capacity_bytes
            || selected.media_type != record.media_type
            || selected.initialize != accepted.request.initialize
            || observation.resource_identity.as_ref() != Some(&physical.resource_identity)
        {
            return Err("sealed medium identity, placement or authorization changed".into());
        }
    }
    if state.retry_supplied.iter().any(|id| !operations.contains(id)) {
        return Err("retry references an unselected supplied operation".into());
    }
    Ok(())
}
