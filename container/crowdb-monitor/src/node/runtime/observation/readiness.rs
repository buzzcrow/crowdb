// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read local durable and runtime facts without accepting or repairing work.

use super::super::{durable, services, setup, Result};
use crate::{DeploymentProfile, NodeIdentity, ProcessManager, StatusStore};
use crowdb_protocol::mgmt::node::observation::NodeRuntimeObservation;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(in crate::node::runtime) fn observe(
    profile: &DeploymentProfile,
    manager: &mut ProcessManager,
) -> Result<NodeRuntimeObservation> {
    let root = &profile.paths.data_root;
    let identity = NodeIdentity::load_existing(root)?;
    let policy = crate::read_node_policy(root)?.ok_or("accepted node policy is absent")?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?;
    let health = StatusStore::open(&profile.paths.run_root)
        .and_then(|store| store.read(Duration::from_secs(3)))
        .ok()
        .filter(|health| health.deployment_id == identity.uuid() && health.monitor_pid == std::process::id());
    let live = manager.live_pids()?;
    let services = services::observe(profile, &live, health.as_ref(), now)?;
    Ok(NodeRuntimeObservation {
        version: 1,
        discovery_id: identity.uuid().to_string(),
        policy,
        binding: durable::read(&root.join("node-binding.json"))?,
        setup: setup::NodeSetup::status(root)?,
        observed_at_unix_ms: now,
        services,
    })
}
