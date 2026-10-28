// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resume one bounded pass without owning the monitor's process manager.

use super::{
    journal::{self, AcceptedSetup},
    Result,
};
use crate::DeploymentProfile;
use crowdb_console_shared::deployment::registry;
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use crowdb_protocol::mgmt::node::{setup::NodeSetupPhase, NodeBinding};
use std::time::Duration;

mod metadata;
mod services;
mod storage;
mod supplied;

pub(super) async fn run(profile: DeploymentProfile, mut state: AcceptedSetup) -> Result<()> {
    let root = &profile.paths.data_root;
    match advance(&profile, &mut state).await {
        Ok(()) => state.status.error = None,
        Err(error) => state.status.error = Some(journal::bounded(&error.to_string(), 512)),
    }
    journal::save(root, &state)
}

async fn advance(profile: &DeploymentProfile, state: &mut AcceptedSetup) -> Result<()> {
    let root = &profile.paths.data_root;
    if root
        .join(format!("node-setup-retired-{}.json", state.request.operation_id))
        .exists()
    {
        state.status.phase = NodeSetupPhase::Failed;
        return Err("accepted node setup was retired by explicit cleanup".into());
    }
    let Some(binding) = crate::node::durable::read::<NodeBinding>(&root.join("node-binding.json"))? else {
        state.status.phase = NodeSetupPhase::WaitingForBinding;
        return Ok(());
    };
    if root
        .join(format!(
            "retired-bootstrap-{}.json",
            binding.bootstrap.operation_id
        ))
        .exists()
        || state.binding.as_ref().is_some_and(|accepted| {
            accepted.node_id != binding.node_id || accepted.bootstrap != binding.bootstrap
        })
        || !crate::node::trust::authorized_operation(root, &state.request.operation_id)?
    {
        state.status.phase = NodeSetupPhase::Failed;
        return Err("node setup admission was retired or rebound".into());
    }
    tokio::time::timeout(Duration::from_secs(3), super::super::verify_binding(&binding)).await??;
    let kv = std::sync::Arc::new(PaxosKvClient::new(ClientConfig::new(
        binding.management_seeds.clone(),
    )));
    let (registry, _) = tokio::time::timeout(Duration::from_secs(3), registry::read(&kv))
        .await??
        .ok_or("node setup admission mapping is unavailable")?;
    if registry.cluster_id != binding.bootstrap.cluster_id
        || !registry.nodes.iter().any(|node| {
            node.discovery_id == state.request.discovery_id
                && node.node_id == binding.node_id
                && node.operation_id == state.request.operation_id
                && node.confirmed
                && !node.cancelled
        })
    {
        state.status.phase = NodeSetupPhase::Failed;
        return Err("node setup no longer matches confirmed admission".into());
    }
    state.binding = Some(binding.clone());
    state.status.node_id = Some(binding.node_id);
    state.status.cluster_id = Some(binding.bootstrap.cluster_id.clone());
    state.status.phase = NodeSetupPhase::Running;
    journal::save(root, state)?;
    let choices = &state.request.choices;
    let preparation_needed = !matches!(
        choices.storage,
        crowdb_protocol::mgmt::node::setup::SetupStorage::Diskless
    ) || choices.services.iter().any(|service| service.enabled);
    if preparation_needed
        && crate::node::read_node_policy(root)?.is_some()
        && !metadata::ready(root, &binding, &kv).await?
    {
        return Ok(());
    }
    // Storage and service prerequisites progress independently. In particular,
    // file preparation may wait for DiskIO while DiskIO is still starting.
    let disk_ready = storage::advance(profile, state, &binding, &kv).await;
    if state
        .status
        .disks
        .iter()
        .any(|step| step.phase == crowdb_protocol::mgmt::node::setup::SetupStepPhase::Failed)
    {
        state.status.phase = NodeSetupPhase::Failed;
        return disk_ready.map(|_| ());
    }
    let services_ready = services::advance(
        profile,
        state,
        &binding,
        &kv,
        disk_ready.as_ref().is_ok_and(|ready| *ready),
    )
    .await;
    let services_ready = services_ready?;
    if state
        .status
        .services
        .iter()
        .any(|step| step.phase == crowdb_protocol::mgmt::node::setup::SetupStepPhase::Failed)
    {
        state.status.phase = NodeSetupPhase::Failed;
        return Err("service startup exhausted its restart budget; inspect service logs and use explicit service controls".into());
    }
    if disk_ready? && services_ready {
        state.status.phase = NodeSetupPhase::Ready;
    }
    Ok(())
}
