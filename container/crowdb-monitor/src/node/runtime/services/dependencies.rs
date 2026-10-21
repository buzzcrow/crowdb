// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recovery waits for live dependencies before consuming another process launch.

use super::super::{probes, service, Result};
use crate::{DeploymentProfile, ProbeExecutor};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeServiceAction, NodeServiceIntent};
use std::{collections::BTreeMap, time::Duration};

pub(in crate::node::runtime) async fn ready(
    profile: &DeploymentProfile,
    intent: &NodeServiceIntent,
    intents: &[NodeServiceIntent],
) -> Result<()> {
    if !matches!(
        intent.action,
        NodeServiceAction::Start | NodeServiceAction::Restart
    ) {
        return Ok(());
    }
    let template = service(profile, &intent.kind)?;
    let executor = ProbeExecutor::new(template.dependencies.iter().any(|kind| kind == "diskio"))?;
    for dependency in &template.dependencies {
        // execute() independently verifies current Group-0 authority before effects.
        if dependency == "kv" {
            continue;
        }
        let dependency = intents
            .iter()
            .find(|candidate| {
                candidate.kind == *dependency
                    && candidate.node_id == intent.node_id
                    && candidate.cluster_id == intent.cluster_id
            })
            .ok_or("service dependency has no committed intent")?;
        if !matches!(
            dependency.action,
            NodeServiceAction::Start | NodeServiceAction::Restart
        ) {
            return Err("service dependency is explicitly stopped".into());
        }
        executor
            .probe_service(&probes::for_intent(profile, dependency)?, &BTreeMap::new())
            .await?;
    }
    if template.dependencies.iter().any(|kind| kind == "diskio") {
        // These clients require a complete DiskIO topology, including owners on
        // other nodes. A local RPC listener alone cannot prove that precondition.
        let binding: NodeBinding = super::durable::read(&profile.paths.data_root.join("node-binding.json"))?
            .ok_or("node is unbound")?;
        tokio::time::timeout(
            Duration::from_secs(3),
            crowdb_diskio_client::DiskioClient::connect(crowdb_diskio_client::DiskioClientConfig {
                management_seeds: binding.management_seeds,
                default_timeout: Duration::from_secs(1),
                ..Default::default()
            }),
        )
        .await??;
    }
    Ok(())
}
