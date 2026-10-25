// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recovery waits for live dependencies before consuming another process launch.

use super::super::{probes, service, Result};
use crate::{DeploymentProfile, ProbeExecutor};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeServiceAction, NodeServiceIntent};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub(super) async fn committed_ready(
    profile: &DeploymentProfile,
    intent: &NodeServiceIntent,
    binding: &NodeBinding,
) -> Result<()> {
    let kv = crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
        binding.management_seeds.clone(),
    ));
    let intents = tokio::time::timeout(
        Duration::from_secs(3),
        crowdb_console_shared::deployment::services::list(&kv),
    )
    .await??;
    ready(profile, intent, &intents).await
}

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
    if template
        .dependencies
        .iter()
        .any(|kind| kind == "diskdb" || kind == "diskio")
    {
        diskdb_routes_ready(profile).await?;
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
    if intent.kind == "access" {
        // An idle local ChunkKV listener does not prove that the catalog owner
        // has finished WAL replay. Probe the same routed read Access needs.
        let binding: NodeBinding = super::durable::read(&profile.paths.data_root.join("node-binding.json"))?
            .ok_or("node is unbound")?;
        let kv = Arc::new(crowdb_kv_client::CrowdbKvClient::new(
            crowdb_kv_client::ClientConfig::new(binding.management_seeds),
        ));
        let catalog = Arc::new(crowdb_chunk_kv_client::Group0ChunkKvRangeCatalogSource::from_shared(kv));
        let transport = Arc::new(crowdb_chunk_kv_client::ChunkKvRpcTransport::new(1, 1, 1));
        let client = crowdb_chunk_kv_client::ChunkKvClient::new(
            crowdb_chunk_kv_client::ClientConfig {
                operation_timeout: Duration::from_secs(1),
                max_attempts: 1,
                max_route_refreshes: 1,
                max_owner_connections: 1,
                ..Default::default()
            },
            catalog,
            transport,
        )?;
        let response = client.get(b"/deployment/access-readiness".to_vec(), None).await?;
        if !matches!(
            response.result,
            Ok(crowdb_protocol::chunk_kv::OperationResult::Value(_))
        ) {
            return Err("routed ChunkKV catalog is not ready".into());
        }
    }
    Ok(())
}

async fn diskdb_routes_ready(profile: &DeploymentProfile) -> Result<()> {
    let binding: NodeBinding =
        super::durable::read(&profile.paths.data_root.join("node-binding.json"))?.ok_or("node is unbound")?;
    let kv = Arc::new(crowdb_kv_client::CrowdbKvClient::new(
        crowdb_kv_client::ClientConfig::new(binding.management_seeds),
    ));
    crowdb_console_shared::deployment::services::diskdb_storage_ready(kv).await?;
    Ok(())
}
