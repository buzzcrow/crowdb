// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable, retryable connection and empty-node rack updates.

use super::registry::{self, NodeRecord};
use crate::{
    error::{Error, Result},
    ops::OpContext,
};
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::common::NodeValue;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Update {
    source: NodeRecord,
    target: NodeRecord,
    node: NodeValue,
    complete: bool,
}

/// Persist fixed update inputs before changing hardware; retries from another
/// UI resume the same stages. UUID, allocation and physical host never change.
///
/// # Errors
/// Rejects conflicting pending changes, occupied rack moves or lost authority.
pub async fn apply(ctx: &OpContext, cluster: &str, target: NodeRecord) -> Result<NodeRecord> {
    let path = format!("/deployment/node-updates/{}", target.discovery_id);
    let (registry, _) = registry::read(ctx.kv())
        .await?
        .filter(|(registry, _)| registry.cluster_id == cluster)
        .ok_or_else(|| Error::Config("cluster mapping unavailable".into()))?;
    let source = registry
        .nodes
        .into_iter()
        .find(|node| node.discovery_id == target.discovery_id)
        .ok_or_else(|| Error::Config("node mapping absent".into()))?;
    if !source.confirmed
        || source.cancelled
        || source.node_id != target.node_id
        || source.physical_host_id != target.physical_host_id
        || source.operation_id != target.operation_id
    {
        return Err(Error::Config(
            "node update changes its authority or identity".into(),
        ));
    }
    let (mut update, revision) = match ctx
        .kv()
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await?
    {
        GetOutcome::Found { value, revision } => {
            let previous: Update = serde_json::from_slice(&value).map_err(config_error)?;
            if !previous.complete || previous.target == target {
                (previous, revision)
            } else {
                (capture(ctx, source, target.clone()).await?, revision)
            }
        }
        GetOutcome::NotFound => (capture(ctx, source, target.clone()).await?, 0),
    };
    if update.target != target {
        return Err(Error::Config(
            "another node update is pending; retry its fixed inputs".into(),
        ));
    }
    if update.complete {
        return Ok(update.target);
    }
    let bytes = serde_json::to_vec(&update).map_err(config_error)?;
    let revision = ctx
        .kv()
        .put_cas(0, 0, path.as_bytes(), &bytes, revision)
        .await?
        .revision;
    crate::ops::hardware::relocate_node(ctx, &update.source, &update.target, &update.node).await?;
    crate::ops::kv_logical::refresh_node_endpoints(ctx, target.node_id).await?;
    publish(ctx, cluster, &update.source, &update.target).await?;
    update.complete = true;
    ctx.kv()
        .put_cas(
            0,
            0,
            path.as_bytes(),
            &serde_json::to_vec(&update).map_err(config_error)?,
            revision,
        )
        .await?;
    Ok(update.target)
}

async fn capture(ctx: &OpContext, source: NodeRecord, target: NodeRecord) -> Result<Update> {
    let node = ctx
        .sysmd()
        .get_node(source.rack_id, source.node_id)
        .await?
        .ok_or_else(|| Error::Config("source hardware node absent".into()))?;
    if source.rack_id != target.rack_id
        && (!node.disk_group_ids.is_empty()
            || !ctx
                .sysmd()
                .list_disk_groups_on_node(source.rack_id, source.node_id)
                .await?
                .is_empty())
    {
        return Err(Error::Config(
            "remove disk groups before moving this node's rack".into(),
        ));
    }
    if ctx.sysmd().get_rack(target.rack_id).await?.is_none() {
        return Err(Error::Config("destination rack absent".into()));
    }
    Ok(Update {
        source,
        target,
        node,
        complete: false,
    })
}

async fn publish(ctx: &OpContext, cluster: &str, source: &NodeRecord, target: &NodeRecord) -> Result<()> {
    for _ in 0..32 {
        let (mut registry, revision) = registry::read(ctx.kv())
            .await?
            .filter(|(registry, _)| registry.cluster_id == cluster)
            .ok_or_else(|| Error::Config("cluster mapping unavailable".into()))?;
        let actual = registry
            .nodes
            .iter_mut()
            .find(|node| node.discovery_id == source.discovery_id)
            .ok_or_else(|| Error::Config("node mapping disappeared".into()))?;
        if actual == target {
            return Ok(());
        }
        if actual != source {
            return Err(Error::Config("node mapping changed during update".into()));
        }
        *actual = target.clone();
        match ctx
            .kv()
            .put_cas(
                0,
                0,
                registry::NODES_KEY,
                &serde_json::to_vec(&registry).map_err(config_error)?,
                revision,
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(
                crowdb_kv_client::Error::CasFailed { .. }
                | crowdb_kv_client::Error::CasBusy
                | crowdb_kv_client::Error::OutcomeUnknown,
            ) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::Config(
        "concurrent node update did not converge; retry".into(),
    ))
}

fn config_error(error: impl std::fmt::Display) -> Error {
    Error::Config(error.to_string())
}

/// # Errors
/// Rejects concurrent admission while a node relocation is incomplete.
pub async fn check_admission(kv: &crowdb_kv_client::CrowdbKvClient, id: &str) -> Result<()> {
    if let GetOutcome::Found { value, .. } = kv
        .get(
            0,
            0,
            format!("/deployment/node-updates/{id}").as_bytes(),
            ReadMode::Linearizable,
            None,
        )
        .await?
    {
        let update: Update = serde_json::from_slice(&value).map_err(config_error)?;
        if !update.complete {
            return Err(Error::Config(
                "node update is pending; retry its fixed inputs".into(),
            ));
        }
    }
    Ok(())
}
