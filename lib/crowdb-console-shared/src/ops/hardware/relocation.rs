// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Rack-fenced connection edits and retryable empty-node relocation.

use crate::{
    deployment::registry::NodeRecord,
    error::{Error, Result},
    ops::OpContext,
};
use crowdb_kv_client::{BatchOp, Error as KvError, GetOutcome, ReadMode};
use crowdb_protocol::{
    common::{NodeValue, RackValue},
    key::{NodeKey, RackKey, TextKey},
};

/// # Errors
/// Rejects new disk children, another connection update, or missing authority.
pub async fn relocate_node(
    ctx: &OpContext,
    source: &NodeRecord,
    target: &NodeRecord,
    retained: &NodeValue,
) -> Result<()> {
    let rack_path = RackKey {
        rack_id: source.rack_id,
    }
    .to_path();
    let node_path = NodeKey {
        rack_id: source.rack_id,
        node_id: source.node_id,
    }
    .to_path();
    let mut intended = retained.clone();
    intended.management_host = target.host.clone();
    intended.ssh_port = target.ssh_port;
    intended.ssh_user = target.ssh_user.clone();
    intended.ssh_credential_ref = Some("id_ed25519".into());
    for _ in 0..32 {
        let rack = ctx
            .kv()
            .get(0, 0, rack_path.as_bytes(), ReadMode::Linearizable, None)
            .await?;
        let current = ctx
            .kv()
            .get(0, 0, node_path.as_bytes(), ReadMode::Linearizable, None)
            .await?;
        if matches!(current, GetOutcome::NotFound) && source.rack_id != target.rack_id {
            break;
        }
        let GetOutcome::Found {
            value: rack,
            revision,
        } = rack
        else {
            return Err(Error::Config("source rack disappeared".into()));
        };
        let mut rack: RackValue = serde_json::from_slice(&rack).map_err(config_error)?;
        let GetOutcome::Found { value: node, .. } = current else {
            return Err(Error::Config("source node disappeared".into()));
        };
        let mut node: NodeValue = serde_json::from_slice(&node).map_err(config_error)?;
        if node.management_host != source.host && node.management_host != target.host {
            return Err(Error::Config("node connection changed during update".into()));
        }
        let mut writes = Vec::new();
        if source.rack_id == target.rack_id {
            node.management_host = intended.management_host.clone();
            node.ssh_port = intended.ssh_port;
            node.ssh_user = intended.ssh_user.clone();
            node.ssh_credential_ref = intended.ssh_credential_ref.clone();
            writes.push(put(&node_path, &node)?);
        } else {
            if !node.disk_group_ids.is_empty()
                || !ctx
                    .sysmd()
                    .list_disk_groups_on_node(source.rack_id, source.node_id)
                    .await?
                    .is_empty()
            {
                return Err(Error::Config(
                    "remove disk groups before moving this node's rack".into(),
                ));
            }
            rack.node_ids.retain(|id| *id != source.node_id);
            writes.push(BatchOp::Delete {
                key: node_path.as_bytes().to_vec().into(),
            });
        }
        writes.push(put(&rack_path, &rack)?);
        match ctx
            .kv()
            .batch_write_cas(0, 0, &writes, rack_path.as_bytes(), revision)
            .await
        {
            Ok(_) => {
                if source.rack_id == target.rack_id {
                    return Ok(());
                }
                break;
            }
            Err(KvError::CasFailed { .. } | KvError::CasBusy | KvError::OutcomeUnknown) => {}
            Err(error) => return Err(error.into()),
        }
    }
    if ctx
        .sysmd()
        .get_node(source.rack_id, source.node_id)
        .await?
        .is_some()
    {
        return Err(Error::Config(
            "source node relocation is incomplete; retry".into(),
        ));
    }
    super::authority::create_node(ctx, target.rack_id, target.node_id, &intended).await
}

fn put(path: &str, value: &impl serde::Serialize) -> Result<BatchOp> {
    Ok(BatchOp::Put {
        key: path.as_bytes().to_vec().into(),
        value: serde_json::to_vec(value).map_err(config_error)?.into(),
    })
}
fn config_error(error: impl std::fmt::Display) -> Error {
    Error::Config(error.to_string())
}
