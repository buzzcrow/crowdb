// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Distribute confirmed contact pins without changing the accepted voting manifest.

use crate::error::{err_409, err_502, map_config_err};
use crowdb_console_shared::{
    config::NodeEntry,
    deployment::{
        admission,
        recovery::{ConfirmedMemberTrust, MemberTrust},
        registry, PreparedBootstrap,
    },
    ops::OpContext,
};
use crowdb_paxos_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeControl};
use std::path::Path;

pub(super) async fn distribute(
    ctx: &OpContext,
    binding: &NodeBinding,
    key: &Path,
) -> Result<(), super::Failure> {
    let GetOutcome::Found { value, .. } = ctx
        .kv()
        .get(
            0,
            0,
            crowdb_console_shared::deployment::CLUSTER_KEY,
            ReadMode::Linearizable,
            None,
        )
        .await
        .map_err(|error| map_config_err(error.into()))?
    else {
        return Err(err_409("Confirmed bootstrap is absent"));
    };
    let operation: PreparedBootstrap =
        serde_json::from_slice(&value).map_err(|error| err_502(error.to_string()))?;
    if operation.identity != binding.bootstrap {
        return Err(err_409("Confirmed bootstrap identity differs"));
    }
    let (registry, _) = registry::read(ctx.kv())
        .await
        .map_err(map_config_err)?
        .ok_or_else(|| err_409("Confirmed node registry is absent"))?;
    let initial = MemberTrust::from_bootstrap(&operation).map_err(map_config_err)?;
    let snapshot = ConfirmedMemberTrust::new(initial, registry).map_err(map_config_err)?;
    snapshot.validate(&operation).map_err(map_config_err)?;
    let command = NodeControl::AcceptConfirmedMemberTrust {
        trust: serde_json::to_value(&snapshot).map_err(|error| err_502(error.to_string()))?,
    };
    let replies = futures::future::join_all(snapshot.current.members.iter().map(|member| {
        let command = &command;
        let node = snapshot
            .registry
            .nodes
            .iter()
            .find(|node| node.node_id == member.node_id);
        async move {
            let node = node.ok_or_else(|| err_409("Confirmed recovery destination is absent"))?;
            let target = NodeEntry {
                id: node.node_id,
                rack_id: node.rack_id,
                host: node.host.clone(),
                ssh_port: node.ssh_port,
                ssh_user: node.ssh_user.clone(),
                ssh_key: Some(key.to_string_lossy().into_owned()),
                ssh_password: None,
                ssh_credential_ref: None,
            };
            admission::remote_control_pinned(&target, member, command)
                .await
                .map_err(map_config_err)?;
            Ok::<_, super::Failure>(())
        }
    }))
    .await;
    for reply in replies {
        reply?;
    }
    Ok(())
}
