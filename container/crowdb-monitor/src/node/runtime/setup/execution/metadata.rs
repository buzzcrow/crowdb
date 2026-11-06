// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Default metadata precedes both disk admission and planned service startup.

use std::sync::Arc;

use crowdb_console_shared::{
    deployment::preparation,
    ops::{kv_logical, OpContext},
    ConsoleConfig,
};
use crowdb_paxos_kv_client::PaxosKvClient;
use crowdb_protocol::mgmt::node::NodeBinding;

use super::Result;

pub(super) async fn ready(
    root: &std::path::Path,
    binding: &NodeBinding,
    kv: &Arc<PaxosKvClient>,
) -> Result<bool> {
    let ctx = OpContext::with_shared_client_preserving_hint(
        Arc::clone(kv),
        &binding.management_seeds,
        ConsoleConfig::default(),
    );
    let accepted = preparation::reserve(&ctx, &binding.bootstrap).await?;
    if let Some(mut groups) = crate::node::durable::read::<
        crowdb_protocol::mgmt::node::recovery::RecoveryGroups,
    >(&root.join("recovery-groups.json"))?
    {
        if groups.bootstrap != binding.bootstrap {
            return Err("recovery group binding differs".into());
        }
        let group = crowdb_protocol::mgmt::node::recovery::RecoveryGroup {
            store_id: accepted.metadata.store_id,
            group_id: accepted.metadata.group_id,
            deployment: Some(crowdb_protocol::mgmt::GroupDeploymentIdentity {
                cluster_id: binding.bootstrap.cluster_id.clone(),
                operation_id: accepted.operation_id.clone(),
            }),
            members: accepted
                .metadata
                .replicas
                .iter()
                .map(|replica| crowdb_protocol::mgmt::node::recovery::RecoveryReplica {
                    replica_id: replica.replica_id,
                    node_id: replica.node_id,
                })
                .collect(),
        };
        if !groups.groups.contains(&group) {
            groups.groups.retain(|existing| {
                (existing.store_id, existing.group_id) != (group.store_id, group.group_id)
            });
            groups.groups.push(group);
            crate::node::durable::write(&root.join("recovery-groups.json"), &groups)?;
        }
    }
    Ok(kv_logical::ensure_default_group(&ctx, &accepted).await?)
}
