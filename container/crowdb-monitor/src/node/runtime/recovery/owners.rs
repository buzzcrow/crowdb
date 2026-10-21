// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Retain committed physical replica owners for recovery without quorum.

use super::super::{durable, Result};
use crowdb_console_shared::ops::OpContext;
use crowdb_protocol::mgmt::{
    node::recovery::{RecoveryGroup, RecoveryGroups, RecoveryReplica},
    GroupCreationStatus, SystemBootstrapIdentity,
};
use std::{collections::BTreeMap, path::Path};

pub(super) async fn capture(
    root: &Path,
    ctx: &OpContext,
    identity: &SystemBootstrapIdentity,
    local: u64,
) -> Result<()> {
    let replicas = ctx.sysmd().list_all_replicas().await?;
    let mut groups = BTreeMap::<(u64, u64), Vec<_>>::new();
    for replica in replicas {
        groups
            .entry((replica.store_id, replica.group_id))
            .or_default()
            .push(replica);
    }
    let mut captured = Vec::new();
    for ((store_id, group_id), replicas) in groups {
        let Some(local_replica) = replicas.iter().find(|replica| replica.node_id == local) else {
            continue;
        };
        let owner = durable::read::<GroupCreationStatus>(&root.join(format!(
            "kv/node-{local}/conf/group-creation-{store_id}-{group_id}.json"
        )))?;
        if owner.as_ref().is_some_and(|owner| {
            owner.retired
                || owner.replica_id != local_replica.replica_id
                || owner
                    .deployment
                    .as_ref()
                    .is_some_and(|deployment| deployment.cluster_id != identity.cluster_id)
        }) {
            continue;
        }
        let mut members: Vec<_> = replicas
            .into_iter()
            .map(|replica| RecoveryReplica {
                node_id: replica.node_id,
                replica_id: replica.replica_id,
            })
            .collect();
        members.sort_by_key(|member| member.replica_id);
        captured.push(RecoveryGroup {
            store_id,
            group_id,
            deployment: owner.and_then(|owner| owner.deployment),
            members,
        });
    }
    let snapshot = RecoveryGroups {
        bootstrap: identity.clone(),
        groups: captured,
    };
    let path = root.join("recovery-groups.json");
    let old: Option<RecoveryGroups> = durable::read(&path)?;
    if old.as_ref().is_some_and(|old| old.bootstrap != *identity) {
        return Err("recovery owner snapshot belongs to another bootstrap".into());
    }
    if old.as_ref() != Some(&snapshot) {
        durable::write(&path, &snapshot)?;
    }
    Ok(())
}
