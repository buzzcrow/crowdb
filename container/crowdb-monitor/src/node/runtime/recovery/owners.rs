// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Retain authorized replica owners for recovery and installation catch-up.

use super::super::{durable, Result};
use crowdb_console_shared::ops::OpContext;
use crowdb_protocol::key::TextKey;
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
    let groups = membership_scope(ctx).await?;
    let mut captured = Vec::new();
    for ((store_id, group_id), mut members) in groups {
        let Some(local_replica) = members.iter().find(|member| member.node_id == local) else {
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

async fn membership_scope(ctx: &OpContext) -> Result<BTreeMap<(u64, u64), Vec<RecoveryReplica>>> {
    let replicas = ctx.sysmd().list_all_replicas().await?;
    let mut groups = BTreeMap::<(u64, u64), Vec<_>>::new();
    for replica in replicas {
        groups
            .entry((replica.store_id, replica.group_id))
            .or_default()
            .push(RecoveryReplica {
                node_id: replica.node_id,
                replica_id: replica.replica_id,
            });
    }
    let memberships = ctx
        .kv()
        .scan_bounded(
            0,
            0,
            crowdb_protocol::key::KvGroupMembersKey::prefix_all().as_bytes(),
            b"",
            b"",
            4096,
            false,
            None,
        )
        .await?;
    if memberships.truncated || memberships.timed_out {
        return Err("recovery membership exceeds its complete observation bound".into());
    }
    for (key, value) in memberships.items {
        let membership: crowdb_protocol::kv_membership::GroupMembership = serde_json::from_slice(&value)?;
        membership.validate()?;
        let expected_key = crowdb_protocol::key::KvGroupMembersKey {
            store_id: membership.store_id,
            group_id: membership.group_id,
        }
        .to_path();
        if expected_key.as_bytes() != key.as_ref() {
            return Err("recovery membership identity differs from its key".into());
        }
        let mut members = membership.members;
        // Joining members need routes before promotion publishes physical
        // replica records. Retiring members remain reachable until fencing ends.
        if let crowdb_protocol::kv_membership::GroupMembershipState::Installing { previous_members } =
            membership.installation
        {
            for previous in previous_members {
                if !members
                    .iter()
                    .any(|member| member.replica_id == previous.replica_id)
                {
                    members.push(previous);
                }
            }
        }
        groups.insert(
            (membership.store_id, membership.group_id),
            members
                .into_iter()
                .map(|member| RecoveryReplica {
                    node_id: member.node_id,
                    replica_id: member.replica_id,
                })
                .collect(),
        );
    }
    Ok(groups)
}

#[cfg(feature = "test-util")]
/// Capture physical recovery membership through the current Group 0 authority.
/// # Errors
/// Propagates authority reads and private snapshot publication failures.
pub async fn capture_recovery_owners_for_tests(
    root: &Path,
    ctx: &OpContext,
    identity: &SystemBootstrapIdentity,
    local: u64,
) -> Result<()> {
    capture(root, ctx, identity, local).await
}
