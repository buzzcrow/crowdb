// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resume the accepted default group without rollback or replacement identities.

use crowdb_protocol::{
    kv_membership::{GroupMember, GroupMembershipState},
    mgmt::{AddGroupInitialRole, AddGroupRequest, GroupDeploymentIdentity},
};

use super::{group_wiring, publication};
use crate::{
    deployment::preparation::{self, DefaultPreparation},
    error::{Error, Result},
    ops::OpContext,
};

/// Create/wire/publish exactly the accepted ordinary group, then observe a read.
///
/// # Errors
/// Rejects changed/retired intent and foreign native or published ownership.
/// Incomplete creation remains owned by the accepted operation for later replay.
pub async fn ensure(ctx: &OpContext, accepted: &DefaultPreparation) -> Result<bool> {
    current(ctx, accepted).await?;
    let group = &accepted.metadata;
    let members: Vec<_> = group
        .replicas
        .iter()
        .map(|replica| (replica.node_id, replica.replica_id))
        .collect();
    let resolved = group_wiring::resolve(ctx, group.store_id, &members).await?;
    let complete = published(
        ctx,
        accepted,
        &resolved
            .iter()
            .map(|(_, endpoint)| endpoint.clone())
            .collect::<Vec<_>>(),
    )
    .await?;
    let deployment = GroupDeploymentIdentity {
        cluster_id: accepted.bootstrap.cluster_id.clone(),
        operation_id: accepted.operation_id.clone(),
    };
    let mut requests = Vec::new();
    for (index, ((client, _), (_, replica))) in resolved.iter().zip(&members).enumerate() {
        let request = AddGroupRequest {
            group_id: group.group_id,
            replica_id: *replica,
            initial_role: Some(if index == 0 {
                AddGroupInitialRole::Leader
            } else {
                AddGroupInitialRole::Follower
            }),
            start_election: Some(members.len() == 1),
            deployment: Some(deployment.clone()),
        };
        if complete {
            let local = client.list_groups(group.store_id).await?;
            if !local
                .iter()
                .any(|local| local.group_id == group.group_id && local.local_replica_id == *replica)
            {
                return Err(Error::Config(
                    "published default replica is missing; recover retained data explicitly".into(),
                ));
            }
        }
        requests.push((client, request));
    }
    for (client, request) in &requests {
        current(ctx, accepted).await?;
        group_wiring::create_local(client, group.store_id, request).await?;
    }
    if !complete {
        current(ctx, accepted).await?;
        group_wiring::wire(ctx, group.store_id, group.group_id, &members).await?;
        current(ctx, accepted).await?;
        publication::group(ctx, group.store_id, group.group_id, &members).await?;
    }
    ctx.kv()
        .seed_leader(group.store_id, group.group_id, resolved[0].1.clone());
    let readable = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        ctx.kv().get(
            group.store_id,
            group.group_id,
            b"/deployment/default-readiness",
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        ),
    )
    .await;
    Ok(matches!(readable, Ok(Ok(_))))
}

async fn current(ctx: &OpContext, accepted: &DefaultPreparation) -> Result<()> {
    let actual = preparation::read(ctx, &accepted.bootstrap).await?;
    if !actual.is_some_and(|(actual, _)| actual == *accepted && !actual.retired) {
        return Err(Error::Config(
            "default preparation changed or was explicitly retired".into(),
        ));
    }
    Ok(())
}

async fn published(ctx: &OpContext, accepted: &DefaultPreparation, endpoints: &[String]) -> Result<bool> {
    let group = &accepted.metadata;
    let expected: Vec<_> = group
        .replicas
        .iter()
        .zip(endpoints)
        .map(|(replica, endpoint)| GroupMember {
            replica_id: replica.replica_id,
            node_id: replica.node_id,
            endpoint: endpoint.clone(),
            voting: true,
        })
        .collect();
    let membership = ctx.membership().read(group.store_id, group.group_id).await?;
    if let Some(membership) = &membership {
        let initial = match &membership.record().installation {
            GroupMembershipState::Ready => true,
            GroupMembershipState::Installing { previous_members } => previous_members.is_empty(),
        };
        if membership.record().epoch != 1 || membership.record().members != expected || !initial {
            return Err(Error::Config("default metadata membership has changed".into()));
        }
    }
    let actual = ctx.sysmd().get_group(group.store_id, group.group_id).await?;
    if actual
        .as_ref()
        .is_some_and(|actual| actual.store_id != group.store_id || actual.group_id != group.group_id)
    {
        return Err(Error::Config(
            "default metadata publication has another identity".into(),
        ));
    }
    let mut replicas = ctx
        .sysmd()
        .list_replicas_in_group(group.store_id, group.group_id)
        .await?;
    let mut expected_replicas: Vec<_> = expected
        .iter()
        .map(|replica| {
            let mut value = publication::replica_value(
                group.store_id,
                group.group_id,
                replica.replica_id,
                replica.node_id,
            );
            value.endpoint.clone_from(&replica.endpoint);
            value
        })
        .collect();
    replicas.sort_unstable_by_key(|replica| replica.replica_id);
    expected_replicas.sort_unstable_by_key(|replica| replica.replica_id);
    if actual.is_some() && (replicas != expected_replicas || membership.is_none())
        || actual.is_none() && !replicas.is_empty()
    {
        return Err(Error::Config(
            "default metadata publication differs from accepted replicas".into(),
        ));
    }
    Ok(actual.is_some()
        && membership
            .is_some_and(|membership| membership.record().installation == GroupMembershipState::Ready))
}
