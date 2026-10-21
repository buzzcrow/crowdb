// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded, read-only application evidence; no runtime access or repair.

mod members;
mod storage;
use crate::{
    deployment::{self, registry, services, PreparedBootstrap},
    error::{Error, Result},
    ops::OpContext,
};
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::mgmt::{
    cluster::{
        ClusterObservation, ClusterStatus, DependencyKind, DependencyObservation, DependencyState,
        ReadinessReason,
    },
    node::observation::NodeRuntimeObservation,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

/// Observe one selected cluster without reserving or repairing anything.
///
/// # Errors
/// Rejects malformed local policy/identity. Dependency failures produce a valid
/// unhealthy report, with unknown runtime facts when no runtime was supplied.
pub async fn collect(ctx: &OpContext, local: NodeRuntimeObservation) -> Result<ClusterStatus> {
    local.policy.validate().map_err(|_| invalid())?;
    if local.version != 1 || uuid::Uuid::parse_str(&local.discovery_id).is_err() {
        return Err(invalid());
    }
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut observation = base(&local);
    let authority = tokio::time::timeout_at(
        deadline.min(Instant::now() + Duration::from_secs(2)),
        authority(ctx),
    )
    .await;
    match authority {
        Ok(Ok(bootstrap)) => {
            if local
                .binding
                .as_ref()
                .is_some_and(|binding| binding.bootstrap != bootstrap.identity)
            {
                observation.dependencies.push(fact(
                    "cluster/identity",
                    DependencyKind::Identity,
                    DependencyState::Failed,
                    "cluster-identity-conflict",
                ));
            } else {
                observation.cluster_id = Some(bootstrap.identity.cluster_id.clone());
                observation.operation_id = Some(bootstrap.identity.operation_id.clone());
                observation.dependencies.push(fact(
                    "cluster/authority",
                    DependencyKind::Authority,
                    DependencyState::Ready,
                    "authority-observed",
                ));
                Box::pin(gather(ctx, &bootstrap, &local, &mut observation, deadline)).await;
            }
        }
        _ => observation.dependencies.push(fact(
            "cluster/authority",
            DependencyKind::Authority,
            DependencyState::Unknown,
            "authority-unavailable",
        )),
    }
    observation.observed_at_unix_ms = now_ms();
    Ok(super::reduce(observation, now_ms()))
}

fn base(local: &NodeRuntimeObservation) -> ClusterObservation {
    ClusterObservation {
        version: 1,
        cluster: local.policy.cluster.clone(),
        cluster_id: local
            .binding
            .as_ref()
            .map(|binding| binding.bootstrap.cluster_id.clone()),
        bound: local.binding.is_some(),
        initial_voters: local.policy.initial_voters,
        policy: Some(local.policy.clone()),
        observed_at_unix_ms: now_ms(),
        members: Vec::new(),
        operation_id: local.setup.as_ref().map(|setup| setup.operation_id.clone()),
        work_accepted: local.binding.is_some() || local.setup.is_some(),
        setup_completed: local
            .setup
            .as_ref()
            .is_some_and(|setup| setup.phase == crowdb_protocol::mgmt::node::setup::NodeSetupPhase::Ready),
        dependencies: vec![fact(
            "runtime",
            DependencyKind::Runtime,
            DependencyState::Unknown,
            "runtime-not-observed",
        )],
        runtime: None,
    }
}

async fn authority(ctx: &OpContext) -> Result<PreparedBootstrap> {
    let GetOutcome::Found { value, .. } = ctx
        .kv()
        .get(0, 0, deployment::CLUSTER_KEY, ReadMode::Linearizable, None)
        .await?
    else {
        return Err(invalid());
    };
    if value.len() > 1024 * 1024 {
        return Err(invalid());
    }
    let bootstrap: PreparedBootstrap = serde_json::from_slice(&value).map_err(|_| invalid())?;
    bootstrap.validate()?;
    Ok(bootstrap)
}

async fn gather(
    ctx: &OpContext,
    bootstrap: &PreparedBootstrap,
    local: &NodeRuntimeObservation,
    observation: &mut ClusterObservation,
    deadline: Instant,
) {
    let (nodes, intents, metadata, storage) = tokio::join!(
        tokio::time::timeout_at(deadline, registry::read(ctx.kv())),
        tokio::time::timeout_at(deadline, services::list(ctx.kv())),
        tokio::time::timeout_at(deadline, storage::metadata(ctx, bootstrap)),
        tokio::time::timeout_at(deadline, storage::read(ctx, bootstrap)),
    );
    observation
        .dependencies
        .push(metadata.ok().and_then(Result::ok).unwrap_or_else(|| {
            fact(
                "cluster/metadata",
                DependencyKind::Metadata,
                DependencyState::Unknown,
                "metadata-unavailable",
            )
        }));
    observation
        .dependencies
        .extend(storage.ok().and_then(Result::ok).unwrap_or_else(|| {
            vec![fact(
                "cluster/storage",
                DependencyKind::Storage,
                DependencyState::Unknown,
                "storage-unavailable",
            )]
        }));
    let Ok(Ok(Some((registry, _)))) = nodes else {
        return;
    };
    if registry.cluster_id != bootstrap.identity.cluster_id || registry.nodes.len() > 4096 {
        observation.dependencies.push(fact(
            "cluster/registry",
            DependencyKind::Identity,
            DependencyState::Failed,
            "registry-identity-conflict",
        ));
        return;
    }
    members::read(
        bootstrap,
        local,
        observation,
        registry,
        intents.ok().and_then(Result::ok),
        deadline,
    )
    .await;
}

pub(super) fn fact(
    id: &str,
    kind: DependencyKind,
    state: DependencyState,
    code: &str,
) -> DependencyObservation {
    DependencyObservation {
        id: id.into(),
        kind,
        required: true,
        state,
        detail: format!("{id}: {code}"),
        observed_at_unix_ms: now_ms(),
        max_age_ms: 3000,
        revision: None,
        operation_id: None,
        current_operation_id: None,
        reason: (state != DependencyState::Ready).then(|| ReadinessReason {
            dependency: id.into(),
            code: code.into(),
            detail: "Required current application evidence is unavailable or conflicts with accepted state"
                .into(),
        }),
    }
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
}
fn invalid() -> Error {
    Error::Config("invalid cluster readiness identity or policy".into())
}
