// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Verify accepted preparation, physical placement, ownership and capacity.

mod checks;

use super::{fact, now_ms};
use crate::{
    deployment::{preparation, PreparedBootstrap},
    error::{Error, Result},
    ops::OpContext,
};
use crowdb_kv_client::{HardwareClient, ReadMode, ServiceRegistryClient};
use crowdb_protocol::mgmt::cluster::{DependencyKind, DependencyObservation, DependencyState};
use std::sync::Arc;

pub(super) async fn metadata(
    ctx: &OpContext,
    bootstrap: &PreparedBootstrap,
) -> Result<DependencyObservation> {
    let accepted = preparation::metadata_binding(ctx, &bootstrap.identity).await?;
    let replicas = ctx
        .sysmd()
        .list_replicas_in_group(accepted.store_id, accepted.group_id)
        .await?;
    let exact = replicas.len() == accepted.replicas.len()
        && accepted.replicas.iter().all(|expected| {
            replicas.iter().any(|replica| {
                replica.node_id == expected.node_id
                    && replica.replica_id == expected.replica_id
                    && replica.voting
            })
        });
    if !exact {
        return Ok(fact(
            "cluster/metadata",
            DependencyKind::Metadata,
            DependencyState::Unknown,
            "metadata-membership-differs",
        ));
    }
    // A linearizable ordinary-group read proves current authority beyond Group 0.
    let _ = ctx
        .kv()
        .get(
            accepted.store_id,
            accepted.group_id,
            b"/deployment/readiness-probe",
            ReadMode::Linearizable,
            None,
        )
        .await?;
    Ok(fact(
        "cluster/metadata",
        DependencyKind::Metadata,
        DependencyState::Ready,
        "metadata-ready",
    ))
}

pub(super) async fn read(
    ctx: &OpContext,
    bootstrap: &PreparedBootstrap,
) -> Result<Vec<DependencyObservation>> {
    let (accepted, revision) = preparation::read(ctx, &bootstrap.identity)
        .await?
        .ok_or_else(|| Error::Config("default preparation is absent".into()))?;
    if accepted.retired {
        return Ok(vec![fact(
            "cluster/storage",
            DependencyKind::Storage,
            DependencyState::Stopped,
            "preparation-retired",
        )]);
    }
    let hardware = HardwareClient::from_shared(Arc::clone(ctx.kv_arc()));
    let services = ServiceRegistryClient::from_shared(Arc::clone(ctx.kv_arc()));
    let (groups, disks, owners, binds, instances) = tokio::join!(
        hardware.list_disk_groups(),
        hardware.list_all_disks(),
        hardware.list_owners(),
        hardware.list_binds(),
        services.read_all_diskdb_instances()
    );
    let mut evidence = Vec::new();
    let now = now_ms();
    for expected in &accepted.storage {
        let id = format!(
            "storage/{}/{}/{}",
            expected.rack_id, expected.node_id, expected.disk_group_id
        );
        let disk = checks::disks(expected, groups.as_deref(), disks.as_deref(), binds.as_deref());
        let owner = if disk.is_ok() {
            checks::owner(expected, owners.as_deref(), instances.as_deref(), now)
        } else {
            Err(("storage-waiting-for-disks", format!("Waiting for {id}")))
        };
        let space = if owner.is_ok() {
            checks::space(expected, owners.as_deref(), instances.as_deref(), now)
        } else {
            Err(("storage-waiting-for-owner", format!("Waiting for {id}/owner")))
        };
        for (suffix, result) in [("", disk), ("/owner", owner), ("/space", space)] {
            let mut observation = checks::observation(&format!("{id}{suffix}"), result);
            observation.revision = Some(revision);
            observation.operation_id = Some(accepted.operation_id.clone());
            observation
                .current_operation_id
                .clone_from(&observation.operation_id);
            evidence.push(observation);
        }
    }
    Ok(evidence)
}
