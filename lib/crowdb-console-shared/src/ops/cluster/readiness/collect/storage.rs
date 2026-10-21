// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Verify accepted preparation, physical placement, ownership and capacity.

use super::{fact, now_ms};
use crate::{
    deployment::{preparation, PreparedBootstrap},
    error::{Error, Result},
    ops::OpContext,
};
use crowdb_kv_client::{HardwareClient, ReadMode, ServiceRegistryClient, SpaceUsageClient};
use crowdb_protocol::{
    common::HwStatus,
    mgmt::cluster::{DependencyKind, DependencyObservation, DependencyState},
};
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
    let usage = SpaceUsageClient::from_shared(hardware.clone());
    let (groups, disks, owners, binds, instances, usage) = tokio::try_join!(
        hardware.list_disk_groups(),
        hardware.list_all_disks(),
        hardware.list_owners(),
        hardware.list_binds(),
        services.read_all_diskdb_instances(),
        usage.list_disk_group_usages()
    )?;
    let mut evidence = Vec::new();
    for expected in &accepted.storage {
        let ready = group_ready(expected, &groups, &disks)
            && binds.iter().any(|bind| {
                (
                    bind.rack_id,
                    bind.node_id,
                    bind.dg_id,
                    bind.store_id,
                    bind.group_id,
                ) == (
                    expected.rack_id,
                    expected.node_id,
                    expected.disk_group_id,
                    expected.store_id,
                    expected.group_id,
                )
            })
            && owners.iter().any(|owner| {
                (owner.rack_id, owner.node_id, owner.dg_id)
                    == (expected.rack_id, expected.node_id, expected.disk_group_id)
                    && owner.lease_expiry_ms > now_ms()
                    && instances.iter().any(|(id, instance)| {
                        *id == owner.instance_id
                            && instance
                                .extra
                                .as_ref()
                                .and_then(|extra| extra.diskdb.as_ref())
                                .is_some_and(|extra| extra.owned_dg_ids.contains(&expected.disk_group_id))
                    })
            })
            && usage.iter().any(|(id, usage)| {
                *id == expected.disk_group_id
                    && usage.usage_known
                    && usage.allocatable_disk_count > 0
                    && usage.allocatable_free_bytes > 0
                    && now_ms()
                        .checked_sub(usage.sampled_at_ms)
                        .is_some_and(|age| age <= 15_000)
            });
        let mut observation = fact(
            &format!(
                "storage/{}/{}/{}",
                expected.rack_id, expected.node_id, expected.disk_group_id
            ),
            DependencyKind::Storage,
            if ready {
                DependencyState::Ready
            } else {
                DependencyState::Unknown
            },
            "storage-placement-owner-capacity-not-ready",
        );
        observation.revision = Some(revision);
        observation.operation_id = Some(accepted.operation_id.clone());
        observation
            .current_operation_id
            .clone_from(&observation.operation_id);
        evidence.push(observation);
    }
    Ok(evidence)
}

fn group_ready(
    expected: &preparation::DefaultStorageBinding,
    groups: &[crowdb_protocol::sysdata::DiskGroupEntry],
    disks: &[crowdb_kv_client::DiskRecord],
) -> bool {
    groups
        .iter()
        .find(|group| {
            (group.rack_id, group.node_id, group.dg_id)
                == (expected.rack_id, expected.node_id, expected.disk_group_id)
        })
        .is_some_and(|group| {
            group.value.status == HwStatus::Up as i32
                && !group.value.disk_ids.is_empty()
                && group.value.disk_ids.iter().all(|id| {
                    disks.iter().any(|disk| {
                        (disk.rack_id, disk.node_id, disk.disk_group_id)
                            == (expected.rack_id, expected.node_id, expected.disk_group_id)
                            && disk.disk_id == *id
                            && disk.value.status == HwStatus::Up as i32
                            && disk.value.capacity_units > 0
                    })
                })
        })
}
