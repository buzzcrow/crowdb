// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish current addresses for recovered owners without transferring partitions.

use super::super::{read_instances, wall_time_ms};
use super::{load_current, publish, replace_one};
use crate::group0_control_plane::Group0ControlPlane;
use crowdb_protocol::chunk_kv::DomainMonitorDescriptor;

pub(in crate::background::domain_monitor::chunk_kv) async fn refresh(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
) -> Result<bool, String> {
    let Some(catalog) = load_current(control).await? else {
        return Ok(false);
    };
    let instances = read_instances(control, descriptor).await?;
    let healthy_after = wall_time_ms().saturating_sub(descriptor.suspect_after_ms);
    for entry in catalog.pages.iter().flat_map(|page| &page.entries) {
        let Some(instance) = instances.get(&entry.owner.instance_id) else {
            continue;
        };
        let hosted = instance
            .extra
            .as_ref()
            .and_then(|extra| extra.chunk_kv.as_ref())
            .is_some_and(|extra| {
                extra.hosted.iter().any(|hosted| {
                    hosted.partition_id == entry.partition_id && hosted.owner_epoch == entry.owner_epoch
                })
            });
        if instance.last_heartbeat_ms < healthy_after
            || !hosted
            || instance.rpc_endpoint == entry.owner.rpc_endpoint
        {
            continue;
        }
        instance
            .rpc_endpoint
            .parse::<std::net::SocketAddr>()
            .map_err(|_| "recovered chunk-KV owner has an invalid RPC address")?;
        let mut replacement = entry.clone();
        replacement.owner.rpc_endpoint.clone_from(&instance.rpc_endpoint);
        let partition = entry.partition_id;
        let revision = catalog.head_revision;
        let (head, pages) = replace_one(
            catalog.head,
            catalog.pages.clone(),
            |entry| entry.partition_id == partition,
            vec![replacement],
        )?;
        publish(control, head, pages, revision).await?;
        return Ok(true);
    }
    Ok(false)
}
