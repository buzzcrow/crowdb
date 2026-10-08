// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Wait for published `DiskDB` groups to agree with current placement authority.

use crate::{
    error::{Error, Result},
    ops::OpContext,
};
use std::sync::Arc;

pub(super) async fn wait_for_diskdb_registration(ctx: &OpContext, expected: usize) -> Result<()> {
    let registry = crowdb_kv_client::ServiceRegistryClient::from_shared(Arc::clone(ctx.kv_arc()));
    let hardware = crowdb_kv_client::HardwareClient::from_shared(Arc::clone(ctx.kv_arc()));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let instances = registry.read_all_diskdb_instances().await?;
        let groups = hardware.list_disk_groups().await?;
        let owners = hardware.list_owners().await?;
        let loads: Vec<_> = instances
            .iter()
            .map(|(id, _)| owners.iter().filter(|owner| owner.instance_id == *id).count())
            .collect();
        let balanced = loads
            .iter()
            .max()
            .copied()
            .unwrap_or(0)
            .saturating_sub(loads.iter().min().copied().unwrap_or(0))
            <= 1;
        let consistent = instances.iter().all(|(id, value)| {
            value
                .extra
                .as_ref()
                .and_then(|extra| extra.diskdb.as_ref())
                .is_some_and(|diskdb| {
                    diskdb.owned_dg_ids.iter().all(|dg| {
                        owners
                            .iter()
                            .any(|owner| owner.dg_id == *dg && owner.instance_id == *id)
                    })
                })
        });
        // A live process may still be loading groups or dropping an old owner.
        // Consumers need the published groups to match current authority.
        if instances.len() >= expected
            && balanced
            && consistent
            && groups.iter().all(|group| {
                owners.iter().any(|owner| {
                    (owner.rack_id, owner.node_id, owner.dg_id) == (group.rack_id, group.node_id, group.dg_id)
                        && instances.iter().any(|(id, value)| {
                            *id == owner.instance_id
                                && value
                                    .extra
                                    .as_ref()
                                    .and_then(|extra| extra.diskdb.as_ref())
                                    .is_some_and(|diskdb| diskdb.owned_dg_ids.contains(&group.dg_id))
                        })
                })
            })
        {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(Error::UpstreamRpc {
                node_id: "group0-service-registry".into(),
                status: format!("expected {expected} living diskdb instances with current disk-group ownership before timeout"),
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
