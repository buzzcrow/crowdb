// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Physical IO availability overlays durable hardware intent for placement.

use crowdb_protocol::{common::InstanceValue, sysdata::DiskGroupEntry, DiskGroupId};
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

pub(super) async fn read(
    service: &crowdb_kv_client::ServiceRegistryClient,
) -> Option<Vec<(u64, InstanceValue)>> {
    match service.read_all_instance_observations("diskio").await {
        Ok(instances) => Some(instances),
        Err(error) => {
            tracing::warn!(%error, "topology refresh: DiskIO availability is unavailable");
            None
        }
    }
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| u64::try_from(time.as_millis()).unwrap_or(u64::MAX))
}

pub(super) fn deadlines(
    groups: &HashMap<DiskGroupId, DiskGroupEntry>,
    instances: &[(u64, InstanceValue)],
) -> HashMap<DiskGroupId, u64> {
    let mut deadlines = HashMap::<DiskGroupId, u64>::new();
    for (_, instance) in instances {
        let Some(extra) = instance.extra.as_ref().and_then(|extra| extra.diskdb.as_ref()) else {
            continue;
        };
        let (Some(rack), Some(node)) = (extra.rack_id, extra.node_id) else {
            continue;
        };
        for group in groups
            .values()
            .filter(|group| group.rack_id == rack && group.node_id == node)
        {
            if extra
                .diskio_scope
                .as_ref()
                .is_some_and(|scope| scope.disk_group_id != 0 && scope.disk_group_id != group.dg_id)
            {
                continue;
            }
            let deadline = if extra.owned_dg_ids.contains(&group.dg_id) {
                instance
                    .last_heartbeat_ms
                    .saturating_add(crowdb_kv_client::SERVICE_INSTANCE_TTL_MS)
            } else {
                0
            };
            deadlines
                .entry(group.dg_id)
                .and_modify(|current| *current = (*current).max(deadline))
                .or_insert(deadline);
        }
    }
    deadlines
}
