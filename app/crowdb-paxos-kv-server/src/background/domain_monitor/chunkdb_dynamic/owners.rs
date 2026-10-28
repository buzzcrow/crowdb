// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::decode;
use crate::group0_control_plane::Group0ControlPlane;
use bytes::Bytes;
use crowdb_protocol::chunk_kv::DomainMonitorDescriptor;
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotAuthority, ChunkSlotMap};
use crowdb_protocol::common::InstanceValue;
use crowdb_protocol::key::InstanceKey;
use std::collections::{BTreeMap, HashMap};

pub(super) async fn live_owners(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
    authority: &ChunkSlotMap<ChunkSlotAuthority>,
) -> Result<(BTreeMap<u64, usize>, HashMap<u64, bool>), String> {
    let observations = control
        .scan_all_prefix(Bytes::from_static(b"/srv/chunkdb-epoch-v1/"), 256)
        .await
        .map_err(|error| error.to_string())?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| u64::try_from(time.as_millis()).unwrap_or(u64::MAX));
    let mut live = BTreeMap::new();
    let mut last_seen = HashMap::new();
    for record in observations {
        let instance: InstanceValue = decode(&record.value)?;
        let path = std::str::from_utf8(&record.key).map_err(|error| error.to_string())?;
        let key = InstanceKey::from_path(path).map_err(|error| error.to_string())?;
        if key.instance_id != instance.instance_id || key.to_path() != path {
            return Err("compatible owner registry key/value mismatch".into());
        }
        last_seen.insert(instance.instance_id, instance.last_heartbeat_ms);
        if instance.instance_id != 0
            && !instance.rpc_endpoint.is_empty()
            && now.saturating_sub(instance.last_heartbeat_ms) <= descriptor.suspect_after_ms
        {
            live.insert(instance.instance_id, 0_usize);
        }
    }
    if live.is_empty() {
        return Err("no compatible prepared chunkdb owners".into());
    }
    for owner in live.keys() {
        let missing = format!("/chunkdb/slot_suspect/{owner}");
        let observed = control
            .get(missing.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        if observed.value.is_some() {
            control
                .compare_and_delete(missing.into(), observed.revision)
                .await
                .map_err(|error| error.to_string())?;
        }
    }
    let mut eligible = HashMap::new();
    for slot in ChunkSlot::all() {
        let owner = authority.owner(slot).instance_id();
        if let Some(count) = live.get_mut(&owner) {
            *count += 1;
        } else if let Some(seen) = last_seen.get(&owner) {
            eligible.insert(owner, now.saturating_sub(*seen) >= descriptor.dead_after_ms);
        } else if let std::collections::hash_map::Entry::Vacant(entry) = eligible.entry(owner) {
            // Persist the first missing observation so controller replacement cannot reset grace.
            let missing = format!("/chunkdb/slot_suspect/{owner}");
            let observed = control
                .get(missing.as_bytes())
                .await
                .map_err(|error| error.to_string())?;
            let first = if let Some(value) = observed.value {
                decode::<u64>(&value)?
            } else {
                control
                    .compare_and_put(
                        missing.into(),
                        Bytes::from(serde_json::to_vec(&now).map_err(|error| error.to_string())?),
                        0,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                now
            };
            entry.insert(now.saturating_sub(first) >= descriptor.dead_after_ms);
        }
    }
    Ok((live, eligible))
}
