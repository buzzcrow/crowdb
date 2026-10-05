// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Count-balanced `DiskDB` management placement; data-group bindings stay fixed.

use super::{DomainMonitorDriver, DomainMonitorFuture};
use crate::group0_control_plane::Group0ControlPlane;
use bytes::Bytes;
use crowdb_protocol::chunk_kv::{DomainFailurePolicy, DomainMonitorDescriptor};
use crowdb_protocol::common::{BindMapValue, InstanceValue, OwnerMapValue};
use crowdb_protocol::key::{BindMapKey, DiskGroupKey, InstanceKey, OwnerMapKey, TextKey};
use std::collections::BTreeMap;

pub struct DiskdbPlacementMonitorDriver;

impl DomainMonitorDriver for DiskdbPlacementMonitorDriver {
    fn domain(&self) -> &'static str {
        "diskdb-ownership"
    }
    fn driver_version(&self) -> u32 {
        1
    }
    fn tick<'a>(
        &'a self,
        control: &'a Group0ControlPlane,
        descriptor: &'a DomainMonitorDescriptor,
    ) -> DomainMonitorFuture<'a> {
        Box::pin(async move {
            if descriptor.failure_policy != DomainFailurePolicy::AutomaticSharedStorage
                || descriptor.balance_policy != "disk-group-count-v1"
            {
                return Err("unsupported DiskDB placement policy".into());
            }
            reconcile(control, descriptor).await
        })
    }
}

async fn reconcile(control: &Group0ControlPlane, policy: &DomainMonitorDescriptor) -> Result<(), String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let instances = control
        .scan_all_prefix(Bytes::from(InstanceKey::text_prefix_for_service("diskdb")), 256)
        .await
        .map_err(|e| e.to_string())?;
    let mut heartbeats = BTreeMap::new();
    let mut loads = BTreeMap::new();
    for row in instances {
        let instance: InstanceValue = decode(&row.value)?;
        heartbeats.insert(instance.instance_id, instance.last_heartbeat_ms);
        // Only the fenced server implementation publishes this capability.
        let capable = control
            .get(format!("/diskdb/ownership-capability/{}", instance.instance_id).as_bytes())
            .await
            .map_err(|e| e.to_string())?
            .value
            .is_some();
        if capable
            && !instance.rpc_endpoint.is_empty()
            && now.saturating_sub(instance.last_heartbeat_ms) < policy.suspect_after_ms
        {
            loads.insert(instance.instance_id, 0usize);
        }
    }
    if loads.is_empty() {
        return Ok(());
    }
    let rows = control
        .scan_all_prefix(Bytes::from_static(b"/hw/dg/"), 256)
        .await
        .map_err(|e| e.to_string())?;
    let mut groups = Vec::new();
    for row in rows {
        let key = DiskGroupKey::from_path(std::str::from_utf8(&row.key).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        if !has_data_binding(control, &key).await? {
            continue;
        }
        let owner_key = OwnerMapKey {
            rack_id: key.rack_id,
            node_id: key.node_id,
            disk_group_id: key.disk_group_id,
        }
        .to_path();
        let owner = control
            .get(owner_key.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let value: Option<OwnerMapValue> = owner.value.as_ref().map(|v| decode(v)).transpose()?;
        if let Some(count) = value.as_ref().and_then(|v| loads.get_mut(&v.instance_id)) {
            *count += 1;
        }
        groups.push((key, owner_key, owner.revision, value));
    }
    for (group, key, revision, owner) in &groups {
        let needs_owner = owner.as_ref().map_or(true, |owner| {
            heartbeats
                .get(&owner.instance_id)
                .map_or(true, |last| now.saturating_sub(*last) >= policy.dead_after_ms)
        });
        if needs_owner {
            if !has_data_binding(control, group).await? {
                continue;
            }
            let target = least_loaded(&loads);
            assign(control, key, *revision, target, now).await?;
            *loads.get_mut(&target).expect("selected live target") += 1;
        }
    }
    // Move at most one healthy group per tick. This bounds recovery work and
    // converges to a count difference of at most one without ping-pong.
    let target = least_loaded(&loads);
    let (&source, &maximum) = loads
        .iter()
        .max_by_key(|(id, count)| (**count, **id))
        .expect("live instances");
    if maximum > loads[&target] + 1 {
        if let Some((group, key, revision, _)) = groups
            .iter()
            .find(|(_, _, _, owner)| owner.as_ref().is_some_and(|v| v.instance_id == source))
        {
            if has_data_binding(control, group).await? {
                assign(control, key, *revision, target, now).await?;
            }
        }
    }
    Ok(())
}

fn least_loaded(loads: &BTreeMap<u64, usize>) -> u64 {
    *loads
        .iter()
        .min_by_key(|(id, count)| (**count, **id))
        .expect("live instances")
        .0
}

async fn assign(
    control: &Group0ControlPlane,
    key: &str,
    revision: u64,
    instance: u64,
    now: u64,
) -> Result<(), String> {
    let value = OwnerMapValue {
        instance_id: instance,
        lease_expiry_ms: now.saturating_add(3_600_000),
    };
    control
        .compare_and_put(
            Bytes::copy_from_slice(key.as_bytes()),
            Bytes::from(serde_json::to_vec(&value).map_err(|e| e.to_string())?),
            revision,
        )
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(
        owner_key = key,
        instance_id = instance,
        "DiskGroup manager assigned"
    );
    Ok(())
}

async fn has_data_binding(control: &Group0ControlPlane, group: &DiskGroupKey) -> Result<bool, String> {
    let key = BindMapKey {
        rack_id: group.rack_id,
        node_id: group.node_id,
        disk_group_id: group.disk_group_id,
    }
    .to_path();
    let record = control.get(key.as_bytes()).await.map_err(|e| e.to_string())?;
    let Some(value) = record.value else {
        return Ok(false);
    };
    let binding: BindMapValue = decode(&value)?;
    Ok(binding.group_id != 0)
}

fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T, String> {
    serde_json::from_slice(value).map_err(|e| e.to_string())
}
