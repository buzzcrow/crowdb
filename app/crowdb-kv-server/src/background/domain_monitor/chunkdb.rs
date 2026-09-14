// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunkdb range-binding monitor backed by the in-process group-0 facade.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use crowdb_protocol::chunk_kv::DomainMonitorDescriptor;
use crowdb_protocol::common::{ChunkdbRangeBindingValue, InstanceValue, RangeStatus};
use crowdb_protocol::key::{ChunkdbRangeBindingKey, InstanceKey, TextKey};

use crate::group0_control_plane::Group0ControlPlane;

use super::{DomainMonitorDriver, DomainMonitorFuture};

pub const DEFAULT_SUB_RANGE_COUNT: u32 = 1024;

pub struct ChunkdbRangeMonitorDriver {
    sub_range_count: u32,
}

impl ChunkdbRangeMonitorDriver {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sub_range_count: DEFAULT_SUB_RANGE_COUNT,
        }
    }

    /// Override the fixed range count for deployments or focused tests.
    #[must_use]
    pub fn with_sub_range_count(sub_range_count: u32) -> Self {
        Self { sub_range_count }
    }
}

impl Default for ChunkdbRangeMonitorDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DomainMonitorDriver for ChunkdbRangeMonitorDriver {
    fn domain(&self) -> &'static str {
        "chunkdb"
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
            let instances = read_live_instances(control, descriptor).await?;
            let current = read_bindings(control).await?;
            let current_values: Vec<_> = current.iter().map(|binding| binding.value.clone()).collect();
            let (desired, changed) =
                compute_incremental_assignment(&current_values, &instances, self.sub_range_count);
            if !changed {
                return Ok(());
            }
            let revisions: HashMap<_, _> = current
                .into_iter()
                .map(|binding| (binding.value.sub_range_index, binding.revision))
                .collect();
            for binding in desired {
                let key = ChunkdbRangeBindingKey {
                    sub_range_index: binding.sub_range_index,
                }
                .to_path();
                let value = serde_json::to_vec(&binding).map_err(|error| error.to_string())?;
                control
                    .compare_and_put(
                        Bytes::from(key),
                        Bytes::from(value),
                        revisions.get(&binding.sub_range_index).copied().unwrap_or(0),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        })
    }
}

struct VersionedBinding {
    value: ChunkdbRangeBindingValue,
    revision: u64,
}

async fn read_live_instances(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
) -> Result<Vec<(u64, InstanceValue)>, String> {
    let prefix = InstanceKey::text_prefix_for_service(&descriptor.service_registry_name);
    let items = control
        .scan_all_prefix(Bytes::from(prefix.clone()), 256)
        .await
        .map_err(|error| error.to_string())?;
    let cutoff = wall_time_ms().saturating_sub(descriptor.dead_after_ms);
    let mut instances = Vec::new();
    for item in items {
        let path = std::str::from_utf8(&item.key).map_err(|error| error.to_string())?;
        let key = InstanceKey::from_path(path).map_err(|error| error.to_string())?;
        if key.service != descriptor.service_registry_name {
            return Err(format!("unexpected service registry key {path}"));
        }
        let value: InstanceValue = serde_json::from_slice(&item.value).map_err(|error| error.to_string())?;
        if value.instance_id != key.instance_id {
            return Err(format!("instance key/value mismatch at {path}"));
        }
        if value.last_heartbeat_ms >= cutoff {
            instances.push((value.instance_id, value));
        }
    }
    Ok(instances)
}

async fn read_bindings(control: &Group0ControlPlane) -> Result<Vec<VersionedBinding>, String> {
    let items = control
        .scan_all_prefix(Bytes::from(ChunkdbRangeBindingKey::text_prefix_all()), 256)
        .await
        .map_err(|error| error.to_string())?;
    items
        .into_iter()
        .map(|item| {
            let path = std::str::from_utf8(&item.key).map_err(|error| error.to_string())?;
            let key = ChunkdbRangeBindingKey::from_path(path).map_err(|error| error.to_string())?;
            let value: ChunkdbRangeBindingValue =
                serde_json::from_slice(&item.value).map_err(|error| error.to_string())?;
            if value.sub_range_index != key.sub_range_index {
                return Err(format!("chunkdb binding key/value mismatch at {path}"));
            }
            Ok(VersionedBinding {
                value,
                revision: item.revision,
            })
        })
        .collect()
}

fn compute_incremental_assignment(
    current: &[ChunkdbRangeBindingValue],
    instances: &[(u64, InstanceValue)],
    sub_range_count: u32,
) -> (Vec<ChunkdbRangeBindingValue>, bool) {
    if instances.is_empty() {
        return (current.to_vec(), false);
    }
    let desired = compute_assignment(instances, sub_range_count);
    let current_by_index: HashMap<_, _> = current
        .iter()
        .map(|binding| (binding.sub_range_index, binding))
        .collect();
    let mut changed = false;
    let bindings = desired
        .into_iter()
        .map(|desired| match current_by_index.get(&desired.sub_range_index) {
            Some(current) if current.instance_id == desired.instance_id => (*current).clone(),
            Some(current) => {
                changed = true;
                ChunkdbRangeBindingValue {
                    original_instance_id: current.instance_id,
                    original_endpoint: current.rpc_endpoint.clone(),
                    status: RangeStatus::InTransition as i32,
                    ..desired
                }
            }
            None => {
                changed = true;
                desired
            }
        })
        .collect();
    (bindings, changed)
}

fn compute_assignment(
    instances: &[(u64, InstanceValue)],
    sub_range_count: u32,
) -> Vec<ChunkdbRangeBindingValue> {
    if instances.is_empty() || sub_range_count == 0 {
        return Vec::new();
    }
    let mut instances: Vec<_> = instances.iter().collect();
    instances.sort_by_key(|(instance_id, _)| *instance_id);
    let instance_count = u32::try_from(instances.len()).unwrap_or(u32::MAX);
    let bucket_count = u32::from(u16::MAX) + 1;
    let width = bucket_count / sub_range_count;
    let now_ms = wall_time_ms();
    (0..sub_range_count)
        .map(|sub_range_index| {
            let owner_index = sub_range_index * instance_count / sub_range_count;
            let (instance_id, instance) = instances[owner_index as usize];
            ChunkdbRangeBindingValue {
                sub_range_index,
                range_start: sub_range_index * width,
                range_end: if sub_range_index == sub_range_count - 1 {
                    u32::from(u16::MAX)
                } else {
                    (sub_range_index + 1) * width - 1
                },
                instance_id: *instance_id,
                rpc_endpoint: instance.rpc_endpoint.clone(),
                original_instance_id: 0,
                original_endpoint: String::new(),
                status: RangeStatus::Stable as i32,
                last_change_time_ms: now_ms,
            }
        })
        .collect()
}

fn wall_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
