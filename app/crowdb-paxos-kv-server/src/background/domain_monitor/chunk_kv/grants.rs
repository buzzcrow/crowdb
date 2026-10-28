// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Grant publication serialized with process replacement and renewal.

use super::{catalog, operation_error, wall_time_ms};
use crate::group0_control_plane::Group0ControlPlane;
use bytes::Bytes;
use crowdb_paxos_kv::cluster::group_operations::KvGroupMutation;
use crowdb_protocol::{
    chunk_kv::{ChunkKvRangeCatalogPartitionState, DomainMonitorDescriptor, ServingAssignment, ServingGrant},
    chunk_stream::{StreamProcessLease, StreamPurpose},
    common::InstanceValue,
    key::{InstanceKey, ServingGrantKey, StreamProcessLeaseKey, TextKey},
};
use std::collections::{HashMap, HashSet};

struct ReadyInstance {
    process_epoch: u64,
    assignments: HashSet<ServingAssignment>,
}

pub(super) async fn issue(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
) -> Result<(), String> {
    let Some(catalog) = catalog::load_current(control).await? else {
        return Ok(());
    };
    let ready = read_ready(control, descriptor).await?;
    let mut assignments: HashMap<u64, Vec<ServingAssignment>> = HashMap::new();
    for entry in catalog.pages.iter().flat_map(|page| &page.entries) {
        let assignment = ServingAssignment {
            partition_id: entry.partition_id,
            owner_epoch: entry.owner_epoch,
        };
        if entry.state == ChunkKvRangeCatalogPartitionState::Serving
            && ready
                .get(&entry.owner.instance_id)
                .is_some_and(|instance| instance.assignments.contains(&assignment))
        {
            assignments
                .entry(entry.owner.instance_id)
                .or_default()
                .push(assignment);
        }
    }
    for (instance_id, assignments) in assignments {
        publish(
            control,
            descriptor,
            instance_id,
            ready[&instance_id].process_epoch,
            catalog.head.generation,
            assignments,
        )
        .await?;
    }
    Ok(())
}

async fn publish(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
    instance_id: u64,
    process_epoch: u64,
    generation: u64,
    assignments: Vec<ServingAssignment>,
) -> Result<(), String> {
    let lease_path = StreamProcessLeaseKey {
        purpose: StreamPurpose::Wal,
        instance_id,
    }
    .to_path();
    let current = control
        .get(lease_path.as_bytes())
        .await
        .map_err(|error| operation_error(&error))?;
    let Some(bytes) = current.value.as_deref() else {
        return Ok(());
    };
    let mut lease: StreamProcessLease = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    lease.validate().map_err(|error| error.to_string())?;
    let now = wall_time_ms();
    if lease.identity.purpose != StreamPurpose::Wal || lease.identity.instance_id != instance_id {
        return Err("WAL process lease does not match its key".into());
    }
    if lease.identity.process_epoch != process_epoch || now >= lease.expires_at_ms {
        return Ok(());
    }
    let mut grant = ServingGrant {
        instance_id,
        process_epoch,
        lease_sequence: lease
            .grant_sequence
            .checked_add(1)
            .ok_or("serving grant sequence overflowed")?,
        catalog_generation: generation,
        issued_at_ms: now,
        expires_at_ms: now
            .checked_add(descriptor.lease_duration_ms)
            .ok_or("serving grant deadline overflowed")?
            .min(lease.expires_at_ms),
        assignments,
        assignment_digest: [0; 32],
    };
    grant.seal();
    grant.validate().map_err(|error| error.to_string())?;
    lease.grant_sequence = grant.lease_sequence;
    lease.grant_expires_at_ms = lease.grant_expires_at_ms.max(grant.expires_at_ms);
    let mutations = [
        KvGroupMutation::Put {
            key: Bytes::from(lease_path.clone()),
            value: Bytes::from(serde_json::to_vec(&lease).map_err(|error| error.to_string())?),
        },
        KvGroupMutation::Put {
            key: Bytes::from(ServingGrantKey { instance_id }.to_path()),
            value: Bytes::from(serde_json::to_vec(&grant).map_err(|error| error.to_string())?),
        },
    ];
    control
        .compare_and_batch(&mutations, Bytes::from(lease_path), current.revision)
        .await
        .map_err(|error| operation_error(&error))?;
    Ok(())
}

async fn read_ready(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
) -> Result<HashMap<u64, ReadyInstance>, String> {
    let items = control
        .scan_all_prefix(
            Bytes::from(InstanceKey::text_prefix_for_service(
                &descriptor.service_registry_name,
            )),
            256,
        )
        .await
        .map_err(|error| operation_error(&error))?;
    let healthy_after = wall_time_ms().saturating_sub(descriptor.suspect_after_ms);
    let mut ready = HashMap::new();
    for item in items {
        let value: InstanceValue = serde_json::from_slice(&item.value).map_err(|error| error.to_string())?;
        if value.last_heartbeat_ms < healthy_after {
            continue;
        }
        let extra = value
            .extra
            .and_then(|extra| extra.chunk_kv)
            .ok_or_else(|| format!("chunk-KV instance {} omitted its observation", value.instance_id))?;
        if extra.process_epoch == 0 {
            return Err("chunk-KV observation has no process epoch".into());
        }
        let assignments = extra
            .hosted
            .into_iter()
            .filter(|hosted| !hosted.recovering)
            .map(|hosted| ServingAssignment {
                partition_id: hosted.partition_id,
                owner_epoch: hosted.owner_epoch,
            })
            .collect();
        ready.insert(
            value.instance_id,
            ReadyInstance {
                process_epoch: extra.process_epoch,
                assignments,
            },
        );
    }
    Ok(ready)
}
