// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk-KV range cutover and serving-grant monitor.

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use crowdb_kv::cluster::group_operations::{KvGroupOperationError, KvGroupScanItem};
use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, DomainFailurePolicy,
    DomainMonitorDescriptor, Id128, OwnerDescriptor, ServingAssignment, ServingGrant, SplitPhase,
    SplitTransition, TransferPhase, TransferTransition,
};
use crowdb_protocol::common::InstanceValue;
use crowdb_protocol::key::{ChunkKvSplitKey, ChunkKvTransferKey, InstanceKey, ServingGrantKey, TextKey};
use sha2::{Digest, Sha256};

use crate::group0_control_plane::Group0ControlPlane;

use super::{DomainMonitorDriver, DomainMonitorFuture};

mod catalog;

pub struct ChunkKvRangeMonitorDriver;

impl ChunkKvRangeMonitorDriver {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for ChunkKvRangeMonitorDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DomainMonitorDriver for ChunkKvRangeMonitorDriver {
    fn domain(&self) -> &'static str {
        "chunk-kv"
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
            plan_dead_owner_transfer(control, descriptor).await?;
            advance_dead_owner_exclusion(control, descriptor).await?;
            publish_ready_transitions(control).await?;
            issue_serving_grants(control, descriptor).await
        })
    }
}

#[allow(clippy::too_many_lines)]
async fn plan_dead_owner_transfer(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
) -> Result<(), String> {
    if descriptor.failure_policy != DomainFailurePolicy::AutomaticSharedStorage {
        return Ok(());
    }
    let Some(catalog) = catalog::load_current(control).await? else {
        return Ok(());
    };
    let now_ms = wall_time_ms();
    let instances = read_instances(control, descriptor).await?;
    let active = read_transfers(control).await?;
    let mut busy_owners = HashSet::new();
    let mut transitioning_partitions = HashSet::new();
    for (transition, _) in &active {
        if !matches!(
            transition.phase,
            TransferPhase::CatalogCommitted | TransferPhase::Aborted
        ) {
            busy_owners.insert(transition.source.instance_id);
            busy_owners.insert(transition.target.instance_id);
            transitioning_partitions.insert(transition.partition_id);
        }
    }
    let healthy_before = now_ms.saturating_sub(descriptor.suspect_after_ms);
    let dead_before = now_ms.saturating_sub(descriptor.dead_after_ms);
    let mut targets: Vec<_> = instances
        .values()
        .filter(|instance| {
            instance.last_heartbeat_ms >= healthy_before && !busy_owners.contains(&instance.instance_id)
        })
        .collect();
    targets.sort_by_key(|instance| {
        let hosted = instance
            .extra
            .as_ref()
            .and_then(|extra| extra.chunk_kv.as_ref())
            .map_or(usize::MAX, |extra| extra.hosted.len());
        (hosted, instance.instance_id)
    });
    for entry in catalog.pages.iter().flat_map(|page| &page.entries) {
        if entry.state != ChunkKvRangeCatalogPartitionState::Serving
            || transitioning_partitions.contains(&entry.partition_id)
            || busy_owners.contains(&entry.owner.instance_id)
            || instances
                .get(&entry.owner.instance_id)
                .is_some_and(|instance| instance.last_heartbeat_ms >= dead_before)
        {
            continue;
        }
        let Some(target) = targets
            .iter()
            .copied()
            .find(|target| target.instance_id != entry.owner.instance_id)
        else {
            return Ok(());
        };
        let target_epoch = entry
            .owner_epoch
            .checked_add(1)
            .ok_or_else(|| "chunk-KV owner epoch overflowed".to_string())?;
        let transition_id = transfer_id(entry, target.instance_id, target_epoch);
        let old_grant_expires_at_ms = read_grant_expiry(control, entry).await?;
        let transition = TransferTransition {
            transition_id,
            partition_id: entry.partition_id,
            range: entry.range.clone(),
            source: entry.owner.clone(),
            source_epoch: entry.owner_epoch,
            target: OwnerDescriptor {
                instance_id: target.instance_id,
                rpc_endpoint: target.rpc_endpoint.clone(),
            },
            target_epoch,
            artifact: entry.artifact.clone(),
            old_grant_expires_at_ms,
            phase: TransferPhase::AwaitingFence,
            release_proof: None,
            readiness_proof: None,
            failure: None,
        };
        transition.validate().map_err(|error| error.to_string())?;
        let path = ChunkKvTransferKey { transition_id }.to_path();
        let encoded = serde_json::to_vec(&transition).map_err(|error| error.to_string())?;
        match control
            .compare_and_put(Bytes::from(path.clone()), Bytes::from(encoded), 0)
            .await
        {
            Ok(_) => return Ok(()),
            Err(KvGroupOperationError::CompareFailed { .. }) => {
                let current = control
                    .get(path.as_bytes())
                    .await
                    .map_err(|error| operation_error(&error))?;
                let persisted: TransferTransition = serde_json::from_slice(
                    current
                        .value
                        .as_deref()
                        .ok_or_else(|| "transfer create race disappeared".to_string())?,
                )
                .map_err(|error| error.to_string())?;
                if persisted == transition {
                    return Ok(());
                }
                return Err("deterministic transfer identity conflicts".into());
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

async fn advance_dead_owner_exclusion(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
) -> Result<(), String> {
    let now_ms = wall_time_ms();
    let dead_before = now_ms.saturating_sub(descriptor.dead_after_ms);
    let instances = read_instances(control, descriptor).await?;
    for (mut transition, item) in read_transfers(control).await? {
        if transition.phase != TransferPhase::AwaitingFence
            || transition.release_proof.is_some()
            || instances
                .get(&transition.source.instance_id)
                .is_some_and(|instance| instance.last_heartbeat_ms >= dead_before)
        {
            continue;
        }
        let activation_not_before_ms = transition
            .old_grant_expires_at_ms
            .checked_add(descriptor.max_clock_skew_ms)
            .ok_or_else(|| "lease exclusion boundary overflowed".to_string())?;
        if now_ms < activation_not_before_ms {
            continue;
        }
        transition.release_proof = Some(AuthorityReleaseProof::LeaseExpired {
            activation_not_before_ms,
        });
        transition.phase = TransferPhase::TargetPreparing;
        transition.validate().map_err(|error| error.to_string())?;
        persist_transition(control, &item, &transition).await?;
    }
    Ok(())
}

async fn publish_ready_transitions(control: &Group0ControlPlane) -> Result<(), String> {
    for item in control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 128)
        .await
        .map_err(|error| operation_error(&error))?
    {
        let mut transition: TransferTransition = decode_transition(&item)?;
        if transition.phase == TransferPhase::TargetPrepared {
            catalog::publish_transfer(control, &transition).await?;
            transition.phase = TransferPhase::CatalogCommitted;
            transition.validate().map_err(|error| error.to_string())?;
            persist_transition(control, &item, &transition).await?;
        }
    }
    for item in control
        .scan_all_prefix(Bytes::from(ChunkKvSplitKey::text_prefix_all()), 128)
        .await
        .map_err(|error| operation_error(&error))?
    {
        let mut transition: SplitTransition = decode_transition(&item)?;
        if transition.phase == SplitPhase::ChildrenPrepared {
            catalog::publish_split(control, &transition).await?;
            transition.phase = SplitPhase::CatalogCommitted;
            transition.validate().map_err(|error| error.to_string())?;
            persist_transition(control, &item, &transition).await?;
        }
    }
    Ok(())
}

async fn issue_serving_grants(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
) -> Result<(), String> {
    let Some(catalog) = catalog::load_current(control).await? else {
        return Ok(());
    };
    let ready_instances = read_ready_instances(control, descriptor).await?;
    let mut assignments: HashMap<u64, Vec<ServingAssignment>> = HashMap::new();
    for entry in catalog.pages.iter().flat_map(|page| &page.entries) {
        let assignment = ServingAssignment {
            partition_id: entry.partition_id,
            owner_epoch: entry.owner_epoch,
        };
        if entry.state == ChunkKvRangeCatalogPartitionState::Serving
            && ready_instances
                .get(&entry.owner.instance_id)
                .is_some_and(|ready| ready.contains(&assignment))
        {
            assignments
                .entry(entry.owner.instance_id)
                .or_default()
                .push(assignment);
        }
    }
    let now_ms = wall_time_ms();
    for (instance_id, assignments) in assignments {
        let path = ServingGrantKey { instance_id }.to_path();
        let current = control
            .get(path.as_bytes())
            .await
            .map_err(|error| operation_error(&error))?;
        let prior_sequence = match current.value.as_deref() {
            Some(bytes) => {
                let grant: ServingGrant = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
                grant.validate().map_err(|error| error.to_string())?;
                grant.lease_sequence
            }
            None => 0,
        };
        let mut grant = ServingGrant {
            instance_id,
            lease_sequence: prior_sequence
                .checked_add(1)
                .ok_or_else(|| "serving grant sequence overflowed".to_string())?,
            catalog_generation: catalog.head.generation,
            issued_at_ms: now_ms,
            expires_at_ms: now_ms
                .checked_add(descriptor.lease_duration_ms)
                .ok_or_else(|| "serving grant deadline overflowed".to_string())?,
            assignments,
            assignment_digest: [0; 32],
        };
        grant.seal();
        grant.validate().map_err(|error| error.to_string())?;
        let encoded = serde_json::to_vec(&grant).map_err(|error| error.to_string())?;
        control
            .compare_and_put(Bytes::from(path), Bytes::from(encoded), current.revision)
            .await
            .map_err(|error| operation_error(&error))?;
    }
    Ok(())
}

async fn read_ready_instances(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
) -> Result<HashMap<u64, HashSet<ServingAssignment>>, String> {
    let prefix = InstanceKey::text_prefix_for_service(&descriptor.service_registry_name);
    let items = control
        .scan_all_prefix(Bytes::from(prefix), 256)
        .await
        .map_err(|error| operation_error(&error))?;
    let healthy_after = wall_time_ms().saturating_sub(descriptor.suspect_after_ms);
    let mut ready = HashMap::new();
    for item in items {
        let value: InstanceValue = serde_json::from_slice(&item.value).map_err(|error| error.to_string())?;
        if value.last_heartbeat_ms < healthy_after {
            continue;
        }
        let Some(extra) = value.extra.and_then(|extra| extra.chunk_kv) else {
            return Err(format!(
                "chunk-KV instance {} omitted its chunk-KV observation",
                value.instance_id
            ));
        };
        let assignments = extra
            .hosted
            .into_iter()
            .filter(|hosted| !hosted.recovering)
            .map(|hosted| ServingAssignment {
                partition_id: hosted.partition_id,
                owner_epoch: hosted.owner_epoch,
            })
            .collect();
        ready.insert(value.instance_id, assignments);
    }
    Ok(ready)
}

async fn read_instances(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
) -> Result<HashMap<u64, InstanceValue>, String> {
    let prefix = InstanceKey::text_prefix_for_service(&descriptor.service_registry_name);
    let items = control
        .scan_all_prefix(Bytes::from(prefix), 256)
        .await
        .map_err(|error| operation_error(&error))?;
    let mut instances = HashMap::new();
    for item in items {
        let value: InstanceValue = serde_json::from_slice(&item.value).map_err(|error| error.to_string())?;
        if instances.insert(value.instance_id, value).is_some() {
            return Err("duplicate chunk-KV instance observation".into());
        }
    }
    Ok(instances)
}

async fn read_transfers(
    control: &Group0ControlPlane,
) -> Result<Vec<(TransferTransition, KvGroupScanItem)>, String> {
    let items = control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 128)
        .await
        .map_err(|error| operation_error(&error))?;
    items
        .into_iter()
        .map(|item| {
            let transition: TransferTransition = decode_transition(&item)?;
            transition.validate().map_err(|error| error.to_string())?;
            Ok((transition, item))
        })
        .collect()
}

async fn read_grant_expiry(
    control: &Group0ControlPlane,
    entry: &ChunkKvRangeCatalogEntry,
) -> Result<u64, String> {
    let path = ServingGrantKey {
        instance_id: entry.owner.instance_id,
    }
    .to_path();
    let read = control
        .get(path.as_bytes())
        .await
        .map_err(|error| operation_error(&error))?;
    let Some(bytes) = read.value else {
        return Ok(0);
    };
    let grant: ServingGrant = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    grant.validate().map_err(|error| error.to_string())?;
    let assignment = ServingAssignment {
        partition_id: entry.partition_id,
        owner_epoch: entry.owner_epoch,
    };
    Ok(if grant.assignments.contains(&assignment) {
        grant.expires_at_ms
    } else {
        0
    })
}

fn transfer_id(entry: &ChunkKvRangeCatalogEntry, target_instance_id: u64, target_epoch: u64) -> Id128 {
    let mut digest = Sha256::new();
    digest.update(b"chunk-kv-transfer-v1");
    digest.update(entry.partition_id.high.to_be_bytes());
    digest.update(entry.partition_id.low.to_be_bytes());
    digest.update(entry.owner.instance_id.to_be_bytes());
    digest.update(entry.owner_epoch.to_be_bytes());
    digest.update(target_instance_id.to_be_bytes());
    digest.update(target_epoch.to_be_bytes());
    let digest = digest.finalize();
    Id128 {
        high: u64::from_be_bytes(digest[0..8].try_into().unwrap_or([0; 8])),
        low: u64::from_be_bytes(digest[8..16].try_into().unwrap_or([0; 8])),
    }
}

fn decode_transition<T>(item: &KvGroupScanItem) -> Result<T, String>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_slice(&item.value).map_err(|error| error.to_string())
}

async fn persist_transition<T>(
    control: &Group0ControlPlane,
    prior: &KvGroupScanItem,
    transition: &T,
) -> Result<(), String>
where
    T: serde::Serialize,
{
    let encoded = serde_json::to_vec(transition).map_err(|error| error.to_string())?;
    control
        .compare_and_put(prior.key.clone(), Bytes::from(encoded), prior.revision)
        .await
        .map(|_| ())
        .map_err(|error| operation_error(&error))
}

fn operation_error(error: &KvGroupOperationError) -> String {
    error.to_string()
}

fn wall_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
