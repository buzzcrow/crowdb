// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Persisted automatic split and count-first placement planning.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use crowdb_kv::cluster::group_operations::KvGroupOperationError;
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeBalancePolicy, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState,
    DomainMonitorDescriptor, Id128, KeyRange, OwnerDescriptor, PartitionArtifact, SplitChildAssignment,
    SplitPhase, SplitTransition, TransferPhase, TransferTransition,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::common::{ChunkKvExtra, ChunkKvPartitionLoad, InstanceValue};
use crowdb_protocol::key::{ChunkKvSplitKey, ChunkKvTransferKey, TextKey};
use sha2::{Digest, Sha256};

use crate::group0_control_plane::Group0ControlPlane;

use super::{
    catalog, operation_error, read_instances, read_splits, read_transfers, transfer_id, wall_time_ms,
};

struct PlanningState {
    healthy: HashMap<u64, (InstanceValue, ChunkKvExtra)>,
    active_partitions: HashSet<Id128>,
    busy_owners: HashSet<u64>,
    last_changed_ms: HashMap<Id128, u64>,
    pending_split_increase: usize,
}

pub async fn plan(control: &Group0ControlPlane, descriptor: &DomainMonitorDescriptor) -> Result<(), String> {
    let Some(catalog) = catalog::load_current(control).await? else {
        return Ok(());
    };
    let policy = descriptor.chunk_kv_range_balance.clone().unwrap_or_default();
    policy.validate().map_err(|error| error.to_string())?;
    let now_ms = wall_time_ms();
    let state = planning_state(control, descriptor, now_ms).await?;
    if state.healthy.is_empty() {
        return Ok(());
    }
    let entries: Vec<_> = catalog
        .pages
        .iter()
        .flat_map(|page| page.entries.iter())
        .collect();
    if let Some(entry) = entries.iter().copied().find(|entry| {
        entry.artifact.tail_overlay.is_some()
            && partition_load(&state, entry).is_some_and(|load| load.independently_recoverable)
    }) {
        catalog::publish_materialized_partition(control, entry.partition_id).await?;
        return Ok(());
    }
    if plan_split(control, &entries, &state, &policy, now_ms).await? {
        return Ok(());
    }
    plan_transfer(control, &entries, &state, &policy, now_ms).await
}

async fn planning_state(
    control: &Group0ControlPlane,
    descriptor: &DomainMonitorDescriptor,
    now_ms: u64,
) -> Result<PlanningState, String> {
    let healthy_after = now_ms.saturating_sub(descriptor.suspect_after_ms);
    let mut healthy = HashMap::new();
    for (_, instance) in read_instances(control, descriptor).await? {
        if instance.last_heartbeat_ms < healthy_after {
            continue;
        }
        let Some(extra) = instance.extra.clone().and_then(|extra| extra.chunk_kv) else {
            return Err(format!(
                "chunk-KV instance {} omitted its chunk-KV observation",
                instance.instance_id
            ));
        };
        healthy.insert(instance.instance_id, (instance, extra));
    }
    let mut state = PlanningState {
        healthy,
        active_partitions: HashSet::new(),
        busy_owners: HashSet::new(),
        last_changed_ms: HashMap::new(),
        pending_split_increase: 0,
    };
    for (transition, _) in read_transfers(control).await? {
        remember_change(
            &mut state.last_changed_ms,
            transition.partition_id,
            transition.planned_at_ms,
        );
        if !matches!(
            transition.phase,
            TransferPhase::CatalogCommitted | TransferPhase::Aborted
        ) {
            state.active_partitions.insert(transition.partition_id);
            state.busy_owners.insert(transition.source.instance_id);
            state.busy_owners.insert(transition.target.instance_id);
        }
    }
    for (transition, _) in read_splits(control).await? {
        for partition_id in [
            transition.parent_id,
            transition.left.partition_id,
            transition.right.partition_id,
        ] {
            remember_change(&mut state.last_changed_ms, partition_id, transition.planned_at_ms);
        }
        if !matches!(
            transition.phase,
            SplitPhase::CatalogCommitted | SplitPhase::Aborted
        ) {
            state.pending_split_increase = state.pending_split_increase.saturating_add(1);
            state.active_partitions.insert(transition.parent_id);
            state.busy_owners.insert(transition.parent_owner.instance_id);
            state.busy_owners.insert(transition.left.owner.instance_id);
            state.busy_owners.insert(transition.right.owner.instance_id);
        }
    }
    Ok(state)
}

fn remember_change(changes: &mut HashMap<Id128, u64>, partition_id: Id128, planned_at_ms: u64) {
    changes
        .entry(partition_id)
        .and_modify(|current| *current = (*current).max(planned_at_ms))
        .or_insert(planned_at_ms);
}

async fn plan_split(
    control: &Group0ControlPlane,
    entries: &[&ChunkKvRangeCatalogEntry],
    state: &PlanningState,
    policy: &ChunkKvRangeBalancePolicy,
    now_ms: u64,
) -> Result<bool, String> {
    if state.pending_split_increase != 0 {
        return Ok(false);
    }
    let desired = state
        .healthy
        .len()
        .saturating_mul(policy.target_partitions_per_owner as usize);
    let count_shortfall = entries.len() < desired;
    let mut candidates = Vec::new();
    for entry in entries {
        let Some(load) = partition_load(state, entry) else {
            continue;
        };
        let effective_bytes = effective_bytes(load);
        if (count_shortfall || effective_bytes > policy.target_partition_bytes)
            && eligible(entry, state, policy, now_ms)
        {
            if let Some(split_key) = live_byte_median(&entry.range, &load.live_byte_samples) {
                candidates.push((*entry, split_key, effective_bytes));
            }
        }
    }
    candidates.sort_unstable_by(|left, right| {
        right
            .2
            .cmp(&left.2)
            .then_with(|| left.0.partition_id.cmp(&right.0.partition_id))
    });
    let Some((entry, split_key, _)) = candidates.into_iter().next() else {
        return Ok(false);
    };
    let transition = split_transition(entry, split_key, now_ms)?;
    persist_new(
        control,
        ChunkKvSplitKey {
            transition_id: transition.transition_id,
        }
        .to_path(),
        &transition,
    )
    .await?;
    Ok(true)
}

async fn plan_transfer(
    control: &Group0ControlPlane,
    entries: &[&ChunkKvRangeCatalogEntry],
    state: &PlanningState,
    policy: &ChunkKvRangeBalancePolicy,
    now_ms: u64,
) -> Result<(), String> {
    let (counts, bytes) = owner_loads(entries, state);
    let Some((entry, target_id)) = choose_transfer(entries, state, policy, now_ms, &counts, &bytes) else {
        return Ok(());
    };
    let (target, _) = state
        .healthy
        .get(&target_id)
        .ok_or_else(|| "chunk-KV balance target disappeared".to_string())?;
    let target_epoch = entry
        .owner_epoch
        .checked_add(1)
        .ok_or_else(|| "chunk-KV owner epoch overflowed".to_string())?;
    let transition_id = transfer_id(entry, target_id, target_epoch);
    let mut target_artifact = entry.artifact.clone();
    target_artifact.stream_name = crowdb_protocol::chunk_stream::StreamName {
        high: transition_id.high,
        low: transition_id.low,
    };
    target_artifact.tail_overlay = None;
    let transition = TransferTransition {
        transition_id,
        partition_id: entry.partition_id,
        range: entry.range.clone(),
        source: entry.owner.clone(),
        source_epoch: entry.owner_epoch,
        target: OwnerDescriptor {
            instance_id: target_id,
            rpc_endpoint: target.rpc_endpoint.clone(),
        },
        target_epoch,
        artifact: entry.artifact.clone(),
        target_artifact,
        readiness_limits: crowdb_protocol::chunk_kv::TransferReadinessLimits {
            max_tail_records: 65_536,
            max_tail_bytes: 256 * 1024 * 1024,
            max_estimated_catchup_ms: policy.cooldown_ms.max(1),
            prepare_deadline_ms: now_ms.saturating_add(policy.cooldown_ms.max(1)),
            forwarding_grace_ms: policy.cooldown_ms.max(1),
        },
        planned_at_ms: now_ms,
        old_grant_expires_at_ms: 0,
        phase: TransferPhase::Planned,
        release_proof: None,
        readiness_proof: None,
        catchup_proof: None,
        failure: None,
    };
    transition.validate().map_err(|error| error.to_string())?;
    persist_new(
        control,
        ChunkKvTransferKey {
            transition_id: transition.transition_id,
        }
        .to_path(),
        &transition,
    )
    .await
}

fn owner_loads(
    entries: &[&ChunkKvRangeCatalogEntry],
    state: &PlanningState,
) -> (HashMap<u64, usize>, HashMap<u64, u64>) {
    let mut counts = state
        .healthy
        .keys()
        .map(|owner| (*owner, 0_usize))
        .collect::<HashMap<_, _>>();
    let mut bytes = state
        .healthy
        .keys()
        .map(|owner| (*owner, 0_u64))
        .collect::<HashMap<_, _>>();
    for entry in entries {
        if counts.contains_key(&entry.owner.instance_id) {
            *counts.entry(entry.owner.instance_id).or_default() += 1;
            let load = partition_load(state, entry).map_or(0, effective_bytes);
            let owner_bytes = bytes.entry(entry.owner.instance_id).or_default();
            *owner_bytes = owner_bytes.saturating_add(load);
        }
    }
    (counts, bytes)
}

fn choose_transfer<'a>(
    entries: &[&'a ChunkKvRangeCatalogEntry],
    state: &PlanningState,
    policy: &ChunkKvRangeBalancePolicy,
    now_ms: u64,
    counts: &HashMap<u64, usize>,
    bytes: &HashMap<u64, u64>,
) -> Option<(&'a ChunkKvRangeCatalogEntry, u64)> {
    let mut best = None;
    for entry in entries
        .iter()
        .copied()
        .filter(|entry| eligible(entry, state, policy, now_ms))
    {
        let source_id = entry.owner.instance_id;
        let partition_bytes = partition_load(state, entry).map_or(0, effective_bytes);
        for (&target_id, (_, target)) in &state.healthy {
            if !target_accepts(state, policy, source_id, target_id, target, partition_bytes) {
                continue;
            }
            let source_count = counts.get(&source_id).copied().unwrap_or_default();
            let target_count = counts.get(&target_id).copied().unwrap_or_default();
            let fixes_count = source_count > target_count.saturating_add(1);
            let source_bytes = bytes.get(&source_id).copied().unwrap_or_default();
            let target_bytes = bytes.get(&target_id).copied().unwrap_or_default();
            let old_spread = source_bytes.abs_diff(target_bytes);
            let new_spread = source_bytes
                .saturating_sub(partition_bytes)
                .abs_diff(target_bytes.saturating_add(partition_bytes));
            let improvement = old_spread.saturating_sub(new_spread);
            let weighted = old_spread != 0
                && u128::from(improvement) * 100
                    >= u128::from(old_spread) * u128::from(policy.minimum_weighted_improvement_percent);
            let score = (
                fixes_count,
                improvement,
                std::cmp::Reverse(entry.partition_id),
                std::cmp::Reverse(target_id),
            );
            if (fixes_count || weighted)
                && best
                    .as_ref()
                    .map_or(true, |(best_score, _, _)| score > *best_score)
            {
                best = Some((score, entry, target_id));
            }
        }
    }
    best.map(|(_, entry, target_id)| (entry, target_id))
}

fn target_accepts(
    state: &PlanningState,
    policy: &ChunkKvRangeBalancePolicy,
    source_id: u64,
    target_id: u64,
    target: &ChunkKvExtra,
    partition_bytes: u64,
) -> bool {
    target_id != source_id
        && !state.busy_owners.contains(&target_id)
        && target.capacity_bytes.saturating_sub(target.durable_bytes) >= partition_bytes
        && (policy.max_owner_request_rate == 0 || target.request_rate <= policy.max_owner_request_rate)
}

fn partition_load<'a>(
    state: &'a PlanningState,
    entry: &ChunkKvRangeCatalogEntry,
) -> Option<&'a ChunkKvPartitionLoad> {
    state
        .healthy
        .get(&entry.owner.instance_id)?
        .1
        .partition_loads
        .iter()
        .find(|load| load.partition_id == entry.partition_id)
}

fn effective_bytes(load: &ChunkKvPartitionLoad) -> u64 {
    load.durable_bytes
        .max(load.live_byte_samples.iter().map(|(_, bytes)| *bytes).sum())
}

fn eligible(
    entry: &ChunkKvRangeCatalogEntry,
    state: &PlanningState,
    policy: &ChunkKvRangeBalancePolicy,
    now_ms: u64,
) -> bool {
    entry.state == ChunkKvRangeCatalogPartitionState::Serving
        && entry.artifact.tail_overlay.is_none()
        && partition_load(state, entry).is_some_and(|load| load.independently_recoverable)
        && state.healthy.contains_key(&entry.owner.instance_id)
        && !state.active_partitions.contains(&entry.partition_id)
        && !state.busy_owners.contains(&entry.owner.instance_id)
        && now_ms.saturating_sub(
            state
                .last_changed_ms
                .get(&entry.partition_id)
                .copied()
                .unwrap_or_default(),
        ) >= policy.cooldown_ms
}

fn live_byte_median(range: &KeyRange, samples: &[(Vec<u8>, u64)]) -> Option<Vec<u8>> {
    let total: u128 = samples.iter().map(|(_, bytes)| u128::from(*bytes)).sum();
    if total == 0 {
        return None;
    }
    let mut accumulated = 0_u128;
    for (index, (key, bytes)) in samples.iter().enumerate() {
        accumulated = accumulated.saturating_add(u128::from(*bytes));
        if accumulated.saturating_mul(2) >= total
            && key > &range.start
            && range.end.as_ref().map_or(true, |end| key < end)
            && samples[..index].iter().any(|(left, _)| left < key)
        {
            return Some(key.clone());
        }
    }
    None
}

fn split_transition(
    parent: &ChunkKvRangeCatalogEntry,
    split_key: Vec<u8>,
    now_ms: u64,
) -> Result<SplitTransition, String> {
    let transition_id = derived_id(parent, &split_key, b"transition");
    let child_epoch = parent
        .owner_epoch
        .checked_add(1)
        .ok_or_else(|| "chunk-KV split owner epoch overflowed".to_string())?;
    let left = split_child(
        parent,
        &split_key,
        b"left",
        parent.owner.clone(),
        child_epoch,
        KeyRange {
            start: parent.range.start.clone(),
            end: Some(split_key.clone()),
        },
    );
    let right = split_child(
        parent,
        &split_key,
        b"right",
        parent.owner.clone(),
        child_epoch,
        KeyRange {
            start: split_key.clone(),
            end: parent.range.end.clone(),
        },
    );
    let transition = SplitTransition {
        transition_id,
        parent_id: parent.partition_id,
        parent_range: parent.range.clone(),
        parent_owner: parent.owner.clone(),
        parent_epoch: parent.owner_epoch,
        parent_artifact: parent.artifact.clone(),
        split_key,
        left,
        right,
        planned_at_ms: now_ms,
        phase: SplitPhase::Planned,
        readiness_proof: None,
        failure: None,
    };
    transition.validate().map_err(|error| error.to_string())?;
    Ok(transition)
}

fn split_child(
    parent: &ChunkKvRangeCatalogEntry,
    split_key: &[u8],
    side: &[u8],
    owner: OwnerDescriptor,
    owner_epoch: u64,
    range: KeyRange,
) -> SplitChildAssignment {
    SplitChildAssignment {
        partition_id: derived_id(parent, split_key, &[side, b"-partition"].concat()),
        range,
        owner,
        owner_epoch,
        artifact: PartitionArtifact {
            tree_id: derived_u64(parent, split_key, &[side, b"-tree"].concat()),
            stream_name: StreamName {
                high: derived_u64(parent, split_key, &[side, b"-stream-high"].concat()),
                low: derived_u64(parent, split_key, &[side, b"-stream-low"].concat()),
            },
            tail_overlay: None,
        },
    }
}

fn derived_id(parent: &ChunkKvRangeCatalogEntry, split_key: &[u8], label: &[u8]) -> Id128 {
    let digest = split_digest(parent, split_key, label);
    Id128 {
        high: nonzero(u64::from_be_bytes(digest[0..8].try_into().unwrap_or([0; 8]))),
        low: nonzero(u64::from_be_bytes(digest[8..16].try_into().unwrap_or([0; 8]))),
    }
}

fn derived_u64(parent: &ChunkKvRangeCatalogEntry, split_key: &[u8], label: &[u8]) -> u64 {
    let digest = split_digest(parent, split_key, label);
    nonzero(u64::from_be_bytes(digest[0..8].try_into().unwrap_or([0; 8])))
}

fn split_digest(parent: &ChunkKvRangeCatalogEntry, split_key: &[u8], label: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"chunk-kv-split-v1");
    digest.update(label);
    digest.update(parent.partition_id.high.to_be_bytes());
    digest.update(parent.partition_id.low.to_be_bytes());
    digest.update(parent.owner_epoch.to_be_bytes());
    digest.update(split_key);
    digest.finalize().into()
}

fn nonzero(value: u64) -> u64 {
    value.max(1)
}

async fn persist_new<T: serde::Serialize + PartialEq + serde::de::DeserializeOwned>(
    control: &Group0ControlPlane,
    path: String,
    transition: &T,
) -> Result<(), String> {
    let encoded = serde_json::to_vec(transition).map_err(|error| error.to_string())?;
    match control
        .compare_and_put(Bytes::from(path.clone()), Bytes::from(encoded), 0)
        .await
    {
        Ok(_) => Ok(()),
        Err(KvGroupOperationError::CompareFailed { .. }) => {
            let read = control
                .get(path.as_bytes())
                .await
                .map_err(|error| operation_error(&error))?;
            let current: T = serde_json::from_slice(
                read.value
                    .as_deref()
                    .ok_or_else(|| "balance transition create race disappeared".to_string())?,
            )
            .map_err(|error| error.to_string())?;
            if &current == transition {
                Ok(())
            } else {
                Err("deterministic balance transition identity conflicts".into())
            }
        }
        Err(error) => Err(error.to_string()),
    }
}
