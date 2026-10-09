// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Persisted automatic split and unified-weight placement planning.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use crowdb_kv::cluster::group_operations::KvGroupOperationError;
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeBalancePolicy, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState,
    DomainMonitorDescriptor, Id128, OwnerDescriptor, SplitPhase, TransferPhase, TransferTransition,
};
use crowdb_protocol::common::{ChunkKvExtra, ChunkKvPartitionLoad, InstanceValue};
use crowdb_protocol::key::{ChunkKvSplitKey, ChunkKvTransferKey, TextKey};
use tracing::info;

use crate::group0_control_plane::Group0ControlPlane;

use super::{
    catalog, operation_error, read_instances, read_splits, read_transfers, transfer_id, wall_time_ms,
};

mod observation;
mod selection;
mod split;

use selection::{choose_transfer, cooled_down, effective_bytes, eligible, live_byte_median, partition_load};
use split::split_transition;

struct PlanningState {
    healthy: HashMap<u64, (InstanceValue, ChunkKvExtra)>,
    partition_loads: HashMap<(u64, Id128), ChunkKvPartitionLoad>,
    partition_bytes: HashMap<(u64, Id128), u64>,
    hosted_epochs: HashMap<(u64, Id128), u64>,
    active_partitions: HashSet<Id128>,
    busy_owners: HashSet<u64>,
    last_changed_ms: HashMap<Id128, u64>,
    last_transferred_ms: HashMap<Id128, u64>,
    pending_split_increase: usize,
}

pub async fn plan(control: &Group0ControlPlane, descriptor: &DomainMonitorDescriptor) -> Result<(), String> {
    let Some(mut catalog) = catalog::load_current(control).await? else {
        return Ok(());
    };
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
        entry.state == ChunkKvRangeCatalogPartitionState::Serving
            && entry.artifact.tail_overlay.is_some()
            && partition_load(&state, entry).is_some_and(|load| load.independently_recoverable)
    }) {
        let committed_transfer = read_transfers(control).await?.into_iter().any(|(transition, _)| {
            transition.phase == TransferPhase::CatalogCommitted
                && entry.transition_id == Some(transition.transition_id)
                && entry.partition_id == transition.partition_id
                && entry.range == transition.range
                && entry.owner == transition.target
                && entry.owner_epoch == transition.target_epoch
                && entry.artifact == transition.target_artifact
        });
        if committed_transfer {
            catalog::publish_materialized_transfer(control, entry.partition_id).await?;
        } else {
            catalog::publish_materialized_partition(control, entry.partition_id).await?;
        }
        // Materialization only releases an immutable parent-stream overlay.
        // It does not affect request routing, so immediately plan from the
        // refreshed catalog instead of inserting a control-plane idle cycle
        // before the next independent split.
        catalog = catalog::load_current(control)
            .await?
            .ok_or_else(|| "catalog disappeared after split materialization".to_string())?;
    }
    let entries: Vec<_> = catalog
        .pages
        .iter()
        .flat_map(|page| page.entries.iter())
        .collect();
    let Some(policy) = descriptor.chunk_kv_range_balance.as_ref() else {
        return Ok(());
    };
    policy.validate().map_err(|error| error.to_string())?;
    let selection = choose_transfer(
        &entries,
        &state,
        policy,
        now_ms,
        catalog.head.generation,
        descriptor.suspect_after_ms,
    );
    if let Some((entry, target_id)) = selection.candidate {
        plan_transfer(control, entry, target_id, &state, now_ms).await?;
    } else {
        plan_split(control, &entries, &state, policy, now_ms).await?;
    }
    observation::publish(control, &selection.observation).await;
    Ok(())
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
    let partition_loads = healthy
        .iter()
        .flat_map(|(id, (_, extra))| {
            extra
                .partition_loads
                .iter()
                .map(move |load| ((*id, load.partition_id), load.clone()))
        })
        .collect();
    let partition_bytes = healthy
        .iter()
        .flat_map(|(id, (_, extra))| {
            extra
                .partition_loads
                .iter()
                .map(move |load| ((*id, load.partition_id), effective_bytes(load)))
        })
        .collect();
    let hosted_epochs = healthy
        .iter()
        .flat_map(|(id, (_, extra))| {
            extra
                .hosted
                .iter()
                .filter(|hosted| !hosted.recovering)
                .map(move |hosted| ((*id, hosted.partition_id), hosted.owner_epoch))
        })
        .collect();
    let mut state = PlanningState {
        healthy,
        partition_loads,
        partition_bytes,
        hosted_epochs,
        active_partitions: HashSet::new(),
        busy_owners: HashSet::new(),
        last_changed_ms: HashMap::new(),
        last_transferred_ms: HashMap::new(),
        pending_split_increase: 0,
    };
    for (transition, _) in read_transfers(control).await? {
        remember_change(
            &mut state.last_transferred_ms,
            transition.partition_id,
            transition.planned_at_ms,
        );
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
        for partition_id in [transition.parent_id, transition.child.partition_id] {
            remember_change(&mut state.last_changed_ms, partition_id, transition.planned_at_ms);
        }
        if !matches!(
            transition.phase,
            SplitPhase::CatalogCommitted | SplitPhase::Aborted
        ) {
            state.pending_split_increase = state.pending_split_increase.saturating_add(1);
            state.active_partitions.insert(transition.parent_id);
            state.busy_owners.insert(transition.parent_owner.instance_id);
            state.busy_owners.insert(transition.child.owner.instance_id);
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
        let effective_bytes = state.partition_bytes[&(entry.owner.instance_id, entry.partition_id)];
        if (count_shortfall || effective_bytes > policy.target_partition_bytes)
            && eligible(entry, state)
            && cooled_down(entry, &state.last_changed_ms, policy, now_ms)
        {
            if let Some(split_key) = live_byte_median(&entry.range, &load.live_byte_samples) {
                candidates.push((*entry, split_key, effective_bytes));
            }
        }
    }
    let candidate = candidates.into_iter().max_by(|left, right| {
        left.2
            .cmp(&right.2)
            .then_with(|| right.0.partition_id.cmp(&left.0.partition_id))
    });
    let Some((entry, split_key, _)) = candidate else {
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
    info!(
        transition_id_high = transition.transition_id.high,
        transition_id_low = transition.transition_id.low,
        parent_id_high = transition.parent_id.high,
        parent_id_low = transition.parent_id.low,
        parent_epoch = transition.parent_epoch,
        parent_next_epoch = transition.parent_next_epoch,
        child_id_high = transition.child.partition_id.high,
        child_id_low = transition.child.partition_id.low,
        "local split planned"
    );
    Ok(true)
}

async fn plan_transfer(
    control: &Group0ControlPlane,
    entry: &ChunkKvRangeCatalogEntry,
    target_id: u64,
    state: &PlanningState,
    now_ms: u64,
) -> Result<bool, String> {
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
            max_estimated_catchup_ms: TRANSFER_SAFETY_WINDOW_MS,
            prepare_deadline_ms: now_ms.saturating_add(TRANSFER_SAFETY_WINDOW_MS),
            forwarding_grace_ms: TRANSFER_SAFETY_WINDOW_MS,
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
    .await?;
    Ok(true)
}

// Preparation and forwarding bounds are independent of placement pacing.
const TRANSFER_SAFETY_WINDOW_MS: u64 = 10 * 60 * 1_000;

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
