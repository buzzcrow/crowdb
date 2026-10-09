// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::PlanningState;
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeBalancePolicy, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, Id128, KeyRange,
};
use crowdb_protocol::common::{ChunkKvExtra, ChunkKvPartitionLoad};
use std::collections::HashMap;

mod candidates;
mod diagnostics;
use candidates::CandidateContext;

pub(super) struct Selection<'a> {
    pub candidate: Option<(&'a ChunkKvRangeCatalogEntry, u64)>,
    pub observation: crowdb_protocol::chunk_kv::balance::BalanceObservation,
}

pub(super) fn choose_transfer<'a>(
    entries: &[&'a ChunkKvRangeCatalogEntry],
    state: &PlanningState,
    policy: &ChunkKvRangeBalancePolicy,
    now_ms: u64,
    generation: u64,
    valid_for_ms: u64,
) -> Selection<'a> {
    use crowdb_protocol::chunk_kv::balance::{
        BalancePartitionObservation, BalanceWeights, MAX_OBSERVED_OWNERS, MAX_OBSERVED_PARTITIONS,
        WEIGHT_SCALE,
    };
    let mut result = Selection {
        candidate: None,
        observation: diagnostics::empty(policy, now_ms, generation, valid_for_ms),
    };
    let Some((counts, bytes)) = owner_loads(entries, state) else {
        return result;
    };
    let loads: Vec<_> = counts.iter().map(|(id, count)| (*count, bytes[id])).collect();
    let Some(weights) = BalanceWeights::new(&loads, policy.byte_weight_percent) else {
        return result;
    };
    result.observation.deviation_percent_millionths =
        u64::try_from(weights.deviation_units * 100_000_000 / u128::from(WEIGHT_SCALE)).unwrap_or(u64::MAX);
    result.observation.loss_millionths =
        Some(u64::try_from(weights.loss / 1_000_000_000_000).unwrap_or(u64::MAX));
    let within = weights.within_tolerance(policy.imbalance_tolerance_percent);
    result.observation.reason = if within {
        "within tolerance"
    } else {
        "no useful safe move"
    }
    .into();
    if counts.len() <= MAX_OBSERVED_OWNERS {
        result.observation.owners = diagnostics::owners(&counts, &bytes, &weights, state);
    }
    let context = CandidateContext {
        state,
        policy,
        weights: &weights,
        counts: &counts,
        bytes: &bytes,
        now_ms,
    };
    let mut best = None;
    let mut best_rejected = None;
    for entry in entries.iter().copied() {
        let partition_bytes = state.partition_bytes[&(entry.owner.instance_id, entry.partition_id)];
        let (accepted, rejected, partition_reason) = context.assess(entry, within);
        candidates::retain_best(&mut best, accepted);
        candidates::retain_best(&mut best_rejected, rejected);
        if entries.len() <= MAX_OBSERVED_PARTITIONS {
            result.observation.partitions.push(BalancePartitionObservation {
                partition_id: entry.partition_id,
                instance_id: entry.owner.instance_id,
                owner_epoch: entry.owner_epoch,
                estimated_bytes: partition_bytes,
                weight: weights.weight(1, partition_bytes),
                reason: partition_reason.into(),
            });
        }
    }
    if let Some((_, entry, target_id)) = best {
        result.candidate = Some((entry, target_id));
        result.observation.reason = "move selected".into();
        if let Some(partition) = result
            .observation
            .partitions
            .iter_mut()
            .find(|partition| partition.partition_id == entry.partition_id)
        {
            partition.reason = "selected move".into();
        }
    }
    result.observation.candidate = best
        .or(best_rejected)
        .map(|choice| diagnostics::candidate(choice, &context));
    result
}

fn exclusion_reason(
    entry: &ChunkKvRangeCatalogEntry,
    state: &PlanningState,
    policy: &ChunkKvRangeBalancePolicy,
    now_ms: u64,
) -> Option<&'static str> {
    if entry.artifact.tail_overlay.is_some() {
        Some("inherited overlay")
    } else if state.busy_owners.contains(&entry.owner.instance_id)
        || state.active_partitions.contains(&entry.partition_id)
        || entry.transition_id.is_some()
    {
        Some("active transition")
    } else if !eligible(entry, state) {
        Some("not independently serving")
    } else if !cooled_down(entry, &state.last_transferred_ms, policy, now_ms) {
        Some("cooldown")
    } else {
        None
    }
}

fn owner_loads(
    entries: &[&ChunkKvRangeCatalogEntry],
    state: &PlanningState,
) -> Option<(HashMap<u64, u64>, HashMap<u64, u64>)> {
    let mut counts: HashMap<_, _> = state.healthy.keys().map(|id| (*id, 0_u64)).collect();
    let mut bytes = counts.clone();
    for entry in entries {
        if state
            .hosted_epochs
            .get(&(entry.owner.instance_id, entry.partition_id))
            != Some(&entry.owner_epoch)
        {
            return None;
        }
        let count = counts.get_mut(&entry.owner.instance_id)?;
        *count = count.checked_add(1)?;
        let total_bytes = bytes.get_mut(&entry.owner.instance_id)?;
        *total_bytes = total_bytes.checked_add(
            *state
                .partition_bytes
                .get(&(entry.owner.instance_id, entry.partition_id))?,
        )?;
    }
    Some((counts, bytes))
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

pub(super) fn partition_load<'a>(
    state: &'a PlanningState,
    entry: &ChunkKvRangeCatalogEntry,
) -> Option<&'a ChunkKvPartitionLoad> {
    state
        .partition_loads
        .get(&(entry.owner.instance_id, entry.partition_id))
}

pub(super) fn effective_bytes(load: &ChunkKvPartitionLoad) -> u64 {
    load.durable_bytes.max(
        load.live_byte_samples
            .iter()
            .fold(0_u64, |sum, (_, bytes)| sum.saturating_add(*bytes)),
    )
}

pub(super) fn eligible(entry: &ChunkKvRangeCatalogEntry, state: &PlanningState) -> bool {
    entry.state == ChunkKvRangeCatalogPartitionState::Serving
        && entry.transition_id.is_none()
        && entry.artifact.tail_overlay.is_none()
        && partition_load(state, entry).is_some_and(|load| load.independently_recoverable)
        && state.healthy.contains_key(&entry.owner.instance_id)
        && !state.active_partitions.contains(&entry.partition_id)
        && !state.busy_owners.contains(&entry.owner.instance_id)
}

pub(super) fn cooled_down(
    entry: &ChunkKvRangeCatalogEntry,
    changes: &HashMap<Id128, u64>,
    policy: &ChunkKvRangeBalancePolicy,
    now_ms: u64,
) -> bool {
    now_ms.saturating_sub(changes.get(&entry.partition_id).copied().unwrap_or_default()) >= policy.cooldown_ms
}

pub(super) fn live_byte_median(range: &KeyRange, samples: &[(Vec<u8>, u64)]) -> Option<Vec<u8>> {
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
