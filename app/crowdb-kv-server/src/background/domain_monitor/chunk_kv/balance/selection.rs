// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::PlanningState;
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeBalancePolicy, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, Id128, KeyRange,
};
use crowdb_protocol::common::{ChunkKvExtra, ChunkKvPartitionLoad};
use std::collections::HashMap;

pub(super) fn choose_transfer<'a>(
    entries: &[&'a ChunkKvRangeCatalogEntry],
    state: &PlanningState,
    policy: &ChunkKvRangeBalancePolicy,
    now_ms: u64,
    counts: &HashMap<u64, usize>,
    bytes: &HashMap<u64, u64>,
    count_only: bool,
) -> Option<(&'a ChunkKvRangeCatalogEntry, u64)> {
    let mut best = None;
    for entry in entries.iter().copied().filter(|entry| {
        eligible(entry, state) && cooled_down(entry, &state.last_transferred_ms, policy, now_ms)
    }) {
        let source_id = entry.owner.instance_id;
        let partition_bytes = partition_load(state, entry).map_or(0, effective_bytes);
        for (&target_id, (_, target)) in &state.healthy {
            if !target_accepts(state, policy, source_id, target_id, target, partition_bytes) {
                continue;
            }
            let source_count = counts.get(&source_id).copied().unwrap_or_default();
            let target_count = counts.get(&target_id).copied().unwrap_or_default();
            let fixes_count = source_count > target_count.saturating_add(1);
            if count_only && !fixes_count {
                continue;
            }
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

pub(super) fn partition_load<'a>(
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

pub(super) fn effective_bytes(load: &ChunkKvPartitionLoad) -> u64 {
    load.durable_bytes
        .max(load.live_byte_samples.iter().map(|(_, bytes)| *bytes).sum())
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
