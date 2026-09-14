// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{Id128, KeyRange};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalanceConfig {
    pub target_partitions_per_owner: usize,
    pub target_partition_bytes: u64,
    pub minimum_weighted_improvement_percent: u32,
    pub cooldown_ms: u64,
    pub max_owner_request_rate: u64,
}

impl Default for BalanceConfig {
    fn default() -> Self {
        Self {
            target_partitions_per_owner: 4,
            target_partition_bytes: 1 << 30,
            minimum_weighted_improvement_percent: 25,
            cooldown_ms: 10 * 60 * 1_000,
            max_owner_request_rate: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerLoad {
    pub instance_id: u64,
    pub healthy: bool,
    pub partition_count: usize,
    pub durable_bytes: u64,
    pub request_rate: u64,
    pub headroom_bytes: u64,
    pub transfer_active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionLoad {
    pub partition_id: Id128,
    pub range: KeyRange,
    pub owner_instance_id: u64,
    pub durable_bytes: u64,
    pub request_rate: u64,
    pub last_moved_ms: u64,
    pub transition_active: bool,
    /// Ordered key/live-byte samples supplied by the partition owner.
    pub live_byte_samples: Vec<(Vec<u8>, u64)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferProposal {
    pub partition_id: Id128,
    pub source_instance_id: u64,
    pub target_instance_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitProposal {
    pub partition_id: Id128,
    pub split_key: Vec<u8>,
}

#[must_use]
pub fn desired_partition_count(
    live_owner_count: usize,
    current_partitions: usize,
    config: &BalanceConfig,
) -> usize {
    current_partitions.max(live_owner_count.saturating_mul(config.target_partitions_per_owner))
}

#[must_use]
pub fn choose_split(
    partitions: &[PartitionLoad],
    now_ms: u64,
    config: &BalanceConfig,
) -> Option<SplitProposal> {
    partitions
        .iter()
        .filter(|partition| eligible_partition(partition, now_ms, config))
        .filter(|partition| partition.durable_bytes > config.target_partition_bytes)
        .filter_map(|partition| {
            live_byte_median(&partition.range, &partition.live_byte_samples).map(|split_key| SplitProposal {
                partition_id: partition.partition_id,
                split_key,
            })
        })
        .max_by_key(|proposal| {
            partitions
                .iter()
                .find(|partition| partition.partition_id == proposal.partition_id)
                .map_or(0, |partition| partition.durable_bytes)
        })
}

#[must_use]
pub fn choose_transfer(
    owners: &[OwnerLoad],
    partitions: &[PartitionLoad],
    now_ms: u64,
    config: &BalanceConfig,
) -> Option<TransferProposal> {
    let sources = owners
        .iter()
        .filter(|owner| owner.healthy && !owner.transfer_active);
    let targets: Vec<&OwnerLoad> = owners
        .iter()
        .filter(|owner| owner.healthy && !owner.transfer_active)
        .collect();
    let mut best: Option<(bool, u128, &PartitionLoad, &OwnerLoad)> = None;
    for source in sources {
        for partition in partitions.iter().filter(|partition| {
            partition.owner_instance_id == source.instance_id && eligible_partition(partition, now_ms, config)
        }) {
            for target in targets
                .iter()
                .copied()
                .filter(|target| target.instance_id != source.instance_id)
            {
                if target.headroom_bytes < partition.durable_bytes
                    || config.max_owner_request_rate != 0
                        && target.request_rate.saturating_add(partition.request_rate)
                            > config.max_owner_request_rate
                {
                    continue;
                }
                let fixes_count = source.partition_count > target.partition_count.saturating_add(1);
                let old_spread = source.durable_bytes.abs_diff(target.durable_bytes);
                let new_source = source.durable_bytes.saturating_sub(partition.durable_bytes);
                let new_target = target.durable_bytes.saturating_add(partition.durable_bytes);
                let new_spread = new_source.abs_diff(new_target);
                let improvement = old_spread.saturating_sub(new_spread);
                let weighted_qualifies = old_spread != 0
                    && u128::from(improvement) * 100
                        >= u128::from(old_spread) * u128::from(config.minimum_weighted_improvement_percent);
                if !fixes_count && !weighted_qualifies {
                    continue;
                }
                let score = u128::from(improvement);
                if best.as_ref().map_or(true, |(best_count, best_score, _, _)| {
                    (fixes_count, score) > (*best_count, *best_score)
                }) {
                    best = Some((fixes_count, score, partition, target));
                }
            }
        }
    }
    best.map(|(_, _, partition, target)| TransferProposal {
        partition_id: partition.partition_id,
        source_instance_id: partition.owner_instance_id,
        target_instance_id: target.instance_id,
    })
}

fn eligible_partition(partition: &PartitionLoad, now_ms: u64, config: &BalanceConfig) -> bool {
    !partition.transition_active && now_ms.saturating_sub(partition.last_moved_ms) >= config.cooldown_ms
}

fn live_byte_median(range: &KeyRange, samples: &[(Vec<u8>, u64)]) -> Option<Vec<u8>> {
    let total: u128 = samples.iter().map(|(_, bytes)| u128::from(*bytes)).sum();
    if total == 0 {
        return None;
    }
    let mut accumulated = 0_u128;
    for (key, bytes) in samples {
        accumulated += u128::from(*bytes);
        if accumulated.saturating_mul(2) >= total
            && key.as_slice() > range.start.as_slice()
            && range.end.as_deref().map_or(true, |end| key.as_slice() < end)
        {
            return Some(key.clone());
        }
    }
    None
}
