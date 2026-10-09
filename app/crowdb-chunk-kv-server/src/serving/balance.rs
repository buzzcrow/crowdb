// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{balance::BalanceWeights, ChunkKvRangeBalancePolicy, Id128, KeyRange};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalanceConfig {
    #[serde(default = "crowdb_protocol::chunk_kv::balance::default_byte_weight_percent")]
    pub byte_weight_percent: u32,
    #[serde(default = "crowdb_protocol::chunk_kv::balance::default_tolerance_percent")]
    pub imbalance_tolerance_percent: u32,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub target_partitions_per_owner: usize,
    pub target_partition_bytes: u64,
    pub minimum_weighted_improvement_percent: u32,
    pub cooldown_ms: u64,
    pub max_owner_request_rate: u64,
}

impl Default for BalanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            byte_weight_percent: 0,
            imbalance_tolerance_percent: 20,
            target_partitions_per_owner: 4,
            target_partition_bytes: 1 << 30,
            minimum_weighted_improvement_percent: 25,
            cooldown_ms: 60 * 1_000,
            max_owner_request_rate: 0,
        }
    }
}

const fn default_enabled() -> bool {
    true
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
    /// True only after split-parent tail ownership has been materialized.
    pub independently_recoverable: bool,
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
            live_byte_median(&partition.range, &partition.live_byte_samples).map(|split_key| {
                (
                    SplitProposal {
                        partition_id: partition.partition_id,
                        split_key,
                    },
                    partition.durable_bytes,
                )
            })
        })
        .max_by_key(|(_, bytes)| *bytes)
        .map(|(proposal, _)| proposal)
}

#[must_use]
pub fn choose_transfer(
    owners: &[OwnerLoad],
    partitions: &[PartitionLoad],
    now_ms: u64,
    config: &BalanceConfig,
) -> Option<TransferProposal> {
    if !config.enabled || config.byte_weight_percent != 0 {
        return None;
    }
    let live: Vec<_> = owners.iter().filter(|owner| owner.healthy).collect();
    let loads: Vec<_> = live
        .iter()
        .map(|owner| (owner.partition_count as u64, owner.durable_bytes))
        .collect();
    let weights = BalanceWeights::new(&loads, config.byte_weight_percent)?;
    let policy = ChunkKvRangeBalancePolicy {
        byte_weight_percent: config.byte_weight_percent,
        imbalance_tolerance_percent: config.imbalance_tolerance_percent,
        minimum_weighted_improvement_percent: config.minimum_weighted_improvement_percent,
        ..ChunkKvRangeBalancePolicy::default()
    };
    if policy.validate().is_err() || weights.within_tolerance(policy.imbalance_tolerance_percent) {
        return None;
    }
    let mut best = None;
    for source in live.iter().filter(|owner| !owner.transfer_active) {
        for partition in partitions.iter().filter(|partition| {
            partition.owner_instance_id == source.instance_id && eligible_partition(partition, now_ms, config)
        }) {
            for target in live
                .iter()
                .filter(|target| target.instance_id != source.instance_id && !target.transfer_active)
            {
                if target.headroom_bytes < partition.durable_bytes
                    || config.max_owner_request_rate != 0
                        && target.request_rate.saturating_add(partition.request_rate)
                            > config.max_owner_request_rate
                {
                    continue;
                }
                let Some(improvement) = weights.improvement(
                    (source.partition_count as u64, source.durable_bytes),
                    (target.partition_count as u64, target.durable_bytes),
                    partition.durable_bytes,
                ) else {
                    continue;
                };
                let score = (
                    improvement,
                    std::cmp::Reverse(partition.partition_id),
                    std::cmp::Reverse(target.instance_id),
                );
                if weights.qualifies(improvement, &policy)
                    && best.as_ref().map_or(true, |(prior, _)| score > *prior)
                {
                    best = Some((
                        score,
                        TransferProposal {
                            partition_id: partition.partition_id,
                            source_instance_id: source.instance_id,
                            target_instance_id: target.instance_id,
                        },
                    ));
                }
            }
        }
    }
    best.map(|(_, proposal)| proposal)
}

fn eligible_partition(partition: &PartitionLoad, now_ms: u64, config: &BalanceConfig) -> bool {
    partition.independently_recoverable
        && !partition.transition_active
        && now_ms.saturating_sub(partition.last_moved_ms) >= config.cooldown_ms
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
            && samples.first().is_some_and(|(left, _)| left < key)
        {
            return Some(key.clone());
        }
    }
    None
}
