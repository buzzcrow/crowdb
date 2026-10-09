// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared fixed-point placement arithmetic and bounded diagnostic observations.

use super::{ChunkKvRangeBalancePolicy, Id128};
use serde::{Deserialize, Serialize};

pub const POLICY_VERSION: u32 = 1;
pub const WEIGHT_SCALE: u64 = 1_000_000_000;
pub const OBSERVATION_KEY: &str = "/chunk-kv/balance-observation";
pub const MAX_OBSERVATION_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_OBSERVED_PARTITIONS: usize = 4096;
pub const MAX_OBSERVED_OWNERS: usize = 256;

#[must_use]
pub const fn default_byte_weight_percent() -> u32 {
    0
}
#[must_use]
pub const fn default_tolerance_percent() -> u32 {
    20
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Weight {
    pub byte_units: u64,
    pub count_units: u64,
}
impl Weight {
    #[must_use]
    pub fn units(self) -> u64 {
        self.byte_units + self.count_units
    }
}

/// Totals are frozen for one planning observation; only metadata is aggregated.
#[derive(Clone, Debug)]
pub struct BalanceWeights {
    pub owners: u64,
    pub partitions: u64,
    pub bytes: u128,
    pub byte_weight_percent: u32,
    pub loss: u128,
    pub deviation_units: u128,
}
impl BalanceWeights {
    #[must_use]
    pub fn new(loads: &[(u64, u64)], byte_weight_percent: u32) -> Option<Self> {
        if loads.is_empty() || byte_weight_percent > 100 {
            return None;
        }
        let mut result = Self {
            owners: loads.len().try_into().ok()?,
            partitions: loads
                .iter()
                .try_fold(0_u64, |sum, (count, _)| sum.checked_add(*count))?,
            bytes: loads.iter().map(|(_, bytes)| u128::from(*bytes)).sum(),
            byte_weight_percent,
            loss: 0,
            deviation_units: 0,
        };
        if result.partitions == 0 {
            return None;
        }
        for &(count, bytes) in loads {
            let deviation = result.deviation(result.weight(count, bytes));
            result.deviation_units = result.deviation_units.max(deviation);
            result.loss = result.loss.checked_add(deviation.checked_mul(deviation)?)?;
        }
        Some(result)
    }
    #[must_use]
    pub fn weight(&self, count: u64, bytes: u64) -> Weight {
        let byte_percent = if self.bytes == 0 {
            0
        } else {
            self.byte_weight_percent
        };
        let count_units = u128::from(count) * u128::from(100 - byte_percent) * u128::from(WEIGHT_SCALE)
            / (u128::from(self.partitions) * 100);
        let byte_units = if self.bytes == 0 {
            0
        } else {
            u128::from(bytes) * u128::from(byte_percent) * u128::from(WEIGHT_SCALE) / (self.bytes * 100)
        };
        Weight {
            byte_units: u64::try_from(byte_units).unwrap_or(WEIGHT_SCALE),
            count_units: u64::try_from(count_units).unwrap_or(WEIGHT_SCALE),
        }
    }
    fn deviation(&self, weight: Weight) -> u128 {
        (u128::from(weight.units()) * u128::from(self.owners)).abs_diff(u128::from(WEIGHT_SCALE))
    }
    #[must_use]
    pub fn within_tolerance(&self, percent: u32) -> bool {
        // Two truncated component units per owner are an arithmetic uncertainty,
        // not a reason to move at an exact tolerance boundary.
        self.deviation_units
            <= u128::from(WEIGHT_SCALE) * u128::from(percent) / 100 + 2 * u128::from(self.owners)
    }
    /// Returns the global squared-loss improvement; underflow/overflow rejects it.
    #[must_use]
    pub fn improvement(&self, source: (u64, u64), target: (u64, u64), bytes: u64) -> Option<u128> {
        let after_source = (source.0.checked_sub(1)?, source.1.checked_sub(bytes)?);
        let after_target = (target.0.checked_add(1)?, target.1.checked_add(bytes)?);
        let square = |load: (u64, u64)| {
            let d = self.deviation(self.weight(load.0, load.1));
            d.checked_mul(d)
        };
        let before = square(source)?.checked_add(square(target)?)?;
        let after = square(after_source)?.checked_add(square(after_target)?)?;
        let improvement = before.checked_sub(after)?;
        // Exclude changes that could be explained by fixed-point rounding alone.
        let rounding_bound = self.pair_rounding_bound()?;
        improvement
            .checked_sub(rounding_bound)
            .filter(|improvement| *improvement > 0)
    }
    fn pair_rounding_bound(&self) -> Option<u128> {
        32_u128
            .checked_mul(u128::from(self.owners))?
            .checked_mul(u128::from(self.owners))?
            .checked_mul(u128::from(WEIGHT_SCALE) + 1)
    }
    #[must_use]
    pub fn qualifies(&self, improvement: u128, policy: &ChunkKvRangeBalancePolicy) -> bool {
        let Some(loss_upper) = self
            .pair_rounding_bound()
            .and_then(|bound| bound.checked_mul(u128::from(self.owners)))
            .and_then(|bound| self.loss.checked_add(bound))
        else {
            return false;
        };
        !self.within_tolerance(policy.imbalance_tolerance_percent)
            && improvement
                .checked_mul(100)
                .zip(loss_upper.checked_mul(u128::from(policy.minimum_weighted_improvement_percent)))
                .is_some_and(|(actual, minimum)| actual >= minimum)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalanceOwnerObservation {
    pub instance_id: u64,
    #[serde(default)]
    pub rpc_endpoint: String,
    pub partition_count: u64,
    pub estimated_bytes: u64,
    pub weight: Weight,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalancePartitionObservation {
    pub partition_id: Id128,
    pub instance_id: u64,
    pub owner_epoch: u64,
    pub estimated_bytes: u64,
    pub weight: Weight,
    pub reason: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalanceCandidateObservation {
    pub partition_id: Id128,
    pub source_id: u64,
    pub target_id: u64,
    pub source_after: Weight,
    pub target_after: Weight,
    pub improvement_percent_millionths: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalanceObservation {
    #[serde(default)]
    pub policy_version: u32,
    pub catalog_generation: u64,
    pub observed_at_ms: u64,
    pub valid_for_ms: u64,
    pub policy: ChunkKvRangeBalancePolicy,
    pub reason: String,
    pub deviation_percent_millionths: u64,
    #[serde(default)]
    pub loss_millionths: Option<u64>,
    pub owners: Vec<BalanceOwnerObservation>,
    pub partitions: Vec<BalancePartitionObservation>,
    pub candidate: Option<BalanceCandidateObservation>,
}
