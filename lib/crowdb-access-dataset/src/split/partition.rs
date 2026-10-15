// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{PartitionWeight, SplitDefinition, SplitError, SplitName};
use sha2::{Digest, Sha256};

/// Validated ordered intervals for the `sample-id-sha256-v1` algorithm.
pub struct SamplePartition {
    seed: u64,
    intervals: Vec<(u32, SplitName)>,
    total: u32,
}
impl SamplePartition {
    /// # Errors
    /// Rejects duplicate/empty names, zero weights and sums exceeding u32.
    pub fn new(seed: u64, weights: Vec<PartitionWeight>) -> Result<Self, SplitError> {
        SplitDefinition::WeightedPartition {
            seed,
            weights: weights.clone(),
            index: 0,
        }
        .validate()?;
        let mut total = 0;
        let intervals = weights
            .into_iter()
            .map(|weight| {
                total += weight.weight;
                (total, weight.name)
            })
            .collect();
        Ok(Self {
            seed,
            intervals,
            total,
        })
    }
    /// # Errors
    /// Rejects empty Sample IDs and lengths not representable as u32.
    pub fn assign(&self, sample_id: &[u8]) -> Result<&SplitName, SplitError> {
        let bucket = partition_bucket(self.seed, sample_id, self.total)?;
        self.intervals
            .iter()
            .find(|(end, _)| bucket < *end)
            .map(|(_, name)| name)
            .ok_or(SplitError::InvalidDefinition)
    }
}

/// Stable integer bucket; no Snapshot/revision identity participates.
/// # Errors
/// Rejects empty IDs, overlong IDs and a zero total weight.
pub fn partition_bucket(seed: u64, sample_id: &[u8], total: u32) -> Result<u32, SplitError> {
    if sample_id.is_empty() || total == 0 {
        return Err(SplitError::InvalidDefinition);
    }
    let length = u32::try_from(sample_id.len()).map_err(|_| SplitError::Limit)?;
    let mut hash = Sha256::new();
    hash.update(b"crowdb-split-v1\0");
    hash.update(seed.to_be_bytes());
    hash.update(length.to_be_bytes());
    hash.update(sample_id);
    let digest = hash.finalize();
    let x = u64::from_be_bytes(digest[..8].try_into().map_err(|_| SplitError::InvalidRecord)?);
    u32::try_from((u128::from(x) * u128::from(total)) >> 64).map_err(|_| SplitError::InvalidRecord)
}
