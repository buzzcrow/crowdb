// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{CoveragePolicy, OverlapPolicy, SplitError, SplitPolicy};
use serde::{Deserialize, Serialize};

/// Incremental preview counts. Callers label them partial until the universe is exhausted.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitValidation {
    pub scanned: u64,
    pub assigned: u64,
    pub unassigned: u64,
    pub overlapping: u64,
    pub unassigned_examples: Vec<Vec<u8>>,
    pub overlap_examples: Vec<Vec<u8>>,
}
impl SplitValidation {
    /// Records one distinct Sample's assignment count, retaining bounded diagnostics.
    /// # Errors
    /// Rejects invalid IDs and count overflow.
    pub fn observe(&mut self, sample_id: &[u8], assignments: usize) -> Result<(), SplitError> {
        if sample_id.is_empty() || sample_id.len() > 16 * 1024 {
            return Err(SplitError::Limit);
        }
        let scanned = self.scanned.checked_add(1).ok_or(SplitError::Limit)?;
        let (assigned, unassigned, overlapping) = (
            self.assigned
                .checked_add(u64::from(assignments > 0))
                .ok_or(SplitError::Limit)?,
            self.unassigned
                .checked_add(u64::from(assignments == 0))
                .ok_or(SplitError::Limit)?,
            self.overlapping
                .checked_add(u64::from(assignments > 1))
                .ok_or(SplitError::Limit)?,
        );
        self.scanned = scanned;
        self.assigned = assigned;
        self.unassigned = unassigned;
        self.overlapping = overlapping;
        if assignments == 0 && can_record(&self.unassigned_examples, sample_id) {
            self.unassigned_examples.push(sample_id.to_vec());
        }
        if assignments > 1 && can_record(&self.overlap_examples, sample_id) {
            self.overlap_examples.push(sample_id.to_vec());
        }
        Ok(())
    }
    #[must_use]
    pub fn satisfies(&self, policy: SplitPolicy) -> bool {
        (policy.overlap == OverlapPolicy::AllowOverlap || self.overlapping == 0)
            && (policy.coverage == CoveragePolicy::AllowUnassigned || self.unassigned == 0)
    }
}

fn can_record(examples: &[Vec<u8>], sample: &[u8]) -> bool {
    examples.len() < 20 && examples.iter().map(Vec::len).sum::<usize>() + sample.len() <= 32 * 1024
}
