// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::PlanningState;
use crowdb_protocol::chunk_kv::{
    balance::BalanceWeights, ChunkKvRangeBalancePolicy, ChunkKvRangeCatalogEntry, Id128,
};
use std::{cmp::Reverse, collections::HashMap};

type Score = (u128, Reverse<Id128>, Reverse<u64>);
pub(super) type Candidate<'a> = (Score, &'a ChunkKvRangeCatalogEntry, u64);

pub(super) struct CandidateContext<'a> {
    pub state: &'a PlanningState,
    pub policy: &'a ChunkKvRangeBalancePolicy,
    pub weights: &'a BalanceWeights,
    pub counts: &'a HashMap<u64, u64>,
    pub bytes: &'a HashMap<u64, u64>,
    pub now_ms: u64,
}

pub(super) fn retain_best<'a>(best: &mut Option<Candidate<'a>>, next: Option<Candidate<'a>>) {
    if let Some(next) = next {
        if best.as_ref().map_or(true, |previous| next.0 > previous.0) {
            *best = Some(next);
        }
    }
}

impl CandidateContext<'_> {
    pub(super) fn assess<'a>(
        &self,
        entry: &'a ChunkKvRangeCatalogEntry,
        within: bool,
    ) -> (Option<Candidate<'a>>, Option<Candidate<'a>>, &'static str) {
        if let Some(reason) = super::exclusion_reason(entry, self.state, self.policy, self.now_ms) {
            return (None, None, reason);
        }
        if within {
            return (None, None, "within tolerance");
        }
        let source_id = entry.owner.instance_id;
        let partition_bytes = self.state.partition_bytes[&(source_id, entry.partition_id)];
        let source = (self.counts[&source_id], self.bytes[&source_id]);
        let mut accepted = None;
        let mut rejected = None;
        let mut safe_target = false;
        for (&target_id, (_, target)) in &self.state.healthy {
            if !super::target_accepts(
                self.state,
                self.policy,
                source_id,
                target_id,
                target,
                partition_bytes,
            ) {
                continue;
            }
            safe_target = true;
            let target_load = (self.counts[&target_id], self.bytes[&target_id]);
            let Some(improvement) = self.weights.improvement(source, target_load, partition_bytes) else {
                continue;
            };
            let candidate = Some((
                (improvement, Reverse(entry.partition_id), Reverse(target_id)),
                entry,
                target_id,
            ));
            if self.weights.qualifies(improvement, self.policy) {
                retain_best(&mut accepted, candidate);
            } else {
                retain_best(&mut rejected, candidate);
            }
        }
        let reason = if accepted.is_some() {
            "eligible improving move"
        } else if rejected.is_some() {
            "insufficient benefit"
        } else if safe_target {
            "no improving target"
        } else {
            "no safe target: health, active transition, capacity or rate limit"
        };
        (accepted, rejected, reason)
    }
}
