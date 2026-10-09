// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    candidates::{Candidate, CandidateContext},
    PlanningState,
};
use crowdb_protocol::chunk_kv::{
    balance::{
        BalanceCandidateObservation, BalanceObservation, BalanceOwnerObservation, BalanceWeights,
        POLICY_VERSION,
    },
    ChunkKvRangeBalancePolicy,
};
use std::collections::HashMap;

pub(super) fn empty(
    policy: &ChunkKvRangeBalancePolicy,
    now_ms: u64,
    generation: u64,
    valid_for_ms: u64,
) -> BalanceObservation {
    BalanceObservation {
        policy_version: POLICY_VERSION,
        catalog_generation: generation,
        observed_at_ms: now_ms,
        valid_for_ms,
        policy: policy.clone(),
        reason: "unavailable observation".into(),
        deviation_percent_millionths: 0,
        loss_millionths: None,
        owners: Vec::new(),
        partitions: Vec::new(),
        candidate: None,
    }
}

pub(super) fn owners(
    counts: &HashMap<u64, u64>,
    bytes: &HashMap<u64, u64>,
    weights: &BalanceWeights,
    state: &PlanningState,
) -> Vec<BalanceOwnerObservation> {
    let mut owners: Vec<_> = counts
        .iter()
        .map(|(id, count)| BalanceOwnerObservation {
            instance_id: *id,
            rpc_endpoint: state.healthy[id].0.rpc_endpoint.clone(),
            partition_count: *count,
            estimated_bytes: bytes[id],
            weight: weights.weight(*count, bytes[id]),
        })
        .collect();
    owners.sort_by_key(|owner| owner.instance_id);
    owners
}

pub(super) fn candidate(
    choice: Candidate<'_>,
    context: &CandidateContext<'_>,
) -> BalanceCandidateObservation {
    let ((improvement, _, _), entry, target_id) = choice;
    let source_id = entry.owner.instance_id;
    let bytes = context.state.partition_bytes[&(source_id, entry.partition_id)];
    BalanceCandidateObservation {
        partition_id: entry.partition_id,
        source_id,
        target_id,
        source_after: context
            .weights
            .weight(context.counts[&source_id] - 1, context.bytes[&source_id] - bytes),
        target_after: context
            .weights
            .weight(context.counts[&target_id] + 1, context.bytes[&target_id] + bytes),
        improvement_percent_millionths: u64::try_from(
            improvement.saturating_mul(100_000_000) / context.weights.loss,
        )
        .unwrap_or(u64::MAX),
    }
}
