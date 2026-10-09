// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{
    balance::{BalanceWeights, WEIGHT_SCALE},
    ChunkKvRangeBalancePolicy,
};

#[test]
fn twenty_eighty_moves_forty_then_stops_at_tolerance() {
    let policy = ChunkKvRangeBalancePolicy {
        byte_weight_percent: 100,
        ..Default::default()
    };
    let before = BalanceWeights::new(&[(1, 20), (2, 80)], 100).unwrap();
    assert!(!before.within_tolerance(20));
    let improvement = before.improvement((2, 80), (1, 20), 40).unwrap();
    assert!(before.qualifies(improvement, &policy));
    let after = BalanceWeights::new(&[(2, 60), (1, 40)], 100).unwrap();
    assert!(after.within_tolerance(20));
    assert!(after.improvement((2, 60), (1, 40), 40).is_none());
}

#[test]
fn count_correction_cannot_undo_a_better_combined_placement() {
    let policy = ChunkKvRangeBalancePolicy::default();
    let before = BalanceWeights::new(&[(4, 140), (4, 20)], 80).unwrap();
    let improvement = before.improvement((4, 140), (4, 20), 60).unwrap();
    assert!(before.qualifies(improvement, &policy));
    let after = BalanceWeights::new(&[(3, 80), (5, 80)], 80).unwrap();
    assert!(after.within_tolerance(20));
    assert!(after.improvement((5, 80), (3, 80), 60).is_none());
}

#[test]
fn insufficient_improvement_and_indivisible_ranges_do_not_move() {
    let policy = ChunkKvRangeBalancePolicy {
        byte_weight_percent: 100,
        ..Default::default()
    };
    let weights = BalanceWeights::new(&[(2, 20), (2, 80)], 100).unwrap();
    let tiny = weights.improvement((2, 80), (2, 20), 1).unwrap();
    assert!(!weights.qualifies(tiny, &policy));
    assert!(weights.improvement((2, 80), (2, 20), 80).is_none());
}

#[test]
fn zero_bytes_use_count_only_and_large_values_do_not_overflow() {
    let weights = BalanceWeights::new(&[(4, 0), (4, 0)], 80).unwrap();
    let part = weights.weight(1, 0);
    assert_eq!(part.byte_units, 0);
    assert_eq!(part.count_units, WEIGHT_SCALE / 8);
    assert!(weights.within_tolerance(0));
    let large = BalanceWeights::new(&[(4, u64::MAX), (4, u64::MAX)], 80).unwrap();
    assert!(large.within_tolerance(0));
    assert!(BalanceWeights::new(&[(u64::MAX, 1), (1, 1)], 80).is_none());
    assert!(BalanceWeights::new(&[(0, 0)], 80).is_none());
}

#[test]
fn small_estimate_jitter_stays_within_configured_tolerance() {
    for bytes in 59_999..=60_001 {
        let weights = BalanceWeights::new(&[(4, bytes), (4, 100_000 - bytes)], 80).unwrap();
        assert!(weights.within_tolerance(20));
    }
}

#[test]
fn legacy_policy_decodes_explicit_new_defaults_and_rejects_invalid_percentages() {
    let mut json = serde_json::to_value(ChunkKvRangeBalancePolicy::default()).unwrap();
    json.as_object_mut().unwrap().remove("byte_weight_percent");
    json.as_object_mut()
        .unwrap()
        .remove("imbalance_tolerance_percent");
    let policy: ChunkKvRangeBalancePolicy = serde_json::from_value(json).unwrap();
    assert_eq!(policy.byte_weight_percent, 80);
    assert_eq!(policy.imbalance_tolerance_percent, 20);
    assert!(policy.validate().is_ok());
    assert!(ChunkKvRangeBalancePolicy {
        byte_weight_percent: 101,
        ..policy.clone()
    }
    .validate()
    .is_err());
    assert!(ChunkKvRangeBalancePolicy {
        imbalance_tolerance_percent: 101,
        ..policy
    }
    .validate()
    .is_err());
}
