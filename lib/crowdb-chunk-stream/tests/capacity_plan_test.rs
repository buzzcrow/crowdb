// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use crowdb_chunk_stream::capacity_target_for_tests;

const MIB: u64 = 1024 * 1024;

#[test]
fn bandwidth_and_preparation_latency_increase_the_reserved_capacity() {
    let plan = |bytes, millis| {
        capacity_target_for_tests(
            &[(bytes, Duration::from_millis(100))],
            Duration::from_millis(millis),
            0,
            8 * MIB,
            256 * MIB,
            4096,
        )
    };
    assert_eq!(plan(MIB, 50), None, "a slow writer has ample ready capacity");
    let fast = plan(20 * MIB, 50).unwrap();
    let slower_preparation = plan(20 * MIB, 100).unwrap();
    assert_eq!(fast, 40 * MIB + 4096);
    assert_eq!(slower_preparation, 80 * MIB + 4096);
}

#[test]
fn refill_starts_with_two_preparation_intervals_still_ready() {
    let request = |capacity| {
        capacity_target_for_tests(
            &[(20 * MIB, Duration::from_millis(100))],
            Duration::from_millis(50),
            MIB,
            capacity,
            256 * MIB,
            4096,
        )
    };
    assert!(request(21 * MIB + 4096 + 1).is_none());
    assert!(request(21 * MIB + 4096).is_some());
}

#[test]
fn cold_start_extreme_rates_and_chunk_end_remain_bounded() {
    assert_eq!(
        capacity_target_for_tests(&[], Duration::ZERO, 0, MIB, 256 * MIB, 4096),
        Some(4 * MIB + 4096)
    );
    let writes = [(u64::MAX, Duration::ZERO)];
    assert_eq!(
        capacity_target_for_tests(&writes, Duration::MAX, MIB, 2 * MIB, 256 * MIB, 4096),
        Some(256 * MIB)
    );
    assert_eq!(
        capacity_target_for_tests(&writes, Duration::MAX, MIB, 256 * MIB, 256 * MIB, 4096),
        None
    );
}

#[test]
fn one_slow_write_does_not_remove_a_fast_writers_margin() {
    let target = capacity_target_for_tests(
        &[
            (20 * MIB, Duration::from_millis(100)),
            (4096, Duration::from_secs(1)),
        ],
        Duration::from_millis(50),
        0,
        8 * MIB,
        256 * MIB,
        4096,
    )
    .unwrap();
    assert!(
        target >= 35 * MIB,
        "a slow sample must not immediately shrink the reserve"
    );
}
