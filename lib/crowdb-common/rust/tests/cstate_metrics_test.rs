// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#![cfg(target_os = "linux")]

use crowdb_common::metrics::perf::{parse_c6_cpu_list_for_tests, TestC6Window};

#[test]
fn pmu_representatives_are_sorted_and_unique() {
    assert_eq!(
        parse_c6_cpu_list_for_tests("0-3,2,8,10-11\n"),
        Some(vec![0, 1, 2, 3, 8, 10, 11])
    );
    for invalid in ["", "-1", "3-1", "0-", "a", "0,,2", "0,"] {
        assert_eq!(parse_c6_cpu_list_for_tests(invalid), None, "{invalid}");
    }
}

#[test]
fn hardware_zero_is_distinct_from_missing_baseline() {
    let mut window = TestC6Window::default();
    assert_eq!(
        window.sample_for_tests(Some([100, 100, 100]), Some([1000, 100, 100])),
        None
    );
    assert_eq!(
        window.sample_for_tests(Some([100, 200, 200]), Some([2000, 200, 200])),
        Some(0.0)
    );
    assert_eq!(
        window.sample_for_tests(Some([600, 300, 300]), Some([3000, 300, 300])),
        Some(50.0)
    );
}

#[test]
fn residency_uses_tsc_and_corrects_multiplexing() {
    let mut window = TestC6Window::default();
    window.sample_for_tests(Some([0, 0, 0]), Some([0, 0, 0]));
    // C6 ran half the interval: 250 measured ticks scale to 500 out of 1000 TSC ticks.
    assert_eq!(
        window.sample_for_tests(Some([250, 100, 50]), Some([1000, 100, 100])),
        Some(50.0)
    );
}

#[test]
fn failed_read_requires_a_fresh_pair_of_baselines() {
    let mut window = TestC6Window::default();
    window.sample_for_tests(Some([0, 0, 0]), Some([0, 0, 0]));
    assert_eq!(window.sample_for_tests(None, Some([1000, 100, 100])), None);
    assert_eq!(
        window.sample_for_tests(Some([1000, 200, 200]), Some([2000, 200, 200])),
        None
    );
    assert_eq!(
        window.sample_for_tests(Some([1250, 300, 300]), Some([3000, 300, 300])),
        Some(25.0)
    );
}

#[test]
fn counter_reset_is_unavailable_then_recovers() {
    let mut window = TestC6Window::default();
    window.sample_for_tests(Some([1000, 100, 100]), Some([2000, 100, 100]));
    assert_eq!(window.sample_for_tests(Some([0, 0, 0]), Some([0, 0, 0])), None);
    assert_eq!(
        window.sample_for_tests(Some([750, 100, 100]), Some([1000, 100, 100])),
        Some(75.0)
    );
}

#[test]
fn unscheduled_or_inconsistent_intervals_are_unavailable() {
    for (c6, tsc) in [
        ([1, 100, 0], [1000, 100, 100]),
        ([1, 50, 100], [1000, 100, 100]),
        ([1, 100, 100], [0, 100, 100]),
        ([2000, 100, 100], [1000, 100, 100]),
    ] {
        let mut window = TestC6Window::default();
        window.sample_for_tests(Some([0, 0, 0]), Some([0, 0, 0]));
        assert_eq!(window.sample_for_tests(Some(c6), Some(tsc)), None);
    }
}
