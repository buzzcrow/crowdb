// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_common::metrics::histogram::HistogramData;
use crowdb_common::metrics::{LatencyHistogram, MetricName};

#[test]
fn shared_nanosecond_golden_vectors() {
    for line in include_str!("../../testdata/latency_histogram.csv").lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let (sample, upper) = line.split_once(',').unwrap();
        let sample: u64 = sample.parse().unwrap();
        let upper: u64 = upper.parse().unwrap();
        let h = LatencyHistogram::new(MetricName::Static("test.latency"));
        h.observe(sample);
        h.observe(u64::MAX);
        let s = h.snapshot_total();
        assert_eq!(s.p50, upper, "sample={sample}");
        assert_eq!(s.p90, u64::MAX);
        assert_eq!(s.min, sample);
        assert_eq!(s.max, u64::MAX);
        assert_eq!(s.sum, u128::from(sample) + u128::from(u64::MAX));
        assert_eq!(s.zero_count, u64::from(sample == 0));
        assert_eq!(s.overflow_count, 1 + u64::from(sample >= 1 << 40));
    }
}

#[test]
fn quantiles_use_nearest_rank_and_preserve_exact_extrema() {
    let mut h = HistogramData::default();
    for ns in 1..=1000 {
        h.observe(ns);
    }
    let s = h.snapshot(0);
    assert_eq!((s.count, s.sum, s.min, s.max), (1000, 500_500, 1, 1000));
    assert_eq!((s.p50, s.p90, s.p95, s.p99, s.p999), (501, 903, 951, 991, 999));
    let mut one = HistogramData::default();
    one.observe(123);
    assert_eq!(one.snapshot(0).p50, 123);
}

#[test]
fn flush_resets_only_the_window_and_shards_merge_without_losing_samples() {
    let h = LatencyHistogram::new(MetricName::Static("test.latency"));
    let empty = h.snapshot_total();
    assert_eq!((empty.count, empty.min, empty.max, empty.p999), (0, 0, 0, 0));
    h.observe(100);
    assert_eq!(h.flush().sum, 100);
    assert_eq!(h.flush().count, 0);
    h.observe(900);
    let s = h.snapshot_total();
    assert_eq!((s.count, s.sum, s.min, s.max), (2, 1000, 100, 900));
    let mut a = HistogramData::default();
    let mut b = HistogramData::default();
    a.observe(100);
    b.observe(900);
    a.merge(&b);
    assert_eq!(a.snapshot(0).p50, s.p50);
    assert_eq!(a.snapshot(0).sum, s.sum);
}

#[test]
fn concurrent_writers_have_exact_final_counts_and_sum() {
    let h = std::sync::Arc::new(LatencyHistogram::new(MetricName::Static("test.latency")));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let h = &h;
            scope.spawn(move || {
                for ns in 0..10_000 {
                    h.observe(ns);
                }
            });
        }
    });
    let s = h.snapshot_total();
    assert_eq!((s.count, s.sum, s.min, s.max), (40_000, 199_980_000, 0, 9999));
    assert_eq!(s.zero_count, 4);
    let captured = h.capture_total();
    let mut merged = HistogramData::default();
    merged.merge(&captured);
    merged.merge(&captured);
    let merged = merged.snapshot(0);
    assert_eq!((merged.count, merged.sum), (80_000, 399_960_000));
    assert_eq!((merged.p90, merged.min, merged.max), (s.p90, 0, 9999));
}
