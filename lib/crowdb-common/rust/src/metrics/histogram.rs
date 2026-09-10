// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use super::MetricName;

// ── HDR bucket parameters ────────────────────────────────────────
//
// HDR-style logarithmic buckets: each power-of-2 magnitude is divided
// into `SUB_BUCKET_COUNT` equal sub-buckets, giving a bounded relative
// error of ≤ 1 / SUB_BUCKET_COUNT ≈ 0.78% everywhere.
//
// Configuration tuned for the crowdb latency range:
//   - LVD (lowest discernible value) = 2^UNIT_MAGNITUDE ≈ 65.5µs.
//     Values below this go into a single underflow bucket (index 0).
//     The user's workload starts at ~100µs, so sub-65µs granularity is
//     irrelevant.
//   - Max magnitude covers 2^(UNIT_MAGNITUDE + NUM_MAGNITUDES) = 2^34
//     ≈ 17.2s. Values above go into the overflow bucket (last index).
//   - 2 significant figures → SUB_BUCKET_COUNT = 128 (2^7).

const UNIT_MAGNITUDE: u32 = 16; // floor(log2(100_000)) → LVD ≈ 65.5µs
const SUB_BUCKET_COUNT: usize = 128; // 2^7 → ≤0.78% relative error
const SUB_BUCKET_BITS: u32 = 7; // log2(SUB_BUCKET_COUNT)
const SUB_BUCKET_MASK: u64 = (SUB_BUCKET_COUNT as u64) - 1; // 127
const NUM_MAGNITUDES: usize = 18; // magnitudes 0..17 → up to 2^34 ≈ 17.2s
/// 1 underflow + `NUM_MAGNITUDES` * `SUB_BUCKET_COUNT` regular + 1 overflow.
const NUM_BUCKETS: usize = 1 + NUM_MAGNITUDES * SUB_BUCKET_COUNT + 1;
const UNDERFLOW: usize = 0;
const OVERFLOW: usize = NUM_BUCKETS - 1;

/// Fixed-bucket HDR latency histogram with window + cumulative tracking.
///
/// Each `observe(ns)` does O(1) bit manipulation to find the bucket,
/// then `fetch_add(1)` on both the window and cumulative bucket arrays,
/// and on `count`/`total_count` + `sum`/`total_sum`. No allocation, no
/// locks. Relative error ≤ ~0.78% for values in [65.5µs, 17.2s].
///
/// `flush()` resets window state (buckets, count, sum) but keeps
/// cumulative state (`total_buckets`, `total_count`, `total_sum`) — so
/// `snapshot_total()` returns full-run percentiles for a final report
/// even after periodic flushes have reset the window.
#[allow(dead_code)]
#[derive(Debug)]
pub struct LatencyHistogram {
    name: MetricName,
    // Window state (reset on flush).
    buckets: [AtomicU64; NUM_BUCKETS],
    count: AtomicU64,
    sum: AtomicU64,
    // Cumulative state (never reset).
    total_buckets: [AtomicU64; NUM_BUCKETS],
    total_count: AtomicU64,
    total_sum: AtomicU64,
}

#[allow(dead_code)]
impl LatencyHistogram {
    #[must_use]
    pub fn new(name: MetricName) -> Self {
        Self {
            name,
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            total_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            total_count: AtomicU64::new(0),
            total_sum: AtomicU64::new(0),
        }
    }

    /// Record a latency observation in nanoseconds. Updates both
    /// window and cumulative state in a single call.
    pub fn observe(&self, ns: u64) {
        let idx = bucket_index(ns);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.total_buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(ns, Ordering::Relaxed);
        self.total_count.fetch_add(1, Ordering::Relaxed);
        self.total_sum.fetch_add(ns, Ordering::Relaxed);
    }

    /// Snapshot and reset window state. Returns count, p50, p99, max
    /// (all in nanoseconds), and `total_count`. Cumulative state is
    /// preserved for `snapshot_total()`.
    pub fn flush(&self) -> HistogramSnapshot {
        let mut bucket_counts = vec![0u64; NUM_BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            bucket_counts[i] = b.swap(0, Ordering::Relaxed);
        }
        let count = self.count.swap(0, Ordering::Relaxed);
        let sum = self.sum.swap(0, Ordering::Relaxed);
        let total_count = self.total_count.load(Ordering::Relaxed);

        let p50 = percentile(&bucket_counts, count, 50);
        let p99 = percentile(&bucket_counts, count, 99);
        let max = max_latency(&bucket_counts);
        let avg = compute_avg(sum, count);

        HistogramSnapshot {
            count,
            avg,
            p50,
            p99,
            max,
            total_count,
        }
    }

    /// Current window values without resetting.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut bucket_counts = vec![0u64; NUM_BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            bucket_counts[i] = b.load(Ordering::Relaxed);
        }
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        let total_count = self.total_count.load(Ordering::Relaxed);

        let p50 = percentile(&bucket_counts, count, 50);
        let p99 = percentile(&bucket_counts, count, 99);
        let max = max_latency(&bucket_counts);
        let avg = compute_avg(sum, count);

        HistogramSnapshot {
            count,
            avg,
            p50,
            p99,
            max,
            total_count,
        }
    }

    /// Cumulative values across all observations (never reset by
    /// `flush()`). Use for a final report after periodic flushes.
    #[must_use]
    pub fn snapshot_total(&self) -> HistogramSnapshot {
        let mut bucket_counts = vec![0u64; NUM_BUCKETS];
        for (i, b) in self.total_buckets.iter().enumerate() {
            bucket_counts[i] = b.load(Ordering::Relaxed);
        }
        let count = self.total_count.load(Ordering::Relaxed);
        let sum = self.total_sum.load(Ordering::Relaxed);

        let p50 = percentile(&bucket_counts, count, 50);
        let p99 = percentile(&bucket_counts, count, 99);
        let max = max_latency(&bucket_counts);
        let avg = compute_avg(sum, count);

        HistogramSnapshot {
            count,
            avg,
            p50,
            p99,
            max,
            total_count: count,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

// ── HDR index / boundary math ─────────────────────────────────────

/// Map a value to its HDR bucket index via O(1) bit manipulation.
/// Values < LVD go to the underflow bucket (0); values > max go to
/// the overflow bucket (`NUM_BUCKETS` - 1).
#[allow(dead_code)]
fn bucket_index(v: u64) -> usize {
    if v < (1u64 << UNIT_MAGNITUDE) {
        return UNDERFLOW;
    }
    let highest_bit = v.ilog2();
    let magnitude = highest_bit - UNIT_MAGNITUDE;
    if magnitude as usize >= NUM_MAGNITUDES {
        return OVERFLOW;
    }
    let sub_bucket = ((v >> (highest_bit - SUB_BUCKET_BITS)) & SUB_BUCKET_MASK) as usize;
    1 + magnitude as usize * SUB_BUCKET_COUNT + sub_bucket
}

/// Upper bound (exclusive) of the bucket at `index`, in nanoseconds.
/// The underflow bucket returns the LVD; the overflow bucket returns
/// the max trackable value (`2^(UNIT_MAGNITUDE + NUM_MAGNITUDES)`).
#[allow(dead_code, clippy::cast_possible_truncation)]
fn bucket_upper_bound(index: usize) -> u64 {
    if index == UNDERFLOW {
        return 1u64 << UNIT_MAGNITUDE;
    }
    if index == OVERFLOW {
        return 1u64 << (UNIT_MAGNITUDE + NUM_MAGNITUDES as u32);
    }
    let linear = index - 1;
    let magnitude = linear / SUB_BUCKET_COUNT;
    let sub_bucket = linear % SUB_BUCKET_COUNT;
    let magnitude_base = 1u64 << (UNIT_MAGNITUDE + magnitude as u32);
    let sub_bucket_width = 1u64 << (UNIT_MAGNITUDE + magnitude as u32 - SUB_BUCKET_BITS);
    magnitude_base + (sub_bucket as u64 + 1) * sub_bucket_width
}

/// Compute the p-th percentile from bucket counts. Returns the upper
/// bound of the bucket that contains the p-th percentile value. The
/// overflow bucket is capped at the last regular bucket's upper bound
/// to avoid reporting the max-trackable sentinel.
#[allow(dead_code)]
fn percentile(bucket_counts: &[u64], count: u64, p: u64) -> u64 {
    if count == 0 {
        return 0;
    }
    let target = count * p / 100;
    let mut cumulative = 0u64;
    for (i, &bc) in bucket_counts.iter().enumerate() {
        cumulative += bc;
        if cumulative >= target {
            let capped = i.min(OVERFLOW - 1);
            return bucket_upper_bound(capped);
        }
    }
    bucket_upper_bound(OVERFLOW - 1)
}

/// Find the highest non-empty bucket's upper bound. The overflow
/// bucket is capped at the last regular bucket's upper bound.
#[allow(dead_code)]
fn max_latency(bucket_counts: &[u64]) -> u64 {
    for (i, &bc) in bucket_counts.iter().enumerate().rev() {
        if bc > 0 {
            let capped = i.min(OVERFLOW - 1);
            return bucket_upper_bound(capped);
        }
    }
    0
}

/// Arithmetic mean of `sum` / `count` as `f64`. Returns `0.0` if `count`
/// is zero (avoids division by zero).
#[allow(clippy::cast_precision_loss)]
fn compute_avg(sum: u64, count: u64) -> f64 {
    if count > 0 {
        sum as f64 / count as f64
    } else {
        0.0
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub avg: f64,
    pub p50: u64,
    pub p99: u64,
    pub max: u64,
    pub total_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `500_000` ns → magnitude 2, sub-bucket 116, upper bound `501_760`.
    const BOUND_500K: u64 = 501_760;
    /// `200_000` ns → magnitude 1, sub-bucket 67, upper bound `200_704`.
    const BOUND_200K: u64 = 200_704;
    /// `10_000_000` ns → magnitude 7, sub-bucket 24, upper bound `10_027_008`.
    const BOUND_10MS: u64 = 10_027_008;

    #[test]
    #[allow(clippy::float_cmp)]
    fn histogram_p50_p99_with_known_distribution() {
        let h = LatencyHistogram::new(MetricName::Static("test.lh"));
        for _ in 0..100 {
            h.observe(500_000);
        }
        let s = h.flush();
        assert_eq!(s.count, 100);
        assert_eq!(s.total_count, 100);
        // p50 and p99 fall in the HDR bucket containing 500µs.
        assert_eq!(s.p50, BOUND_500K);
        assert_eq!(s.p99, BOUND_500K);
        assert_eq!(s.max, BOUND_500K);
        // avg is exact (f64).
        assert_eq!(s.avg, 500_000.0);
    }

    #[test]
    fn histogram_mixed_distribution() {
        let h = LatencyHistogram::new(MetricName::Static("test.lh"));
        // 80 fast (200µs), 20 slow (10ms) — both above LVD.
        for _ in 0..80 {
            h.observe(200_000);
        }
        for _ in 0..20 {
            h.observe(10_000_000);
        }
        let s = h.flush();
        assert_eq!(s.count, 100);
        // p50 should fall in the 200µs bucket (first 80 are at 200µs).
        assert_eq!(s.p50, BOUND_200K);
        // p99 should fall in the 10ms bucket (cumulative at 200µs = 80, target = 99).
        assert_eq!(s.p99, BOUND_10MS);
        assert_eq!(s.max, BOUND_10MS);
    }

    #[test]
    fn histogram_window_resets_after_flush() {
        let h = LatencyHistogram::new(MetricName::Static("test.lh"));
        h.observe(100_000);
        h.observe(200_000);
        let s1 = h.flush();
        assert_eq!(s1.count, 2);
        assert_eq!(s1.total_count, 2);

        let s2 = h.flush();
        assert_eq!(s2.count, 0);
        assert_eq!(s2.p50, 0);
        assert_eq!(s2.total_count, 2); // total accumulates
    }

    #[test]
    fn histogram_snapshot_does_not_reset() {
        let h = LatencyHistogram::new(MetricName::Static("test.lh"));
        h.observe(100_000);
        let s = h.snapshot();
        assert_eq!(s.count, 1);
        let s2 = h.snapshot();
        assert_eq!(s2.count, 1);
    }

    #[test]
    fn bucket_index_correctness() {
        // Underflow: below LVD (2^16 = 65536).
        assert_eq!(bucket_index(0), UNDERFLOW);
        assert_eq!(bucket_index(65_535), UNDERFLOW);
        // First regular bucket: exactly LVD.
        assert_eq!(bucket_index(65_536), 1);
        // 100µs → magnitude 0, sub-bucket 67.
        assert_eq!(bucket_index(100_000), 1 + 67);
        // 500µs → magnitude 2, sub-bucket 116.
        assert_eq!(bucket_index(500_000), 1 + 2 * 128 + 116);
        // Overflow: beyond max trackable (2^34).
        assert_eq!(bucket_index(1u64 << 34), OVERFLOW);
        assert_eq!(bucket_index(u64::MAX), OVERFLOW);
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn hdr_relative_error_within_one_percent() {
        // Verify the HDR guarantee: for any value in range, the bucket
        // upper bound is within 1% of the value.
        for v in [
            100_000u64,
            500_000,
            1_000_000,
            10_000_000,
            100_000_000,
            1_000_000_000,
        ] {
            let idx = bucket_index(v);
            let bound = bucket_upper_bound(idx);
            let error = ((bound as f64 - v as f64) / v as f64 * 100.0).abs();
            assert!(error < 1.0, "v={v} bound={bound} error={error:.3}% exceeds 1%");
        }
    }
}
