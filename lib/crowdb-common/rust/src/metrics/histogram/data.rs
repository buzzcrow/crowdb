// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared layout with C++: exact 0..255 ns, then 128 buckets per power of
//! two up to 2^40 ns. Quantiles use inclusive bucket bounds and nearest rank;
//! relative error is below 1/128. Overflow quantiles return `u64::MAX`.

const SUB_BUCKETS: usize = 128;
const FIRST_MAGNITUDE: u32 = 7;
const LIMIT_MAGNITUDE: u32 = 40;
pub(super) const NUM_BUCKETS: usize = 128 + 33 * SUB_BUCKETS + 1;
const OVERFLOW: usize = NUM_BUCKETS - 1;

pub(super) fn bucket_index(ns: u64) -> usize {
    if ns < 128 {
        return usize::try_from(ns).unwrap_or(0);
    }
    let magnitude = ns.ilog2();
    if magnitude >= LIMIT_MAGNITUDE {
        return OVERFLOW;
    }
    128 + (magnitude - FIRST_MAGNITUDE) as usize * SUB_BUCKETS
        + ((ns >> (magnitude - FIRST_MAGNITUDE)) & 127) as usize
}

fn upper_bound(index: usize) -> u64 {
    if index < 128 {
        return index as u64;
    }
    if index == OVERFLOW {
        return u64::MAX;
    }
    let offset = index - 128;
    let magnitude = FIRST_MAGNITUDE + u32::try_from(offset / SUB_BUCKETS).unwrap_or(0);
    (1_u64 << magnitude) + ((offset % SUB_BUCKETS) as u64 + 1) * (1_u64 << (magnitude - 7)) - 1
}

/// One polling owner's allocation-free observations, mergeable after joining.
#[derive(Debug, Clone)]
pub struct HistogramData {
    pub(super) buckets: Vec<u64>,
    pub(super) sum: u128,
    pub(super) min: u64,
    pub(super) max: u64,
}

impl Default for HistogramData {
    fn default() -> Self {
        Self {
            buckets: vec![0; NUM_BUCKETS],
            sum: 0,
            min: u64::MAX,
            max: 0,
        }
    }
}

impl HistogramData {
    pub fn observe(&mut self, ns: u64) {
        self.buckets[bucket_index(ns)] += 1;
        self.sum += u128::from(ns);
        self.min = self.min.min(ns);
        self.max = self.max.max(ns);
    }

    pub fn merge(&mut self, other: &Self) {
        for (dest, source) in self.buckets.iter_mut().zip(&other.buckets) {
            *dest += source;
        }
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// Pass the cumulative count for a window, or zero for an owned shard.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn snapshot(&self, total_count: u64) -> HistogramSnapshot {
        let count = self.buckets.iter().sum();
        HistogramSnapshot {
            count,
            sum: self.sum,
            avg: if count == 0 {
                0.0
            } else {
                self.sum as f64 / count as f64
            },
            min: if count == 0 { 0 } else { self.min },
            max: self.max,
            p50: self.percentile(count, 500),
            p90: self.percentile(count, 900),
            p95: self.percentile(count, 950),
            p99: self.percentile(count, 990),
            p999: self.percentile(count, 999),
            zero_count: self.buckets[0],
            overflow_count: self.buckets[OVERFLOW],
            total_count: total_count.max(count),
        }
    }

    fn percentile(&self, count: u64, permille: u64) -> u64 {
        if count == 0 {
            return 0;
        }
        let target = count / 1000 * permille + (count % 1000 * permille).div_ceil(1000);
        let mut cumulative = 0;
        for (i, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket;
            if cumulative >= target {
                return if i == OVERFLOW {
                    u64::MAX
                } else {
                    upper_bound(i).min(self.max)
                };
            }
        }
        0
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum: u128,
    pub avg: f64,
    pub min: u64,
    pub max: u64,
    pub p50: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub zero_count: u64,
    pub overflow_count: u64,
    pub total_count: u64,
}
