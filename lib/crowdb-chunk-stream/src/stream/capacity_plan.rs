// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded lookahead from synchronized write bandwidth and preparation latency.

use std::time::Duration;

const MIN_AHEAD: u64 = 2 * 1024 * 1024;
const INITIAL_PREPARATION: Duration = Duration::from_millis(20);

#[derive(Default)]
pub(super) struct CapacityPlan {
    bytes_per_second: u64,
    preparation: Duration,
}

impl CapacityPlan {
    pub(super) fn record_write(&mut self, bytes: u64, elapsed: Duration) {
        let rate =
            u64::try_from(u128::from(bytes) * 1_000_000_000 / elapsed.as_nanos().max(1)).unwrap_or(u64::MAX);
        // React immediately to acceleration; decay gradually after a slow write.
        // Idle time, queueing and foreground capacity waits are not write samples.
        self.bytes_per_second = rate.max(self.bytes_per_second.saturating_mul(7) / 8);
    }

    pub(super) fn record_preparation(&mut self, elapsed: Duration) {
        self.preparation = elapsed.max(self.preparation.mul_f64(0.875));
    }

    pub(super) fn target(&self, cursor: u64, capacity: u64, limit: u64, batch_bytes: u64) -> Option<u64> {
        if capacity >= limit {
            return None;
        }
        // Cover twice the observed preparation time, including allocation and every
        // replica's initialization. Bound speculative capacity independently of
        // stream length and leave room for the batch already being submitted.
        let latency = self.preparation.max(INITIAL_PREPARATION).as_nanos();
        let ahead = u64::try_from(
            u128::from(self.bytes_per_second)
                .saturating_mul(latency)
                .saturating_mul(2)
                / 1_000_000_000,
        )
        .unwrap_or(u64::MAX)
        .clamp(MIN_AHEAD, limit.max(MIN_AHEAD));
        if capacity.saturating_sub(cursor) > ahead.saturating_add(batch_bytes) {
            return None;
        }
        Some(
            cursor
                .saturating_add(ahead.saturating_mul(2))
                .saturating_add(batch_bytes)
                .max(capacity.saturating_add(1))
                .min(limit),
        )
    }
}

#[cfg(feature = "test-util")]
#[must_use]
pub fn capacity_target_for_tests(
    writes: &[(u64, Duration)],
    preparation: Duration,
    cursor: u64,
    capacity: u64,
    limit: u64,
    batch_bytes: u64,
) -> Option<u64> {
    let mut plan = CapacityPlan::default();
    for (bytes, elapsed) in writes {
        plan.record_write(*bytes, *elapsed);
    }
    plan.record_preparation(preparation);
    plan.target(cursor, capacity, limit, batch_bytes)
}
