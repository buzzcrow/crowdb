// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

/// Threshold polls must not postpone the periodic sweep of small backlogs.
#[derive(Default)]
pub struct CompactionSchedule {
    last_sweep_seconds: AtomicU64,
}

impl CompactionSchedule {
    pub fn threshold(&self, elapsed_seconds: u64, cadence_seconds: u64, threshold: u32) -> u32 {
        let last = self.last_sweep_seconds.load(Ordering::Acquire);
        if elapsed_seconds.saturating_sub(last) >= cadence_seconds {
            self.last_sweep_seconds.store(elapsed_seconds, Ordering::Release);
            1
        } else {
            threshold.max(1)
        }
    }
}
