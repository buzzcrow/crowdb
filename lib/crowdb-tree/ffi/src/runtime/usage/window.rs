// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::io::{Result, Write};
use std::time::Instant;

use super::{ResourceUsageSnapshot, TREE_RESOURCE_NAMES};

struct Average {
    area: u128,
    duration_ns: u128,
}

impl std::fmt::Display for Average {
    fn fmt(&self, writer: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let duration = self.duration_ns.max(1);
        let whole = self.area / duration;
        // Duration comes from Instant's u64 seconds, so the remainder times
        // 1000 fits u128. Preserve fractional worker/permit occupancy.
        let fraction = (self.area % duration) * 1000 / duration;
        write!(writer, "{whole}.{fraction:03}")
    }
}

#[derive(Default)]
struct Row {
    area: [u128; 4],
    max: [u64; 4],
}

struct Scope {
    last: ResourceUsageSnapshot,
    baseline: ResourceUsageSnapshot,
    rows: [Row; TREE_RESOURCE_NAMES.len()],
    integrated_at: Instant,
    sampled_at: Instant,
    duration_ns: u128,
    max_gap_ns: u128,
    samples: u64,
    errors: u64,
    seen: u64,
    active: bool,
}

impl Scope {
    fn new(snapshot: ResourceUsageSnapshot, now: Instant, generation: u64) -> Self {
        let mut result = Self {
            last: snapshot,
            baseline: snapshot,
            rows: std::array::from_fn(|_| Row::default()),
            integrated_at: now,
            sampled_at: now,
            duration_ns: 0,
            max_gap_ns: 0,
            samples: 1,
            errors: 0,
            seen: generation,
            active: true,
        };
        result.peaks();
        result
    }

    fn integrate(&mut self, now: Instant) {
        if self.active {
            let elapsed = now.saturating_duration_since(self.integrated_at).as_nanos();
            self.duration_ns = self.duration_ns.saturating_add(elapsed);
            self.max_gap_ns = self
                .max_gap_ns
                .max(now.saturating_duration_since(self.sampled_at).as_nanos());
            for (index, (row, value)) in self.rows.iter_mut().zip(self.last.resources).enumerate() {
                if self.last.resource_mask & (1 << index) == 0 {
                    continue;
                }
                for (area, current) in
                    row.area
                        .iter_mut()
                        .zip([value.used, value.reserved, value.allocated, value.waiters])
                {
                    *area = area.saturating_add(u128::from(current).saturating_mul(elapsed));
                }
            }
        }
        self.integrated_at = now;
    }

    fn peaks(&mut self) {
        for (index, (row, value)) in self.rows.iter_mut().zip(self.last.resources).enumerate() {
            if self.last.resource_mask & (1 << index) == 0 {
                continue;
            }
            for (peak, current) in
                row.max
                    .iter_mut()
                    .zip([value.used, value.reserved, value.allocated, value.waiters])
            {
                *peak = (*peak).max(current);
            }
        }
    }

    fn flush(&mut self, writer: &mut dyn Write, timestamp: &str, name: &str, now: Instant) -> Result<()> {
        self.integrate(now);
        for (index, resource) in TREE_RESOURCE_NAMES.iter().enumerate() {
            if self.last.resource_mask & (1 << index) == 0 {
                continue;
            }
            let value = self.last.resources[index];
            let previous = self.baseline.resources[index];
            let row = &self.rows[index];
            let avg = row.area.map(|area| Average {
                area,
                duration_ns: self.duration_ns,
            });
            let unit = if index < 5 || matches!(index, 10 | 17 | 19) {
                "bytes"
            } else {
                "count"
            };
            let contention_measured = index < 6
                || (13..16).contains(&index)
                || (self.last.resource_mask & (1 << 8) != 0 && index < 8);
            writeln!(writer,
                "{timestamp} {name}.resource.{resource} scope_id={} unit={unit} active={} limit={} used={} reserved={} allocated={} avg={} sample_max={} reserved_avg={} reserved_sample_max={} allocated_avg={} allocated_sample_max={} denied_tree={} denied_host={} waits={} wait_ns={} waiters={} waiters_avg={} waiters_sample_max={} samples={} coverage_ns={} max_sample_gap_ns={} sample_age_ns={} sample_errors={} contention_measured={}",
                self.last.scope_id, u8::from(self.active), value.limit, value.used, value.reserved, value.allocated,
                avg[0], row.max[0], avg[1], row.max[1], avg[2], row.max[2],
                value.denied_tree.saturating_sub(previous.denied_tree), value.denied_host.saturating_sub(previous.denied_host),
                value.waits.saturating_sub(previous.waits), value.wait_ns.saturating_sub(previous.wait_ns), value.waiters, avg[3], row.max[3],
                self.samples, self.duration_ns, self.max_gap_ns, now.saturating_duration_since(self.sampled_at).as_nanos(), self.errors, u8::from(contention_measured))?;
        }
        self.baseline = self.last;
        self.rows = std::array::from_fn(|_| Row::default());
        self.duration_ns = 0;
        self.max_gap_ns = 0;
        self.samples = 0;
        self.errors = 0;
        // A held sample can occupy the entire next window without changing.
        self.peaks();
        Ok(())
    }
}

/// Metrics-task-owned windows; application hot paths do not update this state.
/// Averages integrate the last sampled value over its observed duration.
/// Maxima are sampled maxima, not exact allocation peaks. Sampling gaps and
/// errors are exported so delayed or missing observations remain visible.
#[derive(Default)]
pub struct ResourceUsageLog {
    scopes: BTreeMap<(String, u64), Scope>,
    generation: u64,
    failed_samples: BTreeMap<String, u64>,
}

impl ResourceUsageLog {
    /// Begin one complete host/Tree enumeration, then call `end_sample`.
    pub fn begin_sample(&mut self) {
        self.generation = self.generation.wrapping_add(1);
    }

    pub fn observe(&mut self, name: &str, snapshot: ResourceUsageSnapshot, now: Instant) {
        let key = (name.to_owned(), snapshot.scope_id);
        if let Some(scope) = self.scopes.get_mut(&key) {
            scope.integrate(now);
            scope.last = snapshot;
            scope.sampled_at = now;
            scope.seen = self.generation;
            scope.active = true;
            scope.samples = scope.samples.saturating_add(1);
            scope.peaks();
        } else {
            self.scopes
                .insert(key, Scope::new(snapshot, now, self.generation));
        }
    }

    /// A failed sample retains the last observation and reports degraded
    /// coverage instead of silently treating resource use as zero.
    pub fn sample_failed(&mut self, name: &str) {
        let failures = self.failed_samples.entry(name.to_owned()).or_default();
        *failures = failures.saturating_add(1);
        for ((scope_name, _), scope) in &mut self.scopes {
            if scope_name == name {
                scope.errors = scope.errors.saturating_add(1);
                scope.seen = self.generation;
            }
        }
    }

    pub fn end_sample(&mut self, now: Instant) {
        for scope in self.scopes.values_mut() {
            if scope.seen != self.generation && scope.active {
                scope.integrate(now);
                scope.active = false;
            }
        }
    }

    /// Flush exactly once per log period. Ordinary native snapshot readers
    /// neither advance nor reset these windows.
    pub fn flush(&mut self, writer: &mut dyn Write, timestamp: &str, now: Instant) -> Result<()> {
        for (name, errors) in &self.failed_samples {
            writeln!(writer, "{timestamp} {name}.resource.sample_errors count={errors}")?;
        }
        self.failed_samples.clear();
        for ((name, _), scope) in &mut self.scopes {
            scope.flush(writer, timestamp, name, now)?;
        }
        self.scopes.retain(|_, scope| scope.active);
        Ok(())
    }
}
