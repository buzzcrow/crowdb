// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{sweep::WindowReport, CatalogId, GcLimits, ScanPosition};
use std::time::Duration;
use tokio::time::Instant;

struct Scope {
    catalog: CatalogId,
    cursor: ScanPosition,
    due: Instant,
    failures: u32,
}

#[derive(Default)]
pub(super) struct Schedule {
    scopes: Vec<Scope>,
    next: usize,
}

impl Schedule {
    pub fn retain_scopes(&mut self, configured: &[CatalogId], active: CatalogId) {
        self.scopes
            .retain(|scope| scope.catalog == active || configured.contains(&scope.catalog));
        for catalog in configured.iter().copied().chain(std::iter::once(active)) {
            if !self.scopes.iter().any(|scope| scope.catalog == catalog) {
                self.scopes.push(Scope {
                    catalog,
                    cursor: ScanPosition::default(),
                    due: Instant::now(),
                    failures: 0,
                });
            }
        }
    }

    pub fn next_due(&self) -> Instant {
        self.scopes
            .iter()
            .map(|scope| scope.due)
            .min()
            .unwrap_or_else(Instant::now)
    }

    pub fn ready(&mut self) -> Option<usize> {
        let now = Instant::now();
        for offset in 0..self.scopes.len() {
            let slot = self.next.wrapping_add(offset) % self.scopes.len();
            if self.scopes[slot].due <= now {
                self.next = slot + 1;
                return Some(slot);
            }
        }
        None
    }

    pub fn catalog(&self, slot: usize) -> CatalogId {
        self.scopes[slot].catalog
    }

    pub fn position(&mut self, slot: usize) -> &mut ScanPosition {
        &mut self.scopes[slot].cursor
    }

    pub fn finish(&mut self, slot: usize, report: WindowReport, idle_ms: u64, limits: GcLimits) {
        let scope = &mut self.scopes[slot];
        let now_ms = super::super::runtime::now_ms().unwrap_or(0);
        let delay = next_delay(report, &mut scope.failures, idle_ms, limits, now_ms);
        scope.due = Instant::now() + Duration::from_millis(delay);
    }

    #[cfg(feature = "test-util")]
    pub fn len(&self) -> usize {
        self.scopes.len()
    }
}

pub(super) fn next_delay(
    report: WindowReport,
    failures: &mut u32,
    idle_ms: u64,
    limits: GcLimits,
    now_ms: u64,
) -> u64 {
    if report.failed {
        *failures = failures.saturating_add(1);
        return limits.retry_delay_ms(*failures).max(100);
    }
    *failures = 0;
    if report.busy {
        return u64::from(limits.retry_base_ms).max(100).min(idle_ms);
    }
    report
        .retry_at_ms
        .map_or(idle_ms, |at| idle_ms.min(at.saturating_sub(now_ms).max(100)))
}
