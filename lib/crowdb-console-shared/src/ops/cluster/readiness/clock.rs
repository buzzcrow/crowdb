// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Never admit evidence before its timestamp, including small peer clock offsets.

use crowdb_protocol::mgmt::cluster::{ClusterObservation, ClusterStatus, DependencyKind};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
}

/// Reduce evidence after a bounded wait for a slightly faster peer clock.
///
/// Timestamps and freshness limits are unchanged. Waiting consumes the caller's
/// existing deadline and cannot exceed any future fact's freshness horizon.
/// Larger offsets remain unknown immediately; elapsed evidence stays stale.
pub async fn reduce_current(observation: ClusterObservation, deadline: Instant) -> ClusterStatus {
    let now = now_ms();
    let mut wait_ms = 0;
    for fact in &observation.dependencies {
        if fact.kind == DependencyKind::Runtime || fact.operation_id != fact.current_operation_id {
            continue;
        }
        let ahead = fact.observed_at_unix_ms.saturating_sub(now);
        if ahead > fact.max_age_ms.min(3000) {
            return super::reduce(observation, now);
        }
        wait_ms = wait_ms.max(ahead);
    }
    if wait_ms > 0 {
        if let Some(until) = Instant::now().checked_add(Duration::from_millis(wait_ms)) {
            if until < deadline {
                tokio::time::sleep_until(until).await;
            }
        }
    }
    super::reduce(observation, now_ms())
}
