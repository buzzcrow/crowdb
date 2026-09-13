// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DdbDiskGroupContainer` — per-instance singleton managing all owned disk-groups.

use super::disk_group::DdbDiskGroup;
use crate::liveness::lifecycle::LifecycleState;
use arc_swap::ArcSwap;
use crowdb_protocol::DiskGroupId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

/// Per-instance singleton managing all owned disk-groups.
pub struct DdbDiskGroupContainer {
    disk_groups: ArcSwap<HashMap<DiskGroupId, Arc<DdbDiskGroup>>>,
    pub(crate) instance_id: u64,
    pub(crate) degraded: AtomicBool,
    pub(crate) lifecycle: LifecycleState,
    /// Epoch millis of the last successful keepalive sync (R74
    /// `last_sync_age_secs` gauge). Initialized to construction time
    /// so the age starts at 0.
    last_sync_at_ms: AtomicU64,
}

impl DdbDiskGroupContainer {
    pub fn new(instance_id: u64) -> Self {
        Self {
            disk_groups: ArcSwap::from_pointee(HashMap::new()),
            instance_id,
            degraded: AtomicBool::new(false),
            lifecycle: LifecycleState::new(),
            last_sync_at_ms: AtomicU64::new(now_ms()),
        }
    }

    pub(crate) fn add_disk_group(&self, dg: &Arc<DdbDiskGroup>) {
        self.publish_disk_group(dg);
    }

    /// Replace an existing disk-group with a recovered one (same
    /// `disk_group_id`). Used by startup recovery to swap in the
    /// fully-reconstructed disk-group.
    pub fn replace_disk_group(&self, dg: &Arc<DdbDiskGroup>) {
        self.publish_disk_group(dg);
    }

    pub fn replace_disk_group_if_current(
        &self,
        expected: &Arc<DdbDiskGroup>,
        expected_bind: (u64, u64),
        loaded: &Arc<DdbDiskGroup>,
    ) -> bool {
        loop {
            let current = self.disk_groups.load_full();
            let Some(published) = current.get(&expected.disk_group_id) else {
                return false;
            };
            if !Arc::ptr_eq(published, expected) || published.bind() != expected_bind {
                return false;
            }

            let mut replacement = (*current).clone();
            replacement.insert(expected.disk_group_id, Arc::clone(loaded));
            let previous = self.disk_groups.compare_and_swap(&current, Arc::new(replacement));
            if Arc::ptr_eq(&previous, &current) {
                return true;
            }
        }
    }

    pub(crate) fn remove_disk_group(&self, dg_id: DiskGroupId) {
        loop {
            let current = self.disk_groups.load_full();
            if !current.contains_key(&dg_id) {
                return;
            }
            let mut replacement = (*current).clone();
            replacement.remove(&dg_id);
            let previous = self.disk_groups.compare_and_swap(&current, Arc::new(replacement));
            if Arc::ptr_eq(&previous, &current) {
                return;
            }
        }
    }

    pub fn get_disk_group(&self, dg_id: DiskGroupId) -> Option<Arc<DdbDiskGroup>> {
        self.disk_groups.load().get(&dg_id).cloned()
    }

    pub fn disk_group_ids(&self) -> Vec<DiskGroupId> {
        self.disk_groups.load().keys().copied().collect()
    }

    pub fn enter_degraded_mode(&self) {
        let prev = self.degraded.swap(true, Ordering::SeqCst);
        if !prev {
            warn!("entering degraded mode");
        }
    }

    pub fn exit_degraded_mode(&self) {
        let prev = self.degraded.swap(false, Ordering::SeqCst);
        if prev {
            warn!("exiting degraded mode");
        }
    }

    /// Whether the instance is in degraded mode (missed heartbeats).
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }

    /// Record a successful keepalive sync (called by the keepalive
    /// loop on each successful tick). Updates `last_sync_at_ms`.
    pub fn record_sync_success(&self) {
        self.last_sync_at_ms.store(now_ms(), Ordering::Release);
    }

    /// Seconds since the last successful sync (R74 `last_sync_age_secs`).
    #[must_use]
    pub fn last_sync_age_secs(&self) -> u64 {
        let last = self.last_sync_at_ms.load(Ordering::Acquire);
        let now = now_ms();
        (now.saturating_sub(last)) / 1000
    }

    /// Number of owned disk-groups (R74 `owned_disk_group_count` gauge).
    #[must_use]
    pub fn disk_group_count(&self) -> usize {
        self.disk_groups.load().len()
    }

    /// Current startup phase.
    pub fn lifecycle_phase(&self) -> crate::liveness::lifecycle::StartupPhase {
        self.lifecycle.get()
    }

    /// Set the startup phase.
    pub fn set_lifecycle_phase(&self, phase: crate::liveness::lifecycle::StartupPhase) {
        self.lifecycle.set(phase);
    }

    fn publish_disk_group(&self, disk_group: &Arc<DdbDiskGroup>) {
        loop {
            let current = self.disk_groups.load_full();
            let mut replacement = (*current).clone();
            replacement.insert(disk_group.disk_group_id, Arc::clone(disk_group));
            let previous = self.disk_groups.compare_and_swap(&current, Arc::new(replacement));
            if Arc::ptr_eq(&previous, &current) {
                return;
            }
        }
    }
}

/// Current epoch time in milliseconds.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().try_into().unwrap_or(u64::MAX))
}
