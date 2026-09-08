// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free snapshot of temporarily excluded disks shared by all writers.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use crowdb_protocol::common::DiskId;

#[doc(hidden)]
pub struct FailedDiskList {
    entries: ArcSwap<HashMap<DiskId, FailedDisk>>,
    ttl: Duration,
}

#[derive(Clone, Copy)]
struct FailedDisk {
    expires: Instant,
    generation: u8,
}

const MAX_BACKOFF_SHIFT: u32 = 6;

impl FailedDiskList {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: ArcSwap::from_pointee(HashMap::new()),
            ttl,
        }
    }

    pub fn insert(&self, disk_id: DiskId) {
        self.insert_at(disk_id, Instant::now());
    }

    fn insert_at(&self, disk_id: DiskId, now: Instant) {
        self.entries.rcu(|current| {
            let mut next = (**current).clone();
            let generation = current
                .get(&disk_id)
                .filter(|entry| entry.expires > now)
                .map_or(0, |entry| entry.generation.saturating_add(1));
            let multiplier = 1_u32 << u32::from(generation).min(MAX_BACKOFF_SHIFT);
            let backoff = self.ttl.checked_mul(multiplier).unwrap_or(self.ttl);
            let expires = now
                .checked_add(backoff)
                .or_else(|| now.checked_add(self.ttl))
                .unwrap_or(now);
            next.insert(disk_id, FailedDisk { expires, generation });
            next
        });
    }

    pub fn live(&self) -> Vec<DiskId> {
        self.live_at(Instant::now())
    }

    fn live_at(&self, now: Instant) -> Vec<DiskId> {
        self.entries
            .load()
            .iter()
            .filter_map(|(disk, entry)| (entry.expires > now).then_some(*disk))
            .collect()
    }

    #[cfg(feature = "test-util")]
    pub fn insert_at_for_tests(&self, disk_id: DiskId, now: Instant) {
        self.insert_at(disk_id, now);
    }

    #[cfg(feature = "test-util")]
    pub fn live_at_for_tests(&self, now: Instant) -> Vec<DiskId> {
        self.live_at(now)
    }
}
