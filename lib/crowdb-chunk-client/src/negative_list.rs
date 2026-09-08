// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free snapshot of temporarily excluded disks shared by all writers.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use crowdb_protocol::common::DiskId;

#[doc(hidden)]
pub struct FailedDiskList {
    entries: ArcSwap<HashMap<DiskId, Instant>>,
    ttl: Duration,
}

impl FailedDiskList {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: ArcSwap::from_pointee(HashMap::new()),
            ttl,
        }
    }

    pub fn insert(&self, disk_id: DiskId) {
        let expires = Instant::now() + self.ttl;
        self.entries.rcu(|current| {
            let mut next = (**current).clone();
            next.insert(disk_id, expires);
            next
        });
    }

    pub fn live(&self) -> Vec<DiskId> {
        let now = Instant::now();
        self.entries
            .load()
            .iter()
            .filter_map(|(disk, expires)| (*expires > now).then_some(*disk))
            .collect()
    }
}
