// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Discovery after a cached endpoint has failed or rejected leadership.

use super::TopologyCache;
use crate::error::Result;
use std::time::Instant;

impl TopologyCache {
    pub(in crate::client) async fn refresh_after_failure(
        &self,
        store_id: u64,
        group_id: u64,
        failed: &str,
    ) -> Result<()> {
        let mut last = self.refresh_gate.lock().await;
        if self
            .leader(store_id, group_id)
            .is_some_and(|leader| leader != failed)
            || last.elapsed() < self.min_refresh_interval
        {
            return Ok(());
        }
        let generation = self.snapshot.load().generation;
        // Reachability of a management seed does not establish freshness of
        // its leader view. Keep looking if it repeats the rejected endpoint.
        let result = self
            .fetch_group_and_merge(generation, Some((store_id, group_id)), Some(failed))
            .await;
        *last = Instant::now();
        result.map(|_| ())
    }
}
