// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Scope first, paginate at a fixed cutoff, then select eligible work.

use bytes::Bytes;
use crowdb_protocol::chunk_slot::ChunkSlotBitmap;
use tracing::warn;

use super::{
    BinaryKey, FinalizeChunkTaskKey, KeyError, LeasedChunkTaskKey, ReadyChunkTaskKey, TaskStore,
    TaskStoreError, TASK_KIND_FINALIZE_CHUNK,
};

struct ScanRange {
    prefix: Vec<u8>,
    start: Vec<u8>,
    end: Vec<u8>,
}

impl TaskStore {
    /// Return the highest priority eligible tasks within this runtime's scope.
    ///
    /// # Errors
    /// Fails on missing routes, incomplete pagination, or KV failures.
    pub async fn scan_ready(
        &self,
        now_ms: u64,
        max_keys: u32,
    ) -> Result<Vec<ReadyChunkTaskKey>, TaskStoreError> {
        self.scan_index(ReadyChunkTaskKey::prefix_all(), max_keys, |bytes| {
            let key = ReadyChunkTaskKey::from_bytes(bytes)?;
            Ok((key.eligible_at_ms <= now_ms).then_some(key))
        })
        .await
    }

    /// Return due finalization work without scanning generic task kinds.
    ///
    /// # Errors
    /// Fails on missing routes, incomplete pagination, or KV failures.
    pub async fn scan_finalize_due(
        &self,
        now_ms: u64,
        max_keys: u32,
    ) -> Result<Vec<ReadyChunkTaskKey>, TaskStoreError> {
        self.scan_index(FinalizeChunkTaskKey::prefix_all(), max_keys, |bytes| {
            let key = FinalizeChunkTaskKey::from_bytes(bytes)?;
            Ok((key.expires_at_ms <= now_ms).then_some(ReadyChunkTaskKey {
                priority_inverse: 0,
                eligible_at_ms: key.expires_at_ms,
                partition_id: key.partition_id,
                kind: TASK_KIND_FINALIZE_CHUNK,
                task_id: key.task_id,
            }))
        })
        .await
    }

    /// Return expired claims within the runtime's domain and owned slots.
    ///
    /// # Errors
    /// Fails on missing routes, incomplete pagination, or KV failures.
    pub async fn scan_expired_leases(
        &self,
        now_ms: u64,
        max_keys: u32,
    ) -> Result<Vec<LeasedChunkTaskKey>, TaskStoreError> {
        self.scan_index(LeasedChunkTaskKey::prefix_all(), max_keys, |bytes| {
            let key = LeasedChunkTaskKey::from_bytes(bytes)?;
            Ok((key.lease_deadline_ms <= now_ms).then_some(key))
        })
        .await
    }

    async fn scan_index<T>(
        &self,
        prefix: Vec<u8>,
        max_keys: u32,
        decode: impl Fn(&[u8]) -> Result<Option<T>, KeyError>,
    ) -> Result<Vec<T>, TaskStoreError> {
        let ranges = self.scan_ranges(prefix);
        if max_keys == 0 || ranges.is_empty() {
            return Ok(Vec::new());
        }
        let table = self.bindings.snapshot();
        if table.is_empty() {
            return Err(crate::routing::RouteError::NoBinding.into());
        }
        let limit = usize::try_from(max_keys).unwrap_or(usize::MAX);
        let mut selected = Vec::new();
        for route in table.bindings() {
            for range in &ranges {
                let mut start = Bytes::copy_from_slice(&range.start);
                let mut cutoff = 0;
                loop {
                    let page = self
                        .kv
                        .scan_bounded_at(
                            route.kv_store_id,
                            route.kv_group_id,
                            &range.prefix,
                            &start,
                            &range.end,
                            256,
                            true,
                            None,
                            cutoff,
                        )
                        .await
                        .map_err(|error| TaskStoreError::Kv(error.to_string()))?;
                    if page.timed_out || (page.truncated && page.items.is_empty()) {
                        return Err(TaskStoreError::Kv("task scan made incomplete progress".into()));
                    }
                    cutoff = page.scan_cutoff;
                    for (key, _) in page.items {
                        if key <= start {
                            return Err(TaskStoreError::Kv(
                                "task scan continuation did not advance".into(),
                            ));
                        }
                        start = key.clone();
                        match decode(&key) {
                            // Exclude the domain/slot prefix when merging priority/deadline
                            // order across slots and groups. Keep memory bounded per page.
                            Ok(Some(task)) => selected.push((key.slice(6..), task)),
                            Ok(None) => {}
                            Err(error) => warn!(%error, "skipping malformed task index"),
                        }
                    }
                    selected.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                    selected.dedup_by(|left, right| left.0 == right.0);
                    selected.truncate(limit);
                    if !page.truncated {
                        break;
                    }
                }
            }
        }
        Ok(selected.into_iter().map(|(_, task)| task).collect())
    }

    fn scan_ranges(&self, prefix: Vec<u8>) -> Vec<ScanRange> {
        let Some((guard, domain)) = &self.scope else {
            return vec![ScanRange {
                prefix,
                start: Vec::new(),
                end: Vec::new(),
            }];
        };
        let mut prefix = prefix;
        prefix.push(*domain as u8);
        slot_runs(&guard.owned_slots())
            .into_iter()
            .map(|(start, end)| {
                let mut lower = prefix.clone();
                lower.extend_from_slice(&start.to_be_bytes());
                let mut upper = prefix.clone();
                upper.extend_from_slice(&end.to_be_bytes());
                ScanRange {
                    prefix: prefix.clone(),
                    start: lower,
                    end: upper,
                }
            })
            .collect()
    }
}

/// Half-open contiguous runs avoid one RPC per slot for banded ownership.
fn slot_runs(slots: &ChunkSlotBitmap) -> Vec<(u16, u16)> {
    let mut runs: Vec<(u16, u16)> = Vec::new();
    for slot in slots.slots() {
        if let Some(last) = runs.last_mut().filter(|last| last.1 == slot.value()) {
            last.1 += 1;
        } else {
            runs.push((slot.value(), slot.value() + 1));
        }
    }
    runs
}
