// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Test-only in-memory partition tree.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{Mutex, Notify, RwLock};

use crate::{MutationOperation, PartitionTree, Result, ScanEntry, ValueRevision};

#[derive(Debug)]
pub struct MemoryPartitionTree {
    tree_id: u64,
    values: RwLock<BTreeMap<Vec<u8>, ValueRevision>>,
    last_applied: AtomicU64,
    fail_next_apply: AtomicBool,
    rebuild_paused: Arc<AtomicBool>,
    rebuild_started: Arc<Notify>,
    rebuild_resume: Arc<Notify>,
    split_publish_paused: Arc<AtomicBool>,
    split_publish_started: Arc<Notify>,
    split_publish_resume: Arc<Notify>,
    split_views: Mutex<BTreeMap<u64, BTreeMap<Vec<u8>, ValueRevision>>>,
    next_split_view: AtomicU64,
}

impl Default for MemoryPartitionTree {
    fn default() -> Self {
        Self {
            tree_id: 1,
            values: RwLock::default(),
            last_applied: AtomicU64::new(0),
            fail_next_apply: AtomicBool::new(false),
            rebuild_paused: Arc::new(AtomicBool::new(false)),
            rebuild_started: Arc::new(Notify::new()),
            rebuild_resume: Arc::new(Notify::new()),
            split_publish_paused: Arc::new(AtomicBool::new(false)),
            split_publish_started: Arc::new(Notify::new()),
            split_publish_resume: Arc::new(Notify::new()),
            split_views: Mutex::default(),
            next_split_view: AtomicU64::new(0),
        }
    }
}

impl MemoryPartitionTree {
    #[must_use]
    pub fn with_tree_id(tree_id: u64) -> Self {
        Self {
            tree_id,
            ..Self::default()
        }
    }

    pub fn fail_next_apply(&self) {
        self.fail_next_apply.store(true, Ordering::Release);
    }

    pub fn pause_rebuild(&self) {
        self.rebuild_paused.store(true, Ordering::Release);
    }

    pub async fn wait_for_rebuild(&self) {
        let notified = self.rebuild_started.notified();
        if self.rebuild_paused.load(Ordering::Acquire) {
            notified.await;
        }
    }

    pub fn resume_rebuild(&self) {
        self.rebuild_paused.store(false, Ordering::Release);
        self.rebuild_resume.notify_waiters();
    }

    pub fn pause_split_publish(&self) {
        self.split_publish_paused.store(true, Ordering::Release);
    }

    pub async fn wait_for_split_publish(&self) {
        let notified = self.split_publish_started.notified();
        if self.split_publish_paused.load(Ordering::Acquire) {
            notified.await;
        }
    }

    pub fn resume_split_publish(&self) {
        self.split_publish_paused.store(false, Ordering::Release);
        self.split_publish_resume.notify_waiters();
    }
}

#[async_trait]
impl PartitionTree for MemoryPartitionTree {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn tree_id(&self) -> u64 {
        self.tree_id
    }

    async fn get(&self, key: &[u8]) -> Result<Option<ValueRevision>> {
        Ok(self.values.read().await.get(key).cloned())
    }

    async fn scan_forward(
        &self,
        start_key: Option<&[u8]>,
        start_inclusive: bool,
        end_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        let values = self.values.read().await;
        let mut entries = Vec::new();
        let mut bytes = 0_usize;
        let mut truncated = false;
        for (key, value) in values.iter() {
            if start_key
                .is_some_and(|start| key.as_slice() < start || (!start_inclusive && key.as_slice() == start))
                || end_key.is_some_and(|end| key.as_slice() >= end)
            {
                continue;
            }
            let entry_bytes = key.len().saturating_add(value.value.len());
            if entries.len() == limit
                || (!entries.is_empty() && bytes.saturating_add(entry_bytes) > byte_budget)
            {
                truncated = true;
                break;
            }
            bytes = bytes.saturating_add(entry_bytes);
            entries.push(ScanEntry {
                key: Bytes::copy_from_slice(key),
                value: value.clone(),
            });
        }
        Ok((entries, truncated))
    }

    async fn seek_reverse(
        &self,
        start_key: &[u8],
        start_inclusive: bool,
        begin_key: Option<&[u8]>,
    ) -> Result<Option<ScanEntry>> {
        let values = self.values.read().await;
        Ok(values
            .iter()
            .rev()
            .find(|(key, _)| {
                begin_key.map_or(true, |begin| key.as_slice() >= begin)
                    && (key.as_slice() < start_key || (start_inclusive && key.as_slice() == start_key))
            })
            .map(|(key, value)| ScanEntry {
                key: Bytes::copy_from_slice(key),
                value: value.clone(),
            }))
    }

    async fn scan_reverse(
        &self,
        start_before: Option<&[u8]>,
        begin_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        let values = self.values.read().await;
        let mut entries = Vec::new();
        let mut bytes = 0_usize;
        let mut truncated = false;
        for (key, value) in values.iter().rev() {
            if start_before.is_some_and(|start| key.as_slice() >= start)
                || begin_key.is_some_and(|begin| key.as_slice() < begin)
            {
                continue;
            }
            let entry_bytes = key.len().saturating_add(value.value.len());
            if entries.len() == limit
                || (!entries.is_empty() && bytes.saturating_add(entry_bytes) > byte_budget)
            {
                truncated = true;
                break;
            }
            bytes = bytes.saturating_add(entry_bytes);
            entries.push(ScanEntry {
                key: Bytes::copy_from_slice(key),
                value: value.clone(),
            });
        }
        Ok((entries, truncated))
    }

    async fn apply(&self, mutation_seq: u64, operation: &MutationOperation) -> Result<()> {
        if self.fail_next_apply.swap(false, Ordering::AcqRel) {
            return Err(crate::ChunkKvError::ApplyStateUnknown);
        }
        let mut values = self.values.write().await;
        match operation.successful_value() {
            Some(value) => {
                values.insert(
                    operation.key().to_vec(),
                    ValueRevision {
                        revision: mutation_seq,
                        value: value.to_vec(),
                    },
                );
            }
            None => {
                values.remove(operation.key());
            }
        }
        self.last_applied.store(mutation_seq, Ordering::Release);
        Ok(())
    }

    async fn advance_noop(&self, mutation_seq: u64) -> Result<()> {
        self.last_applied.store(mutation_seq, Ordering::Release);
        Ok(())
    }

    async fn checkpoint(&self, _wal_replay_offset: u64) -> Result<(u64, u64)> {
        let applied = self.last_applied.load(Ordering::Acquire);
        Ok((applied, applied))
    }

    async fn checkpoint_snapshot(
        &self,
        _wal_replay_offset: u64,
    ) -> Result<(u64, u64, Arc<dyn PartitionTree>)> {
        let values = self.values.read().await.clone();
        let applied = self.last_applied.load(Ordering::Acquire);
        let snapshot = Self {
            tree_id: self.tree_id,
            values: RwLock::new(values),
            last_applied: AtomicU64::new(applied),
            fail_next_apply: AtomicBool::new(false),
            rebuild_paused: Arc::clone(&self.rebuild_paused),
            rebuild_started: Arc::clone(&self.rebuild_started),
            rebuild_resume: Arc::clone(&self.rebuild_resume),
            split_publish_paused: Arc::clone(&self.split_publish_paused),
            split_publish_started: Arc::clone(&self.split_publish_started),
            split_publish_resume: Arc::clone(&self.split_publish_resume),
            split_views: Mutex::default(),
            next_split_view: AtomicU64::new(0),
        };
        Ok((applied, applied, Arc::new(snapshot)))
    }

    async fn rebuild_range(
        &self,
        tree_id: u64,
        range: &crate::PartitionRange,
        _config: crowdb_tree_ffi::Config,
    ) -> Result<(Arc<dyn PartitionTree>, crowdb_tree_ffi::RangeRebuildStats)> {
        if self.rebuild_paused.load(Ordering::Acquire) {
            self.rebuild_started.notify_one();
            while self.rebuild_paused.load(Ordering::Acquire) {
                self.rebuild_resume.notified().await;
            }
        }
        let values = self.values.read().await;
        let filtered = values
            .iter()
            .filter(|(key, _)| range.contains(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let stats = crowdb_tree_ffi::RangeRebuildStats {
            entries_examined: values.len() as u64,
            entries_emitted: filtered.len() as u64,
            entries_filtered: values.len().saturating_sub(filtered.len()) as u64,
            ..crowdb_tree_ffi::RangeRebuildStats::default()
        };
        let rebuilt = Self {
            tree_id,
            values: RwLock::new(filtered),
            last_applied: AtomicU64::new(self.last_applied.load(Ordering::Acquire)),
            fail_next_apply: AtomicBool::new(false),
            rebuild_paused: Arc::clone(&self.rebuild_paused),
            rebuild_started: Arc::clone(&self.rebuild_started),
            rebuild_resume: Arc::clone(&self.rebuild_resume),
            split_publish_paused: Arc::clone(&self.split_publish_paused),
            split_publish_started: Arc::clone(&self.split_publish_started),
            split_publish_resume: Arc::clone(&self.split_publish_resume),
            split_views: Mutex::default(),
            next_split_view: AtomicU64::new(0),
        };
        Ok((Arc::new(rebuilt), stats))
    }

    async fn begin_split_memtable_view(&self) -> Result<(u64, u64)> {
        let generation = self
            .next_split_view
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .ok_or_else(|| crate::ChunkKvError::Faulted("split view generation exhausted".into()))?;
        let values = self.values.read().await.clone();
        let mut views = self.split_views.lock().await;
        if !views.is_empty() {
            return Err(crate::ChunkKvError::SplitRetry(
                "split memtable view is already active".into(),
            ));
        }
        views.insert(generation, values);
        Ok((generation, self.last_applied.load(Ordering::Acquire)))
    }

    async fn install_split_memtable_overlay(
        &self,
        source: &dyn PartitionTree,
        journal_frontier: u64,
    ) -> Result<()> {
        let source = source.as_any().downcast_ref::<Self>().ok_or_else(|| {
            crate::ChunkKvError::InvalidRequest("memory split overlay requires a memory source".into())
        })?;
        let inherited = source.values.read().await.clone();
        let mut values = self.values.write().await;
        for (key, value) in inherited {
            if value.revision <= journal_frontier {
                values.entry(key).or_insert(value);
            }
        }
        self.last_applied.fetch_max(journal_frontier, Ordering::AcqRel);
        Ok(())
    }

    async fn clear_split_memtable_overlay(&self, _source: &dyn PartitionTree) -> Result<()> {
        Ok(())
    }

    async fn publish_split_memtable_view(
        &self,
        generation: u64,
        journal_frontier: u64,
        destination: &dyn PartitionTree,
        range: &crate::PartitionRange,
    ) -> Result<()> {
        if self.split_publish_paused.load(Ordering::Acquire) {
            self.split_publish_started.notify_one();
            while self.split_publish_paused.load(Ordering::Acquire) {
                self.split_publish_resume.notified().await;
            }
        }
        let destination = destination.as_any().downcast_ref::<Self>().ok_or_else(|| {
            crate::ChunkKvError::InvalidRequest("memory split view requires a memory destination".into())
        })?;
        let view = self
            .split_views
            .lock()
            .await
            .get(&generation)
            .cloned()
            .ok_or_else(|| crate::ChunkKvError::SplitRetry("split memtable view is stale".into()))?;
        let mut values = destination.values.write().await;
        for (key, value) in view.into_iter().filter(|(key, _)| range.contains(key)) {
            values.insert(key, value);
        }
        destination.last_applied.store(
            destination
                .last_applied
                .load(Ordering::Acquire)
                .max(journal_frontier),
            Ordering::Release,
        );
        Ok(())
    }

    async fn release_split_memtable_view(&self, generation: u64) -> Result<()> {
        if self.split_views.lock().await.remove(&generation).is_none() {
            return Err(crate::ChunkKvError::SplitRetry(
                "split memtable view is stale".into(),
            ));
        }
        Ok(())
    }

    fn last_applied_seq(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }

    fn checkpoint_state(&self) -> Result<(u64, u64)> {
        let applied = self.last_applied.load(Ordering::Acquire);
        Ok((applied, applied))
    }
}
