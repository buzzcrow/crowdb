// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Test-only in-memory partition tree.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Notify;
use tokio::sync::RwLock;

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
}

#[async_trait]
impl PartitionTree for MemoryPartitionTree {
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

    async fn checkpoint(&self) -> Result<(u64, u64)> {
        let applied = self.last_applied.load(Ordering::Acquire);
        Ok((applied, applied))
    }

    async fn checkpoint_snapshot(&self) -> Result<(u64, u64, Arc<dyn PartitionTree>)> {
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
        };
        Ok((Arc::new(rebuilt), stats))
    }

    fn last_applied_seq(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }
}
