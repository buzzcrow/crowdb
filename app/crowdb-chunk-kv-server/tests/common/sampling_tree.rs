// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{
    any::Any,
    future::{poll_fn, Future},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, MutationOperation, PartitionRange, PartitionTree, Result, ScanEntry,
    ValueRevision,
};
use tokio::sync::Notify;

pub struct TestSamplingTree {
    memory: MemoryPartitionTree,
    pub entered: Notify,
    pub release: Notify,
    pub finished: AtomicBool,
    pub active: AtomicBool,
    pub started: AtomicUsize,
}

impl TestSamplingTree {
    pub fn new() -> Self {
        Self {
            memory: MemoryPartitionTree::with_tree_id(71),
            entered: Notify::new(),
            release: Notify::new(),
            finished: AtomicBool::new(false),
            active: AtomicBool::new(false),
            started: AtomicUsize::new(0),
        }
    }

    async fn hold_read(&self) {
        let mut timer = std::pin::pin!(tokio::time::sleep(Duration::from_secs(30)));
        let mut release = std::pin::pin!(self.release.notified());
        let mut announced = false;
        poll_fn(|cx| {
            // Keep a real registered timer in the production blocking job.
            // Runtime shutdown wakes it; polling after shutdown must panic.
            let timer = timer.as_mut().poll(cx);
            let release = release.as_mut().poll(cx);
            if timer.is_ready() || release.is_ready() {
                return Poll::Ready(());
            }
            if !announced {
                announced = true;
                self.entered.notify_one();
            }
            Poll::Pending
        })
        .await;
    }
}

#[async_trait::async_trait]
impl PartitionTree for TestSamplingTree {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn tree_id(&self) -> u64 {
        self.memory.tree_id()
    }

    async fn get(&self, key: &[u8]) -> Result<Option<ValueRevision>> {
        self.memory.get(key).await
    }

    async fn scan_forward(
        &self,
        start: Option<&[u8]>,
        inclusive: bool,
        end: Option<&[u8]>,
        limit: usize,
        bytes: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        self.started.fetch_add(1, Ordering::AcqRel);
        self.active.store(true, Ordering::Release);
        self.hold_read().await;
        let result = self
            .memory
            .scan_forward(start, inclusive, end, limit, bytes)
            .await;
        self.active.store(false, Ordering::Release);
        self.finished.store(true, Ordering::Release);
        result
    }

    async fn seek_reverse(
        &self,
        start: &[u8],
        inclusive: bool,
        begin: Option<&[u8]>,
    ) -> Result<Option<ScanEntry>> {
        self.memory.seek_reverse(start, inclusive, begin).await
    }

    async fn scan_reverse(
        &self,
        start: Option<&[u8]>,
        begin: Option<&[u8]>,
        limit: usize,
        bytes: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        self.memory.scan_reverse(start, begin, limit, bytes).await
    }

    async fn apply(&self, seq: u64, operation: &MutationOperation) -> Result<()> {
        self.memory.apply(seq, operation).await
    }

    async fn advance_noop(&self, seq: u64) -> Result<()> {
        self.memory.advance_noop(seq).await
    }

    async fn checkpoint(&self, offset: u64) -> Result<(u64, u64)> {
        self.memory.checkpoint(offset).await
    }

    async fn checkpoint_snapshot(&self, offset: u64) -> Result<(u64, u64, Arc<dyn PartitionTree>)> {
        self.memory.checkpoint_snapshot(offset).await
    }

    async fn rebuild_range(
        &self,
        tree_id: u64,
        range: &PartitionRange,
        config: crowdb_tree_ffi::Config,
    ) -> Result<(Arc<dyn PartitionTree>, crowdb_tree_ffi::RangeRebuildStats)> {
        self.memory.rebuild_range(tree_id, range, config).await
    }

    fn checkpoint_state(&self) -> Result<(u64, u64)> {
        self.memory.checkpoint_state()
    }

    fn last_applied_seq(&self) -> u64 {
        self.memory.last_applied_seq()
    }
}
