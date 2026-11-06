// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{
    ChunkKvError, MutationOperation, PartitionRange, PartitionTree, Result, ScanEntry, TransitionId,
    ValueRevision,
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
    Arc, OnceLock,
};

/// All storage operations use the real native Tree. Only the publication
/// boundary is paused or failed, before transferring that call to the Tree.
pub struct TestPublishTree {
    inner: Arc<dyn PartitionTree>,
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
    pause_once: AtomicBool,
    fail_publish: AtomicBool,
    fail_release: AtomicBool,
    generation: AtomicU64,
    begins: AtomicU64,
    publications: AtomicU64,
    releases: AtomicU64,
    stage: Arc<AtomicU8>,
    preparations: AtomicU64,
    installations: Arc<AtomicU64>,
    cancel_on_install: Arc<OnceLock<tokio::task::AbortHandle>>,
    pause_prepare: AtomicBool,
    cleanup: Arc<OnceLock<Arc<TestCleanupGate>>>,
    pub prepare_entered: tokio::sync::Notify,
    pub prepare_release: tokio::sync::Notify,
}

impl TestPublishTree {
    pub fn new(inner: Arc<dyn PartitionTree>, fail_publish: bool, fail_release: bool) -> Self {
        Self {
            inner,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            pause_once: AtomicBool::new(true),
            fail_publish: AtomicBool::new(fail_publish),
            fail_release: AtomicBool::new(fail_release),
            generation: AtomicU64::new(0),
            begins: AtomicU64::new(0),
            publications: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            stage: Arc::new(AtomicU8::new(0)),
            preparations: AtomicU64::new(0),
            installations: Arc::new(AtomicU64::new(0)),
            cancel_on_install: Arc::new(OnceLock::new()),
            pause_prepare: AtomicBool::new(false),
            cleanup: Arc::new(OnceLock::new()),
            prepare_entered: tokio::sync::Notify::new(),
            prepare_release: tokio::sync::Notify::new(),
        }
    }

    pub fn stage(&self) -> &'static str {
        match self.stage.load(Ordering::Acquire) {
            0 => "before parent snapshot",
            1 => "parent snapshot",
            2 => "after parent snapshot, before native capture",
            3 => "native capture",
            4 => "native capture complete",
            5 => "publication gate",
            6 => "native publication",
            7 => "native publication complete",
            8 => "native release",
            9 => "native release complete",
            10 => "background preparation gate",
            11 => "native preparation",
            12 => "native preparation complete",
            13 => "native token install",
            14 => "native token installed",
            _ => "unknown test stage",
        }
    }

    pub fn cancel_control_after_install(&self, abort: tokio::task::AbortHandle) {
        self.cancel_on_install.set(abort).unwrap();
    }

    pub fn pause_cleanup(&self) -> TestCleanupPause {
        let gate = Arc::new(TestCleanupGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        assert!(self.cleanup.set(gate.clone()).is_ok());
        TestCleanupPause { gate }
    }

    pub fn pause_preparation(&self) {
        self.pause_prepare.store(true, Ordering::Release);
    }

    pub fn prepared_calls(&self) -> (u64, u64) {
        (
            self.preparations.load(Ordering::Acquire),
            self.installations.load(Ordering::Acquire),
        )
    }

    pub fn calls(&self) -> (u64, u64, u64) {
        (
            self.begins.load(Ordering::Acquire),
            self.publications.load(Ordering::Acquire),
            self.releases.load(Ordering::Acquire),
        )
    }
}

#[async_trait::async_trait]
impl PartitionTree for TestPublishTree {
    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }
    fn tree_id(&self) -> u64 {
        self.inner.tree_id()
    }
    async fn get(&self, key: &[u8]) -> Result<Option<ValueRevision>> {
        self.inner.get(key).await
    }
    async fn scan_forward(
        &self,
        start: Option<&[u8]>,
        inclusive: bool,
        end: Option<&[u8]>,
        limit: usize,
        bytes: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        self.inner.scan_forward(start, inclusive, end, limit, bytes).await
    }
    async fn scan_forward_bounded(
        &self,
        start: Option<&[u8]>,
        inclusive: bool,
        end: Option<&[u8]>,
        limit: usize,
        bytes: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        self.inner
            .scan_forward_bounded(start, inclusive, end, limit, bytes)
            .await
    }
    async fn seek_reverse(
        &self,
        start: &[u8],
        inclusive: bool,
        begin: Option<&[u8]>,
    ) -> Result<Option<ScanEntry>> {
        self.inner.seek_reverse(start, inclusive, begin).await
    }
    async fn scan_reverse(
        &self,
        start: Option<&[u8]>,
        begin: Option<&[u8]>,
        limit: usize,
        bytes: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        self.inner.scan_reverse(start, begin, limit, bytes).await
    }
    async fn apply(&self, sequence: u64, operation: &MutationOperation) -> Result<()> {
        self.inner.apply(sequence, operation).await
    }
    async fn advance_noop(&self, sequence: u64) -> Result<()> {
        self.inner.advance_noop(sequence).await
    }
    async fn checkpoint(&self, offset: u64) -> Result<(u64, u64)> {
        self.inner.checkpoint(offset).await
    }
    async fn checkpoint_snapshot(&self, offset: u64) -> Result<(u64, u64, Arc<dyn PartitionTree>)> {
        self.stage.store(1, Ordering::Release);
        let snapshot = self.inner.checkpoint_snapshot(offset).await?;
        self.stage.store(2, Ordering::Release);
        Ok(snapshot)
    }
    async fn rebuild_range(
        &self,
        tree: u64,
        range: &PartitionRange,
        config: crowdb_tree_ffi::Config,
    ) -> Result<(Arc<dyn PartitionTree>, crowdb_tree_ffi::RangeRebuildStats)> {
        self.inner.rebuild_range(tree, range, config).await
    }
    async fn force_advance_split_frontier(&self, frontier: u64) -> Result<()> {
        self.inner.force_advance_split_frontier(frontier).await
    }
    async fn begin_split_memtable_view(&self) -> Result<(u64, u64)> {
        self.begins.fetch_add(1, Ordering::AcqRel);
        self.stage.store(3, Ordering::Release);
        let captured = self.inner.begin_split_memtable_view().await?;
        self.stage.store(4, Ordering::Release);
        Ok(captured)
    }
    async fn arm_split_overlay(
        &self,
        destination: &dyn PartitionTree,
        limits: crowdb_chunk_kv::SplitOverlayLimits,
    ) -> Result<(Arc<dyn crowdb_chunk_kv::PreparedSplitOverlay>, u64)> {
        self.preparations.fetch_add(1, Ordering::AcqRel);
        if self.pause_prepare.swap(false, Ordering::AcqRel) {
            self.stage.store(10, Ordering::Release);
            self.prepare_entered.notify_one();
            self.prepare_release.notified().await;
        }
        self.stage.store(11, Ordering::Release);
        let (inner, start) = self.inner.arm_split_overlay(destination, limits).await?;
        self.stage.store(12, Ordering::Release);
        Ok((
            Arc::new(TestPreparedOverlay {
                inner,
                stage: self.stage.clone(),
                installations: self.installations.clone(),
                cancel_on_install: self.cancel_on_install.clone(),
                cleanup: self.cleanup.get().cloned(),
            }),
            start,
        ))
    }

    async fn install_split_memtable_overlay(&self, source: &dyn PartitionTree, frontier: u64) -> Result<()> {
        self.inner.install_split_memtable_overlay(source, frontier).await
    }
    async fn clear_split_memtable_overlay(&self, source: &dyn PartitionTree) -> Result<()> {
        self.inner.clear_split_memtable_overlay(source).await
    }
    async fn publish_split_memtable_view(
        &self,
        generation: u64,
        frontier: u64,
        destination: &dyn PartitionTree,
        range: &PartitionRange,
    ) -> Result<()> {
        self.publications.fetch_add(1, Ordering::AcqRel);
        if let Err(previous) =
            self.generation
                .compare_exchange(0, generation, Ordering::AcqRel, Ordering::Acquire)
        {
            assert_eq!(
                generation, previous,
                "retry must retain the exact native source generation"
            );
        }
        if self.pause_once.swap(false, Ordering::AcqRel) {
            self.stage.store(5, Ordering::Release);
            self.entered.notify_one();
            self.release.notified().await;
        }
        if self.fail_publish.swap(false, Ordering::AcqRel) {
            return Err(ChunkKvError::SplitRetry("injected publication failure".into()));
        }
        self.stage.store(6, Ordering::Release);
        self.inner
            .publish_split_memtable_view(generation, frontier, destination, range)
            .await?;
        self.stage.store(7, Ordering::Release);
        Ok(())
    }
    async fn release_split_memtable_view(&self, generation: u64) -> Result<()> {
        self.releases.fetch_add(1, Ordering::AcqRel);
        if self.fail_release.swap(false, Ordering::AcqRel) {
            return Err(ChunkKvError::SplitRetry("injected source release failure".into()));
        }
        self.stage.store(8, Ordering::Release);
        self.inner.release_split_memtable_view(generation).await?;
        self.stage.store(9, Ordering::Release);
        Ok(())
    }
    fn checkpoint_state(&self) -> Result<(u64, u64)> {
        self.inner.checkpoint_state()
    }
    fn root_manifest_generation(&self) -> Result<u64> {
        self.inner.root_manifest_generation()
    }
    fn last_applied_seq(&self) -> u64 {
        self.inner.last_applied_seq()
    }
    fn pin_generation(&self, transition: TransitionId, generation: u64) -> Result<()> {
        self.inner.pin_generation(transition, generation)
    }
    fn unpin_generation(&self, transition: TransitionId) -> Result<()> {
        self.inner.unpin_generation(transition)
    }
}

struct TestCleanupGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

pub struct TestCleanupPause {
    gate: Arc<TestCleanupGate>,
}

impl TestCleanupPause {
    pub async fn entered(&self) {
        self.gate.entered.notified().await;
    }
}

impl Drop for TestCleanupPause {
    fn drop(&mut self) {
        // Also unblock cleanup if a test assertion unwinds.
        self.gate.release.add_permits(1);
    }
}

struct TestPreparedOverlay {
    inner: Arc<dyn crowdb_chunk_kv::PreparedSplitOverlay>,
    stage: Arc<AtomicU8>,
    installations: Arc<AtomicU64>,
    cancel_on_install: Arc<OnceLock<tokio::task::AbortHandle>>,
    cleanup: Option<Arc<TestCleanupGate>>,
}

impl Drop for TestPreparedOverlay {
    fn drop(&mut self) {
        if let Some(gate) = &self.cleanup {
            gate.entered.notify_one();
            futures::executor::block_on(gate.release.acquire())
                .unwrap()
                .forget();
        }
    }
}

#[async_trait::async_trait]
impl crowdb_chunk_kv::PreparedSplitOverlay for TestPreparedOverlay {
    async fn prepare(&self, replayed_prefix: u64) -> Result<()> {
        self.inner.prepare(replayed_prefix).await
    }

    fn try_install(&self, frontier: u64) -> Result<crowdb_chunk_kv::InstalledSplit> {
        self.installations.fetch_add(1, Ordering::AcqRel);
        self.stage.store(13, Ordering::Release);
        let installed = self.inner.try_install(frontier)?;
        self.stage.store(14, Ordering::Release);
        if let Some(abort) = self.cancel_on_install.get() {
            abort.abort();
        }
        Ok(installed)
    }

    fn committed_frontier(&self) -> Option<u64> {
        self.inner.committed_frontier()
    }
}
