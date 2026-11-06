// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::any::Any;

mod snapshot;
mod split_overlay;
pub use split_overlay::{InstalledSplit, PreparedSplitOverlay, SplitOverlayLimits};

use async_trait::async_trait;
use crowdb_tree_ffi::{AsyncCrowdbtree, GetOutcome, ScanDirection, ScanOptions};

use crate::{ChunkKvError, MutationOperation, Result, ScanEntry, ValueRevision};

#[async_trait]
pub trait PartitionTree: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn tree_id(&self) -> u64;
    async fn get(&self, key: &[u8]) -> Result<Option<ValueRevision>>;
    async fn scan_forward(
        &self,
        start_key: Option<&[u8]>,
        start_inclusive: bool,
        end_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool)>;
    async fn scan_forward_bounded(
        &self,
        _start: Option<&[u8]>,
        _inclusive: bool,
        _end: Option<&[u8]>,
        _limit: usize,
        _bytes: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        Err(ChunkKvError::InvalidRequest(
            "strict scans unavailable for this tree".into(),
        ))
    }
    async fn seek_reverse(
        &self,
        start_key: &[u8],
        start_inclusive: bool,
        begin_key: Option<&[u8]>,
    ) -> Result<Option<ScanEntry>>;
    async fn scan_reverse(
        &self,
        start_before: Option<&[u8]>,
        begin_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool)>;
    async fn apply(&self, mutation_seq: u64, operation: &MutationOperation) -> Result<()>;
    async fn advance_noop(&self, mutation_seq: u64) -> Result<()>;
    /// Drains replay's transient buffers without publishing a checkpoint or trimming WAL.
    /// # Errors
    /// Returns an in-memory tree drain or storage-read failure.
    async fn drain_replay(&self) -> Result<()> {
        Ok(())
    }
    async fn checkpoint(&self, wal_replay_offset: u64) -> Result<(u64, u64)>;
    async fn checkpoint_snapshot(
        &self,
        wal_replay_offset: u64,
    ) -> Result<(u64, u64, std::sync::Arc<dyn PartitionTree>)>;
    async fn rebuild_range(
        &self,
        tree_id: u64,
        range: &crate::PartitionRange,
        config: crowdb_tree_ffi::Config,
    ) -> Result<(
        std::sync::Arc<dyn PartitionTree>,
        crowdb_tree_ffi::RangeRebuildStats,
    )>;
    async fn force_advance_split_frontier(&self, _journal_frontier: u64) -> Result<()> {
        Err(ChunkKvError::InvalidRequest(
            "partition tree does not support split frontier advancement".into(),
        ))
    }
    async fn begin_split_memtable_view(&self) -> Result<(u64, u64)> {
        Err(ChunkKvError::InvalidRequest(
            "partition tree does not support split memtable views".into(),
        ))
    }
    /// Prepare a private replayed destination while the parent keeps serving.
    ///
    /// # Errors
    /// Rejects incompatible trees, insufficient capacity, or an expired prefix.
    async fn arm_split_overlay(
        &self,
        _destination: &dyn PartitionTree,
        _limits: SplitOverlayLimits,
    ) -> Result<(std::sync::Arc<dyn PreparedSplitOverlay>, u64)> {
        Err(ChunkKvError::InvalidRequest(
            "partition tree does not support prepared split overlays".into(),
        ))
    }
    async fn install_split_memtable_overlay(
        &self,
        _source: &dyn PartitionTree,
        _journal_frontier: u64,
    ) -> Result<()> {
        Err(ChunkKvError::InvalidRequest(
            "partition tree does not support split memtable overlays".into(),
        ))
    }
    async fn clear_split_memtable_overlay(&self, _source: &dyn PartitionTree) -> Result<()> {
        Err(ChunkKvError::InvalidRequest(
            "partition tree does not support split memtable overlays".into(),
        ))
    }
    async fn publish_split_memtable_view(
        &self,
        _generation: u64,
        _journal_frontier: u64,
        _destination: &dyn PartitionTree,
        _range: &crate::PartitionRange,
    ) -> Result<()> {
        Err(ChunkKvError::InvalidRequest(
            "partition tree does not support split memtable views".into(),
        ))
    }
    async fn release_split_memtable_view(&self, _generation: u64) -> Result<()> {
        Err(ChunkKvError::InvalidRequest(
            "partition tree does not support split memtable views".into(),
        ))
    }
    /// Returns the currently opened durable `(manifest, applied sequence)`.
    ///
    /// # Errors
    ///
    /// Returns a typed tree read error when durable snapshot state is
    /// unavailable or corrupt.
    fn checkpoint_state(&self) -> Result<(u64, u64)>;
    /// Returns the chunk root-catalog generation backing the opened tree. A
    /// non-chunk implementation uses its tree snapshot sequence.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the durable root identity is unavailable.
    fn root_manifest_generation(&self) -> Result<u64> {
        self.checkpoint_state().map(|checkpoint| checkpoint.0)
    }
    fn last_applied_seq(&self) -> u64;
    /// Independently sampled native counters, without scanning keys or pages.
    fn runtime_stats(&self) -> Option<crowdb_tree_ffi::Stats> {
        None
    }
    /// Samples admission resources without page traversal or backend I/O.
    /// Non-runtime trees have no admission resource ledger.
    /// # Errors
    /// Returns a Tree diagnostic error when its resource ledger is unavailable.
    fn resource_usage(&self) -> Result<Option<crowdb_tree_ffi::runtime::ResourceUsageSnapshot>> {
        Ok(None)
    }
    fn tree_summary(&self) -> Option<crowdb_tree_ffi::TreeSummary> {
        None
    }
    /// Copies one structural page without flushing or folding pending writes.
    ///
    /// # Errors
    /// Returns a changed observation, invalid path, or storage read failure.
    fn inspect_page(
        &self,
        _path: &[u32],
        _version: Option<u64>,
    ) -> Result<crowdb_tree_ffi::page::PageInspection> {
        Err(ChunkKvError::InvalidRequest(
            "page inspection is unavailable for this tree backend".into(),
        ))
    }
    /// Samples allocated physical bytes, including mirrors, without I/O.
    /// Old and staged output remains counted until physical deletion completes.
    ///
    /// # Errors
    /// Returns a storage error when the cached estimate cannot be read.
    fn estimated_bytes(&self) -> Result<u64> {
        Ok(0)
    }
    /// Returns an advisory structural separator without I/O or a full scan.
    ///
    /// # Errors
    /// Returns a tree error when the structural hint cannot be read.
    fn approximate_split_key(&self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    /// Samples the physical backing ledger, or `None` for another backend.
    ///
    /// # Errors
    ///
    /// Returns a storage error when native counters cannot be read.
    fn backing_stats(&self) -> Result<Option<crowdb_tree_ffi::BackingStatsSnapshot>> {
        Ok(None)
    }

    /// Returns legacy mutable page-store counters, or `None` for another backend.
    /// Runtime implementations reject this obsolete schema.
    ///
    /// # Errors
    /// Returns a typed storage error when the schema is unsupported.
    fn chunk_stats(&self) -> Result<Option<crowdb_tree_ffi::ChunkPageStoreStats>> {
        Ok(None)
    }
    /// Reclaims generations older than a published retention watermark.
    ///
    /// # Errors
    ///
    /// Returns a storage maintenance error.
    fn reclaim_before(&self, _generation: u64) -> Result<u64> {
        Ok(0)
    }
    /// Persists a transition-scoped reference to one exact root generation.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the generation is absent or conflicts
    /// with an existing transition pin.
    fn pin_generation(&self, _transition: crate::TransitionId, _generation: u64) -> Result<()> {
        Ok(())
    }
    /// Removes a transition-scoped root-generation reference idempotently.
    ///
    /// # Errors
    ///
    /// Returns a storage error when durable pin deletion cannot complete.
    fn unpin_generation(&self, _transition: crate::TransitionId) -> Result<()> {
        Ok(())
    }
    /// Reclaims objects abandoned before manifest publication.
    ///
    /// # Errors
    ///
    /// Returns a storage maintenance error.
    fn reclaim_orphans(&self) -> Result<u64> {
        Ok(0)
    }
    /// Continue one finite physical tree-storage collection pass, without
    /// advancing root or WAL retention. Non-staged backends have no work here.
    ///
    /// # Errors
    /// Returns a catalog authority or backend maintenance failure.
    async fn collect_storage(&self) -> Result<bool> {
        Ok(false)
    }

    /// Materializes one bounded pass of storage inherited from another tree.
    ///
    /// The returned tuple is `(bytes_written, complete)`. Backends without
    /// shared physical ownership are already complete.
    ///
    /// # Errors
    ///
    /// Returns a storage maintenance or corruption error.
    fn materialize_ownership(&self) -> Result<(u64, bool)> {
        Ok((0, true))
    }
}

pub struct CrowdbPartitionTree {
    tree_id: u64,
    tree: std::sync::Arc<crowdb_tree_ffi::Crowdbtree>,
    config: Option<crowdb_tree_ffi::Config>,
}

impl CrowdbPartitionTree {
    #[must_use]
    pub fn new(tree_id: u64, tree: crowdb_tree_ffi::Crowdbtree) -> Self {
        Self {
            tree_id,
            tree: std::sync::Arc::new(tree),
            config: None,
        }
    }

    /// Opens a native tree with one explicit durable identity.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration, storage availability, or corruption
    /// error.
    pub fn open(tree_id: u64, config: &crowdb_tree_ffi::Config) -> Result<Self> {
        if tree_id == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "tree identity must be nonzero".into(),
            ));
        }
        let retained_config = config.clone();
        crowdb_tree_ffi::Crowdbtree::open(config)
            .map(|tree| Self {
                tree_id,
                tree: std::sync::Arc::new(tree),
                config: Some(retained_config),
            })
            .map_err(map_tree_read_error)
    }
}

#[async_trait]
impl PartitionTree for CrowdbPartitionTree {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn tree_id(&self) -> u64 {
        self.tree_id
    }

    async fn get(&self, key: &[u8]) -> Result<Option<ValueRevision>> {
        let reader = AsyncCrowdbtree::from_shared(self.tree.clone());
        let result = match reader.try_get(key) {
            GetOutcome::Ready(result) => result,
            GetOutcome::Pending(pending) => pending.await,
        };
        result
            .map(|value| value.map(|(revision, value)| ValueRevision { revision, value }))
            .map_err(map_tree_read_error)
    }

    async fn scan_forward(
        &self,
        start_key: Option<&[u8]>,
        start_inclusive: bool,
        end_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        if end_key.is_some_and(<[u8]>::is_empty) {
            return Ok((Vec::new(), false));
        }
        let scan = AsyncCrowdbtree::from_shared(self.tree.clone())
            .scan_with_options(ScanOptions {
                start: start_key.map(<[u8]>::to_vec),
                start_inclusive,
                end: end_key.unwrap_or_default().to_vec(),
                limit,
                byte_budget,
                ..Default::default()
            })
            .await;
        let (entries, truncated) = scan.map_err(map_tree_read_error)?;
        Ok((
            entries
                .into_iter()
                .map(|entry| ScanEntry {
                    key: entry.key,
                    value: ValueRevision {
                        revision: entry.slot,
                        value: entry.value.to_vec(),
                    },
                })
                .collect(),
            truncated,
        ))
    }

    async fn scan_forward_bounded(
        &self,
        start: Option<&[u8]>,
        inclusive: bool,
        end: Option<&[u8]>,
        limit: usize,
        bytes: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        if end.is_some_and(<[u8]>::is_empty) {
            return Ok((Vec::new(), false));
        }
        let (entries, truncated) = AsyncCrowdbtree::from_shared(self.tree.clone())
            .scan_with_options(ScanOptions {
                start: start.map(<[u8]>::to_vec),
                start_inclusive: inclusive,
                end: end.unwrap_or_default().to_vec(),
                limit,
                byte_budget: bytes,
                strict_byte_budget: true,
                ..Default::default()
            })
            .await
            .map_err(map_tree_read_error)?;
        Ok((
            entries
                .into_iter()
                .map(|entry| ScanEntry {
                    key: entry.key,
                    value: ValueRevision {
                        revision: entry.slot,
                        value: entry.value.to_vec(),
                    },
                })
                .collect(),
            truncated,
        ))
    }

    async fn seek_reverse(
        &self,
        start_key: &[u8],
        start_inclusive: bool,
        begin_key: Option<&[u8]>,
    ) -> Result<Option<ScanEntry>> {
        AsyncCrowdbtree::from_shared(self.tree.clone())
            .scan_with_options(ScanOptions {
                start: Some(start_key.to_vec()),
                start_inclusive,
                begin: begin_key.unwrap_or_default().to_vec(),
                direction: ScanDirection::Reverse,
                limit: 1,
                ..Default::default()
            })
            .await
            .map(|(entries, _)| {
                entries.into_iter().next().map(|entry| ScanEntry {
                    key: entry.key,
                    value: ValueRevision {
                        revision: entry.slot,
                        value: entry.value.to_vec(),
                    },
                })
            })
            .map_err(map_tree_read_error)
    }

    async fn scan_reverse(
        &self,
        start_before: Option<&[u8]>,
        begin_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        let (entries, truncated) = AsyncCrowdbtree::from_shared(self.tree.clone())
            .scan_with_options(ScanOptions {
                start: start_before.map(<[u8]>::to_vec),
                begin: begin_key.unwrap_or_default().to_vec(),
                direction: ScanDirection::Reverse,
                limit,
                byte_budget,
                ..Default::default()
            })
            .await
            .map_err(map_tree_read_error)?;
        Ok((
            entries
                .into_iter()
                .map(|entry| ScanEntry {
                    key: entry.key,
                    value: ValueRevision {
                        revision: entry.slot,
                        value: entry.value.to_vec(),
                    },
                })
                .collect(),
            truncated,
        ))
    }

    async fn apply(&self, mutation_seq: u64, operation: &MutationOperation) -> Result<()> {
        let tree = self.tree.clone();
        let operation = operation.clone();
        let result = tokio::task::spawn_blocking(move || match operation.successful_value() {
            Some(value) => tree.apply_put(mutation_seq, operation.key(), value),
            None => tree.apply_delete(mutation_seq, operation.key()),
        })
        .await
        .map_err(|error| ChunkKvError::Internal(format!("tree apply task failed: {error}")))?;
        result.map_err(map_tree_apply_error)
    }

    async fn advance_noop(&self, mutation_seq: u64) -> Result<()> {
        self.tree.force_advance_slot(mutation_seq);
        Ok(())
    }

    async fn drain_replay(&self) -> Result<()> {
        let tree = std::sync::Arc::clone(&self.tree);
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let writer = crowdb_tree_ffi::AsyncCrowdbtree::from_shared(tree);
            runtime.block_on(writer.flush())
        })
        .await
        .map_err(|error| ChunkKvError::TreeUnavailable(error.to_string()))?
        .map_err(map_tree_apply_error)
    }

    async fn checkpoint(&self, wal_replay_offset: u64) -> Result<(u64, u64)> {
        if let Some(page_store) = self.config.as_ref().and_then(|config| config.page_store.as_ref()) {
            page_store
                .set_wal_replay_offset(wal_replay_offset)
                .map_err(map_tree_read_error)?;
        }
        let tree = std::sync::Arc::clone(&self.tree);
        let runtime = tokio::runtime::Handle::current();
        let (generation, applied_seq) = tokio::task::spawn_blocking(move || {
            runtime.block_on(async {
                let writer = crowdb_tree_ffi::AsyncCrowdbtree::from_shared(tree.clone());
                writer.flush().await?;
                writer.snapshot().await?;
                tree.snapshot_state()
            })
        })
        .await
        .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
        .map_err(|error| match error {
            crowdb_tree_ffi::CtError::Corruption => ChunkKvError::TreeCorruption(error.to_string()),
            _ => ChunkKvError::MaintenanceDegraded(error.to_string()),
        })?;
        if applied_seq > self.tree.stats().contiguous_slot {
            return Err(ChunkKvError::ApplyStateUnknown);
        }
        Ok((generation, applied_seq))
    }

    async fn checkpoint_snapshot(
        &self,
        wal_replay_offset: u64,
    ) -> Result<(u64, u64, std::sync::Arc<dyn PartitionTree>)> {
        let (generation, applied_seq) = self.checkpoint(wal_replay_offset).await?;
        let tree = self.tree.clone();
        let snapshot = tokio::task::spawn_blocking(move || tree.immutable_snapshot(generation, applied_seq))
            .await
            .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
            .map_err(map_tree_read_error)?;
        Ok((
            generation,
            applied_seq,
            std::sync::Arc::new(snapshot::CapturedPartitionTree::new(
                self.tree_id,
                self.root_manifest_generation()?,
                snapshot,
            )),
        ))
    }

    async fn rebuild_range(
        &self,
        tree_id: u64,
        range: &crate::PartitionRange,
        mut config: crowdb_tree_ffi::Config,
    ) -> Result<(
        std::sync::Arc<dyn PartitionTree>,
        crowdb_tree_ffi::RangeRebuildStats,
    )> {
        if tree_id == 0 || config.page_store.is_none() {
            return Err(ChunkKvError::InvalidRequest(
                "range rebuild requires a child tree identity and page store".into(),
            ));
        }
        config.key_range = crowdb_tree_ffi::KeyRange::Bounded {
            start: range.start.clone(),
            end: range.end.clone(),
        };
        let source = std::sync::Arc::clone(&self.tree);
        let (tree, stats, config) = tokio::task::spawn_blocking(move || {
            source
                .rebuild_range(&config)
                .map(|(tree, stats)| (tree, stats, config))
        })
        .await
        .map_err(|error| ChunkKvError::TreeUnavailable(error.to_string()))?
        .map_err(map_tree_read_error)?;
        Ok((
            std::sync::Arc::new(Self {
                tree_id,
                tree: std::sync::Arc::new(tree),
                config: Some(config),
            }),
            stats,
        ))
    }

    async fn force_advance_split_frontier(&self, journal_frontier: u64) -> Result<()> {
        self.tree.force_advance_slot(journal_frontier);
        Ok(())
    }

    async fn begin_split_memtable_view(&self) -> Result<(u64, u64)> {
        let tree = std::sync::Arc::clone(&self.tree);
        tokio::task::spawn_blocking(move || tree.begin_split_memtable_view())
            .await
            .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
            .map_err(map_tree_read_error)
    }

    async fn arm_split_overlay(
        &self,
        destination: &dyn PartitionTree,
        limits: SplitOverlayLimits,
    ) -> Result<(std::sync::Arc<dyn PreparedSplitOverlay>, u64)> {
        let destination = destination.as_any().downcast_ref::<Self>().ok_or_else(|| {
            ChunkKvError::InvalidRequest("split preparation destination is not a native tree".into())
        })?;
        let source = self.tree.clone();
        let destination = destination.tree.clone();
        let limits = crowdb_tree_ffi::SplitOverlayLimits {
            max_records: u64::try_from(limits.max_records).map_err(|_| {
                ChunkKvError::InvalidRequest("split request capacity exceeds native limits".into())
            })?,
            max_bytes: limits.max_bytes,
        };
        let prepared = tokio::task::spawn_blocking(move || source.arm_split_overlay(destination, limits))
            .await
            .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
            .map_err(map_tree_read_error)?;
        let start = prepared.start_frontier();
        Ok((
            std::sync::Arc::new(split_overlay::NativePreparedOverlay(std::sync::Arc::new(
                prepared,
            ))),
            start,
        ))
    }

    async fn install_split_memtable_overlay(
        &self,
        source: &dyn PartitionTree,
        journal_frontier: u64,
    ) -> Result<()> {
        let source = source.as_any().downcast_ref::<Self>().ok_or_else(|| {
            ChunkKvError::InvalidRequest("split overlay source is not a native tree".into())
        })?;
        let destination = std::sync::Arc::clone(&self.tree);
        let source = std::sync::Arc::clone(&source.tree);
        tokio::task::spawn_blocking(move || {
            destination.install_split_memtable_overlay(&source, journal_frontier)
        })
        .await
        .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
        .map_err(map_tree_read_error)
    }

    async fn clear_split_memtable_overlay(&self, source: &dyn PartitionTree) -> Result<()> {
        let source = source.as_any().downcast_ref::<Self>().ok_or_else(|| {
            ChunkKvError::InvalidRequest("split overlay source is not a native tree".into())
        })?;
        self.tree
            .clear_split_memtable_overlay(&source.tree)
            .map_err(map_tree_read_error)
    }

    async fn publish_split_memtable_view(
        &self,
        generation: u64,
        journal_frontier: u64,
        destination: &dyn PartitionTree,
        range: &crate::PartitionRange,
    ) -> Result<()> {
        let destination = destination
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| ChunkKvError::InvalidRequest("split destination is not a native tree".into()))?;
        let range = crowdb_tree_ffi::KeyRange::Bounded {
            start: range.start.clone(),
            end: range.end.clone(),
        };
        let source = std::sync::Arc::clone(&self.tree);
        let destination = std::sync::Arc::clone(&destination.tree);
        tokio::task::spawn_blocking(move || {
            source.publish_split_memtable_view(generation, journal_frontier, &destination, &range)
        })
        .await
        .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
        .map_err(map_tree_read_error)
    }

    async fn release_split_memtable_view(&self, generation: u64) -> Result<()> {
        self.tree
            .release_split_memtable_view(generation)
            .map_err(map_tree_read_error)
    }

    async fn collect_storage(&self) -> Result<bool> {
        let tree = crowdb_tree_ffi::AsyncCrowdbtree::from_handle(std::sync::Arc::clone(&self.tree));
        let report = match tree.collect_chunk_storage().await {
            Ok(report) => report,
            // Non-Chunk backends have no Chunk storage to collect.
            Err(crowdb_tree_ffi::CtError::NotSupported) => return Ok(false),
            Err(error) => return Err(map_tree_read_error(error)),
        };
        report.status.map_err(map_tree_read_error)?;
        Ok(report.more)
    }

    fn last_applied_seq(&self) -> u64 {
        self.tree.stats().contiguous_slot
    }

    fn runtime_stats(&self) -> Option<crowdb_tree_ffi::Stats> {
        Some(self.tree.stats())
    }
    fn resource_usage(&self) -> Result<Option<crowdb_tree_ffi::runtime::ResourceUsageSnapshot>> {
        self.tree.resource_usage().map(Some).map_err(map_tree_read_error)
    }

    fn tree_summary(&self) -> Option<crowdb_tree_ffi::TreeSummary> {
        Some(self.tree.tree_summary())
    }

    fn inspect_page(
        &self,
        path: &[u32],
        version: Option<u64>,
    ) -> Result<crowdb_tree_ffi::page::PageInspection> {
        self.tree
            .inspect_page(path, version)
            .map_err(|error| match error {
                crowdb_tree_ffi::CtError::Unavailable => ChunkKvError::RequestConflict,
                crowdb_tree_ffi::CtError::InvalidArgument => {
                    ChunkKvError::InvalidRequest("invalid tree page path".into())
                }
                crowdb_tree_ffi::CtError::ResourceExhausted => ChunkKvError::Overloaded,
                other => map_tree_read_error(other),
            })
    }

    fn checkpoint_state(&self) -> Result<(u64, u64)> {
        match self.tree.snapshot_state() {
            Err(error @ crowdb_tree_ffi::CtError::NotFound) => {
                if let Some(store) = self
                    .config
                    .as_ref()
                    .and_then(|config| config.page_store.as_ref())
                    .filter(|store| store.is_chunk_backed())
                {
                    if chunk_root_generation(store)? == 0 {
                        return Ok((0, 0));
                    }
                }
                Err(map_tree_read_error(error))
            }
            result => result.map_err(map_tree_read_error),
        }
    }

    fn root_manifest_generation(&self) -> Result<u64> {
        self.config
            .as_ref()
            .and_then(|config| config.page_store.as_ref())
            .filter(|store| store.is_chunk_backed())
            .map_or_else(
                || self.checkpoint_state().map(|checkpoint| checkpoint.0),
                |store| chunk_root_generation(store),
            )
    }

    fn chunk_stats(&self) -> Result<Option<crowdb_tree_ffi::ChunkPageStoreStats>> {
        if self
            .config
            .as_ref()
            .and_then(|config| config.page_store.as_ref())
            .is_some_and(|store| store.is_chunk_backed())
        {
            return Err(map_tree_read_error(crowdb_tree_ffi::CtError::NotSupported));
        }
        Ok(None)
    }

    fn backing_stats(&self) -> Result<Option<crowdb_tree_ffi::BackingStatsSnapshot>> {
        self.config
            .as_ref()
            .and_then(|config| config.page_store.as_ref())
            .filter(|store| store.is_chunk_backed())
            .map(|_| self.tree.backing_stats().map_err(map_tree_read_error))
            .transpose()
    }

    fn estimated_bytes(&self) -> Result<u64> {
        self.backing_stats()
            .map(|snapshot| snapshot.map_or(0, |stats| stats.allocated_bytes))
    }

    fn approximate_split_key(&self) -> Result<Option<Vec<u8>>> {
        self.tree.approximate_split_key().map_err(map_tree_read_error)
    }

    fn reclaim_before(&self, generation: u64) -> Result<u64> {
        Ok(self
            .config
            .as_ref()
            .and_then(|config| config.page_store.as_ref())
            .map_or(0, |store| {
                store.reclaim_chunk_generations_before(self.tree_id, generation)
            }))
    }

    fn pin_generation(&self, transition: crate::TransitionId, generation: u64) -> Result<()> {
        self.config
            .as_ref()
            .and_then(|config| config.page_store.as_ref())
            .map_or(Ok(()), |store| {
                store
                    .pin_chunk_generation(self.tree_id, transition.high, transition.low, generation)
                    .map_err(map_tree_read_error)
            })
    }

    fn unpin_generation(&self, transition: crate::TransitionId) -> Result<()> {
        self.config
            .as_ref()
            .and_then(|config| config.page_store.as_ref())
            .map_or(Ok(()), |store| {
                store
                    .unpin_chunk_generation(self.tree_id, transition.high, transition.low)
                    .map_err(map_tree_read_error)
            })
    }

    fn reclaim_orphans(&self) -> Result<u64> {
        Ok(self
            .config
            .as_ref()
            .and_then(|config| config.page_store.as_ref())
            .map_or(0, |store| store.reclaim_chunk_orphans()))
    }

    fn materialize_ownership(&self) -> Result<(u64, bool)> {
        self.tree.materialize_ownership().map_err(|error| match error {
            crowdb_tree_ffi::CtError::Corruption => ChunkKvError::TreeCorruption(error.to_string()),
            _ => ChunkKvError::MaintenanceDegraded(error.to_string()),
        })
    }
}

pub(super) fn chunk_root_generation(store: &crowdb_tree_ffi::PageStore) -> Result<u64> {
    // A missing current manifest is an empty catalog, not an existing checkpoint.
    // Durable catalog stores reject a missing manifest at a nonzero authority generation.
    match store.chunk_manifest_generation() {
        Err(crowdb_tree_ffi::CtError::NotFound) => Ok(0),
        result => result.map_err(map_tree_read_error),
    }
}

fn map_tree_read_error(error: crowdb_tree_ffi::CtError) -> ChunkKvError {
    match error {
        crowdb_tree_ffi::CtError::IoError | crowdb_tree_ffi::CtError::Unavailable => {
            ChunkKvError::TreeUnavailable(error.to_string())
        }
        crowdb_tree_ffi::CtError::Corruption => ChunkKvError::TreeCorruption(error.to_string()),
        _ => ChunkKvError::Internal(format!("tree read failed: {error}")),
    }
}

fn map_tree_apply_error(error: crowdb_tree_ffi::CtError) -> ChunkKvError {
    ChunkKvError::Internal(format!("tree apply failed: {error}"))
}
