// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::any::Any;

use async_trait::async_trait;

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
    async fn begin_split_memtable_view(&self) -> Result<u64> {
        Err(ChunkKvError::InvalidRequest(
            "partition tree does not support split memtable views".into(),
        ))
    }
    async fn publish_split_memtable_view(
        &self,
        _generation: u64,
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
    /// Returns chunk-backend counters, or `None` for another backend.
    ///
    /// # Errors
    ///
    /// Returns a storage error when native counters cannot be read.
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
    tree: crowdb_tree_ffi::Crowdbtree,
    config: Option<crowdb_tree_ffi::Config>,
}

impl CrowdbPartitionTree {
    #[must_use]
    pub fn new(tree_id: u64, tree: crowdb_tree_ffi::Crowdbtree) -> Self {
        Self {
            tree_id,
            tree,
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
                tree,
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
        self.tree
            .get(key)
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
        let scan = match (start_key, start_inclusive) {
            // A continuation is always exclusive and uses ct_scan's native
            // lower-bound contract.
            (Some(start_after), false) => self.tree.scan(
                b"",
                start_after,
                end_key.unwrap_or_default(),
                limit,
                byte_budget,
                false,
                0,
                false,
            ),
            (Some(start_key), true) => self.tree.scan_from(
                b"",
                start_key,
                true,
                end_key.unwrap_or_default(),
                limit,
                byte_budget,
                false,
                0,
                false,
            ),
            (None, _) => self.tree.scan(
                b"",
                b"",
                end_key.unwrap_or_default(),
                limit,
                byte_budget,
                false,
                0,
                false,
            ),
        };
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

    async fn seek_reverse(
        &self,
        start_key: &[u8],
        start_inclusive: bool,
        begin_key: Option<&[u8]>,
    ) -> Result<Option<ScanEntry>> {
        self.tree
            .seek_reverse(start_key, start_inclusive, begin_key.unwrap_or_default())
            .map(|entry| {
                entry.map(|entry| ScanEntry {
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
        let (entries, truncated) = self
            .tree
            .scan_reverse(
                start_before,
                false,
                begin_key.unwrap_or_default(),
                limit,
                byte_budget,
            )
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
        let result = match operation.successful_value() {
            Some(value) => self.tree.apply_put(mutation_seq, operation.key(), value),
            None => self.tree.apply_delete(mutation_seq, operation.key()),
        };
        result.map_err(map_tree_apply_error)
    }

    async fn advance_noop(&self, mutation_seq: u64) -> Result<()> {
        self.tree.force_advance_slot(mutation_seq);
        Ok(())
    }

    async fn checkpoint(&self, wal_replay_offset: u64) -> Result<(u64, u64)> {
        if let Some(page_store) = self.config.as_ref().and_then(|config| config.page_store.as_ref()) {
            page_store
                .set_wal_replay_offset(wal_replay_offset)
                .map_err(map_tree_read_error)?;
        }
        self.tree.flush().map_err(|error| match error {
            crowdb_tree_ffi::CtError::Corruption => ChunkKvError::TreeCorruption(error.to_string()),
            _ => ChunkKvError::MaintenanceDegraded(error.to_string()),
        })?;
        let (generation, applied_seq) = self.tree.snapshot_info().map_err(|error| match error {
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
        let config = self.config.as_ref().ok_or_else(|| {
            ChunkKvError::InvalidRequest("native tree was not opened from a retained configuration".into())
        })?;
        let snapshot = Self::open(self.tree_id, config)?;
        let observed = snapshot.tree.snapshot_state().map_err(map_tree_read_error)?;
        if observed != (generation, applied_seq) {
            return Err(ChunkKvError::TreeCorruption(format!(
                "reopened split base is {observed:?}, expected ({generation}, {applied_seq})"
            )));
        }
        Ok((generation, applied_seq, std::sync::Arc::new(snapshot)))
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
        let retained_config = config.clone();
        let (tree, stats) = self.tree.rebuild_range(&config).map_err(map_tree_read_error)?;
        Ok((
            std::sync::Arc::new(Self {
                tree_id,
                tree,
                config: Some(retained_config),
            }),
            stats,
        ))
    }

    async fn begin_split_memtable_view(&self) -> Result<u64> {
        self.tree.begin_split_memtable_view().map_err(map_tree_read_error)
    }

    async fn publish_split_memtable_view(
        &self,
        generation: u64,
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
        self.tree
            .publish_split_memtable_view(generation, &destination.tree, &range)
            .map_err(map_tree_read_error)
    }

    async fn release_split_memtable_view(&self, generation: u64) -> Result<()> {
        self.tree
            .release_split_memtable_view(generation)
            .map_err(map_tree_read_error)
    }

    fn last_applied_seq(&self) -> u64 {
        self.tree.stats().contiguous_slot
    }

    fn checkpoint_state(&self) -> Result<(u64, u64)> {
        self.tree.snapshot_state().map_err(map_tree_read_error)
    }

    fn root_manifest_generation(&self) -> Result<u64> {
        self.config
            .as_ref()
            .and_then(|config| config.page_store.as_ref())
            .filter(|store| store.is_chunk_backed())
            .map_or_else(
                || self.checkpoint_state().map(|checkpoint| checkpoint.0),
                |store| store.chunk_manifest_generation().map_err(map_tree_read_error),
            )
    }

    fn chunk_stats(&self) -> Result<Option<crowdb_tree_ffi::ChunkPageStoreStats>> {
        self.config
            .as_ref()
            .and_then(|config| config.page_store.as_ref())
            .filter(|store| store.is_chunk_backed())
            .map(|store| store.chunk_stats().map_err(map_tree_read_error))
            .transpose()
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

fn map_tree_read_error(error: crowdb_tree_ffi::CtError) -> ChunkKvError {
    match error {
        crowdb_tree_ffi::CtError::IoError | crowdb_tree_ffi::CtError::Unavailable => {
            ChunkKvError::TreeUnavailable(error.to_string())
        }
        crowdb_tree_ffi::CtError::Corruption => ChunkKvError::TreeCorruption(error.to_string()),
        _ => ChunkKvError::Internal(format!("tree read failed: {error}")),
    }
}

fn map_tree_apply_error(_error: crowdb_tree_ffi::CtError) -> ChunkKvError {
    ChunkKvError::ApplyStateUnknown
}
