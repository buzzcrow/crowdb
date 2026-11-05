// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! The split base owns a fixed native mapping, never another backend writer.
use super::{map_tree_read_error, CrowdbPartitionTree, PartitionTree};
use crate::{ChunkKvError, MutationOperation, PartitionRange, Result, ScanEntry, ValueRevision};
use async_trait::async_trait;
use crowdb_tree_ffi::{Config, ImmutableTreeSnapshot, RangeRebuildStats, ScanDirection, ScanOptions};
use std::{any::Any, sync::Arc};

pub(super) struct CapturedPartitionTree {
    tree_id: u64,
    manifest: u64,
    snapshot: Arc<ImmutableTreeSnapshot>,
}

impl CapturedPartitionTree {
    pub(super) fn new(tree_id: u64, manifest: u64, snapshot: ImmutableTreeSnapshot) -> Self {
        Self {
            tree_id,
            manifest,
            snapshot: Arc::new(snapshot),
        }
    }

    async fn scan(&self, options: ScanOptions) -> Result<(Vec<ScanEntry>, bool)> {
        let snapshot = self.snapshot.clone();
        let (entries, truncated) = tokio::task::spawn_blocking(move || snapshot.scan(&options))
            .await
            .map_err(|error| ChunkKvError::TreeUnavailable(error.to_string()))?
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
}

fn readonly() -> ChunkKvError {
    ChunkKvError::InvalidRequest("immutable partition snapshot is read-only".into())
}

#[async_trait]
impl PartitionTree for CapturedPartitionTree {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn tree_id(&self) -> u64 {
        self.tree_id
    }
    async fn get(&self, key: &[u8]) -> Result<Option<ValueRevision>> {
        let snapshot = self.snapshot.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || snapshot.get(&key))
            .await
            .map_err(|error| ChunkKvError::TreeUnavailable(error.to_string()))?
            .map(|entry| entry.map(|(revision, value)| ValueRevision { revision, value }))
            .map_err(map_tree_read_error)
    }
    async fn scan_forward(
        &self,
        start: Option<&[u8]>,
        inclusive: bool,
        end: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        if end.is_some_and(<[u8]>::is_empty) {
            return Ok((Vec::new(), false));
        }
        self.scan(ScanOptions {
            start: start.map(<[u8]>::to_vec),
            start_inclusive: inclusive,
            end: end.unwrap_or_default().to_vec(),
            limit,
            byte_budget,
            ..ScanOptions::default()
        })
        .await
    }
    async fn scan_forward_bounded(
        &self,
        start: Option<&[u8]>,
        inclusive: bool,
        end: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        if end.is_some_and(<[u8]>::is_empty) {
            return Ok((Vec::new(), false));
        }
        self.scan(ScanOptions {
            start: start.map(<[u8]>::to_vec),
            start_inclusive: inclusive,
            end: end.unwrap_or_default().to_vec(),
            limit,
            byte_budget,
            strict_byte_budget: true,
            ..ScanOptions::default()
        })
        .await
    }
    async fn seek_reverse(
        &self,
        start: &[u8],
        inclusive: bool,
        begin: Option<&[u8]>,
    ) -> Result<Option<ScanEntry>> {
        let (entries, _) = self
            .scan(ScanOptions {
                start: Some(start.to_vec()),
                start_inclusive: inclusive,
                begin: begin.unwrap_or_default().to_vec(),
                direction: ScanDirection::Reverse,
                limit: 1,
                ..ScanOptions::default()
            })
            .await?;
        Ok(entries.into_iter().next())
    }
    async fn scan_reverse(
        &self,
        start: Option<&[u8]>,
        begin: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
    ) -> Result<(Vec<ScanEntry>, bool)> {
        self.scan(ScanOptions {
            start: start.map(<[u8]>::to_vec),
            start_inclusive: false,
            begin: begin.unwrap_or_default().to_vec(),
            direction: ScanDirection::Reverse,
            limit,
            byte_budget,
            ..ScanOptions::default()
        })
        .await
    }
    async fn apply(&self, _sequence: u64, _operation: &MutationOperation) -> Result<()> {
        Err(readonly())
    }
    async fn advance_noop(&self, _sequence: u64) -> Result<()> {
        Err(readonly())
    }
    async fn checkpoint(&self, _wal: u64) -> Result<(u64, u64)> {
        Err(readonly())
    }
    async fn checkpoint_snapshot(&self, _wal: u64) -> Result<(u64, u64, Arc<dyn PartitionTree>)> {
        Err(readonly())
    }

    async fn rebuild_range(
        &self,
        tree_id: u64,
        range: &PartitionRange,
        mut config: Config,
    ) -> Result<(Arc<dyn PartitionTree>, RangeRebuildStats)> {
        if tree_id == 0 || config.page_store.is_none() {
            return Err(ChunkKvError::InvalidRequest(
                "snapshot rebuild needs a child tree identity and page store".into(),
            ));
        }
        config.key_range = crowdb_tree_ffi::KeyRange::Bounded {
            start: range.start.clone(),
            end: range.end.clone(),
        };
        let retained_config = config.clone();
        let snapshot = self.snapshot.clone();
        let (tree, stats) = tokio::task::spawn_blocking(move || snapshot.rebuild_range(&config))
            .await
            .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
            .map_err(map_tree_read_error)?;
        Ok((
            Arc::new(CrowdbPartitionTree {
                tree_id,
                tree: Arc::new(tree),
                config: Some(retained_config),
            }),
            stats,
        ))
    }
    fn checkpoint_state(&self) -> Result<(u64, u64)> {
        Ok(self.snapshot.checkpoint_state())
    }
    fn root_manifest_generation(&self) -> Result<u64> {
        Ok(self.manifest)
    }
    fn last_applied_seq(&self) -> u64 {
        self.snapshot.checkpoint_state().1
    }
}
