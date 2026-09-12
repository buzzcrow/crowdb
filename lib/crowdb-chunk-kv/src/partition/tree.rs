// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;

use crate::{ChunkKvError, MutationOperation, Result, ScanEntry, ValueRevision};

#[async_trait]
pub trait PartitionTree: Send + Sync {
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
    async fn checkpoint(&self) -> Result<u64>;
    fn last_applied_seq(&self) -> u64;
}

pub struct CrowdbPartitionTree {
    tree_id: u64,
    tree: crowdb_tree_ffi::Crowdbtree,
}

impl CrowdbPartitionTree {
    #[must_use]
    pub fn new(tree_id: u64, tree: crowdb_tree_ffi::Crowdbtree) -> Self {
        Self { tree_id, tree }
    }
}

#[async_trait]
impl PartitionTree for CrowdbPartitionTree {
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
        let (entries, truncated) = self
            .tree
            .scan_from(
                b"",
                start_key.unwrap_or_default(),
                start_key.is_some() && start_inclusive,
                end_key.unwrap_or_default(),
                limit,
                byte_budget,
                false,
                0,
                false,
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

    async fn checkpoint(&self) -> Result<u64> {
        self.tree.snapshot().map_err(|error| match error {
            crowdb_tree_ffi::CtError::Corruption => ChunkKvError::TreeCorruption(error.to_string()),
            _ => ChunkKvError::MaintenanceDegraded(error.to_string()),
        })
    }

    fn last_applied_seq(&self) -> u64 {
        self.tree.last_applied_slot()
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
