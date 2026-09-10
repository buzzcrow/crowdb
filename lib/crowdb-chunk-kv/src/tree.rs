// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;

use crate::{ChunkKvError, MutationOperation, Result, ValueRevision};

#[async_trait]
pub trait PartitionTree: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<ValueRevision>>;
    async fn apply(&self, mutation_seq: u64, operation: &MutationOperation) -> Result<()>;
    async fn advance_noop(&self, mutation_seq: u64) -> Result<()>;
    fn last_applied_seq(&self) -> u64;
}

pub struct CrowdbPartitionTree {
    tree: crowdb_tree_ffi::Crowdbtree,
}

impl CrowdbPartitionTree {
    #[must_use]
    pub fn new(tree: crowdb_tree_ffi::Crowdbtree) -> Self {
        Self { tree }
    }
}

#[async_trait]
impl PartitionTree for CrowdbPartitionTree {
    async fn get(&self, key: &[u8]) -> Result<Option<ValueRevision>> {
        self.tree
            .get(key)
            .map(|value| value.map(|(revision, value)| ValueRevision { revision, value }))
            .map_err(map_tree_read_error)
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
