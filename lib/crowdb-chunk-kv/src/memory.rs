// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Test-only in-memory partition tree.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;

use crate::{MutationOperation, PartitionTree, Result, ScanEntry, ValueRevision};

#[derive(Debug, Default)]
pub struct MemoryPartitionTree {
    values: RwLock<BTreeMap<Vec<u8>, ValueRevision>>,
    last_applied: AtomicU64,
    fail_next_apply: AtomicBool,
}

impl MemoryPartitionTree {
    pub fn fail_next_apply(&self) {
        self.fail_next_apply.store(true, Ordering::Release);
    }
}

#[async_trait]
impl PartitionTree for MemoryPartitionTree {
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

    async fn checkpoint(&self) -> Result<u64> {
        Ok(self.last_applied.load(Ordering::Acquire))
    }

    fn last_applied_seq(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }
}
