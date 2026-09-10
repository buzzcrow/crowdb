// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Test-only in-memory partition tree.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::{MutationOperation, PartitionTree, Result, ValueRevision};

#[derive(Debug, Default)]
pub struct MemoryPartitionTree {
    values: RwLock<BTreeMap<Vec<u8>, ValueRevision>>,
    last_applied: AtomicU64,
}

#[async_trait]
impl PartitionTree for MemoryPartitionTree {
    async fn get(&self, key: &[u8]) -> Result<Option<ValueRevision>> {
        Ok(self.values.read().await.get(key).cloned())
    }

    async fn apply(&self, mutation_seq: u64, operation: &MutationOperation) -> Result<()> {
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

    fn last_applied_seq(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }
}
