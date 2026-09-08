// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV persistence for canonical task values and runnable/lease indexes.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use crowdb_kv_client::{BatchOp, CrowdbKvClient, GetOutcome, ReadMode, ScanOutcome};
use crowdb_protocol::chunk_task::{ChunkTaskState, ChunkTaskValue};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::{
    decode_chunk_task_value, encode_chunk_task_value, BinaryKey, ChunkTaskKey, ChunkTaskValueError,
    LeasedChunkTaskKey, ReadyChunkTaskKey,
};
use tracing::warn;

use crate::routing::{route, BindingCache, MigrationState, Route};

#[derive(Debug, thiserror::Error)]
pub enum TaskStoreError {
    #[error("task routing error: {0}")]
    Route(#[from] crate::routing::RouteError),
    #[error("task KV operation failed: {0}")]
    Kv(String),
    #[error("task value is invalid: {0}")]
    Value(#[from] ChunkTaskValueError),
    #[error("task transition changes immutable identity")]
    IdentityChanged,
    #[error("task value does not match its canonical key")]
    KeyMismatch,
}

/// Persistent task storage. It is lock-free and relies on the caller's
/// existing resource lifecycle guard for same-process transition ordering.
pub struct TaskStore {
    kv: Arc<CrowdbKvClient>,
    bindings: BindingCache,
}

impl TaskStore {
    #[must_use]
    pub fn new(kv: Arc<CrowdbKvClient>, bindings: BindingCache) -> Self {
        Self { kv, bindings }
    }

    /// Persist a canonical task and update its secondary index atomically.
    ///
    /// # Errors
    /// Returns an error for identity changes, routing failure, or failed KV
    /// writes. `previous` must be the state held by the caller's lifecycle
    /// guard; cross-process duplicates remain fenced by the task operation ID.
    pub async fn write_transition(
        &self,
        previous: Option<&ChunkTaskValue>,
        next: &ChunkTaskValue,
    ) -> Result<(), TaskStoreError> {
        if previous.is_some_and(|old| !same_identity(old, next)) {
            return Err(TaskStoreError::IdentityChanged);
        }
        let mut ops = Vec::with_capacity(3);
        if let Some(old_index) = previous.and_then(index_key) {
            ops.push(BatchOp::Delete {
                key: Bytes::from(old_index),
            });
        }
        ops.push(BatchOp::Put {
            key: Bytes::from(canonical_key(next)),
            value: Bytes::from(encode_chunk_task_value(next)),
        });
        if let Some(new_index) = index_key(next) {
            ops.push(BatchOp::Put {
                key: Bytes::from(new_index),
                value: Bytes::new(),
            });
        }
        self.write_routed(&next.partition_id, &ops).await
    }

    /// Read one task through the partition's current route.
    ///
    /// # Errors
    /// Returns an error for routing, KV, malformed value, or key/value
    /// identity mismatch.
    pub async fn get(
        &self,
        partition_id: &ChunkId,
        kind: u16,
        task_id: &ChunkId,
    ) -> Result<Option<ChunkTaskValue>, TaskStoreError> {
        let task_key = ChunkTaskKey {
            partition_id: *partition_id,
            kind,
            task_id: *task_id,
        };
        let key = task_key.to_bytes();
        let task_route = route(&self.bindings, partition_id)?;
        if let Some(value) = self.read_raw(&task_route, &key).await? {
            return decode_for_key(&task_key, &value).map(Some);
        }
        if matches!(
            task_route.migration_state,
            MigrationState::Copying | MigrationState::Cutover
        ) {
            if let (Some(store), Some(group)) = (task_route.old_kv_store_id, task_route.old_kv_group_id) {
                let old_route = Route {
                    kv_store_id: store,
                    kv_group_id: group,
                    migration_state: MigrationState::NotMigrating,
                    old_kv_store_id: None,
                    old_kv_group_id: None,
                };
                if let Some(value) = self.read_raw(&old_route, &key).await? {
                    return decode_for_key(&task_key, &value).map(Some);
                }
            }
        }
        Ok(None)
    }

    /// Scan runnable indexes whose retry eligibility has arrived.
    ///
    /// # Errors
    /// Returns an error if any routed KV scan fails.
    pub async fn scan_ready(
        &self,
        now_ms: u64,
        max_keys: u32,
    ) -> Result<Vec<ReadyChunkTaskKey>, TaskStoreError> {
        let mut keys = self.scan_index(ReadyChunkTaskKey::prefix_all(), max_keys).await?;
        let mut decoded = Vec::with_capacity(keys.len());
        for key in keys.drain(..) {
            match ReadyChunkTaskKey::from_bytes(&key) {
                Ok(task) if task.eligible_at_ms <= now_ms => decoded.push(task),
                Ok(_) => {}
                Err(error) => warn!(%error, "skipping malformed ready task index"),
            }
        }
        decoded.sort_unstable_by_key(BinaryKey::to_bytes);
        decoded.truncate(usize::try_from(max_keys).unwrap_or(usize::MAX));
        Ok(decoded)
    }

    /// Scan claimed indexes whose lease has expired.
    ///
    /// # Errors
    /// Returns an error if any routed KV scan fails.
    pub async fn scan_expired_leases(
        &self,
        now_ms: u64,
        max_keys: u32,
    ) -> Result<Vec<LeasedChunkTaskKey>, TaskStoreError> {
        let keys = self
            .scan_index(LeasedChunkTaskKey::prefix_all(), max_keys)
            .await?;
        let mut decoded = Vec::with_capacity(keys.len());
        for key in keys {
            match LeasedChunkTaskKey::from_bytes(&key) {
                Ok(task) if task.lease_deadline_ms <= now_ms => decoded.push(task),
                Ok(_) => {}
                Err(error) => warn!(%error, "skipping malformed leased task index"),
            }
        }
        decoded.sort_unstable_by_key(BinaryKey::to_bytes);
        decoded.truncate(usize::try_from(max_keys).unwrap_or(usize::MAX));
        Ok(decoded)
    }

    async fn write_routed(&self, partition_id: &ChunkId, ops: &[BatchOp]) -> Result<(), TaskStoreError> {
        let task_route = route(&self.bindings, partition_id)?;
        self.kv
            .batch_write(task_route.kv_store_id, task_route.kv_group_id, ops)
            .await
            .map_err(|error| TaskStoreError::Kv(error.to_string()))?;
        if matches!(
            task_route.migration_state,
            MigrationState::Copying | MigrationState::Cutover
        ) {
            if let (Some(store), Some(group)) = (task_route.old_kv_store_id, task_route.old_kv_group_id) {
                if let Err(error) = self.kv.batch_write(store, group, ops).await {
                    warn!(%error, "task dual-write to old group failed");
                }
            }
        }
        Ok(())
    }

    async fn read_raw(&self, task_route: &Route, key: &[u8]) -> Result<Option<Bytes>, TaskStoreError> {
        match self
            .kv
            .get(
                task_route.kv_store_id,
                task_route.kv_group_id,
                key,
                ReadMode::Linearizable,
                None,
            )
            .await
            .map_err(|error| TaskStoreError::Kv(error.to_string()))?
        {
            GetOutcome::Found { value, .. } => Ok(Some(value)),
            GetOutcome::NotFound => Ok(None),
        }
    }

    async fn scan_index(&self, prefix: Vec<u8>, max_keys: u32) -> Result<Vec<Bytes>, TaskStoreError> {
        let table = self.bindings.snapshot();
        if table.is_empty() {
            return Err(TaskStoreError::Route(crate::routing::RouteError::NoBinding));
        }
        let mut unique = HashSet::new();
        for binding in table.bindings() {
            let result: ScanOutcome = self
                .kv
                .scan(
                    binding.kv_store_id,
                    binding.kv_group_id,
                    &prefix,
                    &[],
                    &[],
                    max_keys,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await
                .map_err(|error| TaskStoreError::Kv(error.to_string()))?;
            unique.extend(result.items.into_iter().map(|(key, _)| key));
        }
        Ok(unique.into_iter().collect())
    }
}

fn canonical_key(task: &ChunkTaskValue) -> Vec<u8> {
    ChunkTaskKey {
        partition_id: task.partition_id,
        kind: task.kind,
        task_id: task.task_id,
    }
    .to_bytes()
}

fn index_key(task: &ChunkTaskValue) -> Option<Vec<u8>> {
    match task.state {
        ChunkTaskState::Pending | ChunkTaskState::RetryWait => Some(
            ReadyChunkTaskKey {
                priority_inverse: u8::MAX - task.priority,
                eligible_at_ms: task.eligible_at_ms,
                partition_id: task.partition_id,
                kind: task.kind,
                task_id: task.task_id,
            }
            .to_bytes(),
        ),
        ChunkTaskState::Running => Some(
            LeasedChunkTaskKey {
                lease_deadline_ms: task.claim_deadline_ms,
                partition_id: task.partition_id,
                kind: task.kind,
                task_id: task.task_id,
            }
            .to_bytes(),
        ),
        ChunkTaskState::Completed | ChunkTaskState::Failed | ChunkTaskState::Cancelled => None,
    }
}

fn same_identity(left: &ChunkTaskValue, right: &ChunkTaskValue) -> bool {
    left.partition_id == right.partition_id && left.kind == right.kind && left.task_id == right.task_id
}

fn decode_for_key(key: &ChunkTaskKey, bytes: &[u8]) -> Result<ChunkTaskValue, TaskStoreError> {
    let value = decode_chunk_task_value(bytes)?;
    if value.partition_id != key.partition_id || value.kind != key.kind || value.task_id != key.task_id {
        return Err(TaskStoreError::KeyMismatch);
    }
    Ok(value)
}
