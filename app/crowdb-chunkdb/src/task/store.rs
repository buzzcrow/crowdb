// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV persistence for canonical task values and runnable/lease indexes.

use std::collections::HashMap;
use std::sync::Arc;

use crate::range_guard::RangeGuard;
use bytes::Bytes;
use crowdb_kv_client::{BatchOp, CrowdbKvClient, Error as KvError, GetOutcome, ReadMode};
use crowdb_protocol::chunk_domain::ChunkDomain;
use crowdb_protocol::chunk_task::{ChunkTaskState, ChunkTaskValue, TASK_KIND_FINALIZE_CHUNK};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::{
    decode_chunk_task_value, encode_chunk_task_value, BinaryKey, ChunkTaskKey, ChunkTaskValueError,
    FinalizeChunkTaskKey, KeyError, LeasedChunkTaskKey, ReadyChunkTaskKey,
};

mod scans;

use crate::routing::{route, BindingCache, Route};

#[derive(Debug, thiserror::Error)]
pub enum TaskStoreError {
    #[error("task routing error: {0}")]
    Route(#[from] crate::routing::RouteError),
    #[error("task KV operation failed: {0}")]
    Kv(String),
    #[error("task value is invalid: {0}")]
    Value(#[from] ChunkTaskValueError),
    #[error("task key is invalid: {0}")]
    Key(#[from] KeyError),
    #[error("task transition changes immutable identity")]
    IdentityChanged,
    #[error("task value does not match its canonical key")]
    KeyMismatch,
    #[error("task transition lost its compare-and-write race")]
    Conflict,
    #[error("task is outside this runtime domain or service slot authority")]
    Authority,
}

/// Persistent task storage. It is lock-free and relies on the caller's
/// existing resource lifecycle guard for same-process transition ordering.
pub struct TaskStore {
    kv: Arc<CrowdbKvClient>,
    bindings: BindingCache,
    scope: Option<(Arc<RangeGuard>, ChunkDomain)>,
}

impl TaskStore {
    #[must_use]
    pub fn new(kv: Arc<CrowdbKvClient>, bindings: BindingCache) -> Self {
        Self {
            kv,
            bindings,
            scope: None,
        }
    }

    /// Bind admission, publication, and scans to one maintenance domain.
    #[must_use]
    pub fn with_scope(mut self, guard: Arc<RangeGuard>, domain: ChunkDomain) -> Self {
        self.scope = Some((guard, domain));
        self
    }

    fn check_authority(&self, id: &ChunkId) -> Result<(), TaskStoreError> {
        if let Some((guard, domain)) = &self.scope {
            if ChunkDomain::for_chunk(id) != Some(*domain) || guard.check(id).is_err() {
                return Err(TaskStoreError::Authority);
            }
        }
        Ok(())
    }

    /// Persist a canonical task and update its secondary index atomically.
    ///
    /// # Errors
    /// Returns an error for identity changes, routing failure, or failed KV
    /// writes. Every transition conditionally updates the canonical task key,
    /// so a stale claimant cannot overwrite a successor's checkpoint.
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
        self.write_routed(&next.partition_id, &canonical_key(next), previous, &ops)
            .await
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
        self.check_authority(partition_id)?;
        let task_route = route(&self.bindings, partition_id)?;
        if let Some((value, _)) = self.read_raw(&task_route, &key).await? {
            return decode_for_key(&task_key, &value).map(Some);
        }
        Ok(None)
    }

    /// List canonical tasks for one chunk partition. Used by owner
    /// reconciliation to retain a tentative DiskDB target named by a durable
    /// repair task.
    pub async fn list_partition(
        &self,
        partition_id: &ChunkId,
    ) -> Result<Vec<ChunkTaskValue>, TaskStoreError> {
        self.check_authority(partition_id)?;
        let task_route = route(&self.bindings, partition_id)?;
        let prefix = ChunkTaskKey::prefix_for_partition(partition_id);
        let records = self.scan_partition_route(&task_route, &prefix).await?;
        let mut tasks = HashMap::with_capacity(records.len());
        for (key, value) in records {
            let task_key = ChunkTaskKey::from_bytes(&key)?;
            if task_key.partition_id == *partition_id && !tasks.contains_key(&task_key) {
                tasks.insert(task_key, decode_for_key(&task_key, &value)?);
            }
        }
        Ok(tasks.into_values().collect())
    }

    /// Move an Active chunk's one liveness task to a new deadline. The
    /// canonical task is CAS-guarded; the old deadline index deletion and new
    /// index insertion are in the same batch. A claimed or replaced task is a
    /// conflict, which fences the owner from further writes.
    pub async fn renew_finalize_chunk(
        &self,
        chunk_id: &ChunkId,
        owner_generation: u64,
        now_ms: u64,
        liveness_ms: u64,
    ) -> Result<ChunkTaskValue, TaskStoreError> {
        let current = self
            .get(chunk_id, TASK_KIND_FINALIZE_CHUNK, chunk_id)
            .await?
            .ok_or(TaskStoreError::Conflict)?;
        if current.state != ChunkTaskState::Pending || current.source_revision != owner_generation {
            return Err(TaskStoreError::Conflict);
        }
        let mut renewed = current.clone();
        renewed.revision = renewed.revision.saturating_add(1);
        renewed.updated_at_ms = now_ms;
        renewed.eligible_at_ms = now_ms.saturating_add(liveness_ms);
        self.write_transition(Some(&current), &renewed).await?;
        Ok(renewed)
    }

    async fn write_routed(
        &self,
        partition_id: &ChunkId,
        canonical: &[u8],
        previous: Option<&ChunkTaskValue>,
        ops: &[BatchOp],
    ) -> Result<(), TaskStoreError> {
        self.check_authority(partition_id)?;
        let task_route = route(&self.bindings, partition_id)?;
        let expected_revision = match previous {
            Some(previous) => {
                let Some((value, revision)) = self.read_raw(&task_route, canonical).await? else {
                    return Err(TaskStoreError::Conflict);
                };
                let key = ChunkTaskKey {
                    partition_id: previous.partition_id,
                    kind: previous.kind,
                    task_id: previous.task_id,
                };
                if decode_for_key(&key, &value)? != *previous {
                    return Err(TaskStoreError::Conflict);
                }
                revision
            }
            None => 0,
        };
        self.kv
            .batch_write_cas(
                task_route.kv_store_id,
                task_route.kv_group_id,
                ops,
                canonical,
                expected_revision,
            )
            .await
            .map_err(|error| match error {
                KvError::CasFailed { .. } | KvError::CasBusy => TaskStoreError::Conflict,
                _ => TaskStoreError::Kv(error.to_string()),
            })?;
        Ok(())
    }

    async fn read_raw(&self, task_route: &Route, key: &[u8]) -> Result<Option<(Bytes, u64)>, TaskStoreError> {
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
            GetOutcome::Found { value, revision } => Ok(Some((value, revision))),
            GetOutcome::NotFound => Ok(None),
        }
    }

    async fn scan_partition_route(
        &self,
        task_route: &Route,
        prefix: &[u8],
    ) -> Result<Vec<(Bytes, Bytes)>, TaskStoreError> {
        let result = self
            .kv
            .scan(
                task_route.kv_store_id,
                task_route.kv_group_id,
                prefix,
                &[],
                &[],
                0,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await
            .map_err(|error| TaskStoreError::Kv(error.to_string()))?;
        Ok(result.items)
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
        ChunkTaskState::Pending | ChunkTaskState::RetryWait if task.kind == TASK_KIND_FINALIZE_CHUNK => Some(
            FinalizeChunkTaskKey {
                expires_at_ms: task.eligible_at_ms,
                partition_id: task.partition_id,
                task_id: task.task_id,
            }
            .to_bytes(),
        ),
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
