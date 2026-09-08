// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Persistent task admission, claiming, completion, and crash takeover.

use std::sync::Arc;

use crowdb_protocol::chunk_task::{ChunkTaskState, ChunkTaskValue};
use crowdb_protocol::{LeasedChunkTaskKey, ReadyChunkTaskKey};

use super::{TaskStore, TaskStoreError};

#[derive(Debug, thiserror::Error)]
pub enum TaskManagerError {
    #[error(transparent)]
    Store(#[from] TaskStoreError),
    #[error("task claim is stale")]
    StaleClaim,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskAdmission {
    Created(ChunkTaskValue),
    Existing(ChunkTaskValue),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskClaim {
    pub task: ChunkTaskValue,
}

/// Stateless transition manager over a durable [`TaskStore`].
pub struct TaskManager {
    store: Arc<TaskStore>,
    instance_id: u64,
    lease_ms: u64,
    wake: Arc<tokio::sync::Notify>,
}

impl TaskManager {
    #[must_use]
    pub fn new(store: Arc<TaskStore>, instance_id: u64, lease_ms: u64) -> Self {
        Self {
            store,
            instance_id,
            lease_ms: lease_ms.max(1),
            wake: Arc::new(tokio::sync::Notify::new()),
        }
    }

    #[must_use]
    pub fn wake_handle(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.wake)
    }

    /// Create a task unless its deterministic identity already exists.
    ///
    /// # Errors
    /// Returns a persistence error when the canonical record cannot be read or
    /// written.
    pub async fn admit(&self, task: ChunkTaskValue) -> Result<TaskAdmission, TaskManagerError> {
        if let Some(existing) = self.load(&task).await? {
            return Ok(TaskAdmission::Existing(existing));
        }
        self.store.write_transition(None, &task).await?;
        self.wake.notify_one();
        let stored = self.load(&task).await?.ok_or(TaskManagerError::StaleClaim)?;
        if stored == task {
            Ok(TaskAdmission::Created(stored))
        } else {
            Ok(TaskAdmission::Existing(stored))
        }
    }

    /// Claim a ready task and verify that this instance won the persisted
    /// claim generation.
    ///
    /// # Errors
    /// Returns a persistence error. A raced or stale index returns `Ok(None)`.
    pub async fn claim(
        &self,
        index: &ReadyChunkTaskKey,
        now_ms: u64,
    ) -> Result<Option<TaskClaim>, TaskManagerError> {
        let Some(current) = self
            .store
            .get(&index.partition_id, index.kind, &index.task_id)
            .await?
        else {
            return Ok(None);
        };
        if !matches!(current.state, ChunkTaskState::Pending | ChunkTaskState::RetryWait)
            || current.eligible_at_ms > now_ms
            || current.priority != u8::MAX - index.priority_inverse
        {
            return Ok(None);
        }
        let mut claimed = current.clone();
        claimed.state = ChunkTaskState::Running;
        claimed.revision = claimed.revision.saturating_add(1);
        claimed.updated_at_ms = now_ms;
        claimed.attempt = claimed.attempt.saturating_add(1);
        claimed.claim_owner = self.instance_id;
        claimed.claim_generation = claimed.claim_generation.saturating_add(1);
        claimed.claim_deadline_ms = now_ms.saturating_add(self.lease_ms);
        self.store.write_transition(Some(&current), &claimed).await?;
        let Some(stored) = self.load(&claimed).await? else {
            return Ok(None);
        };
        if owns_claim(&stored, self.instance_id, claimed.claim_generation) {
            Ok(Some(TaskClaim { task: stored }))
        } else {
            Ok(None)
        }
    }

    /// Mark an owned task complete.
    ///
    /// # Errors
    /// Returns [`TaskManagerError::StaleClaim`] if ownership changed, or a
    /// persistence error.
    pub async fn complete(&self, claim: &TaskClaim, now_ms: u64) -> Result<(), TaskManagerError> {
        self.finish_claim(claim, now_ms, ChunkTaskState::Completed, 0, "", 0)
            .await
    }

    /// Return an owned task to retry, or fail it when its attempt budget is
    /// exhausted.
    ///
    /// # Errors
    /// Returns [`TaskManagerError::StaleClaim`] if ownership changed, or a
    /// persistence error.
    pub async fn retry(
        &self,
        claim: &TaskClaim,
        now_ms: u64,
        eligible_at_ms: u64,
        error_code: u16,
        error: &str,
    ) -> Result<(), TaskManagerError> {
        let state = if claim.task.attempt >= claim.task.max_attempts {
            ChunkTaskState::Failed
        } else {
            ChunkTaskState::RetryWait
        };
        self.finish_claim(claim, now_ms, state, error_code, error, eligible_at_ms)
            .await?;
        if state == ChunkTaskState::RetryWait {
            self.wake.notify_one();
        }
        Ok(())
    }

    /// Permanently fail an owned task.
    ///
    /// # Errors
    /// Returns [`TaskManagerError::StaleClaim`] if ownership changed, or a
    /// persistence error.
    pub async fn fail(
        &self,
        claim: &TaskClaim,
        now_ms: u64,
        error_code: u16,
        error: &str,
    ) -> Result<(), TaskManagerError> {
        self.finish_claim(claim, now_ms, ChunkTaskState::Failed, error_code, error, 0)
            .await
    }

    /// Requeue a running task whose persisted lease expired.
    ///
    /// # Errors
    /// Returns a persistence error. A repaired or renewed index returns
    /// `Ok(false)`.
    pub async fn recover_expired(
        &self,
        index: &LeasedChunkTaskKey,
        now_ms: u64,
    ) -> Result<bool, TaskManagerError> {
        let Some(current) = self
            .store
            .get(&index.partition_id, index.kind, &index.task_id)
            .await?
        else {
            return Ok(false);
        };
        if current.state != ChunkTaskState::Running
            || current.claim_deadline_ms != index.lease_deadline_ms
            || current.claim_deadline_ms > now_ms
        {
            return Ok(false);
        }
        let mut retry = current.clone();
        retry.state = ChunkTaskState::RetryWait;
        retry.revision = retry.revision.saturating_add(1);
        retry.updated_at_ms = now_ms;
        retry.eligible_at_ms = now_ms;
        retry.claim_owner = 0;
        retry.claim_deadline_ms = 0;
        retry.last_error_code = 1;
        retry.last_error = "executor claim expired".into();
        self.store.write_transition(Some(&current), &retry).await?;
        self.wake.notify_one();
        Ok(true)
    }

    async fn finish_claim(
        &self,
        claim: &TaskClaim,
        now_ms: u64,
        state: ChunkTaskState,
        error_code: u16,
        error: &str,
        eligible_at_ms: u64,
    ) -> Result<(), TaskManagerError> {
        let current = self
            .load(&claim.task)
            .await?
            .ok_or(TaskManagerError::StaleClaim)?;
        if !owns_claim(&current, self.instance_id, claim.task.claim_generation) {
            return Err(TaskManagerError::StaleClaim);
        }
        let mut next = current.clone();
        next.state = state;
        next.revision = next.revision.saturating_add(1);
        next.updated_at_ms = now_ms;
        next.eligible_at_ms = eligible_at_ms;
        next.claim_owner = 0;
        next.claim_deadline_ms = 0;
        next.last_error_code = error_code;
        next.last_error = error.into();
        self.store.write_transition(Some(&current), &next).await?;
        Ok(())
    }

    async fn load(&self, task: &ChunkTaskValue) -> Result<Option<ChunkTaskValue>, TaskStoreError> {
        self.store.get(&task.partition_id, task.kind, &task.task_id).await
    }
}

fn owns_claim(task: &ChunkTaskValue, owner: u64, generation: u64) -> bool {
    task.state == ChunkTaskState::Running && task.claim_owner == owner && task.claim_generation == generation
}
