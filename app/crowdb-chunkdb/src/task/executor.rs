// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded dispatch from generic task envelopes to typed handlers.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crowdb_protocol::chunk_task::ChunkTaskValue;

use super::{TaskClaim, TaskManager, TaskManagerError};

pub type TaskFuture<'a> = Pin<Box<dyn Future<Output = TaskOutcome> + Send + 'a>>;

pub enum TaskOutcome {
    Complete,
    Retry {
        delay_ms: u64,
        error_code: u16,
        error: String,
    },
    Fail {
        error_code: u16,
        error: String,
    },
}

/// One version-aware business-task implementation.
pub trait TaskHandler: Send + Sync + 'static {
    fn kind(&self) -> u16;
    fn supports_version(&self, version: u16) -> bool;
    fn execute<'a>(&'a self, task: &'a ChunkTaskValue) -> TaskFuture<'a>;
}

#[derive(Debug, thiserror::Error)]
pub enum TaskRegistryError {
    #[error("duplicate task handler kind {0}")]
    DuplicateKind(u16),
    #[error("task executor stopped")]
    Stopped,
    #[error(transparent)]
    Manager(#[from] TaskManagerError),
}

/// Immutable handler registry plus a bounded executor semaphore.
pub struct TaskExecutor {
    manager: Arc<TaskManager>,
    handlers: HashMap<u16, Arc<dyn TaskHandler>>,
    permits: Arc<tokio::sync::Semaphore>,
}

impl TaskExecutor {
    /// Build a registry. Handler kinds must be unique.
    ///
    /// # Errors
    /// Returns [`TaskRegistryError::DuplicateKind`] for duplicate handlers.
    pub fn new(
        manager: Arc<TaskManager>,
        max_concurrency: usize,
        handlers: Vec<Arc<dyn TaskHandler>>,
    ) -> Result<Self, TaskRegistryError> {
        let mut registry = HashMap::with_capacity(handlers.len());
        for handler in handlers {
            let kind = handler.kind();
            if registry.insert(kind, handler).is_some() {
                return Err(TaskRegistryError::DuplicateKind(kind));
            }
        }
        Ok(Self {
            manager,
            handlers: registry,
            permits: Arc::new(tokio::sync::Semaphore::new(max_concurrency.max(1))),
        })
    }

    /// Execute one claimed task and persist its outcome.
    ///
    /// # Errors
    /// Returns a task-manager error if the outcome cannot be persisted.
    pub async fn execute(&self, claim: TaskClaim) -> Result<(), TaskRegistryError> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| TaskRegistryError::Stopped)?;
        let now_ms = unix_time_ms();
        let Some(handler) = self.handlers.get(&claim.task.kind) else {
            self.manager
                .fail(&claim, now_ms, 2, "no handler registered for task kind")
                .await?;
            return Ok(());
        };
        if !handler.supports_version(claim.task.kind_version) {
            self.manager
                .fail(&claim, now_ms, 3, "task payload version is unsupported")
                .await?;
            return Ok(());
        }
        match handler.execute(&claim.task).await {
            TaskOutcome::Complete => self.manager.complete(&claim, unix_time_ms()).await?,
            TaskOutcome::Retry {
                delay_ms,
                error_code,
                error,
            } => {
                let now = unix_time_ms();
                self.manager
                    .retry(&claim, now, now.saturating_add(delay_ms), error_code, &error)
                    .await?;
            }
            TaskOutcome::Fail { error_code, error } => {
                self.manager
                    .fail(&claim, unix_time_ms(), error_code, &error)
                    .await?;
            }
        }
        Ok(())
    }
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
