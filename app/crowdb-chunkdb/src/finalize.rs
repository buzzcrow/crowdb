// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Finalization of abandoned Active chunks.

use std::sync::Arc;

use crowdb_protocol::chunk_task::{ChunkTaskValue, FINALIZE_CHUNK_KIND_VERSION, TASK_KIND_FINALIZE_CHUNK};
use crowdb_protocol::chunkdb::rpc::ChunkState;

use crate::lifecycle::LifecycleHandler;
use crate::task::executor::TaskFuture;
use crate::task::{TaskHandler, TaskOutcome};

/// Executes the durable liveness task. Empty chunks have no possible frame
/// boundary and can be reclaimed immediately. Nonempty chunks are retained
/// until the DiskIO-backed frame scanner is available to derive their exact
/// sealed boundary.
pub struct FinalizeChunkTaskHandler {
    lifecycle: Arc<LifecycleHandler>,
}

impl FinalizeChunkTaskHandler {
    #[must_use]
    pub fn new(lifecycle: Arc<LifecycleHandler>) -> Self {
        Self { lifecycle }
    }
}

impl TaskHandler for FinalizeChunkTaskHandler {
    fn kind(&self) -> u16 {
        TASK_KIND_FINALIZE_CHUNK
    }

    fn supports_version(&self, version: u16) -> bool {
        version == FINALIZE_CHUNK_KIND_VERSION
    }

    fn execute<'a>(&'a self, task: &'a ChunkTaskValue) -> TaskFuture<'a> {
        Box::pin(async move {
            let chunk_id = task.partition_id;
            let chunk = match self.lifecycle.query_chunk(&chunk_id).await {
                Ok(chunk) => chunk,
                Err(error) => {
                    return TaskOutcome::Retry {
                        delay_ms: 1_000,
                        error_code: 1,
                        error: error.to_string(),
                    };
                }
            };
            if chunk.state == ChunkState::Deleted as i32 || chunk.state == ChunkState::Sealed as i32 {
                return TaskOutcome::Complete;
            }
            if chunk.acknowledged_cursor == 0 {
                return match self.lifecycle.delete_chunk(&chunk_id).await {
                    Ok(_) => TaskOutcome::Complete,
                    Err(error) => TaskOutcome::Retry {
                        delay_ms: 1_000,
                        error_code: 2,
                        error: error.to_string(),
                    },
                };
            }
            TaskOutcome::Retry {
                delay_ms: 1_000,
                error_code: 3,
                error: "frame scan is required before finalizing a nonempty chunk".into(),
            }
        })
    }
}
