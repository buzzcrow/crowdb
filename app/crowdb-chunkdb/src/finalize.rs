// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Finalization of abandoned Active chunks.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_protocol::chunk_task::{ChunkTaskValue, FINALIZE_CHUNK_KIND_VERSION, TASK_KIND_FINALIZE_CHUNK};
use crowdb_protocol::chunkdb::rpc::ChunkState;

use crate::conversion::io::ConversionDiskIo;
use crate::lifecycle::LifecycleHandler;
use crate::task::executor::TaskFuture;
use crate::task::{TaskHandler, TaskOutcome};

mod scan;

/// Executes durable liveness finalization after old requests have aged out.
/// Grouped tails are recovered from complete content, independently of checkpoints.
pub struct FinalizeChunkTaskHandler {
    lifecycle: Arc<LifecycleHandler>,
    io: Arc<ConversionDiskIo>,
}

impl FinalizeChunkTaskHandler {
    #[must_use]
    pub fn new(lifecycle: Arc<LifecycleHandler>, io: Arc<ConversionDiskIo>) -> Self {
        Self { lifecycle, io }
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
        Box::pin(self.finalize(task))
    }
}

impl FinalizeChunkTaskHandler {
    async fn finalize(&self, task: &ChunkTaskValue) -> TaskOutcome {
        if let Some(waiting) = wait_for_old_requests(task) {
            return waiting;
        }
        let chunk_id = task.partition_id;
        let chunk = match self.lifecycle.query_chunk(&chunk_id).await {
            Ok(chunk) => chunk,
            Err(error) => return retry(1, error.to_string()),
        };
        if chunk.state == ChunkState::Deleted as i32 {
            return self.finish_deletion(&chunk_id).await;
        }
        if chunk.writer_epoch != task.source_revision {
            return retry(6, "finalizer owner generation differs from chunk writer epoch");
        }
        if chunk.state == ChunkState::Sealed as i32 {
            return if chunk.acknowledged_cursor == 0 {
                self.finish_empty_chunk(&chunk).await
            } else {
                TaskOutcome::Complete
            };
        }
        let boundary = match scan::complete_boundary(Arc::clone(&self.io), Arc::new(chunk.clone())).await {
            Ok(boundary) => boundary,
            Err(error) => return retry(3, error),
        };
        if boundary < chunk.acknowledged_cursor {
            return retry(5, format!(
                "chunk {chunk_id:?}: scanned frame boundary {boundary} is below acknowledged cursor {}; retaining data and layout; inspect frame integrity and DiskIO reads",
                chunk.acknowledged_cursor
            ));
        }
        let sealed = match self.lifecycle.seal_observed_chunk(&chunk, boundary).await {
            Ok(sealed) => sealed,
            Err(error) => return retry(4, error.to_string()),
        };
        if boundary == 0 {
            return self.finish_empty_chunk(&sealed).await;
        }
        TaskOutcome::Complete
    }

    async fn finish_empty_chunk(&self, chunk: &crowdb_protocol::chunkdb::rpc::Chunk) -> TaskOutcome {
        if chunk.capacity != 0 || !chunk.strips.is_empty() || !chunk.cleanup_intents.is_empty() {
            return retry(4, "waiting for empty sealed chunk physical cleanup");
        }
        let Some(id) = chunk.id else {
            return retry(4, "sealed chunk has no identity");
        };
        self.finish_deletion(&id).await
    }

    async fn finish_deletion(&self, id: &crowdb_protocol::common::ChunkId) -> TaskOutcome {
        match self.lifecycle.delete_chunk(id).await {
            Ok(_) => TaskOutcome::Complete,
            Err(error) => retry(2, error.to_string()),
        }
    }
}

fn retry(error_code: u16, error: impl Into<String>) -> TaskOutcome {
    TaskOutcome::Retry {
        delay_ms: 1_000,
        error_code,
        error: error.into(),
    }
}

fn wait_for_old_requests(task: &ChunkTaskValue) -> Option<TaskOutcome> {
    let safe_at = task
        .eligible_at_ms
        .saturating_add(crowdb_protocol::timing::MAX_WRITE_REQUEST_AGE_MS)
        .saturating_add(crowdb_protocol::timing::MAX_WRITE_CLOCK_SKEW_MS)
        .saturating_add(crowdb_protocol::timing::DEFAULT_FINALIZER_SCANNER_MARGIN_MS);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        });
    (now_ms < safe_at).then(|| TaskOutcome::Retry {
        delay_ms: safe_at.saturating_sub(now_ms),
        error_code: 0,
        error: "waiting for pre-expiry writes to age out".into(),
    })
}
