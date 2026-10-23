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
        Box::pin(async move {
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
            if now_ms < safe_at {
                return TaskOutcome::Retry {
                    delay_ms: safe_at.saturating_sub(now_ms),
                    error_code: 0,
                    error: "waiting for pre-expiry writes to age out".into(),
                };
            }
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
            if chunk.writer_epoch != task.source_revision {
                return TaskOutcome::Retry {
                    delay_ms: 1_000,
                    error_code: 6,
                    error: "finalizer owner generation differs from chunk writer epoch".into(),
                };
            }
            let boundary = match scan::complete_boundary(Arc::clone(&self.io), Arc::new(chunk.clone())).await
            {
                Ok(boundary) => boundary,
                Err(error) => {
                    return TaskOutcome::Retry {
                        delay_ms: 1_000,
                        error_code: 3,
                        error,
                    };
                }
            };
            // A failed frame scan cannot revoke bytes already acknowledged to
            // readers. Preserve their physical layout until a complete scan is
            // available, including when the first frame is currently unreadable.
            if boundary < chunk.acknowledged_cursor {
                return TaskOutcome::Retry {
                    delay_ms: 1_000,
                    error_code: 5,
                    error: format!(
                        "chunk {chunk_id:?}: scanned frame boundary {boundary} is below acknowledged cursor {}; retaining data and layout; inspect frame integrity and DiskIO reads",
                        chunk.acknowledged_cursor
                    ),
                };
            }
            if boundary == 0 {
                return match self.lifecycle.delete_chunk(&chunk_id).await {
                    Ok(_) => TaskOutcome::Complete,
                    Err(error) => TaskOutcome::Retry {
                        delay_ms: 1_000,
                        error_code: 2,
                        error: error.to_string(),
                    },
                };
            }
            match self.lifecycle.seal_chunk_bytes(&chunk_id, boundary).await {
                Ok(_) => TaskOutcome::Complete,
                Err(error) => TaskOutcome::Retry {
                    delay_ms: 1_000,
                    error_code: 4,
                    error: error.to_string(),
                },
            }
        })
    }
}
