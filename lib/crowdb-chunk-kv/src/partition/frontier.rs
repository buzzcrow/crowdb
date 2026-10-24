// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Capture a published WAL prefix between complete mutation batches.

use super::{lifecycle_from_code, write_state_error, Partition, WorkerRequest, WorkerState};
use crate::{ChunkKvError, PartitionLifecycle, Result};
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalFrontier {
    pub mutation_seq: u64,
    pub offset: u64,
    pub stream_generation: u64,
}

pub(super) struct CaptureRequest {
    completion: oneshot::Sender<Result<JournalFrontier>>,
}

impl Partition {
    /// Publish and capture one exact WAL sequence/offset pair while continuing service.
    /// # Errors
    /// Rejects stale ownership, an unavailable worker or checkpoint failure.
    pub async fn capture_journal_frontier(&self, epoch: u64) -> Result<JournalFrontier> {
        self.validate_epoch(epoch)?;
        let (completion, receiver) = oneshot::channel();
        self.sender
            .send(WorkerRequest::CaptureJournalFrontier(CaptureRequest {
                completion,
            }))
            .await?;
        let result = receiver.await.map_err(|_| ChunkKvError::WriteStalled)?;
        // Retained parents reuse this sequencer under a newer logical epoch.
        // Validate the calling partition, rather than the original worker epoch.
        self.validate_epoch(epoch)?;
        result
    }
}

pub(super) async fn capture(state: &WorkerState, request: CaptureRequest) {
    let result = capture_inner(state).await;
    let _ = request.completion.send(result);
}

async fn capture_inner(state: &WorkerState) -> Result<JournalFrontier> {
    let lifecycle = lifecycle_from_code(state.lifecycle.load(Ordering::Acquire));
    if !matches!(
        lifecycle,
        PartitionLifecycle::Serving
            | PartitionLifecycle::TransferQuiesced
            | PartitionLifecycle::SplitFinalizing
    ) {
        return Err(write_state_error(lifecycle));
    }
    state.journal.checkpoint().await?;
    Ok(JournalFrontier {
        mutation_seq: state.journal_durable_seq.load(Ordering::Acquire),
        offset: state.journal.tail(),
        stream_generation: state.journal.record_generation(),
    })
}
