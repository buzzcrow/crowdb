// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{rollover, ChunkId, Duration, StreamError, WorkerState};

pub(super) async fn maintain(state: &mut WorkerState, chunk_id: ChunkId) {
    if state.stalled || state.manifest.closed {
        return;
    }
    for attempts in 1..=3 {
        if !owns_head(state, chunk_id).await {
            return;
        }
        match state.chunks.renew_liveness(chunk_id, state.writer_epoch).await {
            Ok(()) => return,
            Err(error) => {
                tracing::warn!(stream_high = state.stream_name.high,
                    stream_low = state.stream_name.low, writer_epoch = state.writer_epoch,
                    attempts, %error, "chunk-stream idle liveness renewal failed");
                if matches!(error, StreamError::StaleWriter) || attempts == 3 {
                    if owns_head(state, chunk_id).await {
                        rotate(state).await;
                    }
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn owns_head(state: &mut WorkerState, chunk_id: ChunkId) -> bool {
    match state.metadata.load_current(state.stream_name).await {
        Ok(Some(current))
            if current.writer_epoch == state.writer_epoch
                && current.generation == state.manifest.generation
                && !current.closed
                && current
                    .active
                    .as_ref()
                    .is_some_and(|active| active.chunk_id == chunk_id) =>
        {
            true
        }
        Ok(_) => {
            state.stalled = true;
            tracing::info!(
                stream_high = state.stream_name.high,
                stream_low = state.stream_name.low,
                writer_epoch = state.writer_epoch,
                "chunk-stream idle writer no longer owns manifest head"
            );
            false
        }
        Err(error) => {
            tracing::warn!(%error, "chunk-stream idle authority observation unavailable");
            false
        }
    }
}

async fn rotate(state: &mut WorkerState) {
    match rollover(state).await {
        Ok(()) => {}
        Err(
            error @ (StreamError::StaleWriter | StreamError::Corruption(_) | StreamError::InvalidRequest(_)),
        ) => {
            state.stalled = true;
            tracing::warn!(%error, "chunk-stream idle rollover cannot continue safely");
        }
        Err(error) => {
            tracing::warn!(%error, "chunk-stream idle rollover remains unavailable");
        }
    }
}
