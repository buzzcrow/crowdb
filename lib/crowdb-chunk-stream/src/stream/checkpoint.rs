// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Persist one ordered prefix only when publishing a directory checkpoint.

use super::WorkerState;
use crate::perf::{perf, PhaseTimer};
use crate::{ChunkId, CursorAdvance, Result, StreamError};
use bytes::Bytes;
use std::time::Duration;

pub(super) struct PendingCheckpoint {
    chunk_id: ChunkId,
    expected: u64,
    end: u64,
    checksum: crc32fast::Hasher,
}

pub(super) fn record(state: &mut WorkerState, chunk_id: ChunkId, expected: u64, end: u64, views: &[Bytes]) {
    let pending = state.checkpoint.get_or_insert_with(|| PendingCheckpoint {
        chunk_id,
        expected,
        end: expected,
        checksum: crc32fast::Hasher::new(),
    });
    assert_eq!((pending.chunk_id, pending.end), (chunk_id, expected));
    for view in views {
        pending.checksum.update(view);
    }
    pending.end = end;
}

pub(super) async fn flush(state: &mut WorkerState) -> Result<()> {
    let Some(pending) = &state.checkpoint else {
        return Ok(());
    };
    resolve_cursor_advance(
        state,
        pending.chunk_id,
        pending.expected,
        pending.end,
        pending.checksum.clone().finalize(),
    )
    .await?;
    state.checkpoint = None;
    Ok(())
}

async fn resolve_cursor_advance(
    state: &mut WorkerState,
    chunk_id: ChunkId,
    expected_cursor: u64,
    new_cursor: u64,
    checksum: u32,
) -> Result<()> {
    let _timer = PhaseTimer::start(&perf().cursor);
    loop {
        state
            .authority
            .as_ref()
            .ok_or(StreamError::WriteStalled)?
            .check()?;
        let outcome = state
            .chunks
            .advance_cursor(
                state.stream_name,
                state.writer_epoch,
                chunk_id,
                expected_cursor,
                new_cursor,
                checksum,
            )
            .await;
        match outcome {
            Ok(CursorAdvance::Committed) => return Ok(()),
            Err(
                error @ (StreamError::StaleWriter
                | StreamError::Corruption(_)
                | StreamError::InvalidRequest(_)),
            ) => return Err(error),
            _ => {}
        }
        match state.chunks.durable_cursor(chunk_id, state.writer_epoch).await {
            Ok(durable) if durable.offset == expected_cursor => {
                // The synchronized data write already completed. An older
                // metadata checkpoint cannot prove that append absent.
            }
            Ok(durable)
                if durable.offset == new_cursor && durable.last_advance_checksum == Some(checksum) =>
            {
                return Ok(());
            }
            Ok(durable) if durable.offset == new_cursor => {
                return Err(StreamError::Corruption(
                    "committed cursor has an unexpected checksum".into(),
                ));
            }
            Ok(durable) => {
                return Err(StreamError::Corruption(format!(
                        "durable cursor is outside the append bounds: chunk={chunk_id:?} epoch={} expected={expected_cursor} new={new_cursor} durable={}",
                        state.writer_epoch, durable.offset
                    )));
            }
            Err(StreamError::StaleWriter) => return Err(StreamError::StaleWriter),
            Err(error) => {
                tracing::warn!(%error, "chunk-stream cursor resolution remains unavailable");
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
