// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Ordered frame assembly, mirror durability and fenced cursor resolution.

use super::capacity::rollover;
use super::publication::publish_state;
use super::worker::rotate_externally_sealed_active;
use super::{unix_time_ms, AppendRange, AppendRequest, BatchFailure, Extent, WorkerState};
use crate::perf::{perf, PhaseTimer};
use crate::{ChunkId, CursorAdvance, Result, StreamError};
use bytes::BytesMut;
use crowdb_protocol::frame::{encode_frames, FrameMagic, FRAME_HEADER_PREFIX_BYTES};
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, Instant};

pub(super) async fn write_with_recovery(
    state: &mut WorkerState,
    requests: &[AppendRequest],
    bytes: usize,
) -> std::result::Result<Vec<AppendRange>, BatchFailure> {
    let mut rotated = false;
    loop {
        match write_batch_with_watchdog(state, requests, bytes).await {
            Err(BatchFailure::MirrorWrite(
                error @ (StreamError::StaleWriter
                | StreamError::Corruption(_)
                | StreamError::InvalidRequest(_)),
            )) => return Err(BatchFailure::Other(error)),
            Err(
                BatchFailure::MirrorWrite(error)
                | BatchFailure::Other(error @ StreamError::DefinitelyNotCommitted(_)),
            ) => {
                if rotated {
                    return Err(BatchFailure::Other(error));
                }
                tracing::warn!(
                    stream_high = state.stream_name.high,
                    stream_low = state.stream_name.low,
                    writer_epoch = state.writer_epoch,
                    %error,
                    "chunk-stream append will rotate after an uncommitted write"
                );
                rollover(state).await.map_err(BatchFailure::Other)?;
                rotated = true;
            }
            Err(BatchFailure::Other(error)) => {
                if matches!(rotate_externally_sealed_active(state).await, Ok(true)) {
                    continue;
                }
                return Err(BatchFailure::Other(error));
            }
            success => return success,
        }
    }
}

async fn write_batch_with_watchdog(
    state: &mut WorkerState,
    requests: &[AppendRequest],
    bytes: usize,
) -> std::result::Result<Vec<AppendRange>, BatchFailure> {
    let interval = state.config.watchdog_interval;
    let metrics = Arc::clone(&state.metrics);
    let stream_name = state.stream_name;
    let writer_epoch = state.writer_epoch;
    let logical_begin = state.tail_view.load(Ordering::Acquire);
    let logical_end = logical_begin.saturating_add(bytes as u64);
    let oldest_queue_age = requests
        .first()
        .map_or(Duration::ZERO, |request| request.enqueued_at.elapsed());
    metrics.batches.fetch_add(1, Ordering::Relaxed);
    metrics
        .batch_requests
        .fetch_add(requests.len() as u64, Ordering::Relaxed);
    let _batch_timer = PhaseTimer::start(&perf().batch);
    let write = write_batch(state, requests, bytes);
    tokio::pin!(write);
    let started = Instant::now();
    loop {
        tokio::select! {
            result = &mut write => return result,
            () = tokio::time::sleep(interval) => {
                metrics.watchdog_observations.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    stream_high = stream_name.high,
                    stream_low = stream_name.low,
                    writer_epoch,
                    logical_begin,
                    logical_end,
                    batch_requests = requests.len(),
                    batch_bytes = bytes,
                    operation_age_ms = started.elapsed().as_millis(),
                    oldest_queue_age_ms = oldest_queue_age.as_millis(),
                    stage = "append_durability",
                    "chunk-stream append batch remains in flight"
                );
            }
        }
    }
}

async fn write_batch(
    state: &mut WorkerState,
    requests: &[AppendRequest],
    bytes: usize,
) -> std::result::Result<Vec<AppendRange>, BatchFailure> {
    let active = state
        .manifest
        .active
        .as_ref()
        .ok_or_else(|| BatchFailure::Other(StreamError::Internal("append has no active chunk".into())))?
        .clone();
    let encode_timer = PhaseTimer::start(&perf().encode);
    let expected_cursor = active.acknowledged_cursor;
    let mut new_cursor = expected_cursor;
    let mut staging = BytesMut::with_capacity(bytes);
    let logical_begin = state.tail_view.load(Ordering::Acquire);
    let mut logical_cursor = logical_begin;
    let mut extents = Vec::new();
    let mut ranges = Vec::with_capacity(requests.len());
    for request in requests {
        let frames = encode_frames(
            FrameMagic::StreamV1,
            active.chunk_id,
            &request.data,
            unix_time_ms(),
        )
        .map_err(|error| BatchFailure::Other(StreamError::InvalidRequest(error.to_string())))?;
        for frame in frames {
            let frame_length = u32::try_from(frame.len()).map_err(|_| {
                BatchFailure::Other(StreamError::InvalidRequest("stream frame exceeds u32".into()))
            })?;
            let payload_length = u64::from(frame_length)
                .checked_sub((FRAME_HEADER_PREFIX_BYTES + crowdb_protocol::frame::FRAME_FOOTER_BYTES) as u64)
                .ok_or_else(|| {
                    BatchFailure::Other(StreamError::Corruption("stream frame length underflows".into()))
                })?;
            extents.push(Extent {
                chunk_id: active.chunk_id,
                logical_start: logical_cursor,
                logical_end: logical_cursor.saturating_add(payload_length),
                frame_start: new_cursor,
                frame_length,
            });
            logical_cursor = logical_cursor.saturating_add(payload_length);
            new_cursor = new_cursor.saturating_add(u64::from(frame_length));
            staging.extend_from_slice(&frame);
        }
        ranges.push(AppendRange {
            stream_name: state.stream_name,
            chunk_id: Some(active.chunk_id),
            begin: logical_cursor.saturating_sub(request.logical_len() as u64),
            end: logical_cursor,
        });
    }
    let staging = staging.freeze();
    let images = state
        .mirror_shadow
        .stage(active.chunk_id, expected_cursor, &staging)
        .map_err(BatchFailure::Other)?;
    drop(encode_timer);
    let mirror_timer = PhaseTimer::start(&perf().mirror);
    let write = state
        .chunks
        .write_mirrors_with_images(
            state.stream_name,
            state.writer_epoch,
            active.chunk_id,
            expected_cursor,
            staging.clone(),
            &images,
        )
        .await;
    drop(mirror_timer);
    drop(images);
    state.mirror_shadow.finish();
    if let Err(error) = write {
        return Err(BatchFailure::MirrorWrite(error));
    }
    state
        .metrics
        .physical_append_bytes
        .fetch_add((bytes as u64).saturating_mul(3), Ordering::Relaxed);
    resolve_cursor_advance(
        state,
        active.chunk_id,
        expected_cursor,
        new_cursor,
        crc32fast::hash(&staging),
    )
    .await
    .map_err(BatchFailure::Other)?;

    state.extents.extend(extents);
    if let Some(active) = &mut state.manifest.active {
        active.acknowledged_cursor = new_cursor;
        active.physical_start = new_cursor;
        active.logical_start = logical_cursor;
    }
    state.manifest.sealed_tail = logical_cursor;
    state.tail_view.store(logical_cursor, Ordering::Release);
    publish_state(state).await.map_err(BatchFailure::Other)?;
    Ok(ranges)
}

async fn resolve_cursor_advance(
    state: &mut WorkerState,
    chunk_id: ChunkId,
    expected_cursor: u64,
    new_cursor: u64,
    checksum: u32,
) -> Result<()> {
    let _timer = PhaseTimer::start(&perf().cursor);
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
        Ok(CursorAdvance::Committed) => Ok(()),
        Ok(CursorAdvance::DefinitelyNotCommitted) => Err(StreamError::DefinitelyNotCommitted(
            "durable cursor did not advance".into(),
        )),
        Ok(CursorAdvance::Ambiguous) | Err(_) => loop {
            match state.chunks.durable_cursor(chunk_id, state.writer_epoch).await {
                Ok(durable) if durable.offset == expected_cursor => {
                    return Err(StreamError::DefinitelyNotCommitted(
                        "ambiguous write proved absent".into(),
                    ));
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
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        },
    }
}
