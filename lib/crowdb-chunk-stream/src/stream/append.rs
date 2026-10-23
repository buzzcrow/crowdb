// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Ordered frame assembly, mirror synchronization and local progress publication.

use super::capacity::rollover;
use super::worker::rotate_externally_sealed_active;
use super::{AppendRange, AppendRequest, BatchFailure, WorkerState};
use crate::perf::{perf, PhaseTimer};
use crate::{StreamError, StreamMirrorWrite};
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, Instant};

mod encoding;

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
            Err(BatchFailure::MirrorWrite(error)) => {
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
    let write = write_batch(state, requests);
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
) -> std::result::Result<Vec<AppendRange>, BatchFailure> {
    for request in requests {
        request.authority.check_io().map_err(BatchFailure::Other)?;
    }
    let active = state
        .manifest
        .active
        .as_ref()
        .ok_or_else(|| BatchFailure::Other(StreamError::Internal("append has no active chunk".into())))?
        .clone();
    let encode_timer = PhaseTimer::start(&perf().encode);
    let expected_cursor = active.acknowledged_cursor;
    let encoded = encoding::encode_requests(
        state.stream_name,
        &active,
        state.tail_view.load(Ordering::Acquire),
        requests,
    )
    .map_err(BatchFailure::Other)?;
    let new_cursor = encoded.physical_end;
    let logical_cursor = encoded.logical_end;
    let images = state
        .mirror_shadow
        .stage(active.chunk_id, expected_cursor, new_cursor, &encoded.views)
        .map_err(BatchFailure::Other)?;
    drop(encode_timer);
    let mirror_timer = PhaseTimer::start(&perf().mirror);
    let write = state
        .chunks
        .write_mirrors(
            state.stream_name,
            state.writer_epoch,
            active.chunk_id,
            StreamMirrorWrite {
                authority: requests
                    .iter()
                    .min_by_key(|request| request.authority.deadline())
                    .map(|request| &request.authority),
                physical_offset: expected_cursor,
                complete_end: new_cursor,
                images: &images,
            },
        )
        .await;
    let physical_bytes = images.iter().map(|image| image.data.len() as u64).sum::<u64>();
    drop(mirror_timer);
    drop(images);
    state.mirror_shadow.finish();
    if let Err(error) = write {
        return Err(BatchFailure::MirrorWrite(error));
    }
    state.metrics.physical_append_bytes.fetch_add(
        physical_bytes.saturating_mul(u64::from(state.chunks.mirror_copies())),
        Ordering::Relaxed,
    );
    for request in requests {
        request.authority.check_ack().map_err(BatchFailure::Other)?;
    }
    super::checkpoint::record(
        state,
        active.chunk_id,
        expected_cursor,
        new_cursor,
        &encoded.views,
    );

    let _view_timer = PhaseTimer::start(&perf().local_view);
    state.read_view.load().append(&encoded.extents, logical_cursor);
    state.extents.extend(encoded.extents);
    if let Some(active) = &mut state.manifest.active {
        active.acknowledged_cursor = new_cursor;
        active.physical_start = new_cursor;
        active.logical_start = logical_cursor;
    }
    state.manifest.sealed_tail = logical_cursor;
    state.tail_view.store(logical_cursor, Ordering::Release);
    Ok(encoded.ranges)
}
