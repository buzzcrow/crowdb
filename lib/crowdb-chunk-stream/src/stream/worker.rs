// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Queue dispatch, capacity preparation and lifecycle commands.

use super::append::write_with_recovery;
use super::capacity::{fits_active, prepare_active_for_append};
use super::publication::publish_state;
use super::{
    finish_failed, liveness, preparation, AppendRequest, BatchFailure, Command, ReadView, WorkerState,
};
use crate::perf::{observe, perf, PhaseTimer};
use crate::{Result, StreamError};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub(super) async fn run_worker(mut state: WorkerState, mut receiver: mpsc::Receiver<Command>) {
    let mut pending = None;
    let mut liveness = tokio::time::interval(state.config.liveness_interval);
    liveness.tick().await;
    loop {
        let command = if let Some(command) = pending.take() {
            command
        } else {
            tokio::select! {
                command = receiver.recv() => match command {
                    Some(command) => command,
                    None => break,
                },
                _ = liveness.tick() => {
                    if let Some(chunk_id) = state.manifest.active.as_ref().map(|active| active.chunk_id) {
                        liveness::maintain(&mut state, chunk_id).await;
                    }
                    continue;
                }
            }
        };
        match command {
            Command::Append(first) => {
                process_append_batch(&mut state, first, &mut receiver, &mut pending).await;
            }
            Command::Trim { offset, completion } => {
                let _ = completion.send(process_trim(&mut state, offset).await);
            }
            Command::Capture(completion) => {
                let _ = completion.send(process_capture(&mut state).await);
            }
            Command::Close(completion) => {
                let result = process_close(&mut state).await;
                let _ = completion.send(result);
                break;
            }
        }
    }
    // Never detach initialization IO that could outlive its owner or a seal.
    if let Err(error) = preparation::drain(&mut state).await {
        state.log_unavailable("drain_capacity", &error);
    }
    state.closed_view.store(true, Ordering::Release);
}

async fn process_capture(state: &mut WorkerState) -> Result<Arc<ReadView>> {
    let _timer = PhaseTimer::start(&perf().checkpoint);
    if state.stalled {
        return Err(StreamError::WriteStalled);
    }
    if state.manifest.sealed_tail != state.manifest_view.load().sealed_tail {
        publish_state(state).await?;
    } else if state.metadata.load_current(state.stream_name).await?.as_ref()
        != Some(state.manifest_view.load().as_ref())
    {
        state.stalled = true;
        return Err(StreamError::StaleWriter);
    }
    Ok(Arc::new(ReadView::new(state.manifest_view.load_full())))
}

async fn process_append_batch(
    state: &mut WorkerState,
    first: AppendRequest,
    receiver: &mut mpsc::Receiver<Command>,
    pending: &mut Option<Command>,
) {
    if state.stalled || state.manifest.closed {
        finish_failed(state, vec![first], &StreamError::WriteStalled);
        return;
    }
    observe(&perf().queue, first.enqueued_at);
    let first_len = match first.physical_len() {
        Ok(length) => length,
        Err(error) => {
            finish_failed(state, vec![first], &error);
            return;
        }
    };
    let capacity_timer = PhaseTimer::start(&perf().capacity);
    let preparation = prepare_active_for_append(state, first_len).await;
    drop(capacity_timer);
    if let Err(error) = preparation {
        state.stalled = true;
        finish_failed(state, vec![first], &error);
        return;
    }

    let mut requests = vec![first];
    let mut bytes = first_len;
    while requests.len() < state.config.batch_requests && bytes < state.config.batch_bytes {
        match receiver.try_recv() {
            Ok(Command::Append(request))
                if bytes
                    .checked_add(request.physical_len().unwrap_or(usize::MAX))
                    .is_some_and(|total| total <= state.config.batch_bytes && fits_active(state, total)) =>
            {
                observe(&perf().queue, request.enqueued_at);
                bytes += request.physical_len().expect("checked above");
                requests.push(request);
            }
            Ok(command) => {
                *pending = Some(command);
                break;
            }
            Err(_) => break,
        }
    }
    preparation::start(state, bytes);
    let result = write_with_recovery(state, &requests, bytes).await;
    match result {
        Ok(ranges) => {
            for (request, range) in requests.into_iter().zip(ranges) {
                state
                    .queued_bytes
                    .fetch_sub(request.logical_len() as u64, Ordering::AcqRel);
                state.queued_requests.fetch_sub(1, Ordering::AcqRel);
                state.metrics.completed.fetch_add(1, Ordering::Relaxed);
                state
                    .metrics
                    .logical_append_bytes
                    .fetch_add(request.logical_len() as u64, Ordering::Relaxed);
                observe(&perf().append, request.enqueued_at);
                let _ = request.completion.send(Ok(range));
            }
        }
        Err(BatchFailure::MirrorWrite(error) | BatchFailure::Other(error)) => {
            state.stalled = true;
            finish_failed(state, requests, &error);
        }
    }
}

impl WorkerState {
    pub(super) fn log_unavailable(&mut self, stage: &'static str, error: &StreamError) {
        let now = Instant::now();
        let should_log = self
            .last_unavailable_log
            .map_or(true, |last| now.duration_since(last) >= Duration::from_secs(1));
        if should_log {
            self.last_unavailable_log = Some(now);
            tracing::warn!(
                stream_high = self.stream_name.high,
                stream_low = self.stream_name.low,
                writer_epoch = self.writer_epoch,
                stage,
                %error,
                "chunk-stream storage remains unavailable; retrying"
            );
        }
    }
}

pub(super) async fn rotate_externally_sealed_active(state: &mut WorkerState) -> Result<bool> {
    preparation::drain(state).await?;
    let Some(active) = state.manifest.active.clone() else {
        return Ok(false);
    };
    let durable = state
        .chunks
        .durable_cursor(active.chunk_id, state.writer_epoch)
        .await?;
    if !durable.sealed {
        return Ok(false);
    }
    if durable.offset != active.acknowledged_cursor {
        return Err(StreamError::Corruption(
            "externally sealed active chunk has an unexpected durable cursor".into(),
        ));
    }

    let tail = state.tail_view.load(Ordering::Acquire);
    state.manifest.active = None;
    state.manifest.sealed_tail = tail;
    let mut successor = state
        .chunks
        .allocate_mirrored(state.stream_name, state.writer_epoch, state.manifest.purpose)
        .await?;
    successor.logical_start = tail;
    state.manifest.active = Some(successor);
    publish_state(state).await?;
    state.stalled = false;
    state.metrics.rollovers.fetch_add(1, Ordering::Relaxed);
    Ok(true)
}

async fn process_trim(state: &mut WorkerState, offset: u64) -> Result<u64> {
    if state.stalled {
        return Err(StreamError::WriteStalled);
    }
    let tail = state.tail_view.load(Ordering::Acquire);
    if offset < state.manifest.trim_offset || offset > tail {
        return Err(StreamError::InvalidRequest(
            "trim offset regresses or exceeds durable tail".into(),
        ));
    }
    if offset != state.manifest.trim_offset {
        state.manifest.trim_offset = offset;
        publish_state(state).await?;
    }
    let mut reclaimed = 0_u64;
    let mut removed = 0_usize;
    while removed < state.extents.len() {
        if removed > 0 && reclaimed >= state.config.gc_bytes_per_pass {
            break;
        }
        let remaining = &state.extents[removed..];
        let chunk_id = remaining[0].chunk_id;
        if state
            .manifest
            .active
            .as_ref()
            .is_some_and(|active| active.chunk_id == chunk_id)
        {
            break;
        }
        let count = remaining
            .iter()
            .take_while(|extent| extent.chunk_id == chunk_id)
            .count();
        let last = &remaining[count - 1];
        if last.logical_end > offset {
            break;
        }
        let physical_end = last
            .frame_start
            .checked_add(u64::from(last.frame_length))
            .ok_or_else(|| StreamError::Corruption("trimmed chunk physical end overflows".into()))?;
        reclaimed = reclaimed.saturating_add(
            state
                .chunks
                .release_trimmed(chunk_id, last.logical_end, physical_end)
                .await?
                .reclaimed_bytes,
        );
        removed += count;
    }
    if removed > 0 {
        state.extents.drain(..removed);
        publish_state(state).await?;
    }
    state
        .metrics
        .reclaimed_bytes
        .fetch_add(reclaimed, Ordering::Relaxed);
    Ok(reclaimed)
}

async fn process_close(state: &mut WorkerState) -> Result<()> {
    if state.stalled {
        return Err(StreamError::WriteStalled);
    }
    if state.manifest.closed {
        return Ok(());
    }
    preparation::drain(state).await?;
    if let Some(active) = state.manifest.active.take() {
        state
            .chunks
            .seal(active.chunk_id, state.writer_epoch, active.acknowledged_cursor)
            .await?;
        let tail = state.tail_view.load(Ordering::Acquire);
        state.manifest.sealed_tail = tail;
    }
    state.manifest.closed = true;
    publish_state(state).await?;
    state.closed_view.store(true, Ordering::Release);
    Ok(())
}
