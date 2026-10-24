// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Allocate, grow and rotate the worker-owned active chunk.

use super::publication::publish_state;
use super::{preparation, successor};
use super::{Duration, Ordering, Result, StreamError, WorkerState};
use crate::perf::{perf, PhaseTimer};

pub(super) async fn prepare_active_for_append(state: &mut WorkerState, first_len: usize) -> Result<()> {
    preparation::poll_ready(state).await?;
    if fits_active(state, first_len) {
        return Ok(());
    }
    let _wait = PhaseTimer::start(&perf().capacity_wait);
    preparation::drain(state).await?;
    loop {
        if state.progress.active.is_none() {
            match ensure_active(state).await {
                Ok(()) => {}
                Err(
                    error @ (StreamError::StaleWriter
                    | StreamError::Corruption(_)
                    | StreamError::InvalidRequest(_)),
                ) => {
                    return Err(error);
                }
                Err(error) => {
                    state.log_unavailable("allocate", &error);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }
        if fits_active(state, first_len) {
            return Ok(());
        }
        match grow_or_rollover(state, first_len).await {
            Ok(()) if fits_active(state, first_len) => return Ok(()),
            Ok(()) => return Err(StreamError::Corruption("new chunk cannot fit the append".into())),
            Err(
                error @ (StreamError::StaleWriter
                | StreamError::Corruption(_)
                | StreamError::InvalidRequest(_)),
            ) => {
                return Err(error);
            }
            Err(error) => {
                state.log_unavailable("grow", &error);
                match rollover(state).await {
                    Ok(()) if fits_active(state, first_len) => return Ok(()),
                    Err(StreamError::StaleWriter) => return Err(StreamError::StaleWriter),
                    _ => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        }
    }
}

async fn ensure_active(state: &mut WorkerState) -> Result<()> {
    if state.progress.active.is_some() {
        return Ok(());
    }
    let mut active = state
        .chunks
        .allocate_mirrored(state.stream_name, state.writer_epoch, state.progress.purpose)
        .await?;
    active.logical_start = state.tail_view.load(Ordering::Acquire);
    if active.acknowledged_cursor < active.physical_start || active.acknowledged_cursor > active.capacity {
        return Err(StreamError::Corruption(
            "allocated chunk cursor is invalid".into(),
        ));
    }
    state.progress.active = Some(active);
    publish_state(state).await?;
    successor::start(state);
    Ok(())
}

pub(super) async fn rollover(state: &mut WorkerState) -> Result<()> {
    preparation::drain(state).await?;
    let active = state
        .progress
        .active
        .as_ref()
        .ok_or_else(|| StreamError::Internal("rollover has no active chunk".into()))?;
    let active = active.clone();
    state
        .chunks
        .seal(active.chunk_id, state.writer_epoch, active.acknowledged_cursor)
        .await?;
    state.checkpoint = None;
    let tail = state.tail_view.load(Ordering::Acquire);
    state.progress.logical_tail = tail;
    let mut successor = successor::take(state).await?;
    successor.logical_start = tail;
    state.progress.active = Some(successor);
    publish_state(state).await?;
    successor::start(state);
    state.metrics.rollovers.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

async fn grow_or_rollover(state: &mut WorkerState, bytes: usize) -> Result<()> {
    if grow_active(state, bytes).await? {
        return Ok(());
    }
    rollover(state).await?;
    // Production allocates only the first mirror strip. The waiting append
    // may need several strips even in an otherwise empty successor.
    if fits_active(state, bytes) || grow_active(state, bytes).await? {
        return Ok(());
    }
    Err(StreamError::Corruption("new chunk cannot fit the append".into()))
}

async fn grow_active(state: &mut WorkerState, bytes: usize) -> Result<bool> {
    let active = state
        .progress
        .active
        .as_ref()
        .ok_or_else(|| StreamError::Internal("active chunk is missing".into()))?;
    let required_capacity = active
        .acknowledged_cursor
        .checked_add(
            u64::try_from(bytes)
                .map_err(|_| StreamError::InvalidRequest("append length exceeds addressable range".into()))?,
        )
        .ok_or_else(|| StreamError::InvalidRequest("append cursor overflows".into()))?;
    if required_capacity > state.config.chunk_capacity_bytes {
        return Ok(false);
    }
    let required_capacity = crate::storage::padded_end(required_capacity, state.config.chunk_capacity_bytes)?;
    if let Some(mut grown) = state
        .chunks
        .grow_mirrored(
            state.stream_name,
            state.writer_epoch,
            active.chunk_id,
            required_capacity,
        )
        .await?
    {
        grown.physical_start = active.physical_start;
        grown.logical_start = active.logical_start;
        grown.acknowledged_cursor = active.acknowledged_cursor;
        if grown.capacity < required_capacity {
            return Err(StreamError::Corruption(
                "grown chunk capacity does not cover the requested append".into(),
            ));
        }
        state.progress.active = Some(grown);
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn fits_active(state: &WorkerState, bytes: usize) -> bool {
    state.progress.active.as_ref().is_some_and(|active| {
        active
            .acknowledged_cursor
            .checked_add(bytes as u64)
            .and_then(|end| crate::storage::padded_end(end, state.config.chunk_capacity_bytes).ok())
            .is_some_and(|end| end <= active.capacity)
    })
}
