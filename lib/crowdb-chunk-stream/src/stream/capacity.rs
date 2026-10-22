// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Allocate, grow and rotate the worker-owned active chunk.

use super::publication::publish_state;
use super::{Duration, Ordering, Result, StreamError, WorkerState};

pub(super) async fn prepare_active_for_append(state: &mut WorkerState, first_len: usize) -> Result<()> {
    loop {
        if state.manifest.active.is_none() {
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
    if state.manifest.active.is_some() {
        return Ok(());
    }
    let mut active = state
        .chunks
        .allocate_mirrored(state.stream_name, state.writer_epoch, state.manifest.purpose)
        .await?;
    active.logical_start = state.tail_view.load(Ordering::Acquire);
    if active.acknowledged_cursor < active.physical_start || active.acknowledged_cursor > active.capacity {
        return Err(StreamError::Corruption(
            "allocated chunk cursor is invalid".into(),
        ));
    }
    state.manifest.active = Some(active);
    publish_state(state).await
}

pub(super) async fn rollover(state: &mut WorkerState) -> Result<()> {
    let active = state
        .manifest
        .active
        .as_ref()
        .ok_or_else(|| StreamError::Internal("rollover has no active chunk".into()))?;
    let active = active.clone();
    let durable = state
        .chunks
        .durable_cursor(active.chunk_id, state.writer_epoch)
        .await?;
    if durable.offset != active.acknowledged_cursor {
        return Err(StreamError::Corruption(
            "rollover cursor differs from the acknowledged cursor".into(),
        ));
    }
    if !durable.sealed {
        state
            .chunks
            .seal(active.chunk_id, state.writer_epoch, active.acknowledged_cursor)
            .await?;
    }
    let tail = state.tail_view.load(Ordering::Acquire);
    state.manifest.sealed_tail = tail;
    let mut successor = state
        .chunks
        .allocate_mirrored(state.stream_name, state.writer_epoch, state.manifest.purpose)
        .await?;
    successor.logical_start = tail;
    state.manifest.active = Some(successor);
    publish_state(state).await?;
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
        .manifest
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
        let chunk_id = active.chunk_id;
        state.manifest.active = Some(grown);
        if state.chunks.current_capacity(chunk_id).await?.is_none() {
            // Legacy injected stores without current-layout queries must keep
            // their cached capacity durable until their read/open seam upgrades.
            publish_state(state).await?;
        }
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn fits_active(state: &WorkerState, bytes: usize) -> bool {
    state.manifest.active.as_ref().is_some_and(|active| {
        active
            .acknowledged_cursor
            .checked_add(bytes as u64)
            .is_some_and(|end| end <= active.capacity)
    })
}
