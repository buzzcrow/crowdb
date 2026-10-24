// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One bounded strip preparation overlaps writes to the initialized prefix.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use super::WorkerState;
use crate::perf::{perf, PhaseTimer};
use crate::{ActiveChunkDescriptor, ChunkId, Result, StreamError};

pub(super) struct CapacityPreparation {
    chunk_id: ChunkId,
    task: JoinHandle<(Result<Option<ActiveChunkDescriptor>>, Duration)>,
}

pub(super) fn start(state: &mut WorkerState, batch_bytes: usize) {
    let Some(active) = state.manifest.active.as_ref() else {
        return;
    };
    if state.capacity_preparation.is_some()
        || state.capacity_exhausted == Some(active.chunk_id)
        || state
            .capacity_failure
            .as_ref()
            .is_some_and(|(id, _)| *id == active.chunk_id)
    {
        return;
    }
    let Some(required) = state.capacity_plan.target(
        active.acknowledged_cursor,
        active.capacity,
        state.config.chunk_capacity_bytes,
        batch_bytes as u64,
    ) else {
        return;
    };
    let chunks = Arc::clone(&state.chunks);
    let name = state.stream_name;
    let epoch = state.writer_epoch;
    let chunk_id = active.chunk_id;
    state.capacity_preparation = Some(CapacityPreparation {
        chunk_id,
        task: tokio::spawn(async move {
            let _timer = PhaseTimer::start(&perf().preparation);
            let started = Instant::now();
            let result = chunks.grow_mirrored(name, epoch, chunk_id, required).await;
            (result, started.elapsed())
        }),
    });
}

pub(super) async fn poll_ready(state: &mut WorkerState) -> Result<()> {
    if state
        .capacity_preparation
        .as_ref()
        .is_some_and(|preparation| preparation.task.is_finished())
    {
        complete(state).await;
    }
    // A failed initialization cannot make the new suffix writable. The old
    // initialized prefix remains usable unless authority itself was lost.
    if let Some((id, StreamError::StaleWriter)) = &state.capacity_failure {
        if state
            .manifest
            .active
            .as_ref()
            .is_some_and(|active| active.chunk_id == *id)
        {
            return Err(StreamError::StaleWriter);
        }
    }
    Ok(())
}

pub(super) async fn drain(state: &mut WorkerState) -> Result<()> {
    complete(state).await;
    if matches!(state.capacity_failure, Some((_, StreamError::ReadUnavailable(_)))) {
        // Allocation availability may recover. The ordinary slow path can
        // retry it; uncertain initialization is InvalidRequest and stays fatal.
        state.capacity_failure = None;
    }
    if let Some((id, error)) = &state.capacity_failure {
        if state
            .manifest
            .active
            .as_ref()
            .is_some_and(|active| active.chunk_id == *id)
        {
            return Err(error.clone());
        }
    }
    Ok(())
}

async fn complete(state: &mut WorkerState) {
    let Some(preparation) = state.capacity_preparation.take() else {
        return;
    };
    let chunk_id = preparation.chunk_id;
    let result = match preparation.task.await {
        Ok((result, elapsed)) => {
            state.capacity_plan.record_preparation(elapsed);
            result
        }
        Err(error) => Err(StreamError::Internal(format!(
            "capacity preparation task failed: {error}"
        ))),
    };
    if let Err(error) = install(state, chunk_id, result) {
        tracing::warn!(?chunk_id, %error, "capacity preparation failed; retaining only initialized capacity");
        state.capacity_failure = Some((chunk_id, error));
    }
}

fn install(
    state: &mut WorkerState,
    chunk_id: ChunkId,
    result: Result<Option<ActiveChunkDescriptor>>,
) -> Result<()> {
    let active = state
        .manifest
        .active
        .as_mut()
        .filter(|active| active.chunk_id == chunk_id)
        .ok_or_else(|| StreamError::Internal("capacity preparation outlived its active chunk".into()))?;
    let Some(prepared) = result? else {
        state.capacity_exhausted = Some(chunk_id);
        return Ok(());
    };
    if prepared.chunk_id != chunk_id
        || prepared.capacity < active.capacity
        || prepared.capacity > state.config.chunk_capacity_bytes
    {
        return Err(StreamError::Corruption(
            "prepared capacity has invalid chunk bounds".into(),
        ));
    }
    // The append worker has advanced since preparation began. Only capacity
    // belongs to this result; its captured cursors must never replace progress.
    active.capacity = prepared.capacity;
    Ok(())
}
