// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish only the changed tail chunk record at ordered lifecycle barriers.

use super::{state::StreamViewState, ReadView, WorkerState};
use crate::perf::{perf, PhaseTimer};
use crate::{Result, StreamError};
use crowdb_protocol::chunk_stream::{StreamChunkRecord, StreamChunkState, StreamMappings};
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

pub(super) async fn publish_state(state: &mut WorkerState) -> Result<()> {
    state
        .authority
        .as_ref()
        .ok_or(StreamError::WriteStalled)?
        .check()?;
    super::checkpoint::flush(state).await?;
    let _timer = PhaseTimer::start(&perf().publish);
    let build = PhaseTimer::start(&perf().record_build);
    let (tail, previous) = build_records(state)?;
    drop(build);
    let candidate = if previous.is_some() {
        state
            .metadata
            .candidates(state.stream_name)
            .await?
            .into_iter()
            .find(|candidate| candidate.record().chunk_id == tail.chunk_id)
    } else {
        None
    };
    let snapshot = loop {
        state
            .authority
            .as_ref()
            .ok_or(StreamError::WriteStalled)?
            .check()?;
        match state
            .metadata
            .publish(
                Some(&state.snapshot),
                tail.clone(),
                previous.clone(),
                candidate.as_ref(),
            )
            .await
        {
            Ok(snapshot) => break snapshot,
            Err(
                error @ (StreamError::Corruption(_)
                | StreamError::InvalidRequest(_)
                | StreamError::StaleWriter),
            ) => {
                state.stalled = true;
                return Err(error);
            }
            Err(error) => {
                state.log_unavailable("chunk_record_publish", &error);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    state.progress.generation = snapshot.record().generation;
    state.snapshot = snapshot;
    state.extents.clear();
    let published = Arc::new(StreamViewState::new(
        state.snapshot.clone(),
        state.metadata.group_id(),
    ));
    let view = ReadView::new(state.metadata.clone(), published.clone()).await?;
    let previous_view = state.read_view.swap(Arc::new(view));
    if let Ok(previous_view) = Arc::try_unwrap(previous_view) {
        previous_view.pin.release().await;
    }
    state.published_view.store(published);
    state
        .metrics
        .metadata_publications
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn build_records(state: &WorkerState) -> Result<(StreamChunkRecord, Option<StreamChunkRecord>)> {
    let mut completed = state.snapshot.record().clone();
    let mut lengths = completed.mappings.lengths().map_err(codec_error)?;
    for extent in &state.extents {
        if extent.chunk_id != completed.chunk_id {
            return Err(StreamError::Corruption(
                "unpublished mappings span multiple chunks".into(),
            ));
        }
        lengths.push(
            u32::try_from(extent.logical_end - extent.logical_start)
                .map_err(|_| StreamError::Corruption("append length exceeds u32".into()))?,
        );
    }
    completed.mappings = StreamMappings::new(&lengths).map_err(codec_error)?;
    completed.logical_end = Some(state.progress.logical_tail);
    completed.writer_epoch = state.writer_epoch;
    completed.owner_session = state.owner_session;
    completed.generation = state
        .snapshot
        .record()
        .generation
        .checked_add(1)
        .ok_or_else(|| StreamError::Corruption("Stream publication generation overflow".into()))?;
    completed.trim_offset = state.progress.trim_offset;
    completed.closed = state.progress.closed;
    if let Some(active) = &state.progress.active {
        if active.chunk_id != completed.chunk_id {
            completed.state = StreamChunkState::Sealed;
            let tail = StreamChunkRecord {
                stream_id: state.stream_name,
                purpose: state.progress.purpose,
                chunk_id: active.chunk_id,
                state: StreamChunkState::Active,
                logical_start: Some(state.progress.logical_tail),
                logical_end: Some(state.progress.logical_tail),
                sequence: completed
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| StreamError::Corruption("Stream chunk sequence overflow".into()))?,
                writer_epoch: state.writer_epoch,
                generation: completed.generation,
                owner_session: state.owner_session,
                trim_offset: state.progress.trim_offset,
                closed: false,
                mappings: StreamMappings::default(),
            };
            return Ok((tail, Some(completed)));
        }
        completed.state = StreamChunkState::Active;
    } else {
        completed.state = StreamChunkState::Sealed;
    }
    Ok((completed, None))
}

fn codec_error(error: impl std::fmt::Display) -> StreamError {
    StreamError::Corruption(error.to_string())
}
