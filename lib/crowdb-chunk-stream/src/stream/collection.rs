// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded ordered-record reclamation, serialized with owner lifecycle commands.

use super::WorkerState;
use crate::{Result, StreamError};
use std::sync::atomic::Ordering;

pub(super) async fn reclaim_metadata(state: &mut WorkerState, retained: u64, budget: usize) -> Result<u64> {
    state
        .authority
        .as_ref()
        .ok_or(StreamError::WriteStalled)?
        .check()?;
    if retained == 0 || retained > state.snapshot.record().generation || budget == 0 {
        return Err(StreamError::InvalidRequest(
            "Stream retention horizon or budget".into(),
        ));
    }
    let result = state
        .metadata
        .collect(
            state.stream_name,
            retained,
            state.collection_after,
            budget.min(128),
            state.config.gc_bytes_per_pass,
            state.chunks.as_ref(),
        )
        .await?;
    state.collection_after = result.next;
    state
        .metrics
        .reclaimed_bytes
        .fetch_add(result.physical_bytes, Ordering::Relaxed);
    Ok(result.records)
}

pub(super) async fn reclaim_chunks(state: &mut WorkerState) -> Result<u64> {
    let result = state
        .metadata
        .collect(
            state.stream_name,
            state.snapshot.record().generation,
            state.collection_after,
            32,
            state.config.gc_bytes_per_pass,
            state.chunks.as_ref(),
        )
        .await?;
    state.collection_after = result.next;
    Ok(result.physical_bytes)
}
