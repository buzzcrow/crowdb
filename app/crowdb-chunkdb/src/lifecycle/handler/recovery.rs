// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Replay persisted block intents without admitting requests before completion.

use super::{extract_segments, unix_time_ms, ChunkState, LifecycleError, LifecycleHandler, ProtoChunkState};
use crate::allocator::pool::BlockMutationError;
use std::collections::HashSet;

impl LifecycleHandler {
    /// Finish durable `Init` allocation intents left by an interrupted commit.
    pub async fn reconcile_pending_chunks(&self) -> Result<u64, LifecycleError> {
        let mut start_after = None;
        let mut reconciled = 0u64;
        loop {
            let chunks = self.list_chunks(start_after.as_ref(), 1_000).await?;
            if chunks.is_empty() {
                break;
            }
            for mut chunk in chunks.iter().cloned() {
                if !chunk.cleanup_intents.is_empty() {
                    let intent_count = chunk.cleanup_intents.len();
                    let current_segments: HashSet<_> =
                        chunk.strips.iter().flat_map(extract_segments).collect();
                    let now_ms = unix_time_ms();
                    let mut pending = Vec::new();
                    for intent in std::mem::take(&mut chunk.cleanup_intents) {
                        if intent.not_before_ms > now_ms {
                            pending.push(intent);
                            continue;
                        }
                        let retired: Vec<_> = intent
                            .retired_segments
                            .iter()
                            .filter(|segment| !current_segments.contains(segment))
                            .copied()
                            .collect();
                        self.allocator
                            .pool()
                            .free_blocks_for_recovery(retired)
                            .await
                            .map_err(|error| recovery_error(&error, false))?;
                    }
                    chunk.cleanup_intents = pending;
                    if chunk.cleanup_intents.len() != intent_count {
                        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
                        self.store.put_chunk(&chunk).await?;
                        if let (Some(locks), Some(chunk_id)) = (&self.locks, chunk.id) {
                            locks.populate_cache(&chunk_id, chunk.clone());
                        }
                        reconciled = reconciled.saturating_add(1);
                    }
                }
                match ChunkState::from_proto(chunk.state) {
                    ChunkState::Init => {
                        self.allocator
                            .pool()
                            .commit_blocks_for_recovery(
                                chunk.strips.iter().flat_map(extract_segments).collect(),
                            )
                            .await
                            .map_err(|error| recovery_error(&error, true))?;
                        chunk.state = ProtoChunkState::Active as i32;
                        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
                    }
                    ChunkState::Deleted if !chunk.strips.is_empty() => {
                        let segments = chunk.strips.iter().flat_map(extract_segments).collect();
                        self.allocator
                            .pool()
                            .free_blocks_for_recovery(segments)
                            .await
                            .map_err(|error| recovery_error(&error, false))?;
                        chunk.strips.clear();
                        chunk.capacity = 0;
                        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
                    }
                    _ => continue,
                }
                self.store.put_chunk(&chunk).await?;
                if let (Some(locks), Some(chunk_id)) = (&self.locks, chunk.id) {
                    locks.populate_cache(&chunk_id, chunk);
                }
                reconciled += 1;
            }
            start_after = chunks.last().and_then(|chunk| chunk.id);
            if chunks.len() < 1_000 {
                break;
            }
        }
        Ok(reconciled)
    }
}

fn recovery_error(error: &BlockMutationError, commit: bool) -> LifecycleError {
    if error.is_retryable() {
        LifecycleError::RecoveryDependency(error.to_string())
    } else if commit {
        LifecycleError::Commit(error.to_string())
    } else {
        LifecycleError::Cleanup(error.to_string())
    }
}
