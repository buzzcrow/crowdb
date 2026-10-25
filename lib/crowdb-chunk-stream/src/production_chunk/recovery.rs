// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recover the complete mirrored prefix beyond its persisted checkpoint.

use super::{io_error, update_state, Ordering, ProductionStreamChunkStore};
use crate::{ActiveChunkDescriptor, RecoveredStreamTail, Result, StreamError, StreamName};
use async_trait::async_trait;
use crowdb_chunk_client::{replay_frame_prefix, select_frame_prefix, FrameExtentSink, MirroredFrameReaders};
use crowdb_protocol::chunkdb::rpc::{ChunkState, QueryChunkRequest};
use crowdb_protocol::frame::{ScannedFrameExtent, MAX_FRAME_SCAN_INPUT_BYTES};
use std::{sync::Arc, time::Duration};

struct RecoveredExtents(Vec<ScannedFrameExtent>);

#[async_trait]
impl FrameExtentSink for RecoveredExtents {
    async fn append_extents(&mut self, extents: &[ScannedFrameExtent]) -> crowdb_chunk_client::Result<()> {
        self.0.extend_from_slice(extents);
        Ok(())
    }
}

impl ProductionStreamChunkStore {
    pub(super) async fn recover_chunk_tail(
        &self,
        name: StreamName,
        epoch: u64,
        active: &ActiveChunkDescriptor,
    ) -> Result<RecoveredStreamTail> {
        let context = self.owner_context(name, epoch)?.ok_or(StreamError::StaleWriter)?;
        context.check_io()?;
        let chunk = self
            .allocator
            .query_chunk(QueryChunkRequest {
                chunk_id: Some(active.chunk_id),
            })
            .await
            .map_err(io_error)?
            .chunk
            .ok_or_else(|| StreamError::ReadUnavailable("recovery chunk is missing".into()))?;
        if chunk.id != Some(active.chunk_id)
            || chunk.writer_epoch > epoch
            || chunk.owner_key != name.chunk_owner_key()
        {
            return Err(StreamError::StaleWriter);
        }
        let state = self.install(chunk.clone())?;
        let deadline = tokio::time::Instant::from_std(context.deadline())
            .min(tokio::time::Instant::now() + Duration::from_secs(60));
        for strip in &chunk.strips {
            let Some(crowdb_protocol::chunkdb::rpc::Strip::MirrorStrip(mirror)) = &strip.strip else {
                return Err(StreamError::Corruption("recovery chunk is not mirrored".into()));
            };
            for segment in &mirror.segments {
                context.check_io()?;
                self.disk_writer
                    .drain_segment(segment, u64::from(strip.unit_kb) * 1024, deadline.into_std())
                    .await
                    .map_err(io_error)?;
            }
        }
        let readers =
            MirroredFrameReaders::new(Arc::new(chunk.clone()), name, self.disk_writer.clone(), deadline)
                .map_err(|error| observation_error(&error))?;
        let mut config = readers
            .scan_config(
                active.acknowledged_cursor,
                active.logical_start,
                chunk.acknowledged_cursor.max(active.acknowledged_cursor),
                self.chunk_capacity_bytes,
            )
            .map_err(|error| observation_error(&error))?;
        if chunk.state == ChunkState::Sealed as i32 {
            config.physical_limit = chunk.acknowledged_cursor;
        }
        let selection = select_frame_prefix(
            readers.readers(),
            config,
            u32::try_from(MAX_FRAME_SCAN_INPUT_BYTES).expect("scan window fits u32"),
            deadline,
        )
        .await
        .map_err(|error| observation_error(&error))?;
        let mut extents = RecoveredExtents(Vec::new());
        let observed = replay_frame_prefix(
            readers.readers()[selection.selected_index()].as_ref(),
            selection.observation(),
            deadline,
            &mut extents,
        )
        .await
        .map_err(|error| observation_error(&error))?;
        let refreshed = self
            .allocator
            .query_chunk(QueryChunkRequest {
                chunk_id: Some(active.chunk_id),
            })
            .await
            .map_err(io_error)?
            .chunk;
        if refreshed.as_ref() != Some(&chunk) {
            return Err(StreamError::StaleWriter);
        }
        context.check_io()?;
        update_state(&state, chunk);
        state
            .synchronized_cursor
            .fetch_max(observed.summary().physical_end, Ordering::AcqRel);
        Ok(RecoveredStreamTail {
            physical_end: observed.summary().physical_end,
            logical_end: observed.summary().logical_end,
            extents: extents.0,
        })
    }
}

fn observation_error(error: &crowdb_chunk_client::FrameObservationError) -> StreamError {
    use crowdb_chunk_client::FrameObservationError as E;
    match error {
        E::InvalidLayout(_)
        | E::InvalidRequest
        | E::InvalidPrefix(_)
        | E::ConflictingPrefixes { .. }
        | E::ChangedPrefix => StreamError::Corruption(error.to_string()),
        _ => StreamError::ReadUnavailable(error.to_string()),
    }
}
