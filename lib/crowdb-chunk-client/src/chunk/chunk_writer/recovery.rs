// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded mirror-chunk replay after in-place segment replacement is exhausted.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use crowdb_protocol::chunkdb::rpc::{Chunk, DeleteChunkRequest, QueryChunkRequest, Strip};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{parse_frame, set_frame_chunk_id, FrameError, FRAME_FOOTER_BYTES};

use super::{ChunkWriter, MirrorPending};
use crate::chunk::chunk_prefetch::allocate_new_chunk;
use crate::chunk::strip::StripWriter;
use crate::io::FeedStatus;
use crate::{IoError, Result};

impl ChunkWriter {
    /// Replace the whole active mirror chunk after all ordered writes have
    /// stopped. The committed prefix is read one strip at a time; only the
    /// bounded set of uncommitted writes remains in memory.
    pub(super) async fn rotate_failed_mirror(&mut self, partial: Option<Vec<Bytes>>) -> Result<()> {
        let started = Instant::now();
        self.repair_metrics.chunk_rotations.inc();
        let replayed_bytes = self
            .bytes_in_chunk
            .saturating_add(self.current_strip.as_ref().map_or(0, StripWriter::accepted_bytes));
        let old_id = self
            .current_chunk_id()
            .ok_or_else(|| IoError::Internal("mirror rotation has no chunk".into()))?;
        let (old_chunk, pending) = self.collect_mirror_replay(old_id, partial).await?;
        let copy_count = old_chunk
            .strips
            .first()
            .and_then(|strip| match &strip.strip {
                Some(Strip::MirrorStrip(mirror)) => u32::try_from(mirror.segments.len()).ok(),
                _ => None,
            })
            .filter(|copies| *copies > 0)
            .ok_or_else(|| IoError::Internal("mirror rotation has no copy layout".into()))?;

        let attempts = self.config.large_write_repair_attempts.max(1);
        let mut last_error = IoError::ReplicaRepairExhausted("mirror chunk rotation exhausted".into());
        for _ in 0..attempts {
            let candidate = allocate_new_chunk(
                &*self.allocator,
                self.ec_scheme,
                u32::try_from(self.config.read_buffer_size / 1024).unwrap_or(u32::MAX),
                self.config.chunk_type as u8,
                self.config.prefetch_strips_per_chunk,
                Some(copy_count),
            )
            .await?;
            let new_id = candidate
                .id
                .ok_or_else(|| IoError::AllocationFailed("replacement chunk has no ID".into()))?;
            let mut replacement = ChunkWriter::new_with_repair(
                Arc::clone(&self.allocator),
                Arc::clone(&self.disk_writer),
                self.ec_scheme,
                Arc::clone(&self.config),
                Arc::clone(&self.failed_disks),
                Arc::clone(&self.repair_metrics),
            );
            replacement.replaying_mirror = true;
            replacement.framed_input = self.framed_input;
            replacement.mirror_io_concurrency = Arc::clone(&self.mirror_io_concurrency);
            if let Err(error) = replacement.open(candidate, self.object_size) {
                let _ = self
                    .allocator
                    .delete_chunk(DeleteChunkRequest {
                        chunk_id: Some(new_id),
                    })
                    .await;
                return Err(error);
            }
            let replay = replay_chunk(
                &old_chunk,
                self.committed_mirror_strips,
                &pending,
                old_id,
                new_id,
                self,
                &mut replacement,
            )
            .await;
            match replay {
                Ok(()) => {
                    replacement.replaying_mirror = false;
                    replacement.inherit_write_metrics(self);
                    // The new physical bytes are durable. The old unsealed
                    // chunk can no longer be published by this writer.
                    if let Err(error) = self
                        .allocator
                        .delete_chunk(DeleteChunkRequest {
                            chunk_id: Some(old_id),
                        })
                        .await
                    {
                        tracing::warn!(%error, "retired mirror chunk deletion failed");
                    }
                    self.repair_metrics.rotated_chunks.inc();
                    self.repair_metrics.replayed_bytes.inc_by(replayed_bytes);
                    self.repair_metrics
                        .rotation_ns
                        .inc_by(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                    *self = replacement;
                    return Ok(());
                }
                Err(error) => {
                    last_error = error;
                    let _ = replacement.abort().await;
                }
            }
        }
        self.repair_metrics
            .rotation_ns
            .inc_by(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        Err(IoError::ReplicaRepairExhausted(format!(
            "mirror chunk rotation exhausted: {last_error}"
        )))
    }

    async fn collect_mirror_replay(
        &mut self,
        old_id: ChunkId,
        partial: Option<Vec<Bytes>>,
    ) -> Result<(Chunk, VecDeque<Bytes>)> {
        let current_bytes = self.current_strip.as_ref().map_or(0, StripWriter::accepted_bytes);
        let expected_bytes = self.bytes_in_chunk.saturating_add(current_bytes);
        self.prefetch_rx.take();
        self.prefetch_trigger.take();
        self.prefetch_trigger_index = None;
        if let Some(handle) = self.prefetch_handle.take() {
            handle.abort();
            let _ = handle.await;
        }

        let old_chunk = self
            .allocator
            .query_chunk(QueryChunkRequest {
                chunk_id: Some(old_id),
            })
            .await?
            .chunk
            .ok_or_else(|| IoError::ChunkNotFound(format!("{old_id:?}")))?;
        let mut pending = VecDeque::new();
        for completion in self.mirror_completions.drain(..) {
            match completion {
                MirrorPending::Running { handle, buffer } => {
                    // Submitted DiskIO cannot be cancelled. The buffer remains
                    // owned until the task exits, even when an earlier write
                    // already failed.
                    let _ = handle.await;
                    pending.push_back(buffer);
                }
                MirrorPending::Completed(completion) => pending.push_back(completion.buffer),
                MirrorPending::Failed(buffer) => pending.push_back(buffer),
            }
        }
        if let Some(views) = partial {
            pending.extend(views);
        } else if let Some(StripWriter::Mirror(mirror)) = &self.current_strip {
            pending.extend(mirror.replay_views());
        }
        self.current_strip.take();

        let committed_bytes = old_chunk
            .strips
            .iter()
            .take(self.committed_mirror_strips as usize)
            .try_fold(0_u64, |sum, strip| {
                sum.checked_add(u64::from(strip.capacity) * 1024)
                    .ok_or_else(|| IoError::WriteFailed("mirror replay length overflow".into()))
            })?;
        let pending_bytes = pending.iter().try_fold(0_u64, |sum, bytes| {
            sum.checked_add(bytes.len() as u64)
                .ok_or_else(|| IoError::WriteFailed("mirror replay length overflow".into()))
        })?;
        if committed_bytes.saturating_add(pending_bytes) != expected_bytes {
            return Err(IoError::Internal(format!(
                "mirror replay lost bytes: committed={committed_bytes} pending={pending_bytes} expected={expected_bytes}"
            )));
        }
        Ok((old_chunk, pending))
    }

    fn inherit_write_metrics(&mut self, previous: &Self) {
        self.preparation_stalls += previous.preparation_stalls;
        self.preparation_stall_time += previous.preparation_stall_time;
        self.strip_write_successes += previous.strip_write_successes;
        self.strip_write_success_time += previous.strip_write_success_time;
        self.strip_write_success_max = self.strip_write_success_max.max(previous.strip_write_success_max);
        self.mirror_uncommitted_peak = self.mirror_uncommitted_peak.max(previous.mirror_uncommitted_peak);
        self.ec_encode_time += previous.ec_encode_time;
        self.completion_wait_time += previous.completion_wait_time;
    }
}

async fn replay_chunk(
    old_chunk: &Chunk,
    committed_strips: u32,
    pending: &VecDeque<Bytes>,
    old_id: ChunkId,
    new_id: ChunkId,
    old: &ChunkWriter,
    replacement: &mut ChunkWriter,
) -> Result<()> {
    let mut frames = FrameReplay::default();
    for strip in old_chunk.strips.iter().take(committed_strips as usize) {
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(IoError::Internal("committed strip is not a mirror".into()));
        };
        let length = strip.capacity.saturating_mul(1024);
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let mut read_error = None;
        let mut data = None;
        for segment in &mirror.segments {
            match old.disk_writer.read(segment, unit_bytes, 0, length).await {
                Ok(bytes) if bytes.len() == length as usize => {
                    data = Some(bytes);
                    break;
                }
                Ok(_) => read_error = Some(IoError::ReadFailed("short mirror replay read".into())),
                Err(error) => read_error = Some(error),
            }
        }
        let bytes = data.ok_or_else(|| {
            read_error.unwrap_or_else(|| IoError::ReadFailed("mirror replay has no readable copy".into()))
        })?;
        if old.framed_input {
            frames.feed(bytes, old_id, new_id, replacement).await?;
        } else if Box::pin(replacement.push(bytes)).await? == FeedStatus::Pause {
            return Err(IoError::WriteFailed(
                "mirror replay exceeded chunk capacity".into(),
            ));
        }
    }
    for bytes in pending {
        if old.framed_input {
            frames.feed(bytes.clone(), old_id, new_id, replacement).await?;
        } else if Box::pin(replacement.push(bytes.clone())).await? == FeedStatus::Pause {
            return Err(IoError::WriteFailed(
                "mirror replay exceeded chunk capacity".into(),
            ));
        }
    }
    if !frames.pending.is_empty() {
        return Err(IoError::WriteFailed("mirror replay ended inside a frame".into()));
    }
    while !replacement.mirror_completions.is_empty() {
        Box::pin(replacement.commit_oldest_mirror()).await?;
    }
    if let Some(StripWriter::Mirror(mirror)) = &replacement.current_strip {
        if mirror.has_data() {
            mirror.checkpoint().await?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct FrameReplay {
    pending: BytesMut,
}

impl FrameReplay {
    async fn feed(
        &mut self,
        bytes: Bytes,
        old_id: ChunkId,
        new_id: ChunkId,
        replacement: &mut ChunkWriter,
    ) -> Result<()> {
        self.pending.extend_from_slice(&bytes);
        loop {
            let length = match parse_frame(&self.pending, old_id) {
                Ok(frame) => frame.physical_length,
                Err(FrameError::Incomplete { .. }) => break,
                Err(error) => return Err(IoError::WriteFailed(format!("mirror replay frame: {error}"))),
            };
            let mut frame = self.pending.split_to(length);
            set_frame_chunk_id(new_id, &mut frame[length - FRAME_FOOTER_BYTES..])
                .map_err(|error| IoError::WriteFailed(format!("mirror replay footer: {error}")))?;
            if Box::pin(replacement.push(frame.freeze())).await? == FeedStatus::Pause {
                return Err(IoError::WriteFailed(
                    "mirror replay exceeded chunk capacity".into(),
                ));
            }
        }
        Ok(())
    }
}

pub(super) fn rewrite_frames(bytes: &Bytes, old_id: ChunkId, new_id: ChunkId) -> Result<Bytes> {
    let mut rewritten = BytesMut::from(bytes.as_ref());
    let mut offset = 0;
    while offset < rewritten.len() {
        let frame = parse_frame(&rewritten[offset..], old_id)
            .map_err(|error| IoError::WriteFailed(format!("mirror input frame: {error}")))?;
        let length = frame.physical_length;
        let footer = offset + length - FRAME_FOOTER_BYTES;
        set_frame_chunk_id(new_id, &mut rewritten[footer..footer + FRAME_FOOTER_BYTES])
            .map_err(|error| IoError::WriteFailed(format!("mirror input footer: {error}")))?;
        offset += length;
    }
    Ok(rewritten.freeze())
}
