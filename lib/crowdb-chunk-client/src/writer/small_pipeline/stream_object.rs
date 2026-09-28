use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{
    encode_frame, FrameMagic, FRAME_FOOTER_BYTES, FRAME_HEADER_PREFIX_BYTES, MAX_FRAME_BYTES,
    MAX_FRAME_PAYLOAD_BYTES,
};

use super::{
    IoError, MirrorBatchStats, OwnedChunk, PendingObject, PipelineWorker, Result, SmallWriteMetrics,
};

pub(super) fn physical_bytes(logical: usize) -> Result<usize> {
    logical
        .checked_add(
            logical
                .div_ceil(MAX_FRAME_PAYLOAD_BYTES)
                .checked_mul(FRAME_HEADER_PREFIX_BYTES + FRAME_FOOTER_BYTES)
                .ok_or_else(|| IoError::WriteFailed("shared object frame length overflow".into()))?,
        )
        .ok_or_else(|| IoError::WriteFailed("shared object frame length overflow".into()))
}

impl PipelineWorker {
    pub(super) async fn ensure_stream_object_fits(&mut self, physical: usize) -> Result<()> {
        if physical as u64 > self.runtime.policy.chunk_capacity {
            return Err(IoError::ObjectTooLarge {
                size: physical,
                limit: usize::try_from(self.runtime.policy.chunk_capacity).unwrap_or(usize::MAX),
            });
        }
        if let Ok(strip) = self.chunk.current_strip() {
            if self.chunk.cursor > u64::from(strip.chunk_offset) * 1024 {
                self.chunk.close_strip(&self.runtime.metrics).await?;
            }
        }
        self.chunk.ensure_strip().await?;
        if self.chunk.remaining_in_chunk() < physical as u64 {
            let replacement = match self.replacement.take() {
                Some(chunk) => chunk,
                None => {
                    OwnedChunk::allocate(&self.runtime, Arc::clone(&self.route.conversion_active)).await?
                }
            };
            self.chunk.finish().await?;
            self.chunk = replacement;
            self.chunk.ensure_strip().await?;
        }
        Ok(())
    }
}

struct FragmentCursor<'a> {
    receiver: &'a mut tokio::sync::mpsc::Receiver<Bytes>,
    current: Option<Bytes>,
    offset: usize,
    received: usize,
}

impl FragmentCursor<'_> {
    async fn take(&mut self, length: usize) -> Result<Vec<u8>> {
        let mut output = Vec::with_capacity(length);
        while output.len() < length {
            if self.current.is_none() {
                self.current = self.receiver.recv().await;
                self.received += 1;
            }
            let fragment = self.current.as_ref().ok_or_else(|| {
                IoError::SourceRead("shared object body ended before its declared length".into())
            })?;
            let count = (length - output.len()).min(fragment.len() - self.offset);
            output.extend_from_slice(&fragment[self.offset..self.offset + count]);
            self.offset += count;
            if self.offset == fragment.len() {
                self.current = None;
                self.offset = 0;
            }
        }
        Ok(output)
    }
}

impl OwnedChunk {
    pub(super) async fn try_write_stream_object(
        &mut self,
        object: &mut PendingObject,
        metrics: &SmallWriteMetrics,
    ) -> Result<Location> {
        let start = self.cursor;
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing ID".into()))?;
        let physical = physical_bytes(object.len)?;
        let location = Location {
            chunk_id: Some(chunk_id),
            offset: start,
            length: physical as u64,
            logical_offset: 0,
            logical_length: object.len as u64,
        };
        if let Some(intent) = &object.intent {
            intent.before_write(&location).await?;
        }
        let mut source = FragmentCursor {
            receiver: object
                .stream
                .as_mut()
                .ok_or_else(|| IoError::Internal("shared object stream missing receiver".into()))?,
            current: None,
            offset: 0,
            received: 0,
        };
        let mut remaining = object.len;
        let write_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            });
        while remaining > 0 {
            self.write_stream_strip(&mut source, &mut remaining, chunk_id, write_time_ms)
                .await?;
        }
        if self.cursor - start != physical as u64 {
            return Err(IoError::Internal("shared object physical length mismatch".into()));
        }
        self.flush_pending_advance().await?;
        if self.chunk.acknowledged_cursor < self.cursor {
            self.start_pending_advance(self.cursor)?;
            self.flush_pending_advance().await?;
        }
        if self.chunk.acknowledged_cursor < self.cursor {
            return Err(IoError::MetadataConflict(
                "shared object cursor was not published".into(),
            ));
        }
        self.confirm_batch_publication(std::slice::from_ref(object), self.cursor)
            .await?;
        metrics.record_batch(1, object.len);
        Ok(location)
    }

    async fn write_stream_strip(
        &mut self,
        source: &mut FragmentCursor<'_>,
        remaining: &mut usize,
        chunk_id: ChunkId,
        write_time_ms: u64,
    ) -> Result<()> {
        self.ensure_strip().await?;
        let strip = self.current_strip()?.clone();
        let strip_start = u64::from(strip.chunk_offset) * 1024;
        let strip_end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
        let block_offset = self.cursor - strip_start;
        let frame_capacity = (strip_end - self.cursor) as usize / MAX_FRAME_BYTES;
        if frame_capacity == 0 {
            return Err(IoError::Internal("shared object lost frame alignment".into()));
        }
        let mut shadow = self.take_shadow(strip.capacity as usize * 1024, block_offset as usize);
        let before = shadow.len();
        let mut logical = 0usize;
        for _ in 0..frame_capacity {
            if *remaining == 0 {
                break;
            }
            let length = (*remaining).min(MAX_FRAME_PAYLOAD_BYTES);
            let payload = source.take(length).await?;
            let frame = encode_frame(FrameMagic::RepoSmallV1, chunk_id, &payload, write_time_ms)
                .map_err(|error| IoError::WriteFailed(error.to_string()))?;
            shadow.extend_from_slice(&frame);
            *remaining -= length;
            logical += length;
        }
        let written = shadow.len() - before;
        let end = self.cursor + written as u64;
        self.consume_staged_reservation(end).await?;
        let frozen = shadow.freeze();
        let view = frozen.slice(before..before + written);
        let image = frozen.slice(0..before + written);
        let (_, result) = self
            .write_mirrors_with_repair(
                &strip,
                view,
                image,
                u64::from(strip.unit_kb) * 1024,
                block_offset,
                MirrorBatchStats {
                    object_count: 1,
                    buffer_count: source.received,
                    logical_bytes: logical,
                },
            )
            .await;
        self.shadow = Some(
            frozen
                .try_into_mut()
                .unwrap_or_else(|shared| bytes::BytesMut::from(shared.as_ref())),
        );
        result?;
        self.cursor = end;
        if end == strip_end {
            let closed = self
                .chunk
                .strips
                .iter()
                .find(|current| current.strip_sequence == strip.strip_sequence)
                .cloned()
                .ok_or_else(|| IoError::MetadataConflict("closed mirror strip disappeared".into()))?;
            self.schedule_closed_advance(end, strip.strip_sequence);
            if let Err(error) = self.retain_closed_strip(closed).await {
                tracing::warn!(%error, "mirror-to-EC fast path deferred to chunkdb");
            }
        } else {
            self.refresh_pending_advance().await?;
            self.start_pending_advance(end)?;
        }
        Ok(())
    }
}
