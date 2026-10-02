use std::collections::VecDeque;

use super::{
    batch_shape, Bytes, FrameMagic, IoError, Location, OwnedChunk, PendingObject, Result, SmallWriteMetrics,
    SystemTime, UNIX_EPOCH,
};

impl OwnedChunk {
    pub(super) async fn try_write_batch(
        &mut self,
        batch: &mut [PendingObject],
        metrics: &SmallWriteMetrics,
    ) -> Result<Vec<Location>> {
        let (physical_bytes, logical_bytes, buffer_count) = batch_shape(batch)?;
        if physical_bytes as u64 > self.remaining_in_chunk() {
            return Err(IoError::Internal("assembled batch crosses chunk".into()));
        }
        let start = self.cursor;
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
        let (locations, views) = pack_batch(batch, chunk_id, start)?;
        for (object, location) in batch.iter_mut().zip(&locations) {
            if let Some(intent) = &object.intent {
                intent.before_write(location).await?;
            }
        }
        let mut remaining: VecDeque<Bytes> = views.into();
        let mut first = true;
        while !remaining.is_empty() {
            self.ensure_strip().await?;
            let views = take_views(&mut remaining, self.remaining_in_strip() as usize);
            self.write_view_strip(
                views,
                if first {
                    (batch.len(), logical_bytes, buffer_count)
                } else {
                    (0, 0, 0)
                },
                metrics,
            )
            .await?;
            first = false;
            self.prefetch_at_half();
        }
        if self.cursor != start + physical_bytes as u64 {
            return Err(IoError::Internal("shared batch physical length mismatch".into()));
        }
        self.confirm_batch_publication(batch, self.cursor).await?;
        metrics.record_batch(batch.len(), logical_bytes);
        Ok(locations)
    }

    async fn write_view_strip(
        &mut self,
        views: Vec<Bytes>,
        counts: (usize, usize, usize),
        metrics: &SmallWriteMetrics,
    ) -> Result<()> {
        let strip = self.current_strip()?.clone();
        let written: usize = views.iter().map(Bytes::len).sum();
        let end = self.cursor + written as u64;
        self.consume_staged_reservation(end).await?;
        let strip_start = u64::from(strip.chunk_offset) * 1024;
        let block_offset = self.cursor - strip_start;
        if let Some(shadow) = self.shadow.take() {
            self.metrics
                .shadow_bytes
                .fetch_sub(shadow.capacity() as u64, std::sync::atomic::Ordering::Relaxed);
            self.retained_views.push(shadow.freeze());
        }
        let retained_count = self.retained_views.len();
        self.retained_views.extend(views.iter().cloned());
        let Some(super::Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(IoError::Internal("shared strip is not mirrored".into()));
        };
        metrics.record_aggregate_write(
            mirror.segments.len() as u64,
            counts.0,
            counts.2,
            counts.1,
            written,
        );
        let result = self
            .mirror_flow
            .write_views(
                &mut self.chunk,
                self.cursor,
                strip.strip_sequence,
                block_offset,
                views,
                &self.retained_views,
                &mut self.pending_advance,
            )
            .await;
        if result.is_err() {
            self.retained_views.truncate(retained_count);
        }
        result?;
        self.cursor = end;
        let strip_end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
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

fn take_views(source: &mut VecDeque<Bytes>, mut length: usize) -> Vec<Bytes> {
    let mut views = Vec::new();
    while length > 0 {
        let Some(mut view) = source.pop_front() else {
            break;
        };
        let take = length.min(view.len());
        views.push(view.split_to(take));
        if !view.is_empty() {
            source.push_front(view);
        }
        length -= take;
    }
    views
}

fn pack_batch(
    batch: &mut [PendingObject],
    chunk_id: super::ChunkId,
    start: u64,
) -> Result<(Vec<Location>, Vec<Bytes>)> {
    let mut views = Vec::new();
    let mut copied = 0usize;
    let mut locations = Vec::with_capacity(batch.len());
    let write_time_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        });
    for object in batch {
        let frame_length = if let Some(owner) = &mut object.framed {
            let mut end = 0;
            let mut logical = 0usize;
            for index in 0..owner.frame_count() {
                logical += owner
                    .frame_payload_len(index)
                    .ok_or_else(|| IoError::WriteFailed("missing frame payload".into()))?;
                let range = owner
                    .finalize_frame(index, FrameMagic::RepoSmallV1, chunk_id, write_time_ms)
                    .map_err(|error| IoError::WriteFailed(error.to_string()))?;
                if range.start != end {
                    return Err(IoError::WriteFailed(
                        "small object frames are not contiguous".into(),
                    ));
                }
                end = range.end;
            }
            if logical != object.len || end != super::frame_bytes(object.len)? {
                return Err(IoError::WriteFailed(
                    "small object frame geometry mismatch".into(),
                ));
            }
            views.extend(
                owner
                    .views(0..end)
                    .map_err(|error| IoError::WriteFailed(error.to_string()))?,
            );
            end
        } else {
            let mut fragments: VecDeque<Bytes> = object.fragments.iter().cloned().collect();
            let mut remaining = object.len;
            while remaining > 0 {
                let length = remaining.min(crowdb_protocol::frame::MAX_FRAME_PAYLOAD_BYTES);
                let payload = take_views(&mut fragments, length);
                if payload.iter().map(Bytes::len).sum::<usize>() != length {
                    return Err(IoError::SourceRead(
                        "small object fragments are incomplete".into(),
                    ));
                }
                views.extend(
                    crowdb_protocol::frame::encode_frame_views(
                        FrameMagic::RepoSmallV1,
                        chunk_id,
                        payload,
                        write_time_ms,
                    )
                    .map_err(|error| IoError::WriteFailed(error.to_string()))?,
                );
                remaining -= length;
            }
            super::frame_bytes(object.len)?
        };
        locations.push(Location {
            chunk_id: Some(chunk_id),
            offset: start + copied as u64,
            length: frame_length as u64,
            logical_offset: 0,
            logical_length: object.len as u64,
        });
        copied += frame_length;
    }
    Ok((locations, views))
}
