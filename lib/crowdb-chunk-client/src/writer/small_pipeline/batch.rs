use super::{
    batch_shape, encode_frame, BytesMut, FrameMagic, IoError, Location, MirrorBatchStats, OwnedChunk,
    PendingObject, Result, SmallWriteMetrics, SystemTime, UNIX_EPOCH,
};

impl OwnedChunk {
    pub(super) async fn try_write_batch(
        &mut self,
        batch: &[PendingObject],
        metrics: &SmallWriteMetrics,
    ) -> Result<Vec<Location>> {
        let (physical_bytes, logical_bytes, buffer_count) = batch_shape(batch)?;
        let planned_cursor = self.cursor.saturating_add(physical_bytes as u64);
        self.consume_staged_reservation(planned_cursor).await?;
        let strip = self.current_strip()?.clone();
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        if physical_bytes as u64 > self.remaining_in_strip()
            || physical_bytes as u64 > self.remaining_in_chunk()
        {
            return Err(IoError::Internal(
                "assembled batch crosses mirror strip or chunk".into(),
            ));
        }
        let start = self.cursor;
        let strip_bytes = usize::try_from(strip.capacity)
            .unwrap_or(usize::MAX)
            .saturating_mul(1024);
        let strip_start = u64::from(strip.chunk_offset) * 1024;
        let block_offset = start.saturating_sub(strip_start);
        let block_offset_us = usize::try_from(block_offset).unwrap_or(usize::MAX);

        // Single shadow buffer: allocated once with full strip capacity, no
        // zeroing. Fragments are copied in sequentially; each batch sends a
        // view (slice) of the written portion, not the whole buffer.
        let mut shadow = self.take_shadow(strip_bytes, block_offset_us);

        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
        let locations = pack_batch(batch, chunk_id, start, &mut shadow)?;
        let written_end = block_offset_us + physical_bytes;
        debug_assert_eq!(shadow.len(), written_end);

        // Freeze the buffer, take a view of the written portion, and send
        // views to mirrors. After all mirrors complete, reclaim the buffer.
        for (object, location) in batch.iter().zip(&locations) {
            if let Some(intent) = &object.intent {
                intent.before_write(location).await?;
            }
        }
        let frozen = shadow.freeze();
        let view = frozen.slice(block_offset_us..written_end);
        let full_image = frozen.slice(0..written_end);
        let (_, write_result) = self
            .write_mirrors_with_repair(
                &strip,
                view,
                full_image,
                unit_bytes,
                block_offset,
                MirrorBatchStats {
                    object_count: batch.len(),
                    buffer_count,
                    logical_bytes,
                },
            )
            .await;
        self.shadow = Some(
            frozen
                .try_into_mut()
                .unwrap_or_else(|shared| BytesMut::from(shared.as_ref())),
        );
        write_result?;
        let end = start + physical_bytes as u64;
        let strip_end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
        let closed = (end == strip_end).then_some(strip.strip_sequence);
        self.cursor = end;
        if let Some(sequence) = closed {
            let closed_strip = self
                .chunk
                .strips
                .iter()
                .find(|current| current.strip_sequence == sequence)
                .cloned()
                .ok_or_else(|| IoError::MetadataConflict("closed mirror strip disappeared".into()))?;
            self.schedule_closed_advance(end, sequence);
            if let Err(error) = self.retain_closed_strip(closed_strip).await {
                tracing::warn!(%error, "mirror-to-EC fast path deferred to chunkdb");
            }
        } else {
            self.refresh_pending_advance().await?;
            self.start_pending_advance(end)?;
        }
        self.confirm_batch_publication(batch, end).await?;
        metrics.record_batch(batch.len(), logical_bytes);
        Ok(locations)
    }
}

fn pack_batch(
    batch: &[PendingObject],
    chunk_id: super::ChunkId,
    start: u64,
    shadow: &mut BytesMut,
) -> Result<Vec<Location>> {
    let mut copied = 0usize;
    let mut locations = Vec::with_capacity(batch.len());
    let write_time_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        });
    for object in batch {
        let mut payload = Vec::with_capacity(object.len);
        for fragment in &object.fragments {
            payload.extend_from_slice(fragment);
        }
        let frame = encode_frame(FrameMagic::RepoSmallV1, chunk_id, &payload, write_time_ms)
            .map_err(|error| IoError::WriteFailed(error.to_string()))?;
        let frame_length = frame.len();
        shadow.extend_from_slice(&frame);
        locations.push(Location {
            chunk_id: Some(chunk_id),
            offset: start + copied as u64,
            length: frame_length as u64,
            logical_offset: 0,
            logical_length: object.len as u64,
        });
        copied += frame_length;
    }
    Ok(locations)
}
