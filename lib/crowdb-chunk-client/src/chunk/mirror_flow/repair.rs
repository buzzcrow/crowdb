use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::{
    AllocateReplacementSegmentRequest, Chunk, DiscardReplacementSegmentRequest, QueryChunkRequest,
    ReplaceChunkStripRangeRequest, Strip,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;

use super::MirrorStripFlow;
use crate::{IoError, Result};

impl MirrorStripFlow {
    #[allow(clippy::too_many_lines)]
    pub(super) async fn repair(
        &self,
        chunk: &mut Chunk,
        committed_cursor: u64,
        strip_sequence: u32,
        failed: Segment,
        image: Bytes,
        unit_bytes: u64,
    ) -> Result<()> {
        let started = Instant::now();
        if let Some(metrics) = &self.metrics {
            metrics.active_repairs.fetch_add(1, Ordering::Relaxed);
        }
        let result = self
            .try_repair(chunk, committed_cursor, strip_sequence, failed, image, unit_bytes)
            .await;
        if let Some(metrics) = &self.metrics {
            let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            metrics.active_repairs.fetch_sub(1, Ordering::Relaxed);
            metrics.repair_latency_ns.fetch_add(elapsed, Ordering::Relaxed);
            metrics
                .max_repair_latency_ns
                .fetch_max(elapsed, Ordering::Relaxed);
        }
        result
    }

    #[allow(clippy::too_many_lines)]
    async fn try_repair(
        &self,
        chunk: &mut Chunk,
        committed_cursor: u64,
        strip_sequence: u32,
        failed: Segment,
        image: Bytes,
        unit_bytes: u64,
    ) -> Result<()> {
        let failed_disk = failed
            .disk_id
            .ok_or_else(|| IoError::Internal("failed mirror segment has no disk id".into()))?;
        tracing::warn!(
            chunk_id = ?chunk.id,
            strip_sequence,
            failed_segment = ?failed,
            "mirror strip replica failed; attempting replacement"
        );
        self.failed_disks.insert(failed_disk);
        let mut last_error = "no replacement attempt completed".to_string();
        for _ in 0..self.attempts {
            if let Some(metrics) = &self.metrics {
                metrics.repair_attempts.fetch_add(1, Ordering::Relaxed);
            }
            let strip_index = chunk
                .strips
                .iter()
                .position(|strip| strip.strip_sequence == strip_sequence)
                .ok_or_else(|| IoError::MetadataConflict("mirror strip disappeared during repair".into()))?;
            let old_strip = chunk.strips[strip_index].clone();
            let Some(Strip::MirrorStrip(mut mirror)) = old_strip.strip.clone() else {
                return Err(IoError::MetadataConflict(
                    "mirror strip changed type during repair".into(),
                ));
            };
            let survivors = mirror
                .segments
                .iter()
                .copied()
                .filter(|segment| *segment != failed)
                .collect();
            let excluded = self.failed_disks.live();
            if let Some(metrics) = &self.metrics {
                metrics
                    .negative_list_hits
                    .fetch_add(excluded.len() as u64, Ordering::Relaxed);
            }
            let allocation = self
                .allocator
                .allocate_replacement_segment(AllocateReplacementSegmentRequest {
                    chunk_id: chunk.id,
                    old_segment: Some(failed),
                    surviving_segments: survivors,
                    exclude_disk_ids: excluded,
                })
                .await;
            let response = match allocation {
                Ok(response) => response,
                Err(error) => {
                    last_error = error.to_string();
                    continue;
                }
            };
            let Some(replacement) = response.segment else {
                last_error = "replacement allocation returned no segment".into();
                continue;
            };
            let write = self
                .disk_writer
                .write_at_byte_offset(&replacement, unit_bytes, 0, image.clone())
                .await;
            let sync = if write.is_ok() && self.sync {
                self.disk_writer.fsync(&replacement).await
            } else {
                Ok(())
            };
            if let Err(error) = write.and(sync) {
                last_error = error.to_string();
                if let Some(disk) = replacement.disk_id {
                    self.failed_disks.insert(disk);
                }
                self.discard(chunk.id, replacement).await;
                continue;
            }
            let Some(slot) = mirror.segments.iter_mut().find(|segment| **segment == failed) else {
                self.discard(chunk.id, replacement).await;
                return Err(IoError::MetadataConflict(
                    "failed segment no longer belongs to strip".into(),
                ));
            };
            *slot = replacement;
            let mut new_strip = old_strip.clone();
            new_strip.strip = Some(Strip::MirrorStrip(mirror));
            let operation_id = ChunkId {
                high: self.writer_epoch ^ chunk.modify_ts,
                low: u64::from(strip_sequence) ^ failed.unit_offset,
            };
            let request = ReplaceChunkStripRangeRequest {
                chunk_id: chunk.id,
                expected_modify_ts: chunk.modify_ts,
                start_index: u32::try_from(strip_index).unwrap_or(u32::MAX),
                old_strips: vec![old_strip],
                replacement_strips: vec![new_strip.clone()],
                operation_id: Some(operation_id),
            };
            match self.publish(request, replacement).await {
                Ok(updated) => {
                    if updated
                        .strips
                        .iter()
                        .any(|strip| strip.strip_sequence == strip_sequence)
                    {
                        *chunk = updated;
                    } else {
                        chunk.strips[strip_index] = new_strip;
                        chunk.modify_ts = updated.modify_ts;
                        chunk.cleanup_intents = updated.cleanup_intents;
                        chunk.last_strip_replacement = updated.last_strip_replacement;
                    }
                    if let Some(metrics) = &self.metrics {
                        metrics.repaired_replicas.fetch_add(1, Ordering::Relaxed);
                        metrics.repairs_avoiding_rotation.fetch_add(1, Ordering::Relaxed);
                    }
                    tracing::info!(
                        chunk_id = ?chunk.id,
                        strip_sequence,
                        failed_segment = ?failed,
                        replacement_segment = ?replacement,
                        "mirror strip replica replaced"
                    );
                    return Ok(());
                }
                Err(error @ IoError::MetadataConflict(_)) => return Err(error),
                Err(_) => break,
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics.exhausted_repairs.fetch_add(1, Ordering::Relaxed);
        }
        self.mark_unavailable(chunk, committed_cursor, strip_sequence, failed)
            .await?;
        Err(IoError::WriteFailed(format!(
            "mirror replica repair exhausted: {last_error}"
        )))
    }

    async fn publish(&self, request: ReplaceChunkStripRangeRequest, replacement: Segment) -> Result<Chunk> {
        let mut attempt = 0_usize;
        loop {
            if attempt > 0 {
                if let Some(metrics) = &self.metrics {
                    metrics.repair_attempts.fetch_add(1, Ordering::Relaxed);
                }
            }
            attempt += 1;
            match self.allocator.replace_chunk_strip_range(request.clone()).await {
                Ok(response) => {
                    return response.chunk.ok_or_else(|| {
                        IoError::MetadataConflict("range replacement returned no chunk".into())
                    });
                }
                Err(error @ IoError::MetadataConflict(_)) => {
                    if self.resolve_ambiguity {
                        match self.resolve_replacement(&request).await {
                            Ok(Some(chunk)) => return Ok(chunk),
                            Ok(None) => {}
                            Err(error) => return Err(error),
                        }
                    }
                    self.discard(request.chunk_id, replacement).await;
                    return Err(error);
                }
                Err(_) if self.resolve_ambiguity => match self.resolve_replacement(&request).await {
                    Ok(Some(chunk)) => return Ok(chunk),
                    Ok(None) if attempt < self.attempts => {}
                    Ok(None) => {
                        self.discard(request.chunk_id, replacement).await;
                        return Err(IoError::WriteFailed(
                            "replacement metadata retry exhausted".into(),
                        ));
                    }
                    Err(error) => return Err(error),
                },
                Err(_) if attempt < self.attempts => {}
                Err(_) => {
                    return Err(IoError::WriteFailed(
                        "replacement metadata retry exhausted".into(),
                    ));
                }
            }
        }
    }

    async fn resolve_replacement(&self, request: &ReplaceChunkStripRangeRequest) -> Result<Option<Chunk>> {
        loop {
            match self.inspect_replacement(request).await {
                Ok(state) => return Ok(state),
                Err(error @ IoError::MetadataConflict(_)) => return Err(error),
                Err(error) => {
                    tracing::warn!(%error, "mirror replacement outcome remains unresolved");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn inspect_replacement(&self, request: &ReplaceChunkStripRangeRequest) -> Result<Option<Chunk>> {
        let chunk = self
            .allocator
            .query_chunk(QueryChunkRequest {
                chunk_id: request.chunk_id,
            })
            .await?
            .chunk
            .ok_or_else(|| IoError::MetadataConflict("replacement chunk disappeared".into()))?;
        if chunk.writer_epoch != self.writer_epoch {
            return Err(IoError::MetadataConflict("mirror writer epoch changed".into()));
        }
        let start = usize::try_from(request.start_index)
            .map_err(|_| IoError::MetadataConflict("replacement strip index overflows".into()))?;
        let replacement_end = start.saturating_add(request.replacement_strips.len());
        if chunk.strips.get(start..replacement_end) == Some(request.replacement_strips.as_slice()) {
            return Ok(Some(chunk));
        }
        let old_end = start.saturating_add(request.old_strips.len());
        if chunk.modify_ts == request.expected_modify_ts
            && chunk.strips.get(start..old_end) == Some(request.old_strips.as_slice())
        {
            return Ok(None);
        }
        Err(IoError::MetadataConflict(
            "mirror replacement has a different durable layout".into(),
        ))
    }

    async fn discard(&self, chunk_id: Option<ChunkId>, replacement: Segment) {
        let _ = self
            .allocator
            .discard_replacement_segment(DiscardReplacementSegmentRequest {
                chunk_id,
                segment: Some(replacement),
            })
            .await;
    }

    async fn mark_unavailable(
        &self,
        chunk: &mut Chunk,
        committed_cursor: u64,
        strip_sequence: u32,
        failed: Segment,
    ) -> Result<()> {
        let strip_index = chunk
            .strips
            .iter()
            .position(|strip| strip.strip_sequence == strip_sequence)
            .ok_or_else(|| IoError::MetadataConflict("failed strip disappeared".into()))?;
        let old = chunk.strips[strip_index].clone();
        let strip_start = u64::from(old.chunk_offset) * 1024;
        if committed_cursor <= strip_start {
            return Ok(());
        }
        let mut degraded = old.clone();
        if !degraded.unavailable_segments.contains(&failed) {
            degraded.unavailable_segments.push(failed);
        }
        let operation_id = ChunkId {
            high: self.writer_epoch ^ chunk.modify_ts ^ u64::MAX,
            low: u64::from(strip_sequence) ^ failed.unit_offset,
        };
        let response = self
            .allocator
            .replace_chunk_strip_range(ReplaceChunkStripRangeRequest {
                chunk_id: chunk.id,
                expected_modify_ts: chunk.modify_ts,
                start_index: u32::try_from(strip_index).unwrap_or(u32::MAX),
                old_strips: vec![old],
                replacement_strips: vec![degraded],
                operation_id: Some(operation_id),
            })
            .await?;
        *chunk = response
            .chunk
            .ok_or_else(|| IoError::MetadataConflict("degraded marker returned no chunk".into()))?;
        Ok(())
    }
}
