// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Failed recovery drains feed the existing durable mirror repair queue.

use super::{io_error, ProductionStreamChunkStore};
use crate::{Result, StreamError, StreamWriteContext};
use crowdb_protocol::{
    chunkdb::rpc::{Chunk, ChunkStrip, ReplaceChunkStripRangeRequest, Strip},
    common::ChunkId,
    diskdb::rpc::Segment,
};
use futures::{stream, StreamExt};

type StripDrainResults = Vec<(Segment, Result<()>)>;

impl ProductionStreamChunkStore {
    pub(super) async fn drain_recovery_layout(
        &self,
        chunk: &Chunk,
        context: &StreamWriteContext,
        deadline: std::time::Instant,
    ) -> Result<()> {
        let drained = self.drain_recovery_segments(chunk, context, deadline).await?;
        for (index, (strip, results)) in chunk.strips.iter().zip(drained).enumerate() {
            let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                return Err(StreamError::Corruption("recovery chunk is not mirrored".into()));
            };
            if !strip.unavailable_segments.is_empty() {
                return Err(StreamError::ReadUnavailable(
                    "recovery mirror repair is pending".into(),
                ));
            }
            let failed: Vec<_> = mirror
                .segments
                .iter()
                .copied()
                .filter(|segment| {
                    results
                        .iter()
                        .any(|(observed, result)| observed == segment && result.is_err())
                })
                .collect();
            let Some(error) = results.into_iter().find_map(|(_, result)| result.err()) else {
                continue;
            };
            context.check_io()?;
            // Keep the failed operation unavailable. A drained surviving mirror
            // gives the durable repair executor a stable source; its published
            // replacement must drain successfully on a later recovery attempt.
            if failed.len() < mirror.segments.len() {
                tracing::warn!(chunk_id = ?chunk.id, strip_sequence = strip.strip_sequence,
                    %error, "recovery drain failed; requesting mirror repair");
                self.mark_recovery_mirrors(chunk, index, strip, failed).await?;
            }
            return Err(error);
        }
        Ok(())
    }

    async fn drain_recovery_segments(
        &self,
        chunk: &Chunk,
        context: &StreamWriteContext,
        deadline: std::time::Instant,
    ) -> Result<Vec<StripDrainResults>> {
        let mut segments = Vec::with_capacity(chunk.strips.len() * 2);
        let mut drained = Vec::with_capacity(chunk.strips.len());
        for (index, strip) in chunk.strips.iter().enumerate() {
            let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                return Err(StreamError::Corruption("recovery chunk is not mirrored".into()));
            };
            if !strip.unavailable_segments.is_empty() {
                return Err(StreamError::ReadUnavailable(
                    "recovery mirror repair is pending".into(),
                ));
            }
            drained.push(Vec::with_capacity(mirror.segments.len()));
            segments.extend(
                mirror
                    .segments
                    .iter()
                    .map(|segment| (index, u64::from(strip.unit_kb) * 1024, *segment)),
            );
        }
        let results = stream::iter(segments)
            .map(|(index, unit_bytes, segment)| async move {
                let result = async {
                    context.check_io()?;
                    self.disk_writer
                        .drain_segment(&segment, unit_bytes, deadline)
                        .await
                        .map_err(io_error)
                }
                .await;
                (index, segment, result)
            })
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;
        for (index, segment, result) in results {
            drained[index].push((segment, result));
        }
        Ok(drained)
    }

    async fn mark_recovery_mirrors(
        &self,
        chunk: &Chunk,
        index: usize,
        strip: &ChunkStrip,
        failed: Vec<Segment>,
    ) -> Result<()> {
        let id = chunk
            .id
            .ok_or_else(|| StreamError::Corruption("recovery chunk has no ID".into()))?;
        let mut replacement = strip.clone();
        replacement.unavailable_segments = failed;
        self.allocator
            .replace_chunk_strip_range(ReplaceChunkStripRangeRequest {
                chunk_id: Some(id),
                expected_modify_ts: chunk.modify_ts,
                start_index: u32::try_from(index)
                    .map_err(|_| StreamError::Corruption("strip index overflow".into()))?,
                old_strips: vec![strip.clone()],
                replacement_strips: vec![replacement],
                operation_id: Some(ChunkId {
                    high: id.high ^ chunk.modify_ts ^ 0xd7a1_0000_0000_0001,
                    low: id.low ^ u64::from(strip.strip_sequence),
                }),
            })
            .await
            .map_err(io_error)?;
        Ok(())
    }
}
