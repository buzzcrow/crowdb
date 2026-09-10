// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable EC-segment writes and placement-safe in-line replacement.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::{
    AllocateReplacementSegmentRequest, Chunk, DiscardReplacementSegmentRequest, QueryChunkRequest,
    ReplaceChunkStripRangeRequest, Strip,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
use tokio::task::JoinHandle;

use crate::metrics::LargeWriteRepairMetrics;
use crate::negative_list::FailedDiskList;
use crate::{ChunkAllocator, DiskWriter, IoError, Result};

#[derive(Debug)]
pub(crate) struct FailedSegmentWrite {
    pub strip_sequence: u32,
    pub segment: Segment,
    pub unit_bytes: u64,
    pub data: Bytes,
    pub error: String,
}

pub(crate) type SegmentWriteHandle = JoinHandle<Option<FailedSegmentWrite>>;

pub(crate) fn spawn_segment_write(
    disk_writer: Arc<dyn DiskWriter>,
    strip_sequence: u32,
    segment: Segment,
    unit_bytes: u64,
    data: Bytes,
) -> SegmentWriteHandle {
    tokio::spawn(async move {
        match disk_writer.write(&segment, unit_bytes, data.clone()).await {
            Ok(()) => None,
            Err(error) => Some(FailedSegmentWrite {
                strip_sequence,
                segment,
                unit_bytes,
                data,
                error: error.to_string(),
            }),
        }
    })
}

pub(crate) struct SegmentRepair<'a> {
    pub allocator: &'a Arc<dyn ChunkAllocator>,
    pub disk_writer: &'a Arc<dyn DiskWriter>,
    pub failed_disks: &'a Arc<FailedDiskList>,
    pub metrics: &'a Arc<LargeWriteRepairMetrics>,
    pub attempts: usize,
}

impl SegmentRepair<'_> {
    pub async fn repair(&self, chunk_id: ChunkId, failure: FailedSegmentWrite) -> Result<Chunk> {
        if let Some(disk_id) = failure.segment.disk_id {
            self.failed_disks.insert(disk_id);
        }
        for _ in 0..self.attempts {
            self.metrics.attempts.inc();
            let chunk = self.query_chunk(chunk_id).await?;
            let Some(strip_index) = chunk
                .strips
                .iter()
                .position(|strip| strip.strip_sequence == failure.strip_sequence)
            else {
                return Err(IoError::MetadataConflict(
                    "failed EC strip disappeared during replacement".into(),
                ));
            };
            let old_strip = chunk.strips[strip_index].clone();
            let Some(Strip::EcStrip(mut ec)) = old_strip.strip.clone() else {
                return Err(IoError::MetadataConflict(
                    "failed EC strip changed type during replacement".into(),
                ));
            };
            let Some(segment_index) = ec.segments.iter().position(|segment| *segment == failure.segment)
            else {
                return Ok(chunk);
            };
            let survivors = ec
                .segments
                .iter()
                .copied()
                .filter(|segment| *segment != failure.segment)
                .collect();
            let excluded = self.failed_disks.live();
            self.metrics
                .negative_list_hits
                .inc_by(u64::try_from(excluded.len()).unwrap_or(u64::MAX));
            let allocation = self
                .allocator
                .allocate_replacement_segment(AllocateReplacementSegmentRequest {
                    chunk_id: Some(chunk_id),
                    old_segment: Some(failure.segment),
                    surviving_segments: survivors,
                    exclude_disk_ids: excluded,
                })
                .await;
            let Ok(allocation) = allocation else {
                continue;
            };
            let Some(replacement) = allocation.segment else {
                continue;
            };
            if self
                .disk_writer
                .write(&replacement, failure.unit_bytes, failure.data.clone())
                .await
                .is_err()
            {
                if let Some(disk_id) = replacement.disk_id {
                    self.failed_disks.insert(disk_id);
                }
                self.discard(chunk_id, replacement).await;
                continue;
            }
            ec.segments[segment_index] = replacement;
            let mut replacement_strip = old_strip.clone();
            replacement_strip.strip = Some(Strip::EcStrip(ec));
            replacement_strip
                .unavailable_segments
                .retain(|segment| *segment != failure.segment);
            let request = ReplaceChunkStripRangeRequest {
                chunk_id: Some(chunk_id),
                expected_modify_ts: chunk.modify_ts,
                start_index: u32::try_from(strip_index).unwrap_or(u32::MAX),
                old_strips: vec![old_strip],
                replacement_strips: vec![replacement_strip],
                operation_id: Some(operation_id(chunk_id, failure.strip_sequence, failure.segment)),
            };
            match self.publish(request).await {
                Ok(chunk) => {
                    self.metrics.repaired_segments.inc();
                    return Ok(chunk);
                }
                Err(IoError::MetadataConflict(_)) => {
                    self.discard(chunk_id, replacement).await;
                }
                Err(error) => return Err(error),
            }
        }
        self.metrics.exhausted.inc();
        Err(IoError::WriteFailed(format!(
            "EC segment repair exhausted after durable write failure: {}",
            failure.error
        )))
    }

    async fn query_chunk(&self, chunk_id: ChunkId) -> Result<Chunk> {
        self.allocator
            .query_chunk(QueryChunkRequest {
                chunk_id: Some(chunk_id),
            })
            .await?
            .chunk
            .ok_or_else(|| IoError::ChunkNotFound(format!("{chunk_id:?}")))
    }

    async fn publish(&self, request: ReplaceChunkStripRangeRequest) -> Result<Chunk> {
        let mut last_error = None;
        for _ in 0..self.attempts {
            match self.allocator.replace_chunk_strip_range(request.clone()).await {
                Ok(response) => {
                    return response.chunk.ok_or_else(|| {
                        IoError::MetadataConflict("segment replacement returned no chunk".into())
                    });
                }
                Err(error @ IoError::MetadataConflict(_)) => return Err(error),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error
            .unwrap_or_else(|| IoError::WriteFailed("segment replacement publication exhausted".into())))
    }

    async fn discard(&self, chunk_id: ChunkId, segment: Segment) {
        if self
            .allocator
            .discard_replacement_segment(DiscardReplacementSegmentRequest {
                chunk_id: Some(chunk_id),
                segment: Some(segment),
            })
            .await
            .is_ok()
        {
            self.metrics.discarded_segments.inc();
        }
    }
}

fn operation_id(chunk_id: ChunkId, strip_sequence: u32, segment: Segment) -> ChunkId {
    let disk = segment.disk_id.unwrap_or_default();
    ChunkId {
        high: chunk_id.high ^ disk.high ^ segment.allocation_ts,
        low: chunk_id.low ^ disk.low ^ segment.unit_offset ^ u64::from(strip_sequence),
    }
}
