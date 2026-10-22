// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_chunk_client::{ChunkAllocator, IoError, Result};
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AllocateReplacementSegmentRequest,
    AllocateReplacementSegmentResponse, AppendChunkRequest, AppendChunkResponse, Chunk, ChunkStrip,
    DeleteChunkRequest, DeleteChunkResponse, MirrorStrip, QueryChunkRequest, QueryChunkResponse,
    ReplaceChunkStripRangeRequest, ReplaceChunkStripRangeResponse, SealChunkRequest, SealChunkResponse,
    Strip, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use std::sync::atomic::{AtomicUsize, Ordering};

pub fn segment(high: u64) -> Segment {
    Segment {
        disk_id: Some(DiskId { high, low: 0 }),
        unit_count: 1,
        ..Default::default()
    }
}

pub fn chunk() -> Chunk {
    Chunk {
        id: Some(ChunkId { high: 7, low: 9 }),
        modify_ts: 1,
        strips: vec![ChunkStrip {
            unit_kb: 4,
            capacity: 4,
            strip: Some(Strip::MirrorStrip(MirrorStrip {
                segments: (1..=3).map(segment).collect(),
            })),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[derive(Default)]
pub struct TestAllocator {
    pub publications: AtomicUsize,
}

#[async_trait]
impl ChunkAllocator for TestAllocator {
    async fn allocate_chunk(&self, _: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        Err(IoError::Internal("unexpected allocation".into()))
    }
    async fn append_chunk(&self, _: AppendChunkRequest) -> Result<AppendChunkResponse> {
        Err(IoError::Internal("unexpected growth".into()))
    }
    async fn seal_chunk(&self, _: SealChunkRequest) -> Result<SealChunkResponse> {
        Err(IoError::Internal("unexpected seal".into()))
    }
    async fn delete_chunk(&self, _: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        Err(IoError::Internal("unexpected deletion".into()))
    }
    async fn update_chunk_strip(&self, _: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        Err(IoError::Internal("unexpected strip update".into()))
    }
    async fn query_chunk(&self, _: QueryChunkRequest) -> Result<QueryChunkResponse> {
        Err(IoError::Internal("unexpected metadata query".into()))
    }
    async fn allocate_replacement_segment(
        &self,
        request: AllocateReplacementSegmentRequest,
    ) -> Result<AllocateReplacementSegmentResponse> {
        assert_eq!(request.old_segment, Some(segment(3)));
        assert_eq!(request.surviving_segments.len(), 2);
        assert!(request.exclude_disk_ids.contains(&segment(3).disk_id.unwrap()));
        Ok(AllocateReplacementSegmentResponse {
            segment: Some(segment(4)),
        })
    }
    async fn replace_chunk_strip_range(
        &self,
        request: ReplaceChunkStripRangeRequest,
    ) -> Result<ReplaceChunkStripRangeResponse> {
        assert_eq!(request.old_strips, chunk().strips);
        let Some(Strip::MirrorStrip(mirror)) = &request.replacement_strips[0].strip else {
            panic!("mirror")
        };
        assert_eq!(mirror.segments, vec![segment(1), segment(2), segment(4)]);
        self.publications.fetch_add(1, Ordering::Relaxed);
        Ok(ReplaceChunkStripRangeResponse {
            chunk: Some(Chunk {
                strips: request.replacement_strips,
                modify_ts: 2,
                ..chunk()
            }),
        })
    }
}
