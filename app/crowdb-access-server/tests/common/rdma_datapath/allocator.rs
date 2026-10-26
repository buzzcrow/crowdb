// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Inject only placement; payload, framing and disk I/O use production code.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_chunk_client::{ChunkAllocator, Result};
use crowdb_protocol::chunkdb::rpc::*;
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;

#[derive(Default)]
pub struct TestAllocator {
    next_segment: AtomicU64,
    chunks: ArcSwap<HashMap<(u64, u64), Chunk>>,
}

impl TestAllocator {
    fn update<T>(&self, action: impl Fn(&mut HashMap<(u64, u64), Chunk>) -> T) -> T {
        loop {
            let current = self.chunks.load_full();
            let mut next = (*current).clone();
            let result = action(&mut next);
            let previous = self.chunks.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&previous, &current) {
                return result;
            }
        }
    }

    fn strip(&self, id: ChunkId, sequence: u32) -> ChunkStrip {
        let unit_offset = self.next_segment.fetch_add(256, Ordering::Relaxed);
        ChunkStrip {
            chunk_offset: sequence * 1024,
            strip_sequence: sequence,
            unit_kb: 4,
            capacity: 1024,
            strip_type: StripType::Mirror as i32,
            strip: Some(Strip::MirrorStrip(MirrorStrip {
                segments: (1..=2)
                    .map(|high| Segment {
                        disk_id: Some(DiskId { high, low: 0 }),
                        owner_chunk: Some(id),
                        unit_offset,
                        unit_count: 256,
                        ..Segment::default()
                    })
                    .collect(),
            })),
            ..ChunkStrip::default()
        }
    }
}

#[async_trait]
impl ChunkAllocator for TestAllocator {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        assert_eq!(req.strip_type, StripType::Mirror as i32);
        assert_eq!(req.copy_count, 2);
        let id = req.chunk_id.unwrap();
        let strips: Vec<_> = (0..req.strip_count.max(1))
            .map(|sequence| self.strip(id, sequence))
            .collect();
        let chunk = Chunk {
            id: Some(id),
            modify_ts: 1,
            state: ChunkState::Active as i32,
            capacity: strips.iter().map(|strip| strip.capacity).sum(),
            next_strip_sequence: u32::try_from(strips.len()).unwrap(),
            strips,
            chunk_type: req.chunk_type,
            writer_epoch: req.writer_epoch,
            ..Chunk::default()
        };
        self.update(|chunks| chunks.insert((id.high, id.low), chunk.clone()));
        Ok(AllocateChunkResponse { chunk: Some(chunk) })
    }

    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let id = req.chunk_id.unwrap();
        self.update(|chunks| {
            let chunk = chunks.get_mut(&(id.high, id.low)).unwrap();
            assert_eq!(chunk.modify_ts, req.modify_ts);
            let first = chunk.next_strip_sequence;
            let strips: Vec<_> = (0..req.strip_count.max(1))
                .map(|offset| self.strip(id, first + offset))
                .collect();
            chunk.capacity += strips.iter().map(|strip| strip.capacity).sum::<u32>();
            chunk.next_strip_sequence += u32::try_from(strips.len()).unwrap();
            chunk.modify_ts += 1;
            chunk.strips.extend(strips.iter().cloned());
            Ok(AppendChunkResponse {
                modify_ts: chunk.modify_ts,
                strips,
                chunk: None,
            })
        })
    }

    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let id = req.chunk_id.unwrap();
        self.update(|chunks| {
            let chunk = chunks.get_mut(&(id.high, id.low)).unwrap();
            chunk.state = ChunkState::Sealed as i32;
            chunk.acknowledged_cursor = req.seal_bytes;
            chunk.sealed_length = u32::try_from(req.seal_bytes.div_ceil(1024)).unwrap();
            for strip in &mut chunk.strips {
                strip.sealed_length = chunk
                    .sealed_length
                    .saturating_sub(strip.chunk_offset)
                    .min(strip.capacity);
            }
            Ok(SealChunkResponse {
                chunk: Some(chunk.clone()),
            })
        })
    }

    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let id = req.chunk_id.unwrap();
        self.update(|chunks| {
            let chunk = chunks.get_mut(&(id.high, id.low)).unwrap();
            chunk.state = ChunkState::Deleted as i32;
            Ok(DeleteChunkResponse {
                chunk: Some(chunk.clone()),
            })
        })
    }

    async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let id = req.chunk_id.unwrap();
        Ok(QueryChunkResponse {
            chunk: self.chunks.load().get(&(id.high, id.low)).cloned(),
            layout_validity_ms: 60_000,
        })
    }
}
