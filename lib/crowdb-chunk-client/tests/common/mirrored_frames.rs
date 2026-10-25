// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{DiskWriter, IoError};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkStrip, ChunkType, MirrorStrip, Strip};
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::Segment;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use crate::fixture::TestObservation as Test;

pub struct TestMirroredFrames {
    pub chunk: Chunk,
    pub disks: Arc<TestFrameDisks>,
    pub body_length: u64,
    pub capacity: u64,
}

impl TestMirroredFrames {
    pub const NAME: StreamName = StreamName { high: 31, low: 37 };
    pub fn new() -> Self {
        let body = Test::group(0, &vec![0x5a; 100_000]);
        let strips = body.len().div_ceil(8192);
        let capacity = strips * 8192;
        let chunk = Chunk {
            id: Some(Test::CHUNK),
            chunk_type: ChunkType::Stream as i32,
            owner_key: Self::NAME.chunk_owner_key(),
            writer_epoch: 1,
            state: ChunkState::Active as i32,
            acknowledged_cursor: 0,
            strips: (0..strips)
                .map(|index| ChunkStrip {
                    strip_sequence: u32::try_from(index).unwrap(),
                    chunk_offset: u32::try_from(index).unwrap() * 8,
                    capacity: 8,
                    unit_kb: 4,
                    strip: Some(Strip::MirrorStrip(MirrorStrip {
                        segments: (1..=2)
                            .map(|disk| Segment {
                                disk_id: Some(DiskId { high: disk, low: 1 }),
                                zone_index: u32::try_from(index).unwrap(),
                                unit_offset: 17 + index as u64 * 3,
                                unit_count: 2,
                                owner_chunk: Some(Test::CHUNK),
                                allocation_ts: 1,
                            })
                            .collect(),
                    })),
                    ..ChunkStrip::default()
                })
                .collect(),
            ..Chunk::default()
        };
        let body_length = body.len() as u64;
        let disks = Arc::new(TestFrameDisks {
            bytes: Test::backing(&body, capacity),
            calls: AtomicUsize::new(0),
            short: AtomicBool::new(false),
            delay_ms: AtomicU64::new(0),
        });
        Self {
            chunk,
            disks,
            body_length,
            capacity: capacity as u64,
        }
    }
}

pub struct TestFrameDisks {
    bytes: Bytes,
    pub calls: AtomicUsize,
    pub short: AtomicBool,
    pub delay_ms: AtomicU64,
}

#[async_trait]
impl DiskWriter for TestFrameDisks {
    async fn write(&self, _: &Segment, _: u64, _: Bytes) -> crowdb_chunk_client::Result<()> {
        Err(IoError::WriteFailed("read-only observation fixture".into()))
    }
    async fn write_at_byte_offset(
        &self,
        _: &Segment,
        _: u64,
        _: u64,
        _: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        Err(IoError::WriteFailed("read-only observation fixture".into()))
    }
    async fn read(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        length: u32,
    ) -> crowdb_chunk_client::Result<Bytes> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let delay = self.delay_ms.load(Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        assert_eq!(unit_bytes, 4096);
        assert_eq!(segment.owner_chunk, Some(Test::CHUNK));
        assert_eq!(segment.unit_offset, 17 + u64::from(segment.zone_index) * 3);
        assert!(offset + u64::from(length) <= 8192);
        let start = usize::try_from(segment.zone_index).unwrap() * 8192 + usize::try_from(offset).unwrap();
        let end = start + length as usize - usize::from(self.short.load(Ordering::Relaxed));
        Ok(self.bytes.slice(start..end))
    }
}
