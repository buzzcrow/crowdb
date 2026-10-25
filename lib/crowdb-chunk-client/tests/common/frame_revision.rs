// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{segment, MemoryDiskIo, SequenceAllocator};
use bytes::Bytes;
use crowdb_chunk_client::{ChunkReadPolicy, ChunkReader};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkStrip, Location, MirrorStrip, Strip};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{encode_frames, FrameMagic};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct TestFramedReader {
    pub reader: ChunkReader,
    pub locations: Vec<Location>,
    pub allocator: Arc<SequenceAllocator>,
    pub reads: Arc<AtomicUsize>,
}

impl TestFramedReader {
    pub fn new(objects: &[(FrameMagic, Bytes)], window: usize) -> Self {
        let id = ChunkId { high: 13, low: 37 };
        let mut bytes = Vec::new();
        let mut locations = Vec::new();
        let mut logical_offset = 0;
        for (magic, payload) in objects {
            let frame = encode_frames(*magic, id, payload, 42).unwrap().concat();
            locations.push(Location {
                chunk_id: Some(id),
                offset: bytes.len() as u64,
                length: frame.len() as u64,
                logical_offset,
                logical_length: payload.len() as u64,
            });
            logical_offset += payload.len() as u64;
            bytes.extend(frame);
        }
        let kib = u32::try_from(bytes.len().div_ceil(1024)).unwrap();
        bytes.resize(kib as usize * 1024, 0);
        let mut segment = segment(1);
        segment.unit_count = kib;
        let chunk = Chunk {
            id: Some(id),
            state: ChunkState::Sealed as i32,
            capacity: kib,
            sealed_length: kib,
            strips: vec![ChunkStrip {
                unit_kb: 1,
                capacity: kib,
                sealed_length: kib,
                sealed_ts_ms: 1,
                strip: Some(Strip::MirrorStrip(MirrorStrip {
                    segments: vec![segment],
                })),
                ..ChunkStrip::default()
            }],
            ..Chunk::default()
        };
        let allocator = Arc::new(SequenceAllocator {
            queries: AtomicUsize::new(0),
            first: chunk.clone(),
            current: Mutex::new(chunk),
            first_layout_validity_ms: 1_000,
            full_reply: None,
            full_calls: AtomicUsize::new(0),
        });
        let reads = Arc::new(AtomicUsize::new(0));
        let disk = Arc::new(MemoryDiskIo {
            shards: vec![(segment.disk_id.unwrap(), Bytes::from(bytes))],
            failed: Vec::new(),
            reads: reads.clone(),
        });
        let reader = ChunkReader::new(
            allocator.clone(),
            disk,
            ChunkReadPolicy {
                stream_window_bytes: window,
                ..ChunkReadPolicy::default()
            },
        )
        .unwrap();
        Self {
            reader,
            locations,
            allocator,
            reads,
        }
    }

    pub fn queries(&self) -> usize {
        self.allocator.queries.load(Ordering::Relaxed)
    }
}
