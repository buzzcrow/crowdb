// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::storage::TestPayloadStorage;
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkStrip, Location, MirrorStrip, Strip};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::{encode_frame, FrameMagic};
use crowdb_rpc_ffi::OwnedPayload;
use std::sync::Arc;

pub fn storage() -> (Arc<TestPayloadStorage>, Location) {
    let id = ChunkId { high: 13, low: 37 };
    let mut data = Vec::with_capacity(4096);
    data.extend_from_slice(&encode_frame(FrameMagic::RepoSmallV2, id, b"payload", 1).unwrap());
    let frame_length = data.len();
    let original = OwnedPayload::from(data);
    let segment = Segment {
        disk_id: Some(DiskId { high: 1, low: 1 }),
        unit_count: 1,
        ..Segment::default()
    };
    let storage = Arc::new(TestPayloadStorage {
        chunk: Chunk {
            id: Some(id),
            state: ChunkState::Sealed as i32,
            capacity: 1,
            sealed_length: 1,
            acknowledged_cursor: frame_length as u64,
            strips: vec![ChunkStrip {
                unit_kb: 1,
                capacity: 1,
                sealed_length: 1,
                sealed_ts_ms: 1,
                strip: Some(Strip::MirrorStrip(MirrorStrip {
                    segments: vec![segment],
                })),
                ..ChunkStrip::default()
            }],
            ..Chunk::default()
        },
        data: original,
    });
    let location = Location {
        chunk_id: Some(id),
        length: frame_length as u64,
        logical_length: 7,
        ..Location::default()
    };
    (storage, location)
}
