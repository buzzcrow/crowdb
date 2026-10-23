// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/read_payload.rs"]
mod storage;

use crowdb_chunk_client::{ChunkReadPolicy, ChunkReader};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkStrip, Location, MirrorStrip, Strip};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::{encode_frame, FrameMagic};
use crowdb_rpc_ffi::OwnedPayload;
use std::sync::Arc;

#[tokio::test]
async fn framing_and_stream_credits_preserve_original_allocation_metadata() {
    let id = ChunkId { high: 13, low: 37 };
    let mut data = Vec::with_capacity(4096);
    data.extend_from_slice(&encode_frame(FrameMagic::RepoSmallV1, id, b"payload", 1).unwrap());
    let frame_length = data.len();
    let original = OwnedPayload::from(data);
    let backing = Arc::clone(original.allocation().unwrap());
    let segment = Segment {
        disk_id: Some(DiskId { high: 1, low: 1 }),
        unit_count: 1,
        ..Segment::default()
    };
    let storage = Arc::new(storage::TestPayloadStorage {
        chunk: Chunk {
            id: Some(id),
            state: ChunkState::Sealed as i32,
            capacity: 1,
            sealed_length: 1,
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
    let reader = ChunkReader::new(storage.clone(), storage, ChunkReadPolicy::default()).unwrap();
    let location = Location {
        chunk_id: Some(id),
        length: frame_length as u64,
        logical_length: 7,
        ..Location::default()
    };
    let mut stream = reader.read_stream(&[location]).unwrap();
    let payload = stream.next_payload().await.unwrap().unwrap();
    assert_eq!(payload.as_ref(), b"payload");
    assert!(Arc::ptr_eq(payload.allocation().unwrap(), &backing));
    assert_eq!(payload.allocation().unwrap().capacity(), 4096);
    assert_eq!(
        payload.as_ptr(),
        backing.bytes()[payload.allocation_offset()..].as_ptr()
    );
    assert!(stream.next_payload().await.is_none());
    drop(stream);
    assert_eq!(reader.flow_metrics_snapshot().stream_bytes_released, 0);
    let bytes = payload.into_bytes();
    assert_eq!(reader.flow_metrics_snapshot().stream_bytes_released, 0);
    drop(bytes);
    assert_eq!(
        reader.flow_metrics_snapshot().stream_bytes_released,
        frame_length as u64
    );
}
