// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{
    encode_frame, merge_adjacent_locations, parse_frame, ChunkLocation, FrameError, FrameMagic,
    FRAME_FOOTER_BYTES, FRAME_HEADER_PREFIX_BYTES, MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES,
};

const CHUNK: ChunkId = ChunkId { high: 7, low: 11 };

#[test]
fn frame_round_trips_and_has_canonical_maximum_size() {
    let payload = vec![0xA5; MAX_FRAME_PAYLOAD_BYTES];
    let frame = encode_frame(FrameMagic::RepoLargeV1, CHUNK, &payload, 42).unwrap();
    assert_eq!(frame.len(), MAX_FRAME_BYTES);
    let decoded = parse_frame(&frame, CHUNK).unwrap();
    assert_eq!(decoded.header.magic, FrameMagic::RepoLargeV1);
    assert_eq!(decoded.header.payload_offset as usize, FRAME_HEADER_PREFIX_BYTES);
    assert_eq!(decoded.payload, payload);
    assert_eq!(decoded.physical_length, MAX_FRAME_BYTES);
}

#[test]
fn frame_rejects_corruption_and_wrong_chunk() {
    let mut frame = encode_frame(FrameMagic::RepoSmallV1, CHUNK, b"payload", 42).unwrap();
    frame[FRAME_HEADER_PREFIX_BYTES] ^= 1;
    assert!(matches!(
        parse_frame(&frame, CHUNK),
        Err(FrameError::ChecksumMismatch)
    ));

    let frame = encode_frame(FrameMagic::RepoSmallV1, CHUNK, b"payload", 42).unwrap();
    assert!(matches!(
        parse_frame(&frame, ChunkId { high: 7, low: 12 }),
        Err(FrameError::ChunkIdMismatch)
    ));
}

#[test]
fn location_maps_large_object_subrange_to_containing_frames() {
    let logical = (MAX_FRAME_PAYLOAD_BYTES as u64) + 10;
    let location = ChunkLocation {
        chunk_id: CHUNK,
        frame_offset: 4096,
        logical_length: logical,
    };
    assert_eq!(
        location.physical_length().unwrap(),
        (MAX_FRAME_BYTES + FRAME_HEADER_PREFIX_BYTES + 10 + FRAME_FOOTER_BYTES) as u64
    );
    assert_eq!(
        location
            .physical_range_for_subrange((MAX_FRAME_PAYLOAD_BYTES as u64 - 1)..logical)
            .unwrap(),
        4096..(4096 + location.physical_length().unwrap())
    );
}

#[test]
fn only_frame_aligned_locations_merge() {
    let aligned = ChunkLocation {
        chunk_id: CHUNK,
        frame_offset: 0,
        logical_length: MAX_FRAME_PAYLOAD_BYTES as u64,
    };
    let adjacent = ChunkLocation {
        chunk_id: CHUNK,
        frame_offset: MAX_FRAME_BYTES as u64,
        logical_length: 3,
    };
    let merged = merge_adjacent_locations(&[aligned, adjacent]).unwrap();
    assert_eq!(merged.len(), 1);

    let tail = ChunkLocation {
        chunk_id: CHUNK,
        frame_offset: 0,
        logical_length: 3,
    };
    let next = ChunkLocation {
        chunk_id: CHUNK,
        frame_offset: tail.end_offset().unwrap(),
        logical_length: 3,
    };
    assert_eq!(merge_adjacent_locations(&[tail, next]).unwrap().len(), 2);
}
