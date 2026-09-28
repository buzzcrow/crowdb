// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{
    encode_frame, encode_frame_regions, encode_frames, merge_adjacent_locations, parse_frame,
    parse_frame_views, ChunkLocation, FrameError, FrameMagic, FRAME_FOOTER_BYTES, FRAME_HEADER_PREFIX_BYTES,
    MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES,
};

const CHUNK: ChunkId = ChunkId { high: 7, low: 11 };
const REPO_SMALL_VECTOR: [u8; 37] = [
    0x01, 0x01, 0x0E, 0x00, 0x03, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03,
    0x96, 0x16, 0xD6, 0x1E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x0B,
];

#[test]
fn frame_matches_cross_language_vector() {
    let encoded = encode_frame(FrameMagic::RepoSmallV1, CHUNK, &[1, 2, 3], 42).unwrap();
    assert_eq!(encoded, REPO_SMALL_VECTOR);
    assert_eq!(
        parse_frame(&REPO_SMALL_VECTOR, CHUNK).unwrap().payload,
        &[1, 2, 3]
    );
}

#[test]
fn split_frame_views_verify_without_assembling_payload() {
    let payload = vec![0x6b; 1024];
    let frame = encode_frame(FrameMagic::RepoSmallV1, CHUNK, &payload, 42).unwrap();
    let boundaries = [3, 11, 127, frame.len() - 9, frame.len() - 2];
    let mut views = Vec::new();
    let mut start = 0;
    for end in boundaries.into_iter().chain(std::iter::once(frame.len())) {
        views.push(&frame[start..end]);
        start = end;
    }
    let parsed = parse_frame_views(&views, CHUNK).unwrap();
    assert_eq!(parsed.header.magic, FrameMagic::RepoSmallV1);
    assert_eq!(parsed.physical_length, frame.len());

    let mut corrupted = frame;
    corrupted[127] ^= 1;
    let mut start = 0;
    let mut damaged = Vec::new();
    for end in boundaries.into_iter().chain(std::iter::once(corrupted.len())) {
        damaged.push(&corrupted[start..end]);
        start = end;
    }
    assert_eq!(
        parse_frame_views(&damaged, CHUNK).err(),
        Some(FrameError::ChecksumMismatch)
    );
}

#[test]
fn frame_crc_matches_bitwise_reference_across_header_payload_and_chunk_id() {
    for length in [0, 1, 17, 1024, MAX_FRAME_PAYLOAD_BYTES] {
        let payload: Vec<u8> = (0..length)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect();
        let frame = encode_frame(FrameMagic::RepoLargeV1, CHUNK, &payload, 42).unwrap();
        let footer = frame.len() - FRAME_FOOTER_BYTES;
        let mut crc = 0_u32;
        for byte in frame[..footer].iter().chain(frame[footer + 4..].iter()) {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0x82F6_3B78 & (0_u32.wrapping_sub(crc & 1)));
            }
        }
        assert_eq!(frame[footer..footer + 4], crc.to_le_bytes());
        assert_eq!(parse_frame(&frame, CHUNK).unwrap().payload, payload);
    }
}

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
fn separated_frame_regions_match_contiguous_encoding() {
    let payload = vec![0x5a; MAX_FRAME_PAYLOAD_BYTES];
    let expected = encode_frame(FrameMagic::RepoLargeV1, CHUNK, &payload, 42).unwrap();
    let mut header = [0; FRAME_HEADER_PREFIX_BYTES];
    let mut footer = [0; FRAME_FOOTER_BYTES];
    encode_frame_regions(
        FrameMagic::RepoLargeV1,
        CHUNK,
        &payload,
        42,
        &mut header,
        &mut footer,
    )
    .unwrap();

    assert_eq!(&expected[..header.len()], &header);
    assert_eq!(&expected[header.len()..header.len() + payload.len()], &payload);
    assert_eq!(&expected[header.len() + payload.len()..], &footer);
    let mut short_header = [0; FRAME_HEADER_PREFIX_BYTES - 1];
    assert_eq!(
        encode_frame_regions(
            FrameMagic::RepoLargeV1,
            CHUNK,
            &payload,
            42,
            &mut short_header,
            &mut footer,
        ),
        Err(FrameError::InvalidRegionLength)
    );
}

#[test]
fn large_payload_has_full_interior_and_variable_tail_frames() {
    let payload = vec![0xA5; MAX_FRAME_PAYLOAD_BYTES + 7];
    let frames = encode_frames(FrameMagic::RepoLargeV1, CHUNK, &payload, 42).unwrap();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].len(), MAX_FRAME_BYTES);
    assert_eq!(parse_frame(&frames[1], CHUNK).unwrap().payload, &[0xA5; 7]);
    assert!(encode_frames(FrameMagic::RepoLargeV1, CHUNK, &[], 42)
        .unwrap()
        .is_empty());
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
