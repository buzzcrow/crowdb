// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{
    encode_append_group_views, encode_frame, encode_frame_regions, encode_frame_views, parse_append_group,
    parse_frame, parse_frame_views, AppendGroupValidator, ChunkLocation, FrameError, FrameLayout, FrameMagic,
    FrameRole, FRAME_FOOTER_BYTES, FRAME_V2_HEADER_BYTES, MAX_FRAME_BYTES, MAX_FRAME_GROUP_PAYLOAD_BYTES,
    MAX_FRAME_V2_PAYLOAD_BYTES,
};

const CHUNK: ChunkId = ChunkId { high: 7, low: 11 };

fn vectors() -> impl Iterator<Item = (&'static str, Vec<u8>)> {
    include_str!("data/frame-vectors.txt")
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let (name, hex) = line.split_once(' ').unwrap();
            let bytes = hex
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            (name, bytes)
        })
}

#[test]
fn both_languages_read_all_shared_vectors_and_emit_canonical_v2() {
    let vectors: Vec<_> = vectors().collect();
    assert_eq!(vectors.len(), 6);
    for (name, bytes) in vectors {
        let parsed = parse_frame(&bytes, CHUNK).unwrap();
        assert_eq!(parsed.payload, &[1, 2, 3], "{name}");
        assert_eq!(parsed.header.write_time_ms, 42);
        if name.starts_with("ordinary") {
            assert_eq!(
                encode_frame(parsed.header.magic, CHUNK, parsed.payload, 42).unwrap(),
                bytes
            );
        } else if name == "head" {
            assert_eq!(
                encode_append_group_views(CHUNK, 4096, &Bytes::from_static(&[1, 2, 3]), 42)
                    .unwrap()
                    .concat(),
                bytes
            );
            assert_eq!(parse_append_group(&bytes, CHUNK, 4096).unwrap().frame_count, 1);
        }
        for split in 0..bytes.len() {
            let views = [&bytes[..split], &[], &bytes[split..]];
            let scattered = parse_frame_views(&views, CHUNK).unwrap();
            assert_eq!(scattered.role, parsed.role, "{name} at {split}");
            assert_eq!(scattered.physical_length, bytes.len());
        }
    }
}

#[test]
fn ordinary_v2_regions_and_views_preserve_payload_ownership() {
    for magic in [
        FrameMagic::RepoSmallV2,
        FrameMagic::RepoLargeV2,
        FrameMagic::BtreePageV2,
        FrameMagic::PageIndexV2,
    ] {
        let payload = Bytes::from(vec![0x91; MAX_FRAME_V2_PAYLOAD_BYTES]);
        let mut header = [0; FRAME_V2_HEADER_BYTES];
        let mut footer = [0; FRAME_FOOTER_BYTES];
        encode_frame_regions(magic, CHUNK, &payload, 42, &mut header, &mut footer).unwrap();
        let views = encode_frame_views(magic, CHUNK, vec![payload.clone()], 42).unwrap();
        assert_eq!(views[1].as_ptr(), payload.as_ptr());
        let bytes = views.concat();
        assert_eq!(bytes.len(), MAX_FRAME_BYTES);
        assert_eq!(&bytes[..header.len()], header);
        assert_eq!(&bytes[bytes.len() - footer.len()..], footer);
        assert_eq!(parse_frame(&bytes, CHUNK).unwrap().role, FrameRole::Ordinary);
        assert!(encode_frame(magic, CHUNK, &vec![0; MAX_FRAME_V2_PAYLOAD_BYTES + 1], 42).is_err());
    }
    assert!(encode_frame(FrameMagic::StreamV2, CHUNK, &[1], 42).is_err());
}

#[test]
fn grouped_append_is_zero_copy_and_requires_every_follower() {
    for length in [
        1,
        MAX_FRAME_GROUP_PAYLOAD_BYTES,
        MAX_FRAME_GROUP_PAYLOAD_BYTES + 1,
        3 * MAX_FRAME_GROUP_PAYLOAD_BYTES + 17,
    ] {
        let payload = Bytes::from(vec![0x51; length]);
        let views = encode_append_group_views(CHUNK, 4096, &payload, 42).unwrap();
        for (index, parts) in views.chunks_exact(3).enumerate() {
            assert_eq!(
                parts[1].as_ptr(),
                payload.slice(index * MAX_FRAME_GROUP_PAYLOAD_BYTES..).as_ptr()
            );
        }
        let bytes = views.concat();
        let group = parse_append_group(&bytes, CHUNK, 4096).unwrap();
        assert_eq!(group.logical_length, length as u64);
        assert_eq!(
            group.physical_length,
            FrameLayout::StreamGroupV2.physical_length(length as u64).unwrap()
        );
        assert_eq!(
            group.frame_count as usize,
            length.div_ceil(MAX_FRAME_GROUP_PAYLOAD_BYTES)
        );
        for cursor in (0..bytes.len()).step_by(MAX_FRAME_BYTES) {
            for end in [
                cursor.saturating_sub(1),
                cursor,
                (cursor + 1).min(bytes.len() - 1),
            ] {
                assert!(
                    parse_append_group(&bytes[..end], CHUNK, 4096).is_err(),
                    "length={length}, end={end}"
                );
            }
        }
        assert!(parse_append_group(&bytes[..bytes.len() - 1], CHUNK, 4096).is_err());
        if group.frame_count > 1 {
            assert!(parse_append_group(&bytes, CHUNK, 4097).is_err());
        }
    }
}

#[test]
fn valid_crc_does_not_make_reordered_or_cross_group_followers_valid() {
    let payload = Bytes::from(vec![0x51; MAX_FRAME_GROUP_PAYLOAD_BYTES * 3]);
    let views = encode_append_group_views(CHUNK, 4096, &payload, 42).unwrap();
    let frames: Vec<_> = views.chunks_exact(3).map(<[Bytes]>::concat).collect();
    let reordered = [frames[0].clone(), frames[2].clone(), frames[1].clone()].concat();
    assert_eq!(
        parse_append_group(&reordered, CHUNK, 4096).err(),
        Some(FrameError::InvalidGroup)
    );
    let other = encode_append_group_views(CHUNK, 8192, &payload, 42).unwrap();
    let mixed = [frames[0].clone(), other[3..6].concat(), frames[2].clone()].concat();
    assert_eq!(
        parse_append_group(&mixed, CHUNK, 4096).err(),
        Some(FrameError::InvalidGroup)
    );
    let mut damaged = views.concat();
    damaged[MAX_FRAME_BYTES + 15] ^= 1;
    assert!(parse_append_group(&damaged, CHUNK, 4096).is_err());
}

#[test]
fn group_validator_keeps_previous_boundary_until_finish() {
    let bytes = encode_append_group_views(
        CHUNK,
        4096,
        &Bytes::from(vec![7; MAX_FRAME_GROUP_PAYLOAD_BYTES + 1]),
        42,
    )
    .unwrap()
    .concat();
    let head = parse_frame(&bytes, CHUNK).unwrap();
    let validator = AppendGroupValidator::new(4096, &head).unwrap();
    assert!(!validator.is_complete());
    assert_eq!(validator.finish().err(), Some(FrameError::InvalidGroup));
    let mut validator = AppendGroupValidator::new(4096, &head).unwrap();
    let tail = parse_frame(&bytes[head.physical_length..], CHUNK).unwrap();
    assert!(validator
        .push(4096 + head.physical_length as u64 + 1, &tail)
        .is_err());
    validator.push(4096 + head.physical_length as u64, &tail).unwrap();
    assert!(validator.is_complete());
    assert!(validator.push(4096 + bytes.len() as u64, &tail).is_err());
    assert_eq!(validator.finish().unwrap().physical_length, bytes.len() as u64);
}

#[test]
fn geometry_maps_boundaries_and_rejects_overflow_for_each_revision() {
    for layout in [FrameLayout::OrdinaryV2, FrameLayout::StreamGroupV2] {
        let capacity = layout.payload_capacity();
        let location = ChunkLocation {
            chunk_id: CHUNK,
            frame_offset: 4096,
            logical_length: capacity * 2 + 7,
        };
        let end = 4096 + layout.physical_length(location.logical_length).unwrap();
        assert_eq!(
            layout
                .physical_range(location, capacity - 1..capacity + 1)
                .unwrap(),
            4096..4096 + 2 * MAX_FRAME_BYTES as u64
        );
        assert_eq!(
            layout
                .physical_range(location, capacity * 2..capacity * 2 + 7)
                .unwrap(),
            4096 + 2 * MAX_FRAME_BYTES as u64..end
        );
        assert!(layout.physical_length(u64::MAX).is_err());
        assert!(layout
            .physical_range(
                ChunkLocation {
                    frame_offset: u64::MAX,
                    ..location
                },
                0..1
            )
            .is_err());
        assert!(layout
            .physical_range(location, 0..location.logical_length + 1)
            .is_err());
        assert_eq!(layout.physical_length(0).unwrap(), 0);
    }
}

#[test]
fn malformed_extensions_fail_without_panics_and_v2_crc_is_unambiguous() {
    for (_, mut bytes) in vectors().filter(|(name, _)| name.starts_with("ordinary")) {
        bytes[14] = 1;
        assert!(parse_frame(&bytes, CHUNK).is_err());
        bytes[2..4].copy_from_slice(&14_u16.to_le_bytes());
        assert!(parse_frame_views(&[&bytes], CHUNK).is_err());
    }
    let mut bytes = encode_frame(FrameMagic::RepoSmallV2, CHUNK, &[1, 2, 3], 42).unwrap();
    let footer = bytes.len() - FRAME_FOOTER_BYTES;
    let prefix_crc = crowdb_common::ec_isal::crc32c_update(0, &bytes[..footer]);
    let footer_inclusive_crc = crowdb_common::ec_isal::crc32c_update(prefix_crc, &bytes[footer + 4..]);
    bytes[footer..footer + 4].copy_from_slice(&footer_inclusive_crc.to_le_bytes());
    assert_eq!(
        parse_frame(&bytes, CHUNK).err(),
        Some(FrameError::ChecksumMismatch)
    );
    assert_eq!(
        parse_frame_views(&[&bytes], CHUNK).err(),
        Some(FrameError::ChecksumMismatch)
    );
    assert!(encode_append_group_views(CHUNK, 0, &Bytes::new(), 42).is_err());
    assert!(encode_append_group_views(CHUNK, u64::MAX, &Bytes::from_static(b"x"), 42).is_err());
}

#[test]
fn old_ordinary_magics_are_unsupported() {
    let canonical = encode_frame(FrameMagic::RepoSmallV2, CHUNK, b"data", 42).unwrap();
    for magic in [0x0101_u16, 0x0201, 0x0401, 0x0501, 0x0103] {
        let mut bytes = canonical.clone();
        bytes[..2].copy_from_slice(&magic.to_le_bytes());
        assert!(matches!(parse_frame(&bytes, CHUNK), Err(FrameError::UnknownMagic(value)) if value == magic));
        assert!(
            matches!(parse_frame_views(&[&bytes], CHUNK), Err(FrameError::UnknownMagic(value)) if value == magic)
        );
    }
}
