// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/frame_prefix.rs"]
mod frame_prefix;

use crowdb_protocol::frame::{
    FramePrefixScanner, FrameScanError, FrameScanMode, FrameScanStop, MAX_FRAME_GROUP_PAYLOAD_BYTES,
};
use frame_prefix::TestFramePrefix as Test;
use sha2::{Digest, Sha256};

#[path = "frame_prefix_scan_test/faults.rs"]
mod faults;

#[test]
fn fragmented_input_preserves_complete_groups_offsets_and_exact_prefix_digest() {
    let first = Test::group(0, 1);
    let large_len = 2 * MAX_FRAME_GROUP_PAYLOAD_BYTES + 17;
    let large = Test::group(first.len(), large_len);
    let last = Test::group(first.len() + large.len(), 1);
    let prefix = [first.as_slice(), large.as_slice(), last.as_slice()].concat();
    let mut bytes = prefix.clone();
    bytes.resize(bytes.len() + 512, 0);
    for fragment in [1, 13, 14, 27, 4095, 65535, 1024 * 1024] {
        let mut scanner = FramePrefixScanner::new(Test::config(bytes.len())).unwrap();
        let extents = Test::feed(&mut scanner, &bytes, fragment, 1).unwrap();
        let summary = scanner.finish().unwrap();
        assert_eq!(summary.stop, FrameScanStop::Unwritten);
        assert_eq!(summary.physical_end, prefix.len() as u64);
        assert_eq!(summary.logical_end, 102 + large_len as u64);
        assert_eq!(summary.complete_extents, 3);
        assert_eq!(summary.frames_parsed, 5);
        assert_eq!(summary.digest, <[u8; 32]>::from(Sha256::digest(&prefix)));
        assert_eq!(extents.len(), 3);
        assert_eq!(extents[0].physical_offset, 0);
        assert_eq!(extents[1].physical_offset, first.len() as u64);
        assert_eq!(extents[1].physical_length, large.len() as u64);
        assert_eq!(extents[1].logical_offset, 101);
        assert_eq!(extents[1].logical_length, large_len as u64);
        assert_eq!(extents[2].logical_offset, 101 + large_len as u64);
    }
}

#[test]
fn a_missing_follower_stops_at_the_previous_append_and_cannot_skip_to_later_groups() {
    let first = Test::group(0, 1);
    let group = Test::group(first.len(), 2 * MAX_FRAME_GROUP_PAYLOAD_BYTES + 1);
    let later = Test::group(first.len() + group.len(), 1);
    let mut bytes = [first.as_slice(), group.as_slice(), later.as_slice()].concat();
    bytes[first.len() + 65536..first.len() + group.len()].fill(0);
    let mut config = Test::config(bytes.len());
    config.required_end = first.len() as u64;
    let mut scanner = FramePrefixScanner::new(config).unwrap();
    let extents = Test::feed(&mut scanner, &bytes, 1024 * 1024, 1024).unwrap();
    let summary = scanner.finish().unwrap();
    assert_eq!(extents.len(), 1);
    assert_eq!(summary.physical_end, first.len() as u64);
    assert_eq!(summary.logical_end, 101);
    assert_eq!(summary.frames_parsed, 2);
    assert_eq!(summary.digest, <[u8; 32]>::from(Sha256::digest(&first)));
}

#[test]
fn exact_capacity_and_small_extent_batches_need_no_extra_header_input() {
    let mut bytes = Vec::new();
    for _ in 0..3000 {
        bytes.extend(Test::group(bytes.len(), 1));
    }
    let mut scanner = FramePrefixScanner::new(Test::config(bytes.len())).unwrap();
    let extents = Test::feed(&mut scanner, &bytes, 1024 * 1024, 16).unwrap();
    let summary = scanner.finish().unwrap();
    assert_eq!(summary.stop, FrameScanStop::ExactCapacity);
    assert_eq!(summary.physical_end, bytes.len() as u64);
    assert_eq!(summary.observed_end, summary.physical_end);
    assert_eq!(summary.complete_extents, 3000);
    assert_eq!(extents.len(), 3000);
    assert!(scanner.push(summary.physical_end, &[0], 1).is_err());
}

#[test]
fn an_incomplete_capacity_suffix_is_excluded_but_a_short_observation_is_an_error() {
    let first = Test::group(0, 1);
    let mut bytes = first.clone();
    bytes.extend(&Test::group(first.len(), 1)[..7]);
    let mut config = Test::config(bytes.len());
    config.required_end = first.len() as u64;
    let mut scanner = FramePrefixScanner::new(config).unwrap();
    Test::feed(&mut scanner, &bytes, 13, 16).unwrap();
    let summary = scanner.finish().unwrap();
    assert_eq!(summary.stop, FrameScanStop::IncompleteAtCapacity);
    assert_eq!(summary.physical_end, first.len() as u64);
    let mut scanner = FramePrefixScanner::new(Test::config(bytes.len() + 1)).unwrap();
    Test::feed(&mut scanner, &bytes, 13, 16).unwrap();
    assert!(matches!(
        scanner.finish(),
        Err(FrameScanError::IncompleteObservation { .. })
    ));
}

#[test]
fn canonical_ordinary_frames_keep_their_independent_completion_boundary() {
    use crowdb_protocol::frame::{encode_frame, FrameMagic};
    let bytes = encode_frame(FrameMagic::RepoLargeV2, Test::CHUNK, b"data", 1).unwrap();
    let mut config = Test::config(bytes.len());
    config.mode = FrameScanMode::Ordinary;
    config.required_end = 3;
    let mut scanner = FramePrefixScanner::new(config).unwrap();
    let extents = Test::feed(&mut scanner, &bytes, 1, 1).unwrap();
    let summary = scanner.finish().unwrap();
    assert_eq!(summary.stop, FrameScanStop::ExactCapacity);
    assert_eq!(summary.physical_end, bytes.len() as u64);
    assert_eq!(extents[0].logical_length, 4);
}

#[test]
fn a_validated_checkpoint_starts_at_its_original_physical_and_logical_offsets() {
    let prefix = Test::group(0, 1);
    let bytes = Test::group(prefix.len(), 100_000);
    let mut config = Test::config(prefix.len() + bytes.len());
    config.physical_start = prefix.len() as u64;
    config.required_end = config.physical_start;
    config.logical_start = 700;
    let mut scanner = FramePrefixScanner::new(config).unwrap();
    let batch = scanner.push(config.physical_start, &bytes, 1).unwrap();
    assert_eq!(batch.consumed, bytes.len());
    assert_eq!(batch.extents.len(), 1);
    assert_eq!(batch.extents[0].physical_offset, config.physical_start);
    assert_eq!(batch.extents[0].logical_offset, 700);
    let summary = scanner.finish().unwrap();
    assert_eq!(summary.logical_end, 100_700);
    assert_eq!(summary.digest, <[u8; 32]>::from(Sha256::digest(&bytes)));
}
