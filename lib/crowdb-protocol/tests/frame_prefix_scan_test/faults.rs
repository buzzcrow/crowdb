// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_protocol::frame::{encode_frame, FrameError, FrameMagic};

#[test]
fn corrupt_or_wrong_identity_frames_are_errors_instead_of_shorter_candidates() {
    let first = Test::group(0, 1);
    let second = Test::group(first.len(), 1);
    for offset in [27, second.len() - 1] {
        let mut bad = second.clone();
        bad[offset] ^= 1;
        let bytes = [first.as_slice(), bad.as_slice()].concat();
        let mut scanner = FramePrefixScanner::new(Test::config(bytes.len())).unwrap();
        assert!(matches!(
            Test::feed(&mut scanner, &bytes, 1024, 16),
            Err(FrameScanError::Frame { offset: 48, .. })
        ));
        assert_eq!(scanner.stop(), FrameScanStop::Failed);
        assert!(matches!(scanner.finish(), Err(FrameScanError::FailedObservation)));
    }
}

#[test]
fn wrong_roles_and_unsupported_revisions_are_rejected() {
    let mut unsupported = Test::group(0, 1);
    unsupported[..2].copy_from_slice(&0x0301_u16.to_le_bytes());
    let ordinary = encode_frame(FrameMagic::RepoSmallV2, Test::CHUNK, b"data", 1).unwrap();
    for bytes in [unsupported, ordinary] {
        let mut scanner = FramePrefixScanner::new(Test::config(bytes.len())).unwrap();
        assert!(Test::feed(&mut scanner, &bytes, 1024, 16).is_err());
    }
    let bytes = Test::group(0, 1);
    let mut config = Test::config(bytes.len());
    config.mode = FrameScanMode::Ordinary;
    let mut scanner = FramePrefixScanner::new(config).unwrap();
    assert!(Test::feed(&mut scanner, &bytes, 1024, 16).is_err());
}

#[test]
fn a_full_header_exceeding_allocated_capacity_is_not_a_healthy_short_candidate() {
    let mut bytes = Test::group(0, 1);
    bytes[4..6].copy_from_slice(&100_u16.to_le_bytes());
    let mut scanner = FramePrefixScanner::new(Test::config(bytes.len())).unwrap();
    assert!(matches!(
        Test::feed(&mut scanner, &bytes, 1024, 16),
        Err(FrameScanError::Frame {
            source: FrameError::InvalidRegionLength,
            ..
        })
    ));
}

#[test]
fn grouped_checkpoints_must_be_covered_exact_append_boundaries() {
    let bytes = [Test::group(0, 1), Test::group(48, 1)].concat();
    let mut config = Test::config(bytes.len());
    config.required_end = 49;
    let mut scanner = FramePrefixScanner::new(config).unwrap();
    assert!(matches!(
        Test::feed(&mut scanner, &bytes, 1024, 16),
        Err(FrameScanError::CheckpointBoundary { .. })
    ));
    let mut bytes = Test::group(0, 1);
    bytes.resize(100, 0);
    config.physical_limit = bytes.len() as u64;
    let mut scanner = FramePrefixScanner::new(config).unwrap();
    assert!(matches!(
        Test::feed(&mut scanner, &bytes, 1024, 16),
        Err(FrameScanError::RequiredPrefix { .. })
    ));
}

#[test]
fn declared_group_limits_and_arithmetic_overflow_fail_without_allocating_group_payloads() {
    let bytes = Test::group(0, MAX_FRAME_GROUP_PAYLOAD_BYTES + 1);
    let mut config = Test::config(bytes.len());
    config.max_append_bytes = 1;
    let mut scanner = FramePrefixScanner::new(config).unwrap();
    assert!(matches!(
        Test::feed(&mut scanner, &bytes, 65536, 1),
        Err(FrameScanError::Frame {
            source: FrameError::InvalidGroup,
            ..
        })
    ));
    let mut config = Test::config(bytes.len());
    config.logical_start = u64::MAX;
    let mut scanner = FramePrefixScanner::new(config).unwrap();
    assert!(matches!(
        Test::feed(&mut scanner, &bytes, 65536, 1),
        Err(FrameScanError::Frame {
            source: FrameError::LengthOverflow,
            ..
        })
    ));
}

#[test]
fn input_and_output_budgets_reject_gaps_and_unbounded_batches() {
    let bytes = Test::group(0, 1);
    let mut scanner = FramePrefixScanner::new(Test::config(bytes.len())).unwrap();
    assert!(matches!(
        scanner.push(1, &bytes[..1], 1),
        Err(FrameScanError::OutOfOrder)
    ));
    for budget in [0, 1025] {
        let mut scanner = FramePrefixScanner::new(Test::config(bytes.len())).unwrap();
        assert!(matches!(
            scanner.push(0, &bytes, budget),
            Err(FrameScanError::InvalidBounds)
        ));
    }
    let mut config = Test::config(1024 * 1024 + 1);
    let mut scanner = FramePrefixScanner::new(config).unwrap();
    assert!(matches!(
        scanner.push(0, &vec![0; 1024 * 1024 + 1], 1),
        Err(FrameScanError::InvalidBounds)
    ));
    config.required_end = config.physical_limit + 1;
    assert!(FramePrefixScanner::new(config).is_err());
}
