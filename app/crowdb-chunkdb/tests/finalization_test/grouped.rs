// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::finalization::TestFinalization;
use crowdb_chunkdb::{
    conversion::io::ConversionDiskIo,
    finalize::FinalizeChunkTaskHandler,
    task::{TaskHandler, TaskOutcome},
};
use crowdb_protocol::{
    chunkdb::rpc::{ChunkState, ChunkType},
    frame::{frame_length, parse_header, FRAME_HEADER_PREFIX_BYTES},
};
use std::sync::Arc;

#[tokio::test]
async fn zero_checkpoint_recovers_complete_groups_and_excludes_partial_large_append() {
    for purpose in [ChunkType::Stream, ChunkType::Wal] {
        let test = TestFinalization::start(purpose).await;
        let prefix = test.group(0, &vec![0x51; 100_000]);
        let next = test.group(prefix.len() as u64, &vec![0x62; 180_000]);
        let header = parse_header(&next[..FRAME_HEADER_PREFIX_BYTES]).unwrap();
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&next[..frame_length(header).unwrap()]);
        for replica in 0..3 {
            test.write(replica, bytes.clone()).await;
        }
        let outcome = test.run().await;
        assert!(matches!(outcome, TaskOutcome::Complete), "{outcome:?}");
        let current = test.current().await;
        assert_eq!(current.state, ChunkState::Sealed as i32);
        assert_eq!(current.acknowledged_cursor, prefix.len() as u64);
    }
}

#[tokio::test]
async fn shortest_healthy_complete_prefix_wins_without_first_replica_authority() {
    let test = TestFinalization::start(ChunkType::Stream).await;
    let prefix = test.group(0, b"first");
    let mut longer = prefix.clone();
    longer.extend(test.group(prefix.len() as u64, b"second"));
    test.write(0, longer.clone()).await;
    test.write(1, prefix.clone()).await;
    test.write(2, longer).await;
    let outcome = test.run().await;
    assert!(matches!(outcome, TaskOutcome::Complete), "{outcome:?}");
    assert_eq!(test.current().await.acknowledged_cursor, prefix.len() as u64);
}

#[tokio::test]
async fn corrupt_first_replica_cannot_authorize_empty_deletion() {
    let test = TestFinalization::start(ChunkType::Stream).await;
    let bytes = test.group(0, &vec![0x71; 100_000]);
    let mut corrupt = bytes.clone();
    corrupt[40] ^= 1;
    test.write(0, corrupt).await;
    test.write(1, bytes.clone()).await;
    test.write(2, bytes.clone()).await;
    let outcome = test.run().await;
    assert!(matches!(outcome, TaskOutcome::Complete), "{outcome:?}");
    assert_eq!(test.current().await.acknowledged_cursor, bytes.len() as u64);
}

#[tokio::test]
async fn conflicting_valid_replicas_retain_the_active_layout() {
    let test = TestFinalization::start(ChunkType::Stream).await;
    let first = test.group(0, b"first");
    let second = test.group(0, b"other");
    test.write(0, first.clone()).await;
    test.write(1, second).await;
    test.write(2, first).await;
    let outcome = test.run().await;
    assert!(
        matches!(outcome, TaskOutcome::Retry { ref error, .. } if error.contains("conflict")),
        "{outcome:?}"
    );
    assert_eq!(test.current().await, test.chunk);
}

#[tokio::test]
async fn missing_diskio_with_zero_checkpoint_is_not_evidence_of_an_empty_stream() {
    let test = TestFinalization::start(ChunkType::Stream).await;
    let handler = FinalizeChunkTaskHandler::new(
        Arc::clone(&test.harness.handler),
        Arc::new(ConversionDiskIo::empty_for_tests()),
    );
    let outcome = handler.execute(&test.task).await;
    assert!(
        matches!(outcome, TaskOutcome::Retry { ref error, .. } if error.contains("not connected")),
        "{outcome:?}"
    );
    assert_eq!(test.current().await, test.chunk);
}

#[tokio::test]
async fn a_checkpoint_inside_a_group_cannot_be_used_as_a_seal_boundary() {
    let mut test = TestFinalization::start(ChunkType::Wal).await;
    let bytes = test.group(0, &vec![0x31; 100_000]);
    for replica in 0..3 {
        test.write(replica, bytes.clone()).await;
    }
    test.checkpoint(64 * 1024).await;
    let outcome = test.run().await;
    assert!(matches!(outcome, TaskOutcome::Retry { .. }), "{outcome:?}");
    assert_eq!(test.current().await, test.chunk);
}

#[tokio::test]
async fn a_different_owner_generation_cannot_finalize_the_chunk() {
    let mut test = TestFinalization::start(ChunkType::Stream).await;
    test.task.source_revision += 1;
    let outcome = test.run().await;
    assert!(
        matches!(outcome, TaskOutcome::Retry { error_code: 6, .. }),
        "{outcome:?}"
    );
    assert_eq!(test.current().await, test.chunk);
}
