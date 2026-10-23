// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/read_interruption.rs"]
mod fault;
#[path = "common/read_payload.rs"]
mod storage;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crowdb_chunk_client::{ChunkReadPolicy, ChunkReader, ReadError, StripReader};
use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkState, ChunkStrip, EcState, EcStrip, Location, MirrorStrip, Strip,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_rpc_ffi::OwnedPayload;
use tokio::sync::Semaphore;

fn segments(count: u64) -> Vec<Segment> {
    (1..=count)
        .map(|index| Segment {
            disk_id: Some(DiskId { high: 1, low: index }),
            unit_count: 1,
            ..Segment::default()
        })
        .collect()
}

fn strip(ec: bool) -> ChunkStrip {
    ChunkStrip {
        unit_kb: 1,
        capacity: if ec { 2 } else { 1 },
        sealed_length: if ec { 2 } else { 1 },
        sealed_ts_ms: 1,
        strip: Some(if ec {
            Strip::EcStrip(EcStrip {
                data_num: 2,
                code_num: 2,
                ec_state: EcState::Parity as i32,
                segments: segments(4),
            })
        } else {
            Strip::MirrorStrip(MirrorStrip {
                segments: segments(2),
            })
        }),
        ..ChunkStrip::default()
    }
}

async fn assert_terminal_strip(ec: bool, recovery: bool) {
    let disk = Arc::new(fault::TestInterruptedDisk {
        calls: AtomicUsize::new(0),
        first_disk_unavailable: recovery,
    });
    let reader = StripReader::new(disk.clone(), Arc::new(Semaphore::new(4096)), 4096);
    let error = reader.read(&strip(ec), 0, 0, 8).await.unwrap_err();
    assert!(matches!(error, ReadError::Interrupted(_)), "{error:?}");
    let calls = disk.calls.load(Ordering::Relaxed);
    if recovery {
        // Three ordinary target retries and at most the two necessary shards.
        assert!((4..=5).contains(&calls), "unexpected recovery read count {calls}");
    } else {
        assert_eq!(calls, 1, "interrupted reads must not retry or switch replicas");
    }
}

#[tokio::test]
async fn interrupted_mirror_read_does_not_try_another_replica() {
    assert_terminal_strip(false, false).await;
}

#[tokio::test]
async fn interrupted_ec_target_does_not_fetch_recovery_shards() {
    assert_terminal_strip(true, false).await;
}

#[tokio::test]
async fn interrupted_ec_recovery_does_not_fetch_additional_candidates() {
    assert_terminal_strip(true, true).await;
}

#[tokio::test]
async fn interrupted_framed_stream_preserves_type_without_layout_retry() {
    let id = ChunkId { high: 1, low: 1 };
    let allocator = Arc::new(storage::TestPayloadStorage {
        chunk: Chunk {
            id: Some(id),
            state: ChunkState::Sealed as i32,
            capacity: 1,
            sealed_length: 1,
            strips: vec![strip(false)],
            ..Chunk::default()
        },
        data: OwnedPayload::new(),
    });
    let disk = Arc::new(fault::TestInterruptedDisk {
        calls: AtomicUsize::new(0),
        first_disk_unavailable: false,
    });
    let reader = ChunkReader::new(allocator, disk.clone(), ChunkReadPolicy::default()).unwrap();
    let mut stream = reader
        .read_stream(&[Location {
            chunk_id: Some(id),
            length: 42,
            logical_length: 8,
            ..Location::default()
        }])
        .unwrap();
    assert!(matches!(
        stream.next_payload().await,
        Some(Err(ReadError::Interrupted(_)))
    ));
    assert_eq!(disk.calls.load(Ordering::Relaxed), 1);
}
