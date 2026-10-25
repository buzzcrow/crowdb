// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

const ID: ChunkId = ChunkId { high: 83, low: 89 };
const MAGIC: FrameMagic = FrameMagic::RepoSmallV2;

fn fixture(
    first_end: Option<u64>,
    validity_ms: u64,
    corrupt_second: bool,
) -> (ChunkReader, Arc<SequenceAllocator>, Vec<Bytes>) {
    let frames: Vec<_> = [b"first".as_slice(), b"second".as_slice()]
        .into_iter()
        .map(|bytes| Bytes::from(encode_frame(MAGIC, ID, bytes, 1).unwrap()))
        .collect();
    let bytes = Bytes::from(frames.concat());
    let replicas = vec![segment(1), segment(2)];
    let current = Chunk {
        id: Some(ID),
        state: ChunkState::Active as i32,
        modify_ts: 1,
        capacity: 1,
        acknowledged_cursor: bytes.len() as u64,
        strips: vec![ChunkStrip {
            capacity: 1,
            unit_kb: 1,
            strip: Some(Strip::MirrorStrip(MirrorStrip {
                segments: replicas.clone(),
            })),
            ..ChunkStrip::default()
        }],
        ..Chunk::default()
    };
    let mut first = current.clone();
    if let Some(end) = first_end {
        first.acknowledged_cursor = end;
    }
    let allocator = Arc::new(SequenceAllocator {
        queries: AtomicUsize::new(0),
        first,
        current: Mutex::new(current),
        first_layout_validity_ms: validity_ms,
        full_reply: None,
        full_calls: AtomicUsize::new(0),
    });
    let mut damaged = bytes.to_vec();
    if corrupt_second {
        damaged[frames[0].len() + crowdb_protocol::frame::FRAME_V2_HEADER_BYTES] ^= 1;
    }
    let disks = Arc::new(MemoryDiskIo {
        shards: vec![
            (replicas[0].disk_id.unwrap(), Bytes::from(damaged)),
            (replicas[1].disk_id.unwrap(), bytes),
        ],
        failed: vec![],
        reads: Arc::new(AtomicUsize::new(0)),
    });
    let reader = ChunkReader::new(
        allocator.clone(),
        disks,
        ChunkReadPolicy {
            layout_safety_margin: Duration::ZERO,
            ..ChunkReadPolicy::default()
        },
    )
    .unwrap();
    (reader, allocator, frames)
}

async fn read(reader: &ChunkReader, frames: &[Bytes], index: usize) -> Bytes {
    let offset = if index == 0 { 0 } else { frames[0].len() as u64 };
    reader
        .read_verified_frame(ID, offset, frames[index].len() as u64, MAGIC)
        .await
        .unwrap()
}

#[tokio::test]
async fn immutable_frames_share_one_bounded_layout_across_reader_clones() {
    let (reader, allocator, frames) = fixture(None, 1_000, false);
    for index in 0..32 {
        assert_eq!(read(&reader.clone(), &frames, index % 2).await, frames[index % 2]);
    }
    assert_eq!(allocator.queries.load(Ordering::Acquire), 1);
    assert_eq!(reader.flow_metrics_snapshot().layout_cache_hits, 31);
}

#[tokio::test]
async fn an_append_beyond_the_cached_durable_prefix_refreshes_layout() {
    let first_length = encode_frame(MAGIC, ID, b"first", 1).unwrap().len() as u64;
    let (reader, allocator, frames) = fixture(Some(first_length), 1_000, false);
    assert_eq!(read(&reader, &frames, 0).await, frames[0]);
    assert_eq!(read(&reader, &frames, 1).await, frames[1]);
    assert_eq!(read(&reader, &frames, 0).await, frames[0]);
    assert_eq!(allocator.queries.load(Ordering::Acquire), 2);
    assert_eq!(reader.flow_metrics_snapshot().layout_cache_hits, 1);
}

#[tokio::test]
async fn expired_frame_layout_is_refreshed_before_a_successful_reply() {
    let (reader, allocator, frames) = fixture(None, 0, false);
    assert_eq!(read(&reader, &frames, 0).await, frames[0]);
    assert_eq!(read(&reader, &frames, 1).await, frames[1]);
    assert_eq!(allocator.queries.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn corrupt_cached_replica_invalidates_layout_and_retries_verified_bytes() {
    let (reader, allocator, frames) = fixture(None, 1_000, true);
    assert_eq!(read(&reader, &frames, 0).await, frames[0]);
    assert_eq!(read(&reader, &frames, 1).await, frames[1]);
    // Initial layout, confirmed corrupt-replica observation, then a fresh grant.
    assert_eq!(allocator.queries.load(Ordering::Acquire), 3);
    assert_eq!(
        allocator.current.lock().unwrap().strips[0].unavailable_segments,
        vec![segment(1)]
    );
    assert_eq!(read(&reader, &frames, 0).await, frames[0]);
    assert_eq!(allocator.queries.load(Ordering::Acquire), 3);
    assert_eq!(reader.flow_metrics_snapshot().layout_cache_hits, 2);
}
