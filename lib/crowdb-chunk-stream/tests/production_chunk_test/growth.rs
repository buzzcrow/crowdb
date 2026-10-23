// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn a_small_waiting_append_uses_a_smaller_growth_window() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new(allocator, disks.clone(), 30_000, ChunkReadPolicy::default())
        .unwrap();
    let name = StreamName { high: 181, low: 191 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let payload = Bytes::from(vec![
        0x71;
        15 * crowdb_protocol::frame::MAX_FRAME_GROUP_PAYLOAD_BYTES
    ]);
    let data = Bytes::from(
        crowdb_protocol::frame::encode_append_group_views(active.chunk_id, 0, &payload, 1)
            .unwrap()
            .concat(),
    );
    let end = data.len() as u64;
    store
        .write_mirrors(
            name,
            9,
            active.chunk_id,
            crowdb_chunk_stream::StreamMirrorWrite {
                physical_offset: 0,
                complete_end: end,
                images: &[MirrorStripImage {
                    block_offset: 0,
                    data: data.clone(),
                    full_image: data,
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(end, 15 * 65536);
    let grown = store
        .grow_mirrored(name, 9, active.chunk_id, end + 128 * 1024)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(grown.capacity, 2 * 1024 * 1024);
    assert_eq!(grown.acknowledged_cursor, end);
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 6);
    let grown = store
        .grow_mirrored(name, 9, active.chunk_id, end + 3 * 1024 * 1024)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(grown.capacity, 8 * 1024 * 1024);
    assert_eq!(grown.acknowledged_cursor, end);
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 18);
}

#[tokio::test]
async fn growth_amortizes_allocation_and_initializes_every_replica_within_the_hard_capacity() {
    const MIB: u64 = 1024 * 1024;
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new_with_mirror_copies_and_capacity(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
        2,
        10 * MIB,
    )
    .unwrap();
    let name = StreamName { high: 173, low: 179 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    assert_eq!(active.capacity, MIB);
    let grown = store
        .grow_mirrored(name, 9, active.chunk_id, MIB + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(grown.capacity, 8 * MIB);
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 16);
    let revision = allocator.chunk.lock().unwrap().as_ref().unwrap().modify_ts;
    for required in [2 * MIB, 4 * MIB, 7 * MIB, 8 * MIB] {
        assert_eq!(
            store
                .grow_mirrored(name, 9, active.chunk_id, required)
                .await
                .unwrap()
                .unwrap()
                .capacity,
            8 * MIB
        );
    }
    assert_eq!(
        allocator.chunk.lock().unwrap().as_ref().unwrap().modify_ts,
        revision
    );
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 16);
    assert_eq!(
        store
            .grow_mirrored(name, 9, active.chunk_id, 9 * MIB)
            .await
            .unwrap()
            .unwrap()
            .capacity,
        10 * MIB
    );
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 20);
    assert!(store
        .grow_mirrored(name, 9, active.chunk_id, 10 * MIB + 1)
        .await
        .unwrap()
        .is_none());
    let media = disks.bytes.lock().unwrap();
    assert_eq!(media.len(), 20);
    assert!(media
        .values()
        .all(|bytes| bytes.len() == usize::try_from(MIB).unwrap() && bytes.iter().all(|byte| *byte == 0)));
}
