// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn a_failed_recovery_drain_requests_layout_repair_before_tail_observation() {
    for lost_reply in [false, true] {
        let allocator = Arc::new(Allocator::new());
        let disks = Arc::new(Disks::default());
        let make = || {
            ProductionStreamChunkStore::new(
                allocator.clone(),
                disks.clone(),
                30_000,
                ChunkReadPolicy::default(),
            )
            .unwrap()
        };
        let writer = make();
        let name = StreamName { high: 811, low: 97 };
        let active = writer
            .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Wal)
            .await
            .unwrap();
        let mut encoded = TestGroupedStrip::default();
        let end = encoded
            .append(&writer, name, 9, active.chunk_id, b"retained")
            .await;
        drop(writer);
        let cold = make();
        let memory = MemoryStreamStore::new(1024);
        cold.bind_writer(name, 10, memory.claim_writer(name, 10).await.unwrap());
        disks.fail_fsync_once.store(2, Ordering::Release);
        allocator
            .fail_replacement_response_once
            .store(lost_reply, Ordering::Release);
        assert!(cold.recover_tail(name, 10, &active).await.is_err());
        assert_eq!(disks.completed_drains.load(Ordering::Acquire), 2);
        assert_eq!(allocator.replacement_calls.load(Ordering::Acquire), 1);
        let pending = allocator.chunk.lock().unwrap().clone().unwrap();
        assert_eq!(pending.state, ChunkState::Active as i32);
        let Some(Strip::MirrorStrip(mirror)) = &pending.strips[0].strip else {
            panic!("mirror")
        };
        assert_eq!(pending.strips[0].unavailable_segments, vec![mirror.segments[1]]);
        // Even if a transport recovers, the published unavailable layout still
        // needs its durable repair before it can authorize recovery.
        assert!(cold.recover_tail(name, 10, &active).await.is_err());
        assert_eq!(allocator.replacement_calls.load(Ordering::Acquire), 1);
        // Model the existing repair executor copying the drained surviving mirror
        // into a replacement and publishing its new allocation identity.
        {
            let mut bytes = disks.bytes.lock().unwrap();
            let retained = bytes.get(&1).unwrap().clone();
            bytes.insert(101, retained);
        }
        {
            let mut chunk = allocator.chunk.lock().unwrap();
            let chunk = chunk.as_mut().unwrap();
            let strip = &mut chunk.strips[0];
            let Some(Strip::MirrorStrip(mirror)) = &mut strip.strip else {
                panic!("mirror")
            };
            mirror.segments[1].disk_id.as_mut().unwrap().high = 101;
            mirror.segments[1].allocation_ts += 1;
            strip.unavailable_segments.clear();
            chunk.modify_ts += 1;
        }
        let recovered = cold.recover_tail(name, 10, &active).await.unwrap();
        assert_eq!((recovered.physical_end, recovered.logical_end), (end, 8));
        assert_eq!(recovered.extents.len(), 1);
    }
}

#[tokio::test]
async fn failed_drains_on_every_mirror_cannot_admit_repair_or_a_tail() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
    )
    .unwrap();
    let memory = MemoryStreamStore::new(1024);
    let name = StreamName::generate();
    store.bind_writer(name, 10, memory.claim_writer(name, 10).await.unwrap());
    let active = store
        .allocate_mirrored(name, 10, crowdb_chunk_stream::StreamPurpose::Wal)
        .await
        .unwrap();
    let before = allocator.chunk.lock().unwrap().clone();
    disks.fail_all_drains.store(true, Ordering::Release);
    assert!(store.recover_tail(name, 10, &active).await.is_err());
    assert_eq!(disks.completed_drains.load(Ordering::Acquire), 2);
    assert_eq!(allocator.replacement_calls.load(Ordering::Acquire), 0);
    assert_eq!(*allocator.chunk.lock().unwrap(), before);
}
