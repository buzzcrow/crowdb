// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn seal_bounds_parallel_segment_drains_and_waits_for_every_completion() {
    for fail in [false, true] {
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
        store.bind_writer(name, 1, memory.claim_writer(name, 1).await.unwrap());
        let active = store
            .allocate_mirrored(name, 1, crowdb_chunk_stream::StreamPurpose::Stream)
            .await
            .unwrap();
        store
            .grow_mirrored(name, 1, active.chunk_id, active.capacity * 16)
            .await
            .unwrap()
            .unwrap();
        let expected = allocator.chunk.lock().unwrap().as_ref().unwrap().strips.len() * 2;
        assert!(expected > 8);
        disks.drain_delay_ms.store(10, Ordering::Release);
        if fail {
            disks.fail_fsync_once.store(1, Ordering::Release);
        }
        let sealing = store.seal(active.chunk_id, 1, 0);
        tokio::pin!(sealing);
        assert!(tokio::time::timeout(Duration::from_millis(1), sealing.as_mut())
            .await
            .is_err());
        assert_eq!(disks.active_drains.load(Ordering::Acquire), 8);
        assert_eq!(
            allocator.chunk.lock().unwrap().as_ref().unwrap().state,
            ChunkState::Active as i32
        );
        assert_eq!(sealing.await.is_err(), fail);
        assert_eq!(disks.max_active_drains.load(Ordering::Acquire), 8);
        assert_eq!(disks.completed_drains.load(Ordering::Acquire), expected);
        assert_eq!(disks.active_drains.load(Ordering::Acquire), 0);
        assert_eq!(
            allocator.chunk.lock().unwrap().as_ref().unwrap().state,
            if fail {
                ChunkState::Active
            } else {
                ChunkState::Sealed
            } as i32
        );
    }
}
