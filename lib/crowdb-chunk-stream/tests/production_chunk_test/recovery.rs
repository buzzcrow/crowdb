// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

async fn recover_case(shorter: bool, corrupt: bool) {
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
    let name = StreamName { high: 801, low: 91 };
    let active = writer
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let mut encoded = TestGroupedStrip::default();
    let first_end = encoded.append(&writer, name, 9, active.chunk_id, b"first").await;
    let final_end = encoded.append(&writer, name, 9, active.chunk_id, b"second").await;
    assert_eq!(allocator.advance_calls.load(Ordering::Acquire), 0);
    assert_eq!(
        allocator
            .chunk
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .acknowledged_cursor,
        0
    );
    {
        let mut bytes = disks.bytes.lock().unwrap();
        let replica = bytes.get_mut(&2).unwrap();
        if shorter {
            replica[usize::try_from(first_end).unwrap()..].fill(0);
        }
        if corrupt {
            replica[20] ^= 0x40;
        }
    }
    drop(writer);
    let cold = make();
    let memory = MemoryStreamStore::new(1024);
    cold.bind_writer(name, 10, memory.claim_writer(name, 10).await.unwrap());
    let recovered = cold.recover_tail(name, 10, &active).await.unwrap();
    assert_eq!(
        recovered.physical_end,
        if shorter { first_end } else { final_end }
    );
    assert_eq!(recovered.logical_end, if shorter { 5 } else { 11 });
    assert_eq!(recovered.extents.len(), if shorter { 1 } else { 2 });
    assert_eq!(allocator.advance_calls.load(Ordering::Acquire), 0);
    cold.seal(active.chunk_id, 10, recovered.physical_end)
        .await
        .unwrap();
    assert_eq!(
        allocator
            .chunk
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .acknowledged_cursor,
        recovered.physical_end
    );
}

#[tokio::test]
async fn cold_adapter_recovers_complete_writes_beyond_zero_checkpoint() {
    recover_case(false, false).await;
}

#[tokio::test]
async fn cold_adapter_uses_the_shortest_healthy_complete_prefix() {
    recover_case(true, false).await;
}

#[tokio::test]
async fn cold_adapter_does_not_turn_a_corrupt_replica_into_an_empty_tail() {
    recover_case(false, true).await;
}

#[tokio::test]
async fn cold_adapter_recovers_an_empty_sealed_tail_after_its_strips_are_reclaimed() {
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
    let name = StreamName { high: 803, low: 93 };
    let mut active = writer
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Wal)
        .await
        .unwrap();
    active.logical_start = 42;
    // DiskDB returns all strips when ChunkDB seals a physically empty chunk.
    let sealed = {
        let mut chunk = allocator.chunk.lock().unwrap();
        let chunk = chunk.as_mut().unwrap();
        chunk.state = ChunkState::Sealed as i32;
        chunk.modify_ts += 1;
        chunk.capacity = 0;
        chunk.strips.clear();
        chunk.clone()
    };
    drop(writer);
    let cold = make();
    let memory = MemoryStreamStore::new(1024);
    cold.bind_writer(name, 10, memory.claim_writer(name, 10).await.unwrap());
    let recovered = cold.recover_tail(name, 10, &active).await.unwrap();
    assert_eq!((recovered.physical_end, recovered.logical_end), (0, 42));
    assert!(recovered.extents.is_empty());
    for case in 0..8 {
        let mut invalid = sealed.clone();
        let mut descriptor = active.clone();
        match case {
            0 => invalid.state = ChunkState::Active as i32,
            1 => invalid.capacity = 1,
            2 => invalid.sealed_length = 1,
            3 => invalid.acknowledged_cursor = 1,
            4 => descriptor.acknowledged_cursor = 1,
            5 => descriptor.physical_start = 1,
            6 => invalid.writer_epoch = 0,
            _ => invalid.chunk_type = crowdb_protocol::chunkdb::rpc::ChunkType::S3 as i32,
        }
        *allocator.chunk.lock().unwrap() = Some(invalid);
        assert!(matches!(
            cold.recover_tail(name, 10, &descriptor).await,
            Err(crowdb_chunk_stream::StreamError::Corruption(_))
        ));
    }
    let mut changed = sealed.clone();
    changed.modify_ts += 1;
    *allocator.chunk.lock().unwrap() = Some(changed);
    *allocator.stale_query_once.lock().unwrap() = Some(sealed);
    assert!(matches!(
        cold.recover_tail(name, 10, &active).await,
        Err(crowdb_chunk_stream::StreamError::StaleWriter)
    ));
}
