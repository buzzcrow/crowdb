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
