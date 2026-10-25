// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn small_write_repair_preserves_acknowledged_prefix_in_open_block() {
    let stack = E2eStack::start(policy()).await;
    let (allocator, disk_writer) = real_write_parts(&stack).await;
    let fault = Arc::new(FailSelectedDiskWrite {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        fail_on: policy().mirror_copies as usize + 1,
        failed_disk: Mutex::new(None),
    });
    let client = ChunkIoClient::from_parts_with_small_policy(allocator, fault, policy()).unwrap();
    let prefix_data = Bytes::from(vec![0x31; MAX_FRAME_V2_PAYLOAD_BYTES - KIB]);
    let patch_data = Bytes::from(vec![0x72; MAX_FRAME_V2_PAYLOAD_BYTES]);
    let prefix = write_object(&client, prefix_data.clone()).await;
    let patch = write_object(&client, patch_data.clone()).await;
    assert_eq!(prefix.chunk_id, patch.chunk_id);
    assert_eq!(patch.offset, prefix.length);
    let chunk = stack.query_chunk(&patch).await;
    assert_mirror_data(&stack, &chunk, &prefix, &prefix_data).await;
    assert_mirror_data(&stack, &chunk, &patch, &patch_data).await;
    assert_eq!(client.small_write_metrics().repaired_replicas, 1);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn small_write_new_chunk_repair_exhaustion_preserves_sealed_predecessor() {
    let mut configured = policy();
    configured.chunk_capacity = MIB as u64;
    let stack = E2eStack::start(configured.clone()).await;
    let (allocator, disk_writer) = real_write_parts(&stack).await;
    let fault = Arc::new(FailWritesFromCall {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        first_failed_call: FRAMES_PER_MIB_STRIP * configured.mirror_copies as usize + 1,
    });
    let client = ChunkIoClient::from_parts_with_small_policy(allocator, fault, configured).unwrap();
    let predecessor_group = write_full_small_strip(&client, 0x29).await;
    let predecessor = predecessor_group[0].1.clone();
    let mut failed = client
        .prepare_small_write(MAX_FRAME_V2_PAYLOAD_BYTES)
        .await
        .unwrap();
    failed
        .on_data(Bytes::from(vec![0x81; MAX_FRAME_V2_PAYLOAD_BYTES]))
        .await
        .unwrap();
    assert!(matches!(failed.on_finish().await, Err(IoError::WriteFailed(_))));

    let predecessor_chunk = stack.query_chunk(&predecessor).await;
    assert_eq!(predecessor_chunk.state, ChunkState::Sealed as i32);
    assert_eq!(predecessor_chunk.acknowledged_cursor, MIB as u64);
    for (data, location) in &predecessor_group {
        assert_mirror_data(&stack, &predecessor_chunk, location, data).await;
    }
    let metrics = client.small_write_metrics();
    assert_eq!(metrics.completed, FRAMES_PER_MIB_STRIP as u64);
    assert_eq!(metrics.failed, 1);
    assert_eq!(metrics.exhausted_repairs, 1);
    let _ = client.shutdown_small_writes().await;
}
