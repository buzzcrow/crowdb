// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_mirror_retains_later_framed_buffers_until_failed_first_strip_is_repaired() {
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    let fault = Arc::new(FailWriteCall {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        fail_on: 1,
        persistent: false,
        failed_segment: Mutex::new(None),
        segments: Mutex::new(Vec::new()),
        failure_delay: Duration::from_millis(80),
        failure_pending: AtomicBool::new(false),
        successes_during_failure: AtomicUsize::new(0),
    });
    let client =
        ChunkIoClient::from_parts_with_small_policy(allocator, fault.clone(), small_policy()).unwrap();
    let mut configured = policy(16 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(2);
    let frames = 65;
    let expected = vec![0x5a; frames * MAX_FRAME_PAYLOAD_BYTES];
    let mut writer = client.prepare_large_write(Some(expected.len() as u64), configured);
    writer
        .on_framed_data(Box::new(FullFramedOwner::new(frames)))
        .await
        .unwrap();
    let locations = writer.on_finish().await.unwrap();
    assert!(fault.successes_during_failure.load(Ordering::Acquire) > 0);
    assert_eq!(client.large_write_repair_metrics().repaired_segments, 1);
    assert_eq!(client.read_object(&locations).await.unwrap().concat(), expected);
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_mirror_rotates_after_repair_exhaustion_and_replays_ordered_data() {
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    let fault = Arc::new(FailLaterWritesInFirstChunk {
        inner: disk_writer,
        first_chunk: Mutex::new(None),
        first_chunk_writes: AtomicUsize::new(0),
        successful_writes: 2,
    });
    let client =
        ChunkIoClient::from_parts_with_small_policy(allocator, fault.clone(), small_policy()).unwrap();
    let mut configured = policy(16 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(2);
    let frames = 225;
    let expected = vec![0x5a; frames * MAX_FRAME_PAYLOAD_BYTES];
    let mut writer = client.prepare_large_write(Some(expected.len() as u64), configured);
    writer
        .on_framed_data(Box::new(FullFramedOwner::new(frames)))
        .await
        .unwrap();
    let locations = writer.on_finish().await.unwrap();
    let old_id = fault.first_chunk.lock().unwrap().expect("first chunk");
    assert_ne!(locations[0].chunk_id, Some(old_id));
    let repair = client.large_write_repair_metrics();
    assert_eq!(repair.chunk_rotations, 1);
    assert_eq!(repair.rotated_chunks, 1);
    assert!(repair.replayed_bytes > 0);
    assert!(repair.replayed_bytes < (frames * MAX_FRAME_BYTES) as u64);
    assert_eq!(client.read_object(&locations).await.unwrap().concat(), expected);
    assert_eq!(
        stack
            .query_chunk(&Location {
                chunk_id: Some(old_id),
                ..Location::default()
            })
            .await
            .state,
        ChunkState::Deleted as i32
    );
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_mirror_repair_exhaustion_deletes_unsealed_chunk() {
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    let fault = Arc::new(FailWriteCall {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        fail_on: 1,
        persistent: true,
        failed_segment: Mutex::new(None),
        segments: Mutex::new(Vec::new()),
        failure_delay: Duration::ZERO,
        failure_pending: AtomicBool::new(false),
        successes_during_failure: AtomicUsize::new(0),
    });
    let client =
        ChunkIoClient::from_parts_with_small_policy(allocator, fault.clone(), small_policy()).unwrap();
    let mut configured = policy(16 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(2);
    let result = client
        .prepare_large_write(Some(MIB as u64), configured)
        .write_stream(make_test_data(MIB).as_slice())
        .await;
    assert!(matches!(result, Err(IoError::ReplicaRepairExhausted(_))));
    let repair = client.large_write_repair_metrics();
    assert!(repair.exhausted >= 1);
    assert_eq!(repair.chunk_rotations, 1);
    assert_eq!(repair.rotated_chunks, 0);
    let failed = fault.failed_segment.lock().unwrap().expect("injected segment");
    let chunk = stack
        .query_chunk(&Location {
            chunk_id: failed.owner_chunk,
            ..Location::default()
        })
        .await;
    assert_eq!(chunk.state, ChunkState::Deleted as i32);
}
