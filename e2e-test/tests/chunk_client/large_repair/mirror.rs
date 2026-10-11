// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_mirror_replaces_failed_replica_before_ordered_completion() {
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    for fail_on in [1, 3, 4] {
        let fault = Arc::new(FailWriteCall {
            inner: disk_writer.clone(),
            calls: AtomicUsize::new(0),
            fail_on,
            persistent: false,
            failed_segment: Mutex::new(None),
            segments: Mutex::new(Vec::new()),
            failure_delay: Duration::ZERO,
            failure_pending: AtomicBool::new(false),
            successes_during_failure: AtomicUsize::new(0),
        });
        let client =
            ChunkIoClient::from_parts_with_small_policy(allocator.clone(), fault.clone(), small_policy())
                .unwrap();
        let data = make_test_data(4 * MIB);
        let mut configured = policy(16 * MIB as u64);
        Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(2);
        let result = client
            .prepare_large_write(Some(data.len() as u64), configured)
            .write_stream(data.as_slice())
            .await
            .unwrap();
        let failed = fault.failed_segment.lock().unwrap().expect("injected segment");
        let chunk = stack.query_chunk(&result.locations[0]).await;
        assert!(chunk.strips.iter().all(|strip| {
            let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                return false;
            };
            !mirror.segments.contains(&failed)
        }));
        assert_eq!(client.large_write_repair_metrics().repaired_segments, 1);
        assert_eq!(
            client.read_object(&result.locations).await.unwrap().concat(),
            data
        );
    }
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_mirror_rotates_failed_partial_final_strip() {
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    let fault = Arc::new(FailLaterWritesInFirstChunk {
        inner: disk_writer,
        first_chunk: Mutex::new(None),
        first_chunk_writes: AtomicUsize::new(0),
        successful_writes: 0,
    });
    let client =
        ChunkIoClient::from_parts_with_small_policy(allocator, fault.clone(), small_policy()).unwrap();
    let mut configured = policy(16 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(2);
    let expected = vec![0x5a; MAX_FRAME_PAYLOAD_BYTES];
    let mut writer = client.prepare_large_write(Some(expected.len() as u64), configured);
    writer
        .on_framed_data(Box::new(FullFramedOwner::new(1)))
        .await
        .unwrap();
    let locations = writer.on_finish().await.unwrap();
    let old_id = fault.first_chunk.lock().unwrap().expect("first chunk");
    assert_ne!(locations[0].chunk_id, Some(old_id));
    assert_eq!(client.read_object(&locations).await.unwrap().concat(), expected);
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_mirror_abort_drains_delayed_submitted_write_before_delete() {
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
    let mut writer = client.prepare_large_write(Some((16 * MAX_FRAME_PAYLOAD_BYTES) as u64), configured);
    writer
        .on_framed_data(Box::new(FullFramedOwner::new(16)))
        .await
        .unwrap();
    let started = std::time::Instant::now();
    writer.on_error().await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(70));
    assert!(!fault.failure_pending.load(Ordering::Acquire));
    let old_id = fault.failed_segment.lock().unwrap().unwrap().owner_chunk;
    let chunk = stack
        .query_chunk(&Location {
            chunk_id: old_id,
            ..Location::default()
        })
        .await;
    assert_eq!(chunk.state, ChunkState::Deleted as i32);
}
