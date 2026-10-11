// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_write_repair_exhaustion_deletes_unsealed_chunk() {
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
    let result = client
        .prepare_large_write(Some(MIB as u64), policy(16 * MIB as u64))
        .write_stream(make_test_data(MIB).as_slice())
        .await;
    assert!(matches!(result, Err(IoError::ReplicaRepairExhausted(_))));
    assert_eq!(client.large_write_repair_metrics().exhausted, 1);
    let failed = fault.failed_segment.lock().unwrap().expect("injected segment");
    let location = Location {
        chunk_id: failed.owner_chunk,
        ..Location::default()
    };
    let chunk = stack.query_chunk(&location).await;
    assert_eq!(chunk.state, ChunkState::Deleted as i32);
}
