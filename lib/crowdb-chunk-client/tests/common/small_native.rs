// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn same_hash_admission_probes_other_pipelines_before_waiting() {
    let mut configured = policy();
    configured.min_pipelines = 2;
    configured.max_pipelines = 2;
    let (client, _, _) = client(configured);
    let mut first = client
        .prepare_shared_object_write_for_key(1024 * 1024, b"same")
        .await
        .unwrap();
    let mut second = tokio::time::timeout(
        Duration::from_secs(1),
        client.prepare_shared_object_write_for_key(1024 * 1024, b"same"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(client.small_write_metrics().reserved_bytes, 2 * 1024 * 1024);
    let pending_client = client.clone();
    let pending = tokio::spawn(async move {
        pending_client
            .prepare_shared_object_write_for_key(1024 * 1024, b"same")
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!pending.is_finished());
    first.on_error().await.unwrap();
    let mut third = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    second.on_error().await.unwrap();
    third.on_error().await.unwrap();
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn half_group_refill_appends_and_does_not_block_existing_strips() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let allocator = Arc::new(MockAllocator {
        refill_gate: Some(gate.clone()),
        ..MockAllocator::default()
    });
    let disk = Arc::new(RecordingDiskWriter::default());
    let mut configured = policy();
    configured.small_strip_prefetch_count = 32;
    configured.chunk_capacity = 256 * 1024 * 1024;
    let client = ChunkIoClient::from_parts_with_small_policy(allocator.clone(), disk, configured).unwrap();
    let writing_client = client.clone();
    let writes = tokio::spawn(async move {
        for _ in 0..18 * 16 {
            let mut writer = writing_client
                .prepare_shared_object_write_for_key(MAX_SMALL, b"aligned")
                .await
                .unwrap();
            writer.on_data(Bytes::from(vec![0x5a; MAX_SMALL])).await.unwrap();
            writer.on_finish().await.unwrap();
        }
    });
    tokio::time::timeout(Duration::from_secs(10), allocator.refill_entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), writes)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(allocator.reserve_calls.load(Ordering::Relaxed), 2);
    gate.notify_one();
    client.shutdown_small_writes().await.unwrap();
    let state = allocator.state.lock().unwrap();
    let mut groups: Vec<_> = state.reservations.values().collect();
    groups.sort_by_key(|group| group.strips[0].chunk_offset);
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].strips.len(), 32);
    assert_eq!(groups[1].strips.len(), 32);
    let a_end = groups[0].strips.last().unwrap().chunk_offset + groups[0].strips.last().unwrap().capacity;
    assert_eq!(groups[1].strips[0].chunk_offset, a_end);
}

#[tokio::test]
async fn waiting_shared_admission_terminates_when_the_pool_closes() {
    let mut configured = policy();
    configured.min_pipelines = 1;
    configured.max_pipelines = 1;
    configured.memory_budget = 2 * 1024 * 1024;
    let (client, _, _) = client(configured);
    let mut held = client
        .prepare_shared_object_write_for_key(1024 * 1024, b"held")
        .await
        .unwrap();
    let waiting_client = client.clone();
    let waiting = tokio::spawn(async move {
        waiting_client
            .prepare_shared_object_write_for_key(1024 * 1024, b"waiting")
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished());
    client.shutdown_small_writes().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(IoError::Finished)));
    held.on_error().await.unwrap();
}

#[tokio::test]
async fn a_mismatched_prefetch_offset_cannot_publish_overlapping_objects() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let allocator = Arc::new(MockAllocator {
        mismatched_refill_response: true,
        refill_gate: Some(gate.clone()),
        ..MockAllocator::default()
    });
    let disk = Arc::new(RecordingDiskWriter::default());
    let client =
        ChunkIoClient::from_parts_with_small_policy(allocator.clone(), disk.clone(), policy()).unwrap();
    for _ in 0..5 * 16 {
        let mut writer = client
            .prepare_shared_object_write_for_key(MAX_SMALL, b"aligned")
            .await
            .unwrap();
        writer.on_data(Bytes::from(vec![0x5a; MAX_SMALL])).await.unwrap();
        writer.on_finish().await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), allocator.refill_entered.notified())
        .await
        .unwrap();
    gate.notify_one();
    let mut writer = client
        .prepare_shared_object_write_for_key(MAX_SMALL, b"next")
        .await
        .unwrap();
    writer.on_data(Bytes::from(vec![0x7b; MAX_SMALL])).await.unwrap();
    let result = writer.on_finish().await;
    assert!(
        matches!(result, Err(IoError::WriteFailed(message)) if message.contains("prefetch append offset mismatch"))
    );
    assert_eq!(client.small_write_metrics().completed, 5 * 16);
    assert_eq!(allocator.reserve_calls.load(Ordering::Relaxed), 2);
    assert!(client.shutdown_small_writes().await.is_err());
}
