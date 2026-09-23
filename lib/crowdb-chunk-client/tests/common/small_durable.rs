use super::*;

#[tokio::test]
async fn durable_completion_does_not_publish_a_location_after_cursor_failure() {
    let (client, allocator, disk) = client(policy());
    allocator.fail_advances.store(true, Ordering::Relaxed);
    let mut writer = client.prepare_small_write(16).await.unwrap();
    writer
        .on_data(Bytes::from_static(b"0123456789abcdef"))
        .await
        .unwrap();
    assert!(writer.finish_durable().await.is_err());
    assert!(disk.calls() > 0);
    assert!(
        matches!(client.shutdown_small_writes().await, Err(IoError::AllocationFailed(message)) if message == "injected cursor failure")
    );
}

#[tokio::test]
async fn durable_completion_waits_for_readable_cursor_before_returning_location() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let allocator = Arc::new(MockAllocator {
        advance_gate: Some(gate.clone()),
        ..MockAllocator::default()
    });
    let disk = Arc::new(RecordingDiskWriter::default());
    let client = ChunkIoClient::from_parts_with_small_policy(allocator.clone(), disk, policy()).unwrap();
    let mut writer = client.prepare_small_write(16).await.unwrap();
    writer
        .on_data(Bytes::from_static(b"0123456789abcdef"))
        .await
        .unwrap();
    let completion = tokio::spawn(async move { writer.finish_durable().await });
    tokio::time::timeout(Duration::from_secs(1), allocator.advance_entered.notified())
        .await
        .unwrap();
    assert!(!completion.is_finished());
    gate.notify_one();
    let locations = completion.await.unwrap().unwrap();
    let chunk = allocator
        .query_chunk(QueryChunkRequest {
            chunk_id: locations[0].chunk_id,
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert!(chunk.acknowledged_cursor >= locations[0].offset + locations[0].length);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn durable_completion_covers_new_object_after_an_older_background_advance() {
    let (client, allocator, _) = client(policy());
    allocator.advance_delay_ms.store(20, Ordering::Relaxed);
    let mut first = client.prepare_small_write(16).await.unwrap();
    first
        .on_data(Bytes::from_static(b"0123456789abcdef"))
        .await
        .unwrap();
    first.on_finish().await.unwrap();
    let mut second = client.prepare_small_write(16).await.unwrap();
    second
        .on_data(Bytes::from_static(b"fedcba9876543210"))
        .await
        .unwrap();
    let locations = second.finish_durable().await.unwrap();
    let chunk = allocator
        .query_chunk(QueryChunkRequest {
            chunk_id: locations[0].chunk_id,
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert!(chunk.acknowledged_cursor >= locations[0].offset + locations[0].length);
    assert!(second.finish_durable().await.is_err());
    client.shutdown_small_writes().await.unwrap();
}
