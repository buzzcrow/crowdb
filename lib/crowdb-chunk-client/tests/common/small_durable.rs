use super::*;

struct TestWriteIntent {
    disk: Arc<RecordingDiskWriter>,
    failed: bool,
    calls: AtomicUsize,
    length: AtomicU64,
}

#[async_trait]
impl crowdb_chunk_client::SmallWriteIntent for TestWriteIntent {
    async fn before_write(&self, location: &crowdb_protocol::chunkdb::rpc::Location) -> Result<()> {
        assert_eq!(self.disk.calls(), 0);
        assert!(location.chunk_id.is_some());
        assert_eq!(location.logical_length, 16);
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.length.store(location.length, Ordering::Relaxed);
        if self.failed {
            Err(IoError::WriteFailed("intent persistence failed".into()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn exact_location_intent_precedes_disk_writes_and_matches_the_returned_frame() {
    let (client, _, disk) = client(policy());
    let intent = Arc::new(TestWriteIntent {
        disk: disk.clone(),
        failed: false,
        calls: AtomicUsize::new(0),
        length: AtomicU64::new(0),
    });
    let mut writer = client.prepare_small_write(16).await.unwrap();
    writer
        .on_data(Bytes::from_static(b"0123456789abcdef"))
        .await
        .unwrap();
    let locations = writer.finish_durable_with_intent(intent.clone()).await.unwrap();
    assert_eq!(intent.calls.load(Ordering::Relaxed), 1);
    assert_eq!(intent.length.load(Ordering::Relaxed), locations[0].length);
    assert!(disk.calls() > 0);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn failed_intent_never_writes_object_bytes_or_returns_a_location() {
    let (client, _, disk) = client(policy());
    let intent = Arc::new(TestWriteIntent {
        disk: disk.clone(),
        failed: true,
        calls: AtomicUsize::new(0),
        length: AtomicU64::new(0),
    });
    let mut writer = client.prepare_small_write(16).await.unwrap();
    writer
        .on_data(Bytes::from_static(b"0123456789abcdef"))
        .await
        .unwrap();
    assert!(writer.finish_durable_with_intent(intent.clone()).await.is_err());
    assert_eq!(intent.calls.load(Ordering::Relaxed), 1);
    assert_eq!(disk.calls(), 0);
    if let Err(error) = client.shutdown_small_writes().await {
        assert!(matches!(error, IoError::WriteFailed(message) if message == "intent persistence failed"));
    }
}

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
