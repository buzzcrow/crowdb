// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn small_write_repairs_failed_replica_through_real_chunkdb_and_diskio() {
    let stack = E2eStack::start(policy()).await;
    let (allocator, disk_writer) = real_write_parts(&stack).await;
    let fault = Arc::new(FailSelectedDiskWrite {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        fail_on: 1,
        failed_disk: Mutex::new(None),
    });
    let client = ChunkIoClient::from_parts_with_small_policy(allocator, fault.clone(), policy()).unwrap();
    let data = Bytes::from(vec![0x5a; MAX_FRAME_PAYLOAD_BYTES]);
    let location = write_object(&client, data.clone()).await;
    let failed_disk = fault.failed_disk.lock().unwrap().expect("injected disk identity");
    client.shutdown_small_writes().await.unwrap();
    let chunk = stack.query_chunk(&location).await;
    let Some(Strip::MirrorStrip(mirror)) = &chunk.strips[0].strip else {
        panic!("expected repaired mirror strip");
    };
    assert_eq!(mirror.segments.len(), 2);
    assert!(mirror
        .segments
        .iter()
        .all(|segment| segment.disk_id != Some(failed_disk)));
    assert_mirror_data(&stack, &chunk, &location, &data).await;
    let metrics = client.small_write_metrics();
    assert_eq!(metrics.repair_attempts, 1);
    assert_eq!(metrics.repaired_replicas, 1);
    assert_eq!(metrics.exhausted_repairs, 0);
    assert!(metrics.negative_list_hits >= 1);
    assert_eq!(metrics.active_repairs, 0);
    assert!(metrics.repair_latency_ns > 0);
    assert!(metrics.max_repair_latency_ns > 0);
    assert_eq!(metrics.repairs_avoiding_rotation, 1);
    assert_eq!(client.small_write_metrics().shadow_bytes, 0);
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn protected_small_write_reports_real_diskio_write_failure() {
    let stack = E2eStack::start_with_diskio_fault_rate(policy(), 1.0).await;
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let data = Bytes::from(vec![0x5a; MAX_FRAME_PAYLOAD_BYTES]);
        let mut writer = stack.client.prepare_small_write(data.len()).await?;
        writer.on_data(data).await?;
        writer.on_finish().await.map(|_| ())
    })
    .await
    .expect("real DiskIO write failure did not return within 15 seconds");
    assert!(
        matches!(result, Err(IoError::WriteFailed(_))),
        "unexpected write result: {result:?}"
    );
    let metrics = stack.client.small_write_metrics();
    assert_eq!(metrics.failed, 1);
    assert_eq!(metrics.repairs_avoiding_rotation, 0);
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn protected_read_reports_diskio_process_failure() {
    let mut stack = E2eStack::start(policy()).await;
    let data = Bytes::from_static(b"diskio-read-failure");
    let location = write_object(&stack.client, data.clone()).await;
    stack.client.shutdown_small_writes().await.unwrap();
    assert_eq!(
        stack
            .client
            .read_object(std::slice::from_ref(&location))
            .await
            .unwrap()
            .concat()
            .as_slice(),
        data.as_ref()
    );
    stack.crash_diskio().await;
    let read = tokio::time::timeout(
        Duration::from_secs(15),
        stack.client.read_object(std::slice::from_ref(&location)),
    )
    .await
    .expect("read did not return after DiskIO exited");
    assert!(
        read.is_err(),
        "protected read succeeded after its DiskIO process exited"
    );
}
