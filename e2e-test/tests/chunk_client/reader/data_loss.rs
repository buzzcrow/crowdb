// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn ec_read_reports_data_loss_beyond_parity_tolerance() {
    let stack = E2eStack::start(small_policy(2)).await;
    let data = test_data(4 * MIB);
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), large_policy())
        .write_stream(data.as_slice())
        .await
        .unwrap();
    let chunk = stack.query_chunk(&result.locations[0]).await;
    let Some(Strip::EcStrip(ec)) = &chunk.strips[0].strip else {
        panic!("large write did not produce EC");
    };
    let failed = vec![ec.segments[0].disk_id.unwrap(), ec.segments[1].disk_id.unwrap()];
    let (reader, _) = reader_with_failures(&stack, failed).await;
    let partial = reader
        .read_range_partial(&result.locations, 0, 16 * KIB as u64)
        .await
        .unwrap();
    assert!(partial.ranges.is_empty());
    assert_eq!(partial.failures.len(), 1);
    assert_eq!(
        (partial.failures[0].start, partial.failures[0].end),
        (0, 16 * KIB as u64)
    );
    assert!(matches!(partial.failures[0].error, ReadError::DataLoss(_)));
    assert!(matches!(
        reader.read_range(&result.locations, 0, 16 * KIB as u64).await,
        Err(ReadError::FailedRange { start: 0, end, .. }) if end == 16 * KIB as u64
    ));
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn partial_read_preserves_healthy_ec_shard_ranges_around_data_loss() {
    let stack = E2eStack::start_with_chunkdb_options(
        small_policy(2),
        ChunkdbStartOptions {
            repair_enabled: false,
            ..ChunkdbStartOptions::default()
        },
    )
    .await;
    let data = test_data(4 * MIB);
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), large_policy())
        .write_stream(data.as_slice())
        .await
        .unwrap();
    let chunk = stack.query_chunk(&result.locations[0]).await;
    let Some(Strip::EcStrip(ec)) = &chunk.strips[0].strip else {
        panic!("large write did not produce EC");
    };
    let reader = reader_with_segment_failures(&stack, ec.segments[1..3].to_vec()).await;
    let payload_per_shard = 16 * MAX_FRAME_PAYLOAD_BYTES as u64;
    let partial = reader
        .read_range_partial(&result.locations, 0, 4 * MIB as u64)
        .await
        .unwrap();
    assert_eq!(partial.failures.len(), 1);
    assert_eq!(
        (partial.failures[0].start, partial.failures[0].end),
        (payload_per_shard, 3 * payload_per_shard)
    );
    assert_eq!(partial.ranges.len(), 33);
    assert_eq!(
        (partial.ranges[0].start, partial.ranges[15].end),
        (0, payload_per_shard)
    );
    assert_eq!(
        partial.ranges[..16]
            .iter()
            .map(|range| range.data.as_ref())
            .collect::<Vec<_>>()
            .concat(),
        data[..usize::try_from(payload_per_shard).unwrap()]
    );
    assert_eq!(
        (partial.ranges[16].start, partial.ranges[32].end),
        (3 * payload_per_shard, 4 * MIB as u64)
    );
    assert_eq!(
        partial.ranges[16..]
            .iter()
            .map(|range| range.data.as_ref())
            .collect::<Vec<_>>()
            .concat(),
        data[usize::try_from(3 * payload_per_shard).unwrap()..]
    );

    let mut stream = reader.read_stream(&result.locations).unwrap();
    let mut prefix = Vec::new();
    for _ in 0..16 {
        prefix.extend_from_slice(&stream.next_chunk().await.unwrap().unwrap());
    }
    assert_eq!(prefix, data[..usize::try_from(payload_per_shard).unwrap()]);
    assert!(matches!(
        stream.next_chunk().await.unwrap(),
        Err(ReadError::FailedRange { start, end, .. })
            if start == payload_per_shard && end == payload_per_shard + MAX_FRAME_PAYLOAD_BYTES as u64
    ));
    assert!(stream.next_chunk().await.is_none());
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn mirror_read_succeeds_and_reports_replica_loss() {
    let stack = E2eStack::start(small_policy(2)).await;
    let data = Bytes::from(test_data(60 * KIB));
    let mut writer = stack.client.prepare_small_write(data.len()).await.unwrap();
    writer.on_data(data.clone()).await.unwrap();
    let location: Location = writer.on_finish().await.unwrap().remove(0);
    let chunk = stack.query_chunk(&location).await;
    let strip = chunk
        .strips
        .iter()
        .find(|strip| u64::from(strip.chunk_offset) * KIB as u64 <= location.offset)
        .unwrap();
    let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
        panic!("small write did not produce a mirror strip");
    };
    assert_eq!(mirror.segments.len(), 2);
    let primary = mirror.segments[0].disk_id.unwrap();
    assert_eq!(
        stack
            .client
            .read_object(std::slice::from_ref(&location))
            .await
            .unwrap()
            .concat(),
        data
    );
    let (fallback, _) = reader_with_failures(&stack, vec![primary]).await;
    assert_eq!(
        fallback
            .read_object(std::slice::from_ref(&location))
            .await
            .unwrap()
            .concat(),
        data
    );
    let failed = mirror
        .segments
        .iter()
        .map(|segment| segment.disk_id.unwrap())
        .collect();
    let (unreadable, _) = reader_with_failures(&stack, failed).await;
    assert!(matches!(
        unreadable.read_object(std::slice::from_ref(&location)).await,
        Err(ReadError::FailedRange { start: 0, end, .. }) if end == data.len() as u64
    ));
    // The durable read-failure marker changes the active chunk revision. The
    // owning small-write pipeline must refresh that metadata and keep writing.
    let next = Bytes::from(test_data(32 * KIB));
    let mut writer = stack.client.prepare_small_write(next.len()).await.unwrap();
    writer.on_data(next).await.unwrap();
    let next_location = writer.on_finish().await.unwrap().remove(0);
    assert_eq!(next_location.chunk_id, location.chunk_id);
    stack.client.shutdown_small_writes().await.unwrap();
}
