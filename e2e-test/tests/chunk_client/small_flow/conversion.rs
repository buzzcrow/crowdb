// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn consecutive_conversion_groups_keep_one_mibibyte_shard_geometry() {
    let mut configured = policy();
    configured.chunk_capacity = 16 * MIB as u64;
    let stack = E2eStack::start(configured).await;
    let mut locations = Vec::new();
    for value in 0_u8..16 {
        let group = write_full_small_strip(&stack.client, value).await;
        locations.push(group[0].1.clone());
    }
    let chunk = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let chunk = stack.query_chunk(&locations[0]).await;
            if chunk.strips.len() == 2
                && chunk
                    .strips
                    .iter()
                    .all(|strip| matches!(strip.strip, Some(Strip::EcStrip(_))) && strip.capacity == 8 * 1024)
            {
                break chunk;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("two fixed-geometry conversion groups");
    assert_eq!(chunk.capacity, 16 * 1024);
    stack.client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn chunkdb_takes_over_durable_conversion_task_after_client_io_failure() {
    let mut configured = policy();
    configured.chunk_capacity = 16 * MIB as u64;
    configured.writer_lease = Duration::from_secs(2);
    let stack = E2eStack::start(configured.clone()).await;
    let (allocator, disk_writer) = real_write_parts(&stack).await;
    let fault = Arc::new(FailWritesFromCall {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        first_failed_call: 8 * FRAMES_PER_MIB_STRIP * configured.mirror_copies as usize + 1,
    });
    let client = ChunkIoClient::from_parts_with_small_policy(allocator, fault, configured).unwrap();
    let mut object_groups = Vec::new();
    for value in 0_u8..8 {
        object_groups.push(write_full_small_strip(&client, value + 1).await);
    }
    let locations: Vec<Location> = object_groups
        .iter()
        .flat_map(|group| group.iter().map(|(_, location)| location.clone()))
        .collect();
    let location = locations[0].clone();
    let failed_fast_path = stack.query_chunk(&location).await;
    assert_eq!(failed_fast_path.strips.len(), 8);
    assert!(failed_fast_path
        .strips
        .iter()
        .all(|strip| matches!(strip.strip, Some(Strip::MirrorStrip(_)))));
    assert_eq!(client.small_write_metrics().failed, 0);
    let converted = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let chunk = stack.query_chunk(&location).await;
            if matches!(chunk.strips.first(), Some(strip) if matches!(strip.strip, Some(Strip::EcStrip(_)))) {
                break chunk;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("chunkdb did not take over the expired client task");
    let Some(Strip::EcStrip(ec)) = &converted.strips[0].strip else {
        unreachable!();
    };
    assert_eq!((ec.data_num, ec.code_num, ec.segments.len()), (8, 4, 12));
    assert_eq!(ec.ec_state, crowdb_protocol::chunkdb::rpc::EcState::Parity as i32);
    let unit_bytes = u64::from(converted.strips[0].unit_kb) * KIB as u64;
    let before = failed_fast_path;
    let mut expected_data = Vec::with_capacity(object_groups.len());
    for group in &object_groups {
        expected_data.push(read_data_image(&stack, &before, &group[0].1).await);
    }
    for (segment, expected) in ec.segments[..8].iter().zip(&expected_data) {
        let actual = stack
            .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
            .await;
        assert_eq!(&actual, expected);
    }
    let refs: Vec<&[u8]> = expected_data.iter().map(Bytes::as_ref).collect();
    let parity = encode_parity_from_shards(EcScheme::new(8, 4), &refs).unwrap();
    for (segment, expected) in ec.segments[8..].iter().zip(parity) {
        let actual = stack
            .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
            .await;
        assert_eq!(actual, expected);
    }
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
#[allow(clippy::too_many_lines)]
async fn manual_chunkdb_trigger_converts_closed_active_range_end_to_end() {
    let mut configured = policy();
    configured.chunk_capacity = 16 * MIB as u64;
    configured.conversion_enabled = false;
    configured.chunk_type = ChunkType::S3;
    let stack = E2eStack::start(configured).await;
    let mut object_groups = Vec::new();
    for value in 11_u8..19 {
        object_groups.push(write_full_small_strip(&stack.client, value).await);
    }
    let locations: Vec<Location> = object_groups
        .iter()
        .flat_map(|group| group.iter().map(|(_, location)| location.clone()))
        .collect();
    let location = &locations[0];
    let before = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let chunk = stack.query_chunk(location).await;
            if chunk.closed_strip_sequence == Some(7) {
                break chunk;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background close checkpoint did not reach strip 7");
    assert_eq!(before.state, ChunkState::Active as i32);
    assert_eq!(before.chunk_type, ChunkType::S3 as i32);
    assert_eq!(before.id.unwrap().high >> 56, ChunkType::S3 as u64);
    assert_eq!(before.closed_strip_sequence, Some(7));
    assert_eq!(before.strips.len(), 8);
    assert!(before
        .strips
        .iter()
        .all(|strip| matches!(strip.strip, Some(Strip::MirrorStrip(_)))));
    let mut data = Vec::with_capacity(object_groups.len());
    for group in &object_groups {
        data.push(read_data_image(&stack, &before, &group[0].1).await);
    }

    let (chunkdb, _) = real_write_parts(&stack).await;
    let triggered = chunkdb
        .trigger_conversion(TriggerConversionRequest {
            chunk_id: location.chunk_id,
        })
        .await
        .expect("manual conversion trigger");
    assert_eq!(triggered.accepted_groups, 1);

    let converted = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let chunk = stack.query_chunk(location).await;
            if matches!(chunk.strips.first(), Some(strip) if matches!(strip.strip, Some(Strip::EcStrip(_)))) {
                break chunk;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("manual conversion task did not finish");
    let strip = &converted.strips[0];
    assert_eq!(converted.id, before.id);
    assert_eq!(converted.chunk_type, ChunkType::S3 as i32);
    let Some(Strip::EcStrip(ec)) = &strip.strip else {
        unreachable!();
    };
    assert_eq!((ec.data_num, ec.code_num, ec.segments.len()), (8, 4, 12));
    let unit_bytes = u64::from(strip.unit_kb) * KIB as u64;
    for (segment, expected) in ec.segments[..8].iter().zip(&data) {
        let actual = stack
            .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
            .await;
        assert_eq!(&actual, expected.as_ref());
    }
    let refs: Vec<&[u8]> = data.iter().map(Bytes::as_ref).collect();
    let parity = encode_parity_from_shards(EcScheme::new(8, 4), &refs).unwrap();
    for (segment, expected) in ec.segments[8..].iter().zip(parity) {
        let actual = stack
            .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
            .await;
        assert_eq!(actual, expected);
    }
    let appended_data = Bytes::from(vec![0xd3; MAX_FRAME_PAYLOAD_BYTES]);
    let appended = write_object(&stack.client, appended_data.clone()).await;
    assert_eq!(appended.chunk_id, location.chunk_id);
    assert_eq!(appended.offset, 8 * MIB as u64);
    let after_append = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let chunk = stack.query_chunk(&appended).await;
            if chunk.strips.len() == 2 {
                break chunk;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reserved append confirmation");
    assert_eq!(after_append.strips.len(), 2);
    assert!(matches!(after_append.strips[0].strip, Some(Strip::EcStrip(_))));
    assert_mirror_data(&stack, &after_append, &appended, &appended_data).await;
    for group in &object_groups {
        for (expected, location) in group {
            assert_eq!(
                stack
                    .client
                    .read_object(std::slice::from_ref(location))
                    .await
                    .unwrap()
                    .concat(),
                *expected
            );
        }
    }
    assert_eq!(
        stack
            .client
            .read_object(std::slice::from_ref(&appended))
            .await
            .unwrap()
            .concat(),
        appended_data
    );
    stack.client.shutdown_small_writes().await.unwrap();
}
