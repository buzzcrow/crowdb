// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn small_write_batches_concurrent_objects_and_reads_them_back() {
    let mut configured = policy();
    configured.max_batch_objects = 64;
    configured.max_batch_bytes = MIB;
    let stack = E2eStack::start(configured).await;
    let completed = concurrent_writes(&stack, 16, 16 * KIB).await;

    let metrics = stack.client.small_write_metrics();
    assert_eq!(metrics.submitted, 16);
    assert_eq!(metrics.completed, 16);
    assert_eq!(metrics.failed, 0);
    assert!(metrics.batches < metrics.completed);
    assert!(metrics.max_batch_objects > 1);
    stack.client.shutdown_small_writes().await.unwrap();
    for (data, location) in completed {
        assert_eq!(
            stack
                .client
                .read_object(std::slice::from_ref(&location))
                .await
                .unwrap()
                .concat(),
            data
        );
        let chunk = stack.query_chunk(&location).await;
        assert_mirror_data(&stack, &chunk, &location, &data).await;
    }
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn eight_closed_mirror_strips_become_one_durable_ec_strip_without_reread() {
    let mut configured = policy();
    configured.chunk_capacity = 16 * MIB as u64;
    let stack = E2eStack::start(configured).await;
    let mut object_groups = Vec::new();
    for index in 0_u8..8 {
        object_groups
            .push(write_full_small_strip(&stack.client, index.wrapping_mul(29).wrapping_add(7)).await);
    }
    let locations: Vec<Location> = object_groups
        .iter()
        .flat_map(|group| group.iter().map(|(_, location)| location.clone()))
        .collect();
    assert!(locations
        .iter()
        .all(|location| location.chunk_id == locations[0].chunk_id));
    let before = stack.query_chunk(&locations[0]).await;
    let mut data = Vec::with_capacity(object_groups.len());
    for group in &object_groups {
        data.push(read_data_image(&stack, &before, &group[0].1).await);
    }
    // Draining joins the already-scheduled foreground conversion task. This
    // is a deterministic completion boundary and still proves that parity was
    // produced from retained mirror images rather than rereading the data.
    stack.client.shutdown_small_writes().await.unwrap();
    let chunk = stack.query_chunk(&locations[0]).await;
    assert_eq!(chunk.strips.len(), 1);
    let strip = &chunk.strips[0];
    let Some(Strip::EcStrip(ec)) = &strip.strip else {
        panic!("expected converted EC strip");
    };
    assert_eq!((ec.data_num, ec.code_num), (8, 4));
    assert_eq!(ec.ec_state, crowdb_protocol::chunkdb::rpc::EcState::Parity as i32);
    assert_eq!(ec.segments.len(), 12);
    let unit_bytes = u64::from(strip.unit_kb) * KIB as u64;
    for (segment, expected) in ec.segments[..8].iter().zip(&data) {
        let actual = stack
            .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
            .await;
        assert_eq!(&actual, expected.as_ref());
    }
    let refs: Vec<&[u8]> = data.iter().map(Bytes::as_ref).collect();
    let expected_parity = encode_parity_from_shards(EcScheme::new(8, 4), &refs).unwrap();
    for (segment, expected) in ec.segments[8..].iter().zip(expected_parity) {
        let actual = stack
            .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
            .await;
        assert_eq!(actual, expected);
    }
    let mut degraded = Vec::with_capacity(12);
    for segment in &ec.segments {
        degraded.push(Some(
            stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
                .await,
        ));
    }
    degraded[3] = None;
    let reconstructed = decode(EcScheme::new(8, 4), degraded).unwrap();
    assert_eq!(reconstructed[3], data[3]);
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
    let sealed = stack.query_chunk(&locations[0]).await;
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    assert_eq!(sealed.strips.len(), 1);
    assert_eq!(sealed.capacity, 8 * 1024);
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn small_write_scales_out_on_queued_bytes_then_scales_in_when_empty() {
    let mut configured = policy();
    configured.max_pipelines = 2;
    configured.max_batch_objects = 1;
    configured.max_batch_bytes = 64 * KIB;
    configured.scale_out_queue_bytes = 64 * KIB;
    configured.scale_out_queue_objects = configured.queue_capacity;
    let stack = E2eStack::start(configured).await;
    let completed = concurrent_writes(&stack, 64, MAX_FRAME_V2_PAYLOAD_BYTES).await;

    wait_for_metrics(&stack.client, || stack.client.small_write_metrics().scale_out > 0).await;
    assert_eq!(completed.len(), 64);
    wait_for_metrics(&stack.client, || {
        let metrics = stack.client.small_write_metrics();
        metrics.active_pipelines == 1 && metrics.draining_pipelines == 0 && metrics.scale_in > 0
    })
    .await;
    stack.client.shutdown_small_writes().await.unwrap();
    assert_eq!(stack.client.small_write_metrics().active_pipelines, 0);
}
