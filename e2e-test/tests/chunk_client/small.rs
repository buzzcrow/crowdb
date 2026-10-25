// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::stack::TestStack as E2eStack;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoWriter, SmallWritePolicy};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkType, Location, Strip};
use crowdb_protocol::frame::MAX_FRAME_V2_PAYLOAD_BYTES;
use crowdb_protocol::frame::{parse_frame, FrameMagic};
use std::time::Duration;
const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const FRAMES_PER_MIB_STRIP: usize = MIB / (64 * KIB);
fn policy() -> SmallWritePolicy {
    SmallWritePolicy {
        memory_budget: 16 * MIB,
        queue_capacity: 256,
        min_pipelines: 1,
        max_pipelines: 1,
        scale_out_queue_bytes: 16 * MIB,
        scale_out_queue_objects: 256,
        scale_in_delay: Duration::from_millis(50),
        control_interval: Duration::from_millis(1),
        cooldown: Duration::from_millis(1),
        chunk_capacity: 2 * MIB as u64,
        object_limit: MAX_FRAME_V2_PAYLOAD_BYTES,
        mirror_copies: 2,
        ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
    }
}

async fn write_object(client: &ChunkIoClient, data: Bytes) -> Location {
    let mut writer = client.prepare_small_write(data.len()).await.unwrap();
    writer.on_data(data).await.unwrap();
    writer.on_finish().await.unwrap().remove(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release storage containers"]
async fn shared_object_spans_mirror_strips_with_one_location() {
    let mut configured = policy();
    configured.chunk_capacity = 16 * MIB as u64;
    configured.object_limit = 2 * MIB;
    let stack = E2eStack::start(configured).await;
    let _prefix = write_object(&stack.client, Bytes::from_static(b"PAR1datafoot\x04\0\0\0PAR1")).await;
    let data = Bytes::from(
        (0_u32..1_200_000)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>(),
    );
    let mut writer = stack
        .client
        .prepare_shared_object_write_for_key(data.len(), b"shared-span")
        .await
        .unwrap();
    let submitted_before = stack.client.small_write_metrics().submitted;
    for (index, fragment) in data.chunks(1024 * 1024).enumerate() {
        writer.on_data(Bytes::copy_from_slice(fragment)).await.unwrap();
        if index == 0 {
            assert_eq!(stack.client.small_write_metrics().submitted, submitted_before);
        }
    }
    let locations = writer.on_finish().await.unwrap();
    assert_eq!(locations.len(), 1);
    let read = stack
        .client
        .read_range(&locations, 1_048_550, 1_048_601)
        .await
        .unwrap();
    assert_eq!(read.concat(), &data[1_048_550..1_048_601]);
    let mut stream = stack
        .client
        .read_range_stream(&locations, 1_048_550, 1_048_601)
        .unwrap();
    let mut streamed = Vec::new();
    while let Some(chunk) = stream.next_chunk().await {
        streamed.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(streamed, &data[1_048_550..1_048_601]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release storage containers"]
async fn cancelled_shared_object_keeps_pipeline_available() {
    let mut configured = policy();
    configured.chunk_capacity = 16 * MIB as u64;
    configured.object_limit = 2 * MIB;
    let stack = E2eStack::start(configured).await;
    let mut cancelled = stack
        .client
        .prepare_shared_object_write_for_key(1_200_000, b"cancelled")
        .await
        .unwrap();
    cancelled.on_data(Bytes::from(vec![7; MIB])).await.unwrap();
    cancelled.on_error().await.unwrap();

    let data = Bytes::from_static(b"surviving object");
    let mut survivor = stack.client.prepare_small_write(data.len()).await.unwrap();
    survivor.on_data(data.clone()).await.unwrap();
    let location = survivor.finish_durable().await.unwrap().remove(0);
    let read = stack
        .client
        .read_range(&[location], 0, data.len() as u64)
        .await
        .unwrap();
    assert_eq!(read.concat(), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release storage containers"]
async fn small_write_rotates_strips_and_chunks_without_splitting_objects() {
    let mut configured = policy();
    configured.chunk_type = ChunkType::IcebergTable;
    let stack = E2eStack::start(configured).await;
    let first_group = write_full_small_strip(&stack.client, 3).await;
    let second_group = write_full_small_strip(&stack.client, 5).await;
    let third_group = write_full_small_strip(&stack.client, 7).await;
    let first = first_group[0].1.clone();
    let second = second_group[0].1.clone();
    let third = third_group[0].1.clone();

    assert_eq!(first.chunk_id, second.chunk_id);
    assert_eq!(first.offset, 0);
    assert_eq!(second.offset, MIB as u64);
    assert_ne!(second.chunk_id, third.chunk_id);
    assert_eq!(third.offset, 0);
    let first_chunk = stack.query_chunk(&first).await;
    assert_eq!(first_chunk.chunk_type, ChunkType::IcebergTable as i32);
    assert_eq!(first_chunk.id.unwrap().high >> 56, ChunkType::IcebergTable as u64);
    assert_eq!(first_chunk.state, ChunkState::Sealed as i32);
    assert_eq!(first_chunk.strips.len(), 2);
    assert_eq!(first_chunk.acknowledged_cursor, 2 * MIB as u64);
    assert_eq!(first_chunk.closed_strip_sequence, Some(1));
    for (data, location) in &first_group {
        assert_mirror_data(&stack, &first_chunk, location, data).await;
    }
    for (data, location) in &second_group {
        assert_mirror_data(&stack, &first_chunk, location, data).await;
    }
    let third_chunk = stack.query_chunk(&third).await;
    assert_eq!(third_chunk.chunk_type, ChunkType::IcebergTable as i32);
    assert_eq!(third_chunk.id.unwrap().high >> 56, ChunkType::IcebergTable as u64);
    for (data, location) in &third_group {
        assert_mirror_data(&stack, &third_chunk, location, data).await;
    }

    stack.client.shutdown_small_writes().await.unwrap();
    let third_chunk = stack.query_chunk(&third).await;
    assert_eq!(third_chunk.state, ChunkState::Sealed as i32);
}

async fn write_full_small_strip(client: &ChunkIoClient, value: u8) -> Vec<(Bytes, Location)> {
    let mut frames = Vec::with_capacity(FRAMES_PER_MIB_STRIP);
    for _ in 0..FRAMES_PER_MIB_STRIP {
        let payload = Bytes::from(vec![value; MAX_FRAME_V2_PAYLOAD_BYTES]);
        let location = write_object(client, payload.clone()).await;
        frames.push((payload, location));
    }
    frames
}

async fn assert_mirror_data(stack: &E2eStack, chunk: &Chunk, location: &Location, expected: &[u8]) {
    let strip = chunk
        .strips
        .iter()
        .find(|strip| {
            let start = u64::from(strip.chunk_offset) * KIB as u64;
            let end = start + u64::from(strip.capacity) * KIB as u64;
            start <= location.offset && location.offset + location.length <= end
        })
        .expect("location strip");
    let Strip::MirrorStrip(mirror) = strip.strip.as_ref().expect("strip body") else {
        panic!("small-write chunk contains a non-mirror strip");
    };
    assert!(!mirror.segments.is_empty());
    let strip_start = u64::from(strip.chunk_offset) * KIB as u64;
    let unit_bytes = u64::from(strip.unit_kb) * KIB as u64;
    for segment in &mirror.segments {
        let actual = stack
            .read_segment(
                segment,
                unit_bytes,
                location.offset - strip_start,
                u32::try_from(location.length).unwrap(),
            )
            .await;
        let frame = parse_frame(&actual, location.chunk_id.expect("location chunk"))
            .expect("durable RepoSmall frame");
        assert_eq!(frame.header.magic, FrameMagic::RepoSmallV2);
        assert_eq!(frame.payload, expected);
    }
}
