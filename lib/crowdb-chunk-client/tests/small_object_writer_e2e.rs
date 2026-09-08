// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Core small-write E2E coverage through real service processes.

#[path = "common/e2e_stack.rs"]
mod e2e_stack;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoWriter, SmallWritePolicy};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, Location, Strip};

use e2e_stack::{all_binaries_available, E2eStack};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

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
        mirror_copies: 1,
        ..SmallWritePolicy::default()
    }
}

async fn write_object(client: &ChunkIoClient, data: Bytes) -> Location {
    let mut writer = client.prepare_small_write(data.len()).await.unwrap();
    writer.on_data(data).await.unwrap();
    writer.on_finish().await.unwrap().remove(0)
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
        assert_eq!(actual, expected);
    }
}

async fn wait_for_metrics(client: &ChunkIoClient, predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "small-write metrics did not converge: {:?}",
            client.small_write_metrics()
        )
    });
}

async fn concurrent_writes(stack: &E2eStack, count: usize, size: usize) -> Vec<(Bytes, Location)> {
    let barrier = Arc::new(tokio::sync::Barrier::new(count + 1));
    let mut tasks = Vec::new();
    for value in 0..count {
        let client = stack.client.clone();
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            let data = Bytes::from(vec![u8::try_from(value % 251).unwrap(); size]);
            let mut writer = client.prepare_small_write(data.len()).await.unwrap();
            writer.on_data(data.clone()).await.unwrap();
            barrier.wait().await;
            (data, writer.on_finish().await.unwrap().remove(0))
        }));
    }
    barrier.wait().await;
    let mut completed = Vec::new();
    for task in tasks {
        completed.push(task.await.unwrap());
    }
    completed
}

#[tokio::test]
async fn small_write_batches_concurrent_objects_and_reads_them_back() {
    if !all_binaries_available() {
        return;
    }
    let mut configured = policy();
    configured.max_batch_objects = 64;
    configured.max_batch_bytes = MIB;
    configured.batch_deadline = Duration::from_millis(50);
    let stack = E2eStack::start(configured).await;
    let completed = concurrent_writes(&stack, 16, 16 * KIB).await;

    let metrics = stack.client.small_write_metrics();
    assert_eq!(metrics.submitted, 16);
    assert_eq!(metrics.completed, 16);
    assert_eq!(metrics.failed, 0);
    assert!(metrics.batches < metrics.completed);
    assert!(metrics.max_batch_objects > 1);
    for (data, location) in completed {
        let chunk = stack.query_chunk(&location).await;
        assert_mirror_data(&stack, &chunk, &location, &data).await;
    }
    stack.client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_write_rotates_strips_and_chunks_without_splitting_objects() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(policy()).await;
    let first_data = Bytes::from(vec![3; 700 * KIB]);
    let second_data = Bytes::from(vec![5; 400 * KIB]);
    let third_data = Bytes::from(vec![7; 700 * KIB]);
    let first = write_object(&stack.client, first_data.clone()).await;
    let second = write_object(&stack.client, second_data.clone()).await;
    let third = write_object(&stack.client, third_data.clone()).await;

    assert_eq!(first.chunk_id, second.chunk_id);
    assert_eq!(first.offset, 0);
    assert_eq!(second.offset, MIB as u64);
    assert_ne!(second.chunk_id, third.chunk_id);
    assert_eq!(third.offset, 0);
    let first_chunk = stack.query_chunk(&first).await;
    assert_eq!(first_chunk.state, ChunkState::Sealed as i32);
    assert_eq!(first_chunk.strips.len(), 2);
    assert_eq!(first_chunk.acknowledged_cursor, 2 * MIB as u64);
    assert_eq!(first_chunk.closed_strip_sequence, Some(1));
    assert_mirror_data(&stack, &first_chunk, &first, &first_data).await;
    assert_mirror_data(&stack, &first_chunk, &second, &second_data).await;
    let third_chunk = stack.query_chunk(&third).await;
    assert_mirror_data(&stack, &third_chunk, &third, &third_data).await;

    stack.client.shutdown_small_writes().await.unwrap();
    let third_chunk = stack.query_chunk(&third).await;
    assert_eq!(third_chunk.state, ChunkState::Sealed as i32);
}

#[tokio::test]
async fn small_write_scales_out_on_queued_bytes_then_scales_in_when_empty() {
    if !all_binaries_available() {
        return;
    }
    let mut configured = policy();
    configured.max_pipelines = 2;
    configured.max_batch_objects = 1;
    configured.max_batch_bytes = 64 * KIB;
    configured.scale_out_queue_bytes = 64 * KIB;
    configured.scale_out_queue_objects = configured.queue_capacity;
    let stack = E2eStack::start(configured).await;
    let completed = concurrent_writes(&stack, 64, 64 * KIB).await;

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

#[tokio::test]
async fn small_write_scales_out_on_queued_object_count() {
    if !all_binaries_available() {
        return;
    }
    let mut configured = policy();
    configured.max_pipelines = 2;
    configured.max_batch_objects = 1;
    configured.max_batch_bytes = 4 * KIB;
    configured.scale_out_queue_bytes = configured.memory_budget;
    configured.scale_out_queue_objects = 2;
    let stack = E2eStack::start(configured).await;
    let completed = concurrent_writes(&stack, 64, 4 * KIB).await;

    wait_for_metrics(&stack.client, || stack.client.small_write_metrics().scale_out > 0).await;
    assert_eq!(completed.len(), 64);
    assert_eq!(stack.client.small_write_metrics().failed, 0);
    stack.client.shutdown_small_writes().await.unwrap();
}
