// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Core small-write E2E coverage through real service processes.

#[path = "common/e2e_stack.rs"]
mod e2e_stack;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkIoClient, ChunkIoWriter, DiskWriter, IoError, Result, RoutedDiskWriter, SmallWritePolicy,
};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_common::ec::{encode_parity_from_shards, EcScheme};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, Location, Strip, TriggerConversionRequest};
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_test_harness::chunkdb::ChunkdbStartOptions;

use e2e_stack::{all_binaries_available, E2eStack};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

struct FailSelectedDiskWrite {
    inner: Arc<dyn DiskWriter>,
    calls: AtomicUsize,
    fail_on: usize,
    failed_disk: Mutex<Option<DiskId>>,
}

struct FailWritesFromCall {
    inner: Arc<dyn DiskWriter>,
    calls: AtomicUsize,
    first_failed_call: usize,
}

#[async_trait]
impl DiskWriter for FailSelectedDiskWrite {
    async fn write(&self, segment: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        if self.calls.fetch_add(1, Ordering::AcqRel) + 1 == self.fail_on {
            *self.failed_disk.lock().unwrap() = segment.disk_id;
            return Err(IoError::WriteFailed("injected first replica failure".into()));
        }
        self.inner.write(segment, unit_bytes, data).await
    }
}

#[async_trait]
impl DiskWriter for FailWritesFromCall {
    async fn write(&self, segment: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        if self.calls.fetch_add(1, Ordering::AcqRel) + 1 >= self.first_failed_call {
            return Err(IoError::WriteFailed("injected persistent disk failure".into()));
        }
        self.inner.write(segment, unit_bytes, data).await
    }
}

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

async fn real_write_parts(stack: &E2eStack) -> (Arc<ChunkdbClient>, Arc<RoutedDiskWriter>) {
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
        stack.cluster.mgmt_endpoints.clone(),
    )));
    let service = ServiceRegistryClient::from_shared(kv.clone());
    let hardware = HardwareClient::from_shared(kv);
    let chunkdb = Arc::new(ChunkdbClient::new(
        service.clone(),
        Arc::new(ChunkdbRpcTransport::new()),
    ));
    chunkdb.refresh_endpoints().await.unwrap();
    let disk_writer = Arc::new(RoutedDiskWriter::connect(&service, &hardware).await.unwrap());
    (chunkdb, disk_writer)
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
async fn eight_closed_mirror_strips_become_one_durable_ec_strip_without_reread() {
    if !all_binaries_available() {
        return;
    }
    let mut configured = policy();
    configured.chunk_capacity = 16 * MIB as u64;
    let stack = E2eStack::start(configured).await;
    let mut data = Vec::new();
    let mut locations = Vec::new();
    for index in 0_u8..8 {
        let shard = Bytes::from(vec![index.wrapping_mul(29).wrapping_add(7); MIB]);
        locations.push(write_object(&stack.client, shard.clone()).await);
        data.push(shard);
    }
    assert!(locations
        .iter()
        .all(|location| location.chunk_id == locations[0].chunk_id));
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
    stack.client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn chunkdb_takes_over_durable_conversion_task_after_client_io_failure() {
    if !all_binaries_available() {
        return;
    }
    let mut configured = policy();
    configured.chunk_capacity = 16 * MIB as u64;
    configured.writer_lease = Duration::from_millis(200);
    let stack = E2eStack::start(configured.clone()).await;
    let (allocator, disk_writer) = real_write_parts(&stack).await;
    let fault = Arc::new(FailWritesFromCall {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        first_failed_call: 9,
    });
    let client = ChunkIoClient::from_parts_with_small_policy(allocator, fault, configured).unwrap();
    let mut locations = Vec::new();
    for value in 0_u8..8 {
        locations.push(write_object(&client, Bytes::from(vec![value + 1; MIB])).await);
    }
    let location = locations[0].clone();
    let converted = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let chunk = stack.query_chunk(&location).await;
            if matches!(chunk.strips.as_slice(), [strip] if matches!(strip.strip, Some(Strip::EcStrip(_)))) {
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
    let expected_data: Vec<Vec<u8>> = (1_u8..=8).map(|value| vec![value; MIB]).collect();
    for (segment, expected) in ec.segments[..8].iter().zip(&expected_data) {
        let actual = stack
            .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
            .await;
        assert_eq!(&actual, expected);
    }
    let refs: Vec<&[u8]> = expected_data.iter().map(Vec::as_slice).collect();
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
async fn manual_chunkdb_trigger_converts_closed_active_range_end_to_end() {
    if !all_binaries_available() {
        return;
    }
    let mut configured = policy();
    configured.chunk_capacity = 16 * MIB as u64;
    configured.conversion_enabled = false;
    let stack = E2eStack::start(configured).await;
    let mut data = Vec::new();
    let mut locations = Vec::new();
    for value in 11_u8..19 {
        let shard = Bytes::from(vec![value; MIB]);
        locations.push(write_object(&stack.client, shard.clone()).await);
        data.push(shard);
    }
    let location = &locations[0];
    let before = stack.query_chunk(location).await;
    assert_eq!(before.state, ChunkState::Active as i32);
    assert_eq!(before.closed_strip_sequence, Some(7));
    assert_eq!(before.strips.len(), 8);
    assert!(before
        .strips
        .iter()
        .all(|strip| matches!(strip.strip, Some(Strip::MirrorStrip(_)))));

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
            if matches!(chunk.strips.as_slice(), [strip] if matches!(strip.strip, Some(Strip::EcStrip(_)))) {
                break chunk;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("manual conversion task did not finish");
    let strip = &converted.strips[0];
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
    stack.client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn automatic_chunkdb_scan_converts_three_groups_and_preserves_tail() {
    if !all_binaries_available() {
        return;
    }
    let mut configured = policy();
    configured.chunk_capacity = 32 * MIB as u64;
    configured.conversion_enabled = false;
    let stack = E2eStack::start_with_chunkdb_options(
        configured,
        ChunkdbStartOptions {
            allow_unsafe_ec: true,
            conversion_enabled: true,
            conversion_min_seal_age_secs: 10,
            conversion_scan_interval_secs: 1,
            ..ChunkdbStartOptions::default()
        },
    )
    .await;
    let mut data = Vec::new();
    let mut locations = Vec::new();
    for value in 31_u8..57 {
        let shard = Bytes::from(vec![value; MIB]);
        locations.push(write_object(&stack.client, shard.clone()).await);
        data.push(shard);
    }
    assert!(locations
        .iter()
        .all(|location| location.chunk_id == locations[0].chunk_id));
    stack.client.shutdown_small_writes().await.unwrap();

    let converted = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let chunk = stack.query_chunk(&locations[0]).await;
            if chunk
                .strips
                .iter()
                .filter(|strip| matches!(strip.strip, Some(Strip::EcStrip(_))))
                .count()
                == 3
            {
                break chunk;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("automatic conversion scan did not finish");
    assert_eq!(converted.state, ChunkState::Sealed as i32);
    assert_eq!(converted.strips.len(), 5);
    assert!(converted.strips[3..]
        .iter()
        .all(|strip| matches!(strip.strip, Some(Strip::MirrorStrip(_)))));
    for group in 0..3 {
        let ec_strip = &converted.strips[group];
        let Some(Strip::EcStrip(ec)) = &ec_strip.strip else {
            panic!("converted group {group} is not EC");
        };
        assert_eq!((ec.data_num, ec.code_num, ec.segments.len()), (8, 4, 12));
        let unit_bytes = u64::from(ec_strip.unit_kb) * KIB as u64;
        let group_data = &data[group * 8..group * 8 + 8];
        for (segment, expected) in ec.segments[..8].iter().zip(group_data) {
            let actual = stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
                .await;
            assert_eq!(&actual, expected.as_ref());
        }
        let refs: Vec<&[u8]> = group_data.iter().map(Bytes::as_ref).collect();
        let parity = encode_parity_from_shards(EcScheme::new(8, 4), &refs).unwrap();
        for (segment, expected) in ec.segments[8..].iter().zip(parity) {
            let actual = stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
                .await;
            assert_eq!(actual, expected);
        }
    }
    for (location, expected) in locations[24..].iter().zip(&data[24..]) {
        assert_mirror_data(&stack, &converted, location, expected).await;
    }
}

#[tokio::test]
async fn chunkdb_restart_recovers_an_inflight_conversion_claim() {
    if !all_binaries_available() {
        return;
    }
    let mut configured = policy();
    configured.chunk_capacity = 32 * MIB as u64;
    configured.conversion_enabled = false;
    let mut stack = E2eStack::start_with_chunkdb_options(
        configured,
        ChunkdbStartOptions {
            allow_unsafe_ec: true,
            conversion_enabled: true,
            conversion_min_seal_age_secs: 5,
            conversion_scan_interval_secs: 1,
            conversion_max_bandwidth_mbps: 1,
            conversion_task_lease_secs: 1,
        },
    )
    .await;
    let mut data = Vec::new();
    let mut locations = Vec::new();
    for value in 71_u8..87 {
        let shard = Bytes::from(vec![value; MIB]);
        locations.push(write_object(&stack.client, shard.clone()).await);
        data.push(shard);
    }
    stack.client.shutdown_small_writes().await.unwrap();
    stack.wait_for_conversion_active().await;
    stack.crash_and_restart_chunkdb().await;

    let recovered = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let chunk = stack.query_chunk(&locations[0]).await;
            if chunk
                .strips
                .iter()
                .filter(|strip| matches!(strip.strip, Some(Strip::EcStrip(_))))
                .count()
                == 2
            {
                break chunk;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("restarted chunkdb did not recover conversion task");
    assert_eq!(recovered.strips.len(), 2);
    for (group, strip) in recovered.strips.iter().enumerate() {
        let Some(Strip::EcStrip(ec)) = &strip.strip else {
            panic!("recovered group {group} is not EC");
        };
        let group_data = &data[group * 8..group * 8 + 8];
        let unit_bytes = u64::from(strip.unit_kb) * KIB as u64;
        for (segment, expected) in ec.segments[..8].iter().zip(group_data) {
            let actual = stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
                .await;
            assert_eq!(&actual, expected.as_ref());
        }
        let refs: Vec<&[u8]> = group_data.iter().map(Bytes::as_ref).collect();
        let parity = encode_parity_from_shards(EcScheme::new(8, 4), &refs).unwrap();
        for (segment, expected) in ec.segments[8..].iter().zip(parity) {
            let actual = stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
                .await;
            assert_eq!(actual, expected);
        }
    }
}

#[tokio::test]
async fn small_write_repairs_failed_replica_through_real_chunkdb_and_diskio() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(policy()).await;
    let (allocator, disk_writer) = real_write_parts(&stack).await;
    let fault = Arc::new(FailSelectedDiskWrite {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        fail_on: 1,
        failed_disk: Mutex::new(None),
    });
    let client = ChunkIoClient::from_parts_with_small_policy(allocator, fault.clone(), policy()).unwrap();
    let data = Bytes::from(vec![0x5a; 96 * KIB]);
    let location = write_object(&client, data.clone()).await;
    let failed_disk = fault.failed_disk.lock().unwrap().expect("injected disk identity");
    let chunk = stack.query_chunk(&location).await;
    let Some(Strip::MirrorStrip(mirror)) = &chunk.strips[0].strip else {
        panic!("expected repaired mirror strip");
    };
    assert_eq!(mirror.segments.len(), 1);
    assert_ne!(mirror.segments[0].disk_id, Some(failed_disk));
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
    client.shutdown_small_writes().await.unwrap();
    assert_eq!(client.small_write_metrics().shadow_bytes, 0);
}

#[tokio::test]
async fn small_write_repair_preserves_acknowledged_prefix_in_open_block() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(policy()).await;
    let (allocator, disk_writer) = real_write_parts(&stack).await;
    let fault = Arc::new(FailSelectedDiskWrite {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        fail_on: 2,
        failed_disk: Mutex::new(None),
    });
    let client = ChunkIoClient::from_parts_with_small_policy(allocator, fault, policy()).unwrap();
    let prefix_data = Bytes::from(vec![0x31; 64 * KIB]);
    let patch_data = Bytes::from(vec![0x72; 80 * KIB]);
    let prefix = write_object(&client, prefix_data.clone()).await;
    let patch = write_object(&client, patch_data.clone()).await;
    assert_eq!(prefix.chunk_id, patch.chunk_id);
    assert_eq!(patch.offset, prefix.length);
    let chunk = stack.query_chunk(&patch).await;
    assert_mirror_data(&stack, &chunk, &prefix, &prefix_data).await;
    assert_mirror_data(&stack, &chunk, &patch, &patch_data).await;
    assert_eq!(client.small_write_metrics().repaired_replicas, 1);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_write_new_chunk_repair_exhaustion_preserves_sealed_predecessor() {
    if !all_binaries_available() {
        return;
    }
    let mut configured = policy();
    configured.chunk_capacity = MIB as u64;
    let stack = E2eStack::start(configured.clone()).await;
    let (allocator, disk_writer) = real_write_parts(&stack).await;
    let fault = Arc::new(FailWritesFromCall {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        first_failed_call: 2,
    });
    let client = ChunkIoClient::from_parts_with_small_policy(allocator, fault, configured).unwrap();
    let predecessor_data = Bytes::from(vec![0x29; 700 * KIB]);
    let predecessor = write_object(&client, predecessor_data.clone()).await;
    let mut failed = client.prepare_small_write(400 * KIB).await.unwrap();
    failed.on_data(Bytes::from(vec![0x81; 400 * KIB])).await.unwrap();
    assert!(matches!(failed.on_finish().await, Err(IoError::WriteFailed(_))));

    let predecessor_chunk = stack.query_chunk(&predecessor).await;
    assert_eq!(predecessor_chunk.state, ChunkState::Sealed as i32);
    assert_eq!(predecessor_chunk.acknowledged_cursor, 700 * KIB as u64);
    assert_mirror_data(&stack, &predecessor_chunk, &predecessor, &predecessor_data).await;
    let metrics = client.small_write_metrics();
    assert_eq!(metrics.completed, 1);
    assert_eq!(metrics.failed, 1);
    assert_eq!(metrics.exhausted_repairs, 1);
    let _ = client.shutdown_small_writes().await;
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
    assert_eq!(first_chunk.acknowledged_cursor, (MIB + 400 * KIB) as u64);
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
