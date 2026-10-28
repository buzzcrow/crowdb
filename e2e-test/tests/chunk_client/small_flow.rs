// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Core small-write E2E coverage through real service processes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::stack::TestChunkdbOptions as ChunkdbStartOptions;
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkIoClient, ChunkIoWriter, DiskWriter, IoError, Result, RoutedDiskWriter, SmallWritePolicy,
};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_common::ec::{decode, encode_parity_from_shards, EcScheme};
use crowdb_paxos_kv_client::{ClientConfig, HardwareClient, PaxosKvClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkState, ChunkType, ConversionFilter, Location, Strip, TriggerConversionBatchRequest,
    TriggerConversionRequest,
};
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::{parse_frame, FrameMagic, MAX_FRAME_V2_PAYLOAD_BYTES};

use super::stack::TestStack as E2eStack;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const FRAMES_PER_MIB_STRIP: usize = MIB / (64 * KIB);

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

    async fn write_views_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Vec<Bytes>,
    ) -> Result<()> {
        if self.calls.fetch_add(1, Ordering::AcqRel) + 1 == self.fail_on {
            *self.failed_disk.lock().unwrap() = segment.disk_id;
            return Err(IoError::WriteFailed("injected first replica failure".into()));
        }
        self.inner
            .write_views_at_byte_offset(segment, unit_bytes, byte_offset, data)
            .await
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        if self.calls.fetch_add(1, Ordering::AcqRel) + 1 == self.fail_on {
            *self.failed_disk.lock().unwrap() = segment.disk_id;
            return Err(IoError::WriteFailed("injected first replica failure".into()));
        }
        self.inner
            .write_at_byte_offset(segment, unit_bytes, byte_offset, data)
            .await
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

    async fn write_views_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Vec<Bytes>,
    ) -> Result<()> {
        if self.calls.fetch_add(1, Ordering::AcqRel) + 1 >= self.first_failed_call {
            return Err(IoError::WriteFailed("injected persistent disk failure".into()));
        }
        self.inner
            .write_views_at_byte_offset(segment, unit_bytes, byte_offset, data)
            .await
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        if self.calls.fetch_add(1, Ordering::AcqRel) + 1 >= self.first_failed_call {
            return Err(IoError::WriteFailed("injected persistent disk failure".into()));
        }
        self.inner
            .write_at_byte_offset(segment, unit_bytes, byte_offset, data)
            .await
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

async fn write_full_small_strip(client: &ChunkIoClient, value: u8) -> Vec<(Bytes, Location)> {
    let mut frames = Vec::with_capacity(FRAMES_PER_MIB_STRIP);
    for _ in 0..FRAMES_PER_MIB_STRIP {
        let payload = Bytes::from(vec![value; MAX_FRAME_V2_PAYLOAD_BYTES]);
        let location = write_object(client, payload.clone()).await;
        frames.push((payload, location));
    }
    frames
}

async fn read_data_image(stack: &E2eStack, chunk: &Chunk, location: &Location) -> Bytes {
    let strip = chunk
        .strips
        .iter()
        .find(|strip| {
            let start = u64::from(strip.chunk_offset) * KIB as u64;
            let end = start + u64::from(strip.capacity) * KIB as u64;
            start <= location.offset && location.offset < end
        })
        .unwrap_or_else(|| {
            panic!(
                "location strip missing while reading image: chunk_id={:?}, offset={}, length={}, chunk={chunk:#?}",
                location.chunk_id, location.offset, location.length
            )
        });
    let strip_start = u64::from(strip.chunk_offset) * KIB as u64;
    let unit_bytes = u64::from(strip.unit_kb) * KIB as u64;
    let segment = match strip.strip.as_ref().expect("strip body") {
        Strip::MirrorStrip(mirror) => &mirror.segments[0],
        Strip::EcStrip(ec) => {
            // Conversion may finish while the writer returns its final
            // location. The data images remain at their deterministic shard
            // indexes, so capture them from either representation.
            let shard =
                usize::try_from((location.offset - strip_start) / MIB as u64).expect("EC data shard index");
            &ec.segments[shard]
        }
    };
    Bytes::from(
        stack
            .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
            .await,
    )
}

async fn real_write_parts(stack: &E2eStack) -> (Arc<ChunkdbClient>, Arc<RoutedDiskWriter>) {
    let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(
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
        let frame = parse_frame(&actual, location.chunk_id.expect("location chunk"))
            .expect("durable RepoSmall frame");
        assert_eq!(frame.header.magic, FrameMagic::RepoSmallV2);
        assert_eq!(frame.payload, expected);
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

mod batching;

mod queue_pressure;

mod conversion;

mod background_conversion;

mod replica_repair;

mod repair_exhaustion;
