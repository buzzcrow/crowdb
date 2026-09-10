// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Core object-read coverage through real KV, `DiskDB`, `DiskIO`, and `ChunkDB` services.

#[path = "common/e2e_stack.rs"]
mod e2e_stack;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoWriter, DiskWriter, IoError, LargeWritePolicy, ReadError,
    Result, SmallWritePolicy,
};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_common::ec::EcScheme;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{Location, Strip};
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_test_harness::chunkdb::ChunkdbStartOptions;

use e2e_stack::{all_binaries_available, E2eStack};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

struct FailDiskReads {
    inner: Arc<dyn DiskWriter>,
    failed: Vec<DiskId>,
    failed_segments: Vec<Segment>,
    transient: bool,
    max_read: AtomicUsize,
}

impl FailDiskReads {
    fn fails(&self, segment: &Segment) -> bool {
        self.failed_segments.contains(segment)
            || segment
                .disk_id
                .is_some_and(|disk_id| self.failed.contains(&disk_id))
    }

    fn observe_read(&self, length: u32) {
        self.max_read.fetch_max(length as usize, Ordering::Relaxed);
    }
}

#[async_trait]
impl DiskWriter for FailDiskReads {
    async fn write(&self, segment: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        self.inner.write(segment, unit_bytes, data).await
    }

    async fn write_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        if byte_offset % unit_bytes == 0 {
            return self.write_at(seg, unit_bytes, byte_offset, data).await;
        }
        Err(IoError::WriteFailed(
            "byte-offset writes not supported by this writer".into(),
        ))
    }

    async fn fsync(&self, segment: &Segment) -> Result<()> {
        self.inner.fsync(segment).await
    }

    async fn read(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        segment_offset: u64,
        length: u32,
    ) -> Result<Bytes> {
        self.observe_read(length);
        if self.fails(segment) {
            return Err(if self.transient {
                IoError::TransientRead("injected transient read failure".into())
            } else {
                IoError::ReadFailed("injected disk read failure".into())
            });
        }
        self.inner.read(segment, unit_bytes, segment_offset, length).await
    }
}

fn small_policy(copies: u32) -> SmallWritePolicy {
    SmallWritePolicy {
        mirror_copies: copies,
        conversion_enabled: false,
        ..SmallWritePolicy::default()
    }
}

fn large_policy() -> LargeWritePolicy {
    LargeWritePolicy {
        ec_scheme: EcScheme::new(4, 1),
        client: Arc::new(ChunkClientConfig {
            max_chunk_size: 64 * MIB as u64,
            ..ChunkClientConfig::default()
        }),
    }
}

fn test_data(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| u8::try_from((index * 31 + 19) % 251).unwrap())
        .collect()
}

async fn real_parts(stack: &E2eStack) -> (Arc<ChunkdbClient>, Arc<dyn DiskWriter>) {
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
    let disk_io = Arc::new(
        crowdb_chunk_client::RoutedDiskWriter::connect(&service, &hardware)
            .await
            .unwrap(),
    );
    (chunkdb, disk_io)
}

async fn reader_with_failures(stack: &E2eStack, failed: Vec<DiskId>) -> (ChunkIoClient, Arc<FailDiskReads>) {
    let (chunkdb, disk_io) = real_parts(stack).await;
    let fault = Arc::new(FailDiskReads {
        inner: disk_io,
        failed,
        failed_segments: Vec::new(),
        transient: false,
        max_read: AtomicUsize::new(0),
    });
    let reader = ChunkIoClient::from_parts(chunkdb, fault.clone());
    (reader, fault)
}

async fn reader_with_segment_failures(stack: &E2eStack, failed_segments: Vec<Segment>) -> ChunkIoClient {
    let (chunkdb, disk_io) = real_parts(stack).await;
    let fault = Arc::new(FailDiskReads {
        inner: disk_io,
        failed: Vec::new(),
        failed_segments,
        transient: false,
        max_read: AtomicUsize::new(0),
    });
    ChunkIoClient::from_parts(chunkdb, fault)
}

async fn reader_with_transient_failure(stack: &E2eStack, failed: DiskId) -> ChunkIoClient {
    let (chunkdb, disk_io) = real_parts(stack).await;
    ChunkIoClient::from_parts(
        chunkdb,
        Arc::new(FailDiskReads {
            inner: disk_io,
            failed: vec![failed],
            failed_segments: Vec::new(),
            transient: true,
            max_read: AtomicUsize::new(0),
        }),
    )
}

#[tokio::test]
async fn ec_range_read_recovers_only_the_requested_bytes() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy(1)).await;
    let data = test_data(12 * MIB);
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
    let failed = ec.segments[1].disk_id.unwrap();
    let (reader, fault) = reader_with_failures(&stack, vec![failed]).await;
    let start = MIB as u64 + 123;
    let end = start + 16 * KIB as u64;
    let actual = reader.read_range(&result.locations, start, end).await.unwrap();
    assert_eq!(
        actual,
        data[usize::try_from(start).unwrap()..usize::try_from(end).unwrap()]
    );
    let direct = stack.read_segment(&ec.segments[0], MIB as u64, 0, 4096).await;
    assert_eq!(direct, data[..4096]);
    assert!(fault.max_read.load(Ordering::Relaxed) <= 16 * KIB);
    assert_eq!(reader.read_object(&result.locations).await.unwrap(), data);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let repaired = stack.query_chunk(&result.locations[0]).await;
        let Some(Strip::EcStrip(current)) = &repaired.strips[0].strip else {
            panic!("large write did not remain EC");
        };
        if !current.segments.contains(&ec.segments[1]) && repaired.strips[0].unavailable_segments.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "chunkdb repair task did not replace the failed full shard"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let metrics = stack.repair_metrics().await;
    assert!(metrics["tasks_admitted"].as_u64().unwrap_or(0) >= 1);
    assert!(metrics["attempts_completed"].as_u64().unwrap_or(0) >= 1);
    assert!(metrics["segments_repaired"].as_u64().unwrap_or(0) >= 1);
    assert!(metrics["bytes_written"].as_u64().unwrap_or(0) >= MIB as u64);
    assert_eq!(metrics["memory_bytes"].as_u64(), Some(0));
    assert_eq!(metrics["memory_limit_bytes"].as_u64(), Some(64 * MIB as u64));
}

#[tokio::test]
async fn ec_read_reports_data_loss_beyond_parity_tolerance() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy(1)).await;
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
async fn partial_read_preserves_healthy_ec_shard_ranges_around_data_loss() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start_with_chunkdb_options(
        small_policy(1),
        ChunkdbStartOptions {
            allow_unsafe_ec: true,
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
    let partial = reader
        .read_range_partial(&result.locations, 0, 4 * MIB as u64)
        .await
        .unwrap();
    assert_eq!(partial.failures.len(), 2);
    assert_eq!(
        (partial.failures[0].start, partial.failures[0].end),
        (MIB as u64, 2 * MIB as u64)
    );
    assert_eq!(
        (partial.failures[1].start, partial.failures[1].end),
        (2 * MIB as u64, 3 * MIB as u64)
    );
    assert_eq!(partial.ranges.len(), 2);
    assert_eq!((partial.ranges[0].start, partial.ranges[0].end), (0, MIB as u64));
    assert_eq!(partial.ranges[0].data, data[..MIB]);
    assert_eq!(
        (partial.ranges[1].start, partial.ranges[1].end),
        (3 * MIB as u64, 4 * MIB as u64)
    );
    assert_eq!(partial.ranges[1].data, data[3 * MIB..4 * MIB]);

    let mut stream = reader.read_stream(&result.locations).unwrap();
    assert_eq!(stream.next_chunk().await.unwrap().unwrap(), data[..MIB]);
    assert!(matches!(
        stream.next_chunk().await.unwrap(),
        Err(ReadError::FailedRange { start, end, .. })
            if start == MIB as u64 && end == 2 * MIB as u64
    ));
    assert!(stream.next_chunk().await.is_none());
}

#[tokio::test]
async fn mirror_read_succeeds_and_reports_replica_loss() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy(1)).await;
    let data = Bytes::from(test_data(96 * KIB));
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
    assert_eq!(mirror.segments.len(), 1);
    let primary = mirror.segments[0].disk_id.unwrap();
    assert_eq!(
        stack
            .client
            .read_object(std::slice::from_ref(&location))
            .await
            .unwrap(),
        data
    );
    let (unreadable, _) = reader_with_failures(&stack, vec![primary]).await;
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

#[tokio::test]
async fn transient_mirror_read_failure_does_not_mark_segment_unavailable() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy(1)).await;
    let data = Bytes::from(test_data(8 * KIB));
    let mut writer = stack.client.prepare_small_write(data.len()).await.unwrap();
    writer.on_data(data.clone()).await.unwrap();
    let location = writer.on_finish().await.unwrap().remove(0);
    let before = stack.query_chunk(&location).await;
    let strip = before
        .strips
        .iter()
        .find(|strip| u64::from(strip.chunk_offset) * KIB as u64 <= location.offset)
        .unwrap();
    let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
        panic!("small write did not produce a mirror strip");
    };
    let reader = reader_with_transient_failure(&stack, mirror.segments[0].disk_id.unwrap()).await;
    assert!(reader.read_object(std::slice::from_ref(&location)).await.is_err());
    let after = stack.query_chunk(&location).await;
    assert!(after.strips[0].unavailable_segments.is_empty());
    assert_eq!(
        stack
            .client
            .read_object(std::slice::from_ref(&location))
            .await
            .unwrap(),
        data
    );
    stack.client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn chunkdb_restart_admits_durable_read_failure_and_repairs_full_shard() {
    if !all_binaries_available() {
        return;
    }
    let mut options = ChunkdbStartOptions {
        allow_unsafe_ec: true,
        repair_enabled: false,
        ..ChunkdbStartOptions::default()
    };
    let mut stack = E2eStack::start_with_chunkdb_options(small_policy(1), options).await;
    let data = test_data(4 * MIB);
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), large_policy())
        .write_stream(data.as_slice())
        .await
        .unwrap();
    let before = stack.query_chunk(&result.locations[0]).await;
    let Some(Strip::EcStrip(ec)) = &before.strips[0].strip else {
        panic!("large write did not produce EC");
    };
    let failed_segment = ec.segments[0];
    let failed_disk = failed_segment.disk_id.unwrap();
    let (reader, fault) = reader_with_failures(&stack, vec![failed_disk]).await;
    let length = 16 * KIB as u64;
    assert_eq!(
        reader.read_range(&result.locations, 0, length).await.unwrap(),
        data[..16 * KIB]
    );
    assert!(fault.max_read.load(Ordering::Relaxed) <= 16 * KIB);
    let marked = stack.query_chunk(&result.locations[0]).await;
    assert!(marked.strips[0].unavailable_segments.contains(&failed_segment));

    options.repair_enabled = true;
    options.repair_allow_unsafe_placement = true;
    stack.crash_and_restart_chunkdb_with_options(options).await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let repaired = stack.query_chunk(&result.locations[0]).await;
        let Some(Strip::EcStrip(current)) = &repaired.strips[0].strip else {
            panic!("large write did not remain EC");
        };
        if !current.segments.contains(&failed_segment) && repaired.strips[0].unavailable_segments.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "restarted chunkdb did not reconstruct the full failed shard"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (after_restart, _) = reader_with_failures(&stack, vec![failed_disk]).await;
    assert_eq!(
        after_restart
            .read_range(&result.locations, 0, length)
            .await
            .unwrap(),
        data[..16 * KIB]
    );
}
