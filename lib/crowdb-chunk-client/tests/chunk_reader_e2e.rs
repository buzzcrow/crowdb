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

use e2e_stack::{all_binaries_available, E2eStack};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

struct FailDiskReads {
    inner: Arc<dyn DiskWriter>,
    failed: Vec<DiskId>,
    max_read: AtomicUsize,
}

impl FailDiskReads {
    fn fails(&self, segment: &Segment) -> bool {
        segment
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
            return Err(IoError::ReadFailed("injected disk read failure".into()));
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
        max_read: AtomicUsize::new(0),
    });
    let reader = ChunkIoClient::from_parts(chunkdb, fault.clone());
    (reader, fault)
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
    assert!(matches!(
        reader.read_range(&result.locations, 0, 16 * KIB as u64).await,
        Err(ReadError::DataLoss(_))
    ));
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
        Err(ReadError::DataLoss(_))
    ));
    stack.client.shutdown_small_writes().await.unwrap();
}
