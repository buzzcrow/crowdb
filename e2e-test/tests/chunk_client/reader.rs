// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Core object-read coverage through real KV, `DiskDB`, `DiskIO`, and `ChunkDB` services.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::stack::TestChunkdbOptions as ChunkdbStartOptions;
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoWriter, DiskWriter, IoError, LargeWritePolicy, ReadError,
    Result, SmallWritePolicy,
};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_common::ec::EcScheme;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{
    AdHocEcRecoveryDisposition, AdHocEcRecoveryRequest, Location, ReplaceChunkStripRangeRequest, Strip,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::{parse_frame, MAX_FRAME_PAYLOAD_BYTES};

use super::stack::TestStack as E2eStack;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

struct FailDiskReads {
    inner: Arc<dyn DiskWriter>,
    failed: Vec<DiskId>,
    failed_segments: Vec<Segment>,
    transient: bool,
    corrupt: bool,
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
            if self.corrupt {
                let mut bytes = self
                    .inner
                    .read(segment, unit_bytes, segment_offset, length)
                    .await?
                    .to_vec();
                if let Some(first) = bytes.first_mut() {
                    *first ^= 0xff;
                }
                return Ok(Bytes::from(bytes));
            }
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
        ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
    }
}

fn large_policy() -> LargeWritePolicy {
    LargeWritePolicy {
        ec_scheme: EcScheme::new(4, 1),
        client: Arc::new(ChunkClientConfig {
            max_chunk_size: 64 * MIB as u64,
            ..ChunkClientConfig::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
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
        corrupt: false,
        max_read: AtomicUsize::new(0),
    });
    let reader = ChunkIoClient::from_parts(
        chunkdb,
        fault.clone(),
        crowdb_protocol::chunkdb::rpc::ChunkType::S3,
    );
    (reader, fault)
}

async fn reader_with_segment_failures(stack: &E2eStack, failed_segments: Vec<Segment>) -> ChunkIoClient {
    let (chunkdb, disk_io) = real_parts(stack).await;
    let fault = Arc::new(FailDiskReads {
        inner: disk_io,
        failed: Vec::new(),
        failed_segments,
        transient: false,
        corrupt: false,
        max_read: AtomicUsize::new(0),
    });
    ChunkIoClient::from_parts(chunkdb, fault, crowdb_protocol::chunkdb::rpc::ChunkType::S3)
}

async fn reader_with_transient_failure(stack: &E2eStack, failed: Vec<DiskId>) -> ChunkIoClient {
    let (chunkdb, disk_io) = real_parts(stack).await;
    ChunkIoClient::from_parts(
        chunkdb,
        Arc::new(FailDiskReads {
            inner: disk_io,
            failed,
            failed_segments: Vec::new(),
            transient: true,
            corrupt: false,
            max_read: AtomicUsize::new(0),
        }),
        crowdb_protocol::chunkdb::rpc::ChunkType::S3,
    )
}

async fn reader_with_corruption(
    stack: &E2eStack,
    failed: Vec<DiskId>,
) -> (ChunkIoClient, Arc<FailDiskReads>) {
    let (chunkdb, disk_io) = real_parts(stack).await;
    let fault = Arc::new(FailDiskReads {
        inner: disk_io,
        failed,
        failed_segments: Vec::new(),
        transient: false,
        corrupt: true,
        max_read: AtomicUsize::new(0),
    });
    (
        ChunkIoClient::from_parts(
            chunkdb,
            fault.clone(),
            crowdb_protocol::chunkdb::rpc::ChunkType::S3,
        ),
        fault,
    )
}

mod data;

mod data_loss;

mod recovery;
