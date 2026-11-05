// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Core large-write E2E coverage through real service processes.

use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoWriter, DiskWriter, FramedWriteBuffer, IoError,
    LargeWritePolicy, Result, RoutedDiskWriter, SmallWritePolicy,
};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_common::ec::{encode_parity_from_shards, EcScheme};
use crowdb_paxos_kv_client::{ClientConfig, HardwareClient, PaxosKvClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, Location, Strip};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::{
    encode_frame_regions, FrameError, FrameMagic, FRAME_FOOTER_BYTES, FRAME_V2_HEADER_BYTES, MAX_FRAME_BYTES,
    MAX_FRAME_V2_PAYLOAD_BYTES,
};

use super::stack::TestStack as E2eStack;

const MIB: usize = 1024 * 1024;

struct FullFramedOwner {
    bytes: Vec<u8>,
    frames: usize,
}

impl FullFramedOwner {
    fn new(frames: usize) -> Self {
        let mut bytes = vec![0; frames * MAX_FRAME_BYTES];
        for index in 0..frames {
            let start = index * MAX_FRAME_BYTES + FRAME_V2_HEADER_BYTES;
            bytes[start..start + MAX_FRAME_V2_PAYLOAD_BYTES].fill(0x5a);
        }
        Self { bytes, frames }
    }
}

impl FramedWriteBuffer for FullFramedOwner {
    fn logical_len(&self) -> u64 {
        (self.frames * MAX_FRAME_V2_PAYLOAD_BYTES) as u64
    }

    fn frame_count(&self) -> usize {
        self.frames
    }

    fn frame_payload_len(&self, index: usize) -> Option<usize> {
        (index < self.frames).then_some(MAX_FRAME_V2_PAYLOAD_BYTES)
    }

    fn finalize_frame(
        &mut self,
        index: usize,
        magic: FrameMagic,
        chunk_id: crowdb_protocol::common::ChunkId,
        write_time_ms: u64,
    ) -> std::result::Result<Range<usize>, FrameError> {
        let start = index * MAX_FRAME_BYTES;
        let end = start + MAX_FRAME_BYTES;
        let frame = &mut self.bytes[start..end];
        let (header, remainder) = frame.split_at_mut(FRAME_V2_HEADER_BYTES);
        let (payload, footer) = remainder.split_at_mut(MAX_FRAME_V2_PAYLOAD_BYTES);
        debug_assert_eq!(footer.len(), FRAME_FOOTER_BYTES);
        encode_frame_regions(magic, chunk_id, payload, write_time_ms, header, footer)?;
        Ok(start..end)
    }

    fn views(&self, range: Range<usize>) -> std::result::Result<Vec<Bytes>, FrameError> {
        Ok(vec![Bytes::copy_from_slice(&self.bytes[range])])
    }
}

struct FailWriteCall {
    inner: Arc<dyn DiskWriter>,
    calls: AtomicUsize,
    fail_on: usize,
    persistent: bool,
    failed_segment: Mutex<Option<Segment>>,
    segments: Mutex<Vec<Segment>>,
    failure_delay: Duration,
    failure_pending: AtomicBool,
    successes_during_failure: AtomicUsize,
}

impl FailWriteCall {
    fn inject_failure(&self, segment: &Segment) -> Result<()> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel) + 1;
        self.segments.lock().unwrap().push(*segment);
        if call == self.fail_on || (self.persistent && call >= self.fail_on) {
            *self.failed_segment.lock().unwrap() = Some(*segment);
            return Err(IoError::WriteFailed("injected large-write disk failure".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl DiskWriter for FailWriteCall {
    async fn write(&self, segment: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        self.inject_failure(segment)?;
        self.inner.write(segment, unit_bytes, data).await
    }

    async fn write_views(&self, segment: &Segment, unit_bytes: u64, data: Vec<Bytes>) -> Result<()> {
        self.inject_failure(segment)?;
        self.inner.write_views(segment, unit_bytes, data).await
    }

    async fn write_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let injected = self.inject_failure(seg);
        if injected.is_err() && !self.failure_delay.is_zero() {
            self.failure_pending.store(true, Ordering::Release);
            tokio::time::sleep(self.failure_delay).await;
            self.failure_pending.store(false, Ordering::Release);
        }
        injected?;
        let result = self
            .inner
            .write_at_byte_offset(seg, unit_bytes, byte_offset, data)
            .await;
        if result.is_ok() && self.failure_pending.load(Ordering::Acquire) {
            self.successes_during_failure.fetch_add(1, Ordering::AcqRel);
        }
        result
    }

    async fn read(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        segment_offset: u64,
        length: u32,
    ) -> Result<Bytes> {
        self.inner.read(segment, unit_bytes, segment_offset, length).await
    }

    async fn fsync(&self, segment: &Segment) -> Result<()> {
        self.inner.fsync(segment).await
    }
}

struct FailLaterWritesInFirstChunk {
    inner: Arc<dyn DiskWriter>,
    first_chunk: Mutex<Option<crowdb_protocol::common::ChunkId>>,
    first_chunk_writes: AtomicUsize,
    successful_writes: usize,
}

impl FailLaterWritesInFirstChunk {
    fn check(&self, segment: &Segment) -> Result<()> {
        let chunk_id = segment
            .owner_chunk
            .ok_or_else(|| IoError::WriteFailed("fault segment has no chunk".into()))?;
        let mut first = self.first_chunk.lock().unwrap();
        let first_id = *first.get_or_insert(chunk_id);
        drop(first);
        if chunk_id == first_id
            && self.first_chunk_writes.fetch_add(1, Ordering::AcqRel) >= self.successful_writes
        {
            return Err(IoError::WriteFailed(
                "old chunk is persistently unwritable".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl DiskWriter for FailLaterWritesInFirstChunk {
    async fn write(&self, segment: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        self.check(segment)?;
        self.inner.write(segment, unit_bytes, data).await
    }

    async fn write_views(&self, segment: &Segment, unit_bytes: u64, data: Vec<Bytes>) -> Result<()> {
        self.check(segment)?;
        self.inner.write_views(segment, unit_bytes, data).await
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        self.check(segment)?;
        self.inner
            .write_at_byte_offset(segment, unit_bytes, byte_offset, data)
            .await
    }

    async fn read(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        segment_offset: u64,
        length: u32,
    ) -> Result<Bytes> {
        self.inner.read(segment, unit_bytes, segment_offset, length).await
    }

    async fn fsync(&self, segment: &Segment) -> Result<()> {
        self.inner.fsync(segment).await
    }
}

fn ec_4_1() -> EcScheme {
    EcScheme {
        data_num: 4,
        code_num: 1,
    }
}

fn small_policy() -> SmallWritePolicy {
    SmallWritePolicy {
        mirror_copies: 2,
        ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
    }
}

fn policy(max_chunk_size: u64) -> LargeWritePolicy {
    LargeWritePolicy {
        ec_scheme: ec_4_1(),
        client: Arc::new(ChunkClientConfig {
            chunk_type: crowdb_protocol::chunkdb::rpc::ChunkType::S3,
            large_mirror_copies: None,
            max_chunk_size,
            prefetch_strips_per_chunk: 2,
            large_prefetch_max_strips_per_batch: 32,
            large_parallel_strip_writes: 4,
            large_held_buffers: 4,
            parity_depth: 2,
            chunk_preparation_depth: 1,
            large_write_repair_attempts: 3,
            read_buffer_size: MIB,
            max_cached_buffer: 4 * MIB,
            memory_budget: 0,
        }),
    }
}

fn make_test_data(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| u8::try_from((index * 17 + 37) % 251).unwrap())
        .collect()
}

async fn real_parts(stack: &E2eStack) -> (Arc<ChunkdbClient>, Arc<RoutedDiskWriter>) {
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

async fn assert_ec_parity(stack: &E2eStack, chunk: &Chunk, location: &Location) {
    let mut remaining = location.length;
    for strip in &chunk.strips {
        if remaining == 0 {
            break;
        }
        let Strip::EcStrip(ec) = strip.strip.as_ref().expect("strip body") else {
            panic!("large-write chunk contains a non-EC strip");
        };
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let mut data = Vec::new();
        for segment in ec.segments.iter().take(ec.data_num as usize) {
            let length = remaining.min(unit_bytes);
            let mut shard = vec![0; usize::try_from(unit_bytes).unwrap()];
            if length > 0 {
                let actual = stack
                    .read_segment(segment, unit_bytes, 0, u32::try_from(length).unwrap())
                    .await;
                shard[..usize::try_from(length).unwrap()].copy_from_slice(&actual);
                remaining -= length;
            }
            data.push(shard);
        }
        let refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
        let expected = encode_parity_from_shards(ec_4_1(), &refs).unwrap();
        for (segment, expected) in ec.segments.iter().skip(ec.data_num as usize).zip(expected) {
            let actual = stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(unit_bytes).unwrap())
                .await;
            assert_eq!(actual, expected);
        }
    }
}

mod mirror;

mod ec;

mod mirror_repair;

mod ec_repair;
