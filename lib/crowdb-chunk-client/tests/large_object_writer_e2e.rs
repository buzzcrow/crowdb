// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Core large-write E2E coverage through real service processes.

#[path = "common/e2e_stack.rs"]
mod e2e_stack;

use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoWriter, ChunkReadPolicy, DiskWriter, FramedWriteBuffer, IoError,
    LargeWritePolicy, Result, RoutedDiskWriter, SmallWritePolicy,
};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_common::ec::{encode_parity_from_shards, EcScheme};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, EcState, Location, Strip};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::{
    encode_frame_regions, ChunkLocation, FrameError, FrameMagic, FRAME_FOOTER_BYTES,
    FRAME_HEADER_PREFIX_BYTES, MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES,
};

use e2e_stack::{all_binaries_available, E2eStack};

const MIB: usize = 1024 * 1024;

struct FullFramedOwner {
    bytes: Vec<u8>,
    frames: usize,
}

impl FullFramedOwner {
    fn new(frames: usize) -> Self {
        let mut bytes = vec![0; frames * MAX_FRAME_BYTES];
        for index in 0..frames {
            let start = index * MAX_FRAME_BYTES + FRAME_HEADER_PREFIX_BYTES;
            bytes[start..start + MAX_FRAME_PAYLOAD_BYTES].fill(0x5a);
        }
        Self { bytes, frames }
    }
}

impl FramedWriteBuffer for FullFramedOwner {
    fn logical_len(&self) -> u64 {
        (self.frames * MAX_FRAME_PAYLOAD_BYTES) as u64
    }

    fn frame_count(&self) -> usize {
        self.frames
    }

    fn frame_payload_len(&self, index: usize) -> Option<usize> {
        (index < self.frames).then_some(MAX_FRAME_PAYLOAD_BYTES)
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
        let (header, remainder) = frame.split_at_mut(FRAME_HEADER_PREFIX_BYTES);
        let (payload, footer) = remainder.split_at_mut(MAX_FRAME_PAYLOAD_BYTES);
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

fn ec_4_1() -> EcScheme {
    EcScheme {
        data_num: 4,
        code_num: 1,
    }
}

fn small_policy() -> SmallWritePolicy {
    SmallWritePolicy {
        mirror_copies: 1,
        ..SmallWritePolicy::default()
    }
}

fn policy(max_chunk_size: u64) -> LargeWritePolicy {
    LargeWritePolicy {
        ec_scheme: ec_4_1(),
        client: Arc::new(ChunkClientConfig {
            chunk_type: crowdb_protocol::chunkdb::rpc::ChunkType::default(),
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

fn assert_bytes_match(actual: &[u8], expected: &[u8]) {
    assert_eq!(actual.len(), expected.len());
    if let Some(index) = actual
        .iter()
        .zip(expected)
        .position(|(left, right)| left != right)
    {
        panic!(
            "first byte mismatch at {index}: actual={}, expected={}, MiB starts={:?}",
            actual[index],
            expected[index],
            (0..actual.len() / MIB)
                .map(|block| actual[block * MIB])
                .collect::<Vec<_>>()
        );
    }
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

#[tokio::test]
async fn large_write_multi_strip_persists_data_metadata_and_parity() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy()).await;
    let data = make_test_data(12 * MIB);
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), policy(1024 * MIB as u64))
        .write_stream(data.as_slice())
        .await
        .unwrap();

    assert_eq!(result.locations.len(), 1);
    let location = &result.locations[0];
    let chunk = stack.query_chunk(location).await;
    assert_eq!(chunk.state, ChunkState::Sealed as i32);
    assert_eq!(
        chunk.sealed_length,
        u32::try_from(location.length.div_ceil(1024)).unwrap()
    );
    assert_eq!(chunk.strips.len(), 4);
    assert_ec_parity(&stack, &chunk, location).await;
    let read = stack.client.read_object(&result.locations).await.unwrap();
    assert!(read.len() > 1, "complete read must retain verified frame buffers");
    assert_bytes_match(&read.concat(), &data);
    assert_eq!(
        stack
            .client
            .read_range(&result.locations, 3 * MIB as u64 + 17, 9 * MIB as u64 + 31)
            .await
            .unwrap()
            .concat(),
        data[3 * MIB + 17..9 * MIB + 31]
    );
}

#[tokio::test]
async fn large_one_copy_mirror_reads_across_strips() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy()).await;
    let data = make_test_data(9 * MIB);
    let mut configured = policy(16 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(1);
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), configured)
        .write_stream(data.as_slice())
        .await
        .unwrap();
    let chunk = stack.query_chunk(&result.locations[0]).await;
    assert!(chunk.strips.len() >= 9);
    let Strip::MirrorStrip(mirror) = chunk.strips[0].strip.as_ref().unwrap() else {
        panic!("large mirror policy allocated a different strip");
    };
    assert_eq!(mirror.segments.len(), 1);
    assert!(chunk.strips.iter().take(9).all(|strip| strip.sealed_length > 0));
    assert_eq!(
        stack
            .client
            .read_object(&result.locations)
            .await
            .unwrap()
            .concat(),
        data
    );
    assert_eq!(
        stack
            .client
            .read_range(&result.locations, MIB as u64 - 100, MIB as u64 + 100)
            .await
            .unwrap()
            .concat(),
        data[MIB - 100..MIB + 100]
    );
}

#[tokio::test]
async fn large_mirror_replaces_failed_replica_before_ordered_completion() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    for fail_on in [1, 3] {
        let fault = Arc::new(FailWriteCall {
            inner: disk_writer.clone(),
            calls: AtomicUsize::new(0),
            fail_on,
            persistent: false,
            failed_segment: Mutex::new(None),
            segments: Mutex::new(Vec::new()),
            failure_delay: Duration::ZERO,
            failure_pending: AtomicBool::new(false),
            successes_during_failure: AtomicUsize::new(0),
        });
        let client =
            ChunkIoClient::from_parts_with_small_policy(allocator.clone(), fault.clone(), small_policy())
                .unwrap();
        let data = make_test_data(4 * MIB);
        let mut configured = policy(16 * MIB as u64);
        Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(1);
        let result = client
            .prepare_large_write(Some(data.len() as u64), configured)
            .write_stream(data.as_slice())
            .await
            .unwrap();
        let failed = fault.failed_segment.lock().unwrap().expect("injected segment");
        let chunk = stack.query_chunk(&result.locations[0]).await;
        assert!(chunk.strips.iter().all(|strip| {
            let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                return false;
            };
            !mirror.segments.contains(&failed)
        }));
        assert_eq!(client.large_write_repair_metrics().repaired_segments, 1);
        assert_eq!(
            client.read_object(&result.locations).await.unwrap().concat(),
            data
        );
    }
}

#[tokio::test]
async fn large_mirror_retains_later_framed_buffers_until_failed_first_strip_is_repaired() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    let fault = Arc::new(FailWriteCall {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        fail_on: 1,
        persistent: false,
        failed_segment: Mutex::new(None),
        segments: Mutex::new(Vec::new()),
        failure_delay: Duration::from_millis(80),
        failure_pending: AtomicBool::new(false),
        successes_during_failure: AtomicUsize::new(0),
    });
    let client =
        ChunkIoClient::from_parts_with_small_policy(allocator, fault.clone(), small_policy()).unwrap();
    let mut configured = policy(16 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(1);
    let frames = 64;
    let expected = vec![0x5a; frames * MAX_FRAME_PAYLOAD_BYTES];
    let mut writer = client.prepare_large_write(Some(expected.len() as u64), configured);
    writer
        .on_framed_data(Box::new(FullFramedOwner::new(frames)))
        .await
        .unwrap();
    let locations = writer.on_finish().await.unwrap();
    assert!(fault.successes_during_failure.load(Ordering::Acquire) > 0);
    assert_eq!(client.large_write_repair_metrics().repaired_segments, 1);
    assert_eq!(client.read_object(&locations).await.unwrap().concat(), expected);
}

#[tokio::test]
async fn large_mirror_repair_exhaustion_deletes_unsealed_chunk() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    let fault = Arc::new(FailWriteCall {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        fail_on: 1,
        persistent: true,
        failed_segment: Mutex::new(None),
        segments: Mutex::new(Vec::new()),
        failure_delay: Duration::ZERO,
        failure_pending: AtomicBool::new(false),
        successes_during_failure: AtomicUsize::new(0),
    });
    let client =
        ChunkIoClient::from_parts_with_small_policy(allocator, fault.clone(), small_policy()).unwrap();
    let mut configured = policy(16 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(1);
    let result = client
        .prepare_large_write(Some(MIB as u64), configured)
        .write_stream(make_test_data(MIB).as_slice())
        .await;
    assert!(matches!(result, Err(IoError::WriteFailed(_))));
    assert_eq!(client.large_write_repair_metrics().exhausted, 1);
    let failed = fault.failed_segment.lock().unwrap().expect("injected segment");
    let chunk = stack
        .query_chunk(&Location {
            chunk_id: failed.owner_chunk,
            ..Location::default()
        })
        .await;
    assert_eq!(chunk.state, ChunkState::Deleted as i32);
}

#[tokio::test]
async fn large_write_rotates_chunks_without_losing_data() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy()).await;
    let data = make_test_data(20 * MIB);
    let mut configured = policy(8 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().chunk_type =
        crowdb_protocol::chunkdb::rpc::ChunkType::IcebergTable;
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), configured)
        .write_stream(data.as_slice())
        .await
        .unwrap();

    assert_eq!(result.locations.len(), 3);
    assert_eq!(result.locations[0].length, 8 * MIB as u64);
    assert_eq!(result.locations[1].length, 8 * MIB as u64);
    let tail = result.locations.last().unwrap();
    assert_eq!(
        tail.length,
        ChunkLocation {
            chunk_id: tail.chunk_id.unwrap(),
            frame_offset: tail.offset,
            logical_length: tail.logical_length,
        }
        .physical_length()
        .unwrap()
    );
    let mut read_back = Vec::new();
    for location in &result.locations {
        let chunk = stack.query_chunk(location).await;
        assert_eq!(
            chunk.chunk_type,
            crowdb_protocol::chunkdb::rpc::ChunkType::IcebergTable as i32
        );
        assert_eq!(
            chunk.id.unwrap().high >> 56,
            crowdb_protocol::chunkdb::rpc::ChunkType::IcebergTable as u64
        );
        assert_eq!(chunk.state, ChunkState::Sealed as i32);
        assert_eq!(
            chunk.sealed_length,
            u32::try_from(location.length.div_ceil(1024)).unwrap()
        );
        let written_strips = chunk
            .strips
            .iter()
            .take_while(|strip| strip.sealed_length > 0)
            .count();
        assert!(chunk.strips.len() >= written_strips);
        for strip in chunk.strips.iter().skip(written_strips) {
            let Strip::EcStrip(ec) = strip.strip.as_ref().unwrap() else {
                panic!("large-write chunk contains a non-EC strip");
            };
            assert_eq!(ec.ec_state, EcState::NoParity as i32);
            assert_eq!(strip.sealed_length, 0);
        }
        assert_ec_parity(&stack, &chunk, location).await;
    }

    read_back.extend(
        stack
            .client
            .read_object(&result.locations)
            .await
            .unwrap()
            .concat(),
    );
    assert_eq!(read_back, data);
    assert_eq!(
        stack
            .client
            .read_range(&result.locations, 7 * MIB as u64, 10 * MIB as u64)
            .await
            .unwrap()
            .concat(),
        data[7 * MIB..10 * MIB]
    );
    let read_client = stack
        .client
        .clone()
        .with_read_policy(ChunkReadPolicy {
            stream_window_bytes: 3 * MIB,
            ..ChunkReadPolicy::default()
        })
        .unwrap();
    let mut stream = read_client.read_stream(&result.locations).unwrap();
    let mut streamed = Vec::new();
    while let Some(part) = stream.next_chunk().await {
        let part = part.unwrap();
        assert!(part.len() <= MAX_FRAME_PAYLOAD_BYTES);
        streamed.extend_from_slice(&part);
    }
    assert_eq!(streamed, data);
}

#[tokio::test]
async fn large_write_unknown_size_partial_tail_is_durable() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy()).await;
    let data = make_test_data(5 * MIB + 123);
    let result = stack
        .client
        .prepare_large_write(None, policy(16 * MIB as u64))
        .write_stream(data.as_slice())
        .await
        .unwrap();

    assert_eq!(result.locations.len(), 1);
    let location = &result.locations[0];
    assert_eq!(
        location.length,
        ChunkLocation {
            chunk_id: location.chunk_id.unwrap(),
            frame_offset: location.offset,
            logical_length: data.len() as u64,
        }
        .physical_length()
        .unwrap()
    );
    let chunk = stack.query_chunk(location).await;
    assert_eq!(chunk.state, ChunkState::Sealed as i32);
    assert_eq!(
        chunk.sealed_length,
        u32::try_from(location.length.div_ceil(1024)).unwrap()
    );
    assert!(chunk.strips.len() >= 2);
    assert_eq!(
        stack
            .client
            .read_object(&result.locations)
            .await
            .unwrap()
            .concat(),
        data
    );
}

#[tokio::test]
async fn large_write_replaces_failed_data_and_parity_segments_end_to_end() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    for fail_on in [1, 5] {
        let fault = Arc::new(FailWriteCall {
            inner: disk_writer.clone(),
            calls: AtomicUsize::new(0),
            fail_on,
            persistent: false,
            failed_segment: Mutex::new(None),
            segments: Mutex::new(Vec::new()),
            failure_delay: Duration::ZERO,
            failure_pending: AtomicBool::new(false),
            successes_during_failure: AtomicUsize::new(0),
        });
        let client =
            ChunkIoClient::from_parts_with_small_policy(allocator.clone(), fault.clone(), small_policy())
                .unwrap();
        let data = make_test_data(4 * MIB);
        let result = client
            .prepare_large_write(Some(data.len() as u64), policy(16 * MIB as u64))
            .write_stream(data.as_slice())
            .await
            .unwrap();
        let failed = fault.failed_segment.lock().unwrap().expect("injected segment");
        let chunk = stack.query_chunk(&result.locations[0]).await;
        let Some(Strip::EcStrip(ec)) = &chunk.strips[0].strip else {
            panic!("large write did not produce EC");
        };
        assert!(!ec.segments.contains(&failed));
        let initial = fault.segments.lock().unwrap()[..5].to_vec();
        assert_eq!(
            initial
                .iter()
                .filter(|segment| ec.segments.contains(segment))
                .count(),
            4
        );
        assert_eq!(client.large_write_repair_metrics().repaired_segments, 1);
        assert_eq!(
            client.read_object(&result.locations).await.unwrap().concat(),
            data
        );
        assert_ec_parity(&stack, &chunk, &result.locations[0]).await;
    }
}

#[tokio::test]
async fn large_write_repair_exhaustion_deletes_unsealed_chunk() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    let fault = Arc::new(FailWriteCall {
        inner: disk_writer,
        calls: AtomicUsize::new(0),
        fail_on: 1,
        persistent: true,
        failed_segment: Mutex::new(None),
        segments: Mutex::new(Vec::new()),
        failure_delay: Duration::ZERO,
        failure_pending: AtomicBool::new(false),
        successes_during_failure: AtomicUsize::new(0),
    });
    let client =
        ChunkIoClient::from_parts_with_small_policy(allocator, fault.clone(), small_policy()).unwrap();
    let result = client
        .prepare_large_write(Some(MIB as u64), policy(16 * MIB as u64))
        .write_stream(make_test_data(MIB).as_slice())
        .await;
    assert!(matches!(result, Err(IoError::WriteFailed(_))));
    assert_eq!(client.large_write_repair_metrics().exhausted, 1);
    let failed = fault.failed_segment.lock().unwrap().expect("injected segment");
    let location = Location {
        chunk_id: failed.owner_chunk,
        ..Location::default()
    };
    let chunk = stack.query_chunk(&location).await;
    assert_eq!(chunk.state, ChunkState::Deleted as i32);
}
