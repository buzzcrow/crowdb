// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unit tests for `ChunkWriter` drive loop: strip rotation, on-demand
//! append, `is_full` + seal, abort, empty seal.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::similar_names
)]

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crowdb_test_harness::test_dirs;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkAllocator, ChunkClientConfig, ChunkReadPolicy, ChunkReader, ChunkWriter, DiskWriter, IoError, Result,
};
use crowdb_common::ec::EcScheme;
use crowdb_diskio_client::DiskId;
use crowdb_protocol::chunkdb::rpc::Strip as StripOneof;
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, Chunk, ChunkState,
    ChunkStrip, ChunkType, DeleteChunkRequest, DeleteChunkResponse, EcStrip, MirrorStrip, QueryChunkRequest,
    QueryChunkResponse, SealChunkRequest, SealChunkResponse, StripType, UpdateChunkStripRequest,
    UpdateChunkStripResponse,
};
use crowdb_protocol::common::{ChunkId, DiskId as ProtoDiskId};
use crowdb_protocol::diskdb::rpc::Segment;

use common::LocalFileDiskWriter;

const UNIT_BYTES: u64 = 4096;
const DATA_NUM: usize = 4;

#[derive(Debug, Default)]
struct OrderingDiskWriter {
    events: Mutex<Vec<String>>,
    parity_inflight: AtomicUsize,
    parity_max: AtomicUsize,
}

#[derive(Debug, Default)]
struct ConcurrentDiskWriter {
    inflight: AtomicUsize,
    max_inflight: AtomicUsize,
}

#[derive(Default)]
struct RejectingDiskWriter {
    attempts: AtomicUsize,
}

#[derive(Debug)]
struct OrderedMirrorDiskWriter {
    first_release: tokio::sync::Semaphore,
    first_four_started: tokio::sync::Barrier,
    inflight: AtomicUsize,
    max_inflight: AtomicUsize,
    completed: Mutex<Vec<u64>>,
}

impl Default for OrderedMirrorDiskWriter {
    fn default() -> Self {
        Self {
            first_release: tokio::sync::Semaphore::new(0),
            first_four_started: tokio::sync::Barrier::new(4),
            inflight: AtomicUsize::new(0),
            max_inflight: AtomicUsize::new(0),
            completed: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl DiskWriter for OrderedMirrorDiskWriter {
    async fn write(&self, segment: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        self.write_at_byte_offset(segment, unit_bytes, 0, data).await
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        _unit_bytes: u64,
        _byte_offset: u64,
        _data: Bytes,
    ) -> Result<()> {
        let inflight = self.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_inflight.fetch_max(inflight, Ordering::Relaxed);
        if segment.unit_offset < 4 {
            self.first_four_started.wait().await;
        }
        if segment.unit_offset == 0 {
            self.first_release.acquire().await.unwrap().forget();
        }
        self.completed.lock().unwrap().push(segment.unit_offset);
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl DiskWriter for RejectingDiskWriter {
    async fn write(&self, _seg: &Segment, _unit_bytes: u64, _data: Bytes) -> Result<()> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        Err(IoError::WriteFailed("injected single-copy failure".into()))
    }

    async fn write_at_byte_offset(
        &self,
        _seg: &Segment,
        _unit_bytes: u64,
        _byte_offset: u64,
        _data: Bytes,
    ) -> Result<()> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        Err(IoError::WriteFailed("injected single-copy failure".into()))
    }
}

#[async_trait]
impl DiskWriter for ConcurrentDiskWriter {
    async fn write(&self, _seg: &Segment, _unit_bytes: u64, _data: Bytes) -> Result<()> {
        let inflight = self.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_inflight.fetch_max(inflight, Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        Ok(())
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
}

impl OrderingDiskWriter {
    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }

    fn parity_max(&self) -> usize {
        self.parity_max.load(Ordering::Relaxed)
    }

    fn parity_inflight(&self) -> usize {
        self.parity_inflight.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl DiskWriter for OrderingDiskWriter {
    async fn write(&self, seg: &Segment, _unit_bytes: u64, _data: Bytes) -> Result<()> {
        let disk_id = seg.disk_id.unwrap_or_default();
        if disk_id.high == 1004 {
            let inflight = self.parity_inflight.fetch_add(1, Ordering::Relaxed) + 1;
            self.parity_max.fetch_max(inflight, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            self.parity_inflight.fetch_sub(1, Ordering::Relaxed);
        }
        self.events
            .lock()
            .unwrap()
            .push(format!("write:{}", disk_id.high));
        Ok(())
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
}

// ── Mock ChunkAllocator (cumulative chunks) ──────────────────────

#[derive(Debug, Clone, Default)]
struct MockChunkAllocator {
    state: Arc<Mutex<MockChunkState>>,
}

#[derive(Debug, Default)]
struct MockChunkState {
    chunks: HashMap<(u64, u64), (Vec<ChunkStrip>, u32, bool)>,
    next_segment_offset: u64,
    allocate_calls: usize,
    append_calls: usize,
    append_strip_counts: Vec<u32>,
    seal_calls: usize,
    delete_calls: usize,
}

impl MockChunkAllocator {
    fn new() -> Self {
        Self::default()
    }

    fn snapshot(&self) -> std::sync::MutexGuard<'_, MockChunkState> {
        self.state.lock().unwrap()
    }
}

fn make_segments(chunk_id: ChunkId, count: usize, offset: &mut u64) -> Vec<Segment> {
    let mut segments = Vec::with_capacity(count);
    for i in 0..count {
        segments.push(Segment {
            disk_id: Some(ProtoDiskId {
                high: 1000 + i as u64,
                low: i as u64,
            }),
            zone_index: 0,
            unit_offset: *offset,
            unit_count: 1,
            owner_chunk: Some(chunk_id),
            allocation_ts: *offset + 1,
        });
        *offset += 1;
    }
    segments
}

fn make_strip(strip_seq: u32, data_num: u32, code_num: u32, segments: Vec<Segment>) -> ChunkStrip {
    ChunkStrip {
        chunk_offset: strip_seq,
        strip_sequence: strip_seq,
        unit_kb: 4,
        capacity: data_num,
        create_ts_ms: 0,
        sealed_ts_ms: 0,
        sealed_length: 0,
        strip_type: StripType::Ec as i32,
        strip: Some(StripOneof::EcStrip(EcStrip {
            data_num,
            code_num,
            ec_state: 0,
            segments,
        })),
        usage_bitmap: Vec::new(),
        unavailable_segments: Vec::new(),
        placement_priority: 0,
        placement_assessment: None,
        placement_repair_required: false,
    }
}

#[async_trait]
impl ChunkAllocator for MockChunkAllocator {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.allocate_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let data_num = req.data_num as usize;
        let code_num = req.code_num as usize;
        let total = data_num + code_num;

        let mut strips = Vec::with_capacity(req.strip_count as usize);
        for sequence in 0..req.strip_count {
            let segments = make_segments(chunk_id, total, &mut st.next_segment_offset);
            strips.push(make_strip(sequence, req.data_num, req.code_num, segments));
        }
        let chunk = Chunk {
            id: Some(chunk_id),
            modify_ts: 1,
            state: 1,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            capacity: strips.iter().map(|strip| strip.capacity).sum(),
            sealed_length: 0,
            strips: strips.clone(),
            chunk_type: req.chunk_type,
            writer_epoch: req.writer_epoch,
            acknowledged_cursor: 0,
            closed_strip_sequence: None,
            writer_lease_deadline_ms: 0,
            next_strip_sequence: req.strip_count,
            cleanup_intents: vec![],
            last_strip_replacement: None,
            owner_key: req.owner_key,
        };
        st.chunks
            .insert((chunk_id.high, chunk_id.low), (strips, 0, false));
        Ok(AllocateChunkResponse { chunk: Some(chunk) })
    }

    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.append_calls += 1;
        st.append_strip_counts.push(req.strip_count);
        let chunk_id = req.chunk_id.unwrap_or_default();
        let data_num = req.data_num as usize;
        let code_num = req.code_num as usize;
        let total = data_num + code_num;

        let strip_seq = st
            .chunks
            .get(&(chunk_id.high, chunk_id.low))
            .map_or(0, |e| e.0.len() as u32);

        let mut strips = Vec::with_capacity(req.strip_count as usize);
        for index in 0..req.strip_count {
            let segments = make_segments(chunk_id, total, &mut st.next_segment_offset);
            strips.push(make_strip(
                strip_seq.saturating_add(index),
                req.data_num,
                req.code_num,
                segments,
            ));
        }

        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("append to unknown chunk");
        entry.0.extend(strips.iter().cloned());
        Ok(AppendChunkResponse {
            modify_ts: u64::from(strip_seq.saturating_add(req.strip_count)),
            strips,
            chunk: None,
        })
    }

    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.seal_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("seal unknown chunk");
        entry.1 = req.seal_length;
        Ok(SealChunkResponse { chunk: None })
    }

    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.delete_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("delete unknown chunk");
        entry.2 = true;
        Ok(DeleteChunkResponse { chunk: None })
    }

    async fn update_chunk_strip(&self, _req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        Ok(UpdateChunkStripResponse { chunk: None })
    }

    async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let chunk_id = req.chunk_id.unwrap();
        let state = self.state.lock().unwrap();
        let (strips, length, deleted) = state
            .chunks
            .get(&(chunk_id.high, chunk_id.low))
            .ok_or_else(|| IoError::MetadataConflict("chunk is missing".into()))?;
        let chunk = Chunk {
            id: Some(chunk_id),
            strips: strips.clone(),
            capacity: strips.iter().map(|strip| strip.capacity).sum(),
            state: if *deleted {
                ChunkState::Deleted as i32
            } else {
                ChunkState::Sealed as i32
            },
            sealed_length: *length,
            acknowledged_cursor: u64::from(*length) * 1024,
            ..Chunk::default()
        };
        Ok(QueryChunkResponse {
            chunk: Some(chunk),
            layout_validity_ms: 30_000,
        })
    }
}

// ── Helpers ──────────────────────────────────────────────────────

fn test_config(max_chunk_size: u64) -> Arc<ChunkClientConfig> {
    Arc::new(ChunkClientConfig {
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
        read_buffer_size: 4096,
        max_cached_buffer: 8 * 4096,
        memory_budget: 0,
    })
}

#[tokio::test]
async fn large_chunk_prefetch_preserves_type_in_id_and_metadata() {
    let prefetch = crowdb_chunk_client::ChunkPrefetch::new(
        Arc::new(MockChunkAllocator::new()),
        ec_4_1(),
        test_config(1024 * 1024),
        crowdb_protocol::CHUNK_TYPE_S3,
    );
    let chunk = prefetch.on_demand().await.unwrap();
    assert_eq!(
        chunk.id.unwrap().high >> 56,
        u64::from(crowdb_protocol::CHUNK_TYPE_S3)
    );
    assert_eq!(chunk.chunk_type, ChunkType::S3 as i32);

    let (mut prepared, task) = prefetch.spawn(Some(2 * 1024 * 1024));
    let first = prepared.recv().await.unwrap().unwrap();
    let second = prepared.recv().await.unwrap().unwrap();
    assert_ne!(first.id, second.id);
    for prepared_chunk in [first, second] {
        assert_eq!(
            prepared_chunk.id.unwrap().high >> 56,
            u64::from(crowdb_protocol::CHUNK_TYPE_S3)
        );
        assert_eq!(prepared_chunk.chunk_type, ChunkType::S3 as i32);
    }
    task.await.unwrap();
}

#[tokio::test]
async fn chunk_writer_crosses_mirror_and_ec_strip_boundaries() {
    let allocator = Arc::new(MockChunkAllocator::new());
    let temp = test_dirs::tempdir_in_test_data("chunk-client");
    let disk = Arc::new(LocalFileDiskWriter::new(temp.path()));
    let chunk_id = ChunkId { high: 1, low: 9 };
    let mut offset = 0;
    let mut mirror_segments = make_segments(chunk_id, 1, &mut offset);
    mirror_segments[0].unit_count = 2;
    let mirror = ChunkStrip {
        unit_kb: 4,
        capacity: 8,
        strip_sequence: 0,
        strip_type: StripType::Mirror as i32,
        strip: Some(StripOneof::MirrorStrip(MirrorStrip {
            segments: mirror_segments.clone(),
        })),
        ..ChunkStrip::default()
    };
    let ec_segments = make_segments(chunk_id, 2, &mut offset);
    let mut ec = make_strip(1, 1, 1, ec_segments.clone());
    ec.chunk_offset = 8;
    ec.capacity = 4;
    let strips = vec![mirror, ec];
    allocator
        .state
        .lock()
        .unwrap()
        .chunks
        .insert((chunk_id.high, chunk_id.low), (strips.clone(), 0, false));
    let chunk = Chunk {
        id: Some(chunk_id),
        strips,
        capacity: 12,
        modify_ts: 1,
        ..Chunk::default()
    };
    let mut config = (*test_config(16 * 1024)).clone();
    config.read_buffer_size = 4 * 1024;
    let mut writer = ChunkWriter::new(
        allocator.clone(),
        disk.clone(),
        EcScheme::new(1, 1),
        Arc::new(config),
    );
    writer.open(chunk, Some(12 * 1024)).unwrap();
    writer.push(Bytes::from(vec![5; 12 * 1024])).await.unwrap();
    let location = writer.seal().await.unwrap();
    assert_eq!(location.length, 12 * 1024);
    assert_eq!(
        disk.read_block(
            DiskId::new(
                mirror_segments[0].disk_id.unwrap().high,
                mirror_segments[0].disk_id.unwrap().low
            ),
            0,
            8 * 1024,
        )
        .unwrap(),
        vec![5; 8 * 1024]
    );
    assert_eq!(
        disk.read_block(
            DiskId::new(
                ec_segments[0].disk_id.unwrap().high,
                ec_segments[0].disk_id.unwrap().low
            ),
            4 * 1024,
            4 * 1024,
        )
        .unwrap(),
        vec![5; 4 * 1024]
    );
    drop(writer);
    drop(disk);
    let reopened_disk = Arc::new(LocalFileDiskWriter::new(temp.path()));
    let reader = ChunkReader::new(allocator, reopened_disk, ChunkReadPolicy::default()).unwrap();
    assert_eq!(
        reader
            .read_object(std::slice::from_ref(&location))
            .await
            .unwrap()
            .concat(),
        vec![5; 12 * 1024]
    );
}

#[tokio::test]
async fn single_copy_mirror_write_reports_error_at_finish() {
    let chunk_id = ChunkId { high: 1, low: 10 };
    let mut offset = 0;
    let strip = ChunkStrip {
        unit_kb: 4,
        capacity: 4,
        strip_type: StripType::Mirror as i32,
        strip: Some(StripOneof::MirrorStrip(MirrorStrip {
            segments: make_segments(chunk_id, 1, &mut offset),
        })),
        ..ChunkStrip::default()
    };
    let chunk = Chunk {
        id: Some(chunk_id),
        strips: vec![strip],
        capacity: 4,
        ..Chunk::default()
    };
    let disk = Arc::new(RejectingDiskWriter::default());
    let mut writer = ChunkWriter::new(
        Arc::new(MockChunkAllocator::new()),
        disk.clone(),
        EcScheme::new(2, 1),
        test_config(4 * 1024),
    );
    writer.open(chunk, Some(1024)).unwrap();
    writer.push(Bytes::from(vec![7; 1024])).await.unwrap();
    assert!(writer.seal().await.is_err());
    assert_eq!(disk.attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn mirror_strip_writes_keep_four_in_flight_and_commit_in_order() {
    let chunk_id = ChunkId { high: 1, low: 11 };
    let mut offset = 0;
    let strips = (0..5)
        .map(|index| ChunkStrip {
            chunk_offset: index * 4,
            strip_sequence: index,
            unit_kb: 4,
            capacity: 4,
            strip_type: StripType::Mirror as i32,
            strip: Some(StripOneof::MirrorStrip(MirrorStrip {
                segments: make_segments(chunk_id, 1, &mut offset),
            })),
            ..ChunkStrip::default()
        })
        .collect::<Vec<_>>();
    let allocator = MockChunkAllocator::new();
    allocator
        .state
        .lock()
        .unwrap()
        .chunks
        .insert((chunk_id.high, chunk_id.low), (strips.clone(), 0, false));
    let disk = Arc::new(OrderedMirrorDiskWriter::default());
    let mut writer = ChunkWriter::new(
        Arc::new(allocator.clone()),
        disk.clone(),
        EcScheme::new(2, 1),
        test_config(5 * UNIT_BYTES),
    );
    writer
        .open(
            Chunk {
                id: Some(chunk_id),
                strips,
                capacity: 20,
                ..Chunk::default()
            },
            Some(5 * UNIT_BYTES),
        )
        .unwrap();
    for index in 0..4 {
        writer.push(block(index, UNIT_BYTES as usize)).await.unwrap();
    }
    {
        let fifth = writer.push(block(4, UNIT_BYTES as usize));
        tokio::pin!(fifth);
        tokio::select! {
            result = &mut fifth => panic!("fifth write passed the four-write window: {result:?}"),
            () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
        }
        assert_eq!(disk.max_inflight.load(Ordering::Relaxed), 4);
        let mut completed = disk.completed.lock().unwrap().clone();
        completed.sort_unstable();
        assert_eq!(completed, vec![1, 2, 3]);
        assert_eq!(allocator.snapshot().seal_calls, 0);
        disk.first_release.add_permits(1);
        fifth.await.unwrap();
    }
    assert_eq!(writer.seal().await.unwrap().length, 5 * UNIT_BYTES);
    assert_eq!(disk.max_inflight.load(Ordering::Relaxed), 4);
    assert_eq!(allocator.snapshot().seal_calls, 1);
}

#[tokio::test]
async fn failed_full_mirror_strip_cannot_seal_after_async_dispatch() {
    let chunk_id = ChunkId { high: 1, low: 12 };
    let mut offset = 0;
    let strip = ChunkStrip {
        unit_kb: 4,
        capacity: 4,
        strip_type: StripType::Mirror as i32,
        strip: Some(StripOneof::MirrorStrip(MirrorStrip {
            segments: make_segments(chunk_id, 1, &mut offset),
        })),
        ..ChunkStrip::default()
    };
    let allocator = MockChunkAllocator::new();
    allocator
        .state
        .lock()
        .unwrap()
        .chunks
        .insert((chunk_id.high, chunk_id.low), (vec![strip.clone()], 0, false));
    let disk = Arc::new(RejectingDiskWriter::default());
    let mut writer = ChunkWriter::new(
        Arc::new(allocator.clone()),
        disk.clone(),
        EcScheme::new(2, 1),
        test_config(UNIT_BYTES),
    );
    writer
        .open(
            Chunk {
                id: Some(chunk_id),
                strips: vec![strip],
                capacity: 4,
                ..Chunk::default()
            },
            Some(UNIT_BYTES),
        )
        .unwrap();
    writer.push(block(7, UNIT_BYTES as usize)).await.unwrap();
    let result = writer.seal().await;
    assert!(result.is_err(), "faulty mirror unexpectedly sealed: {result:?}");
    assert_eq!(allocator.snapshot().seal_calls, 0);
    writer.abort().await.unwrap();
    assert!(allocator.snapshot().delete_calls >= 1);
    assert_eq!(disk.attempts.load(Ordering::Relaxed), 1);
}

fn ec_4_1() -> EcScheme {
    EcScheme::new(4, 1)
}

fn block(value: u8, size: usize) -> Bytes {
    Bytes::from(vec![value; size])
}

fn make_writer(
    chunkdb: MockChunkAllocator,
    diskio: Arc<LocalFileDiskWriter>,
    ec: EcScheme,
    config: Arc<ChunkClientConfig>,
) -> ChunkWriter {
    ChunkWriter::new(Arc::new(chunkdb), diskio, ec, config)
}

// ── Tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn chunk_writer_strip_rotation() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = Arc::new(LocalFileDiskWriter::new(tmp.path()));
    let ec = ec_4_1();
    let config = test_config(1024 * 1024 * 1024);
    let mut cw = make_writer(chunkdb.clone(), diskio, ec, config);

    // Allocate a chunk with 1 strip (mock allocator).
    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb.clone()),
            ec,
            test_config(1024 * 1024 * 1024),
            crowdb_protocol::chunk_id::CHUNK_TYPE_S3,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();

    // Push data_num * 3 blocks (3 strips).
    for i in 0..(DATA_NUM * 3) as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }

    // Seal to finish the last strip (push auto-rotates but doesn't
    // finish the final strip — seal does that).
    let location = cw.seal().await.unwrap();

    // Verify 3 strips written, bytes correct.
    assert_eq!(location.length, DATA_NUM as u64 * 3 * UNIT_BYTES);
    assert_eq!(cw.strips_in_chunk(), 3);
}

#[tokio::test]
async fn chunk_writer_on_demand_append() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = Arc::new(LocalFileDiskWriter::new(tmp.path()));
    let ec = ec_4_1();
    let config = test_config(1024 * 1024 * 1024);
    let mut cw = make_writer(chunkdb.clone(), diskio, ec, config);

    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb.clone()),
            ec,
            test_config(1024 * 1024 * 1024),
            crowdb_protocol::chunk_id::CHUNK_TYPE_S3,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();

    // Push four strips. The initial chunk owns two; unknown-size writes append
    // one strip per request while the bounded prefetcher stays ahead.
    for i in 0..(DATA_NUM * 4) as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }

    // Verify append_chunk was called (at least once — prefetch may
    // have appended more).
    let st = chunkdb.snapshot();
    assert_eq!(st.append_calls, 2, "append_calls = {}", st.append_calls);
    assert_eq!(st.append_strip_counts, vec![1, 1]);
}

#[tokio::test]
async fn chunk_writer_is_full_and_seal() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = Arc::new(LocalFileDiskWriter::new(tmp.path()));
    let ec = ec_4_1();
    // max_chunk_size = 2 strips = 2 * 4 * 4096 = 32768 bytes.
    let config = test_config(DATA_NUM as u64 * UNIT_BYTES * 2);
    let mut cw = make_writer(chunkdb.clone(), diskio, ec, config);

    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb.clone()),
            ec,
            test_config(DATA_NUM as u64 * UNIT_BYTES * 2),
            crowdb_protocol::chunk_id::CHUNK_TYPE_S3,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();

    // Push data_num * 2 blocks (2 strips = max_chunk_size).
    for i in 0..(DATA_NUM * 2) as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }

    // After 2 strips, the chunk should be full.
    // (push auto-rotates + finishes strips; is_full is checked
    // internally.)
    let location = cw.seal().await.unwrap();
    assert_eq!(location.length, DATA_NUM as u64 * 2 * UNIT_BYTES);
    assert!(location.chunk_id.is_some());

    let st = chunkdb.snapshot();
    assert_eq!(st.seal_calls, 1);
}

#[tokio::test]
async fn chunk_writer_abort() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = Arc::new(LocalFileDiskWriter::new(tmp.path()));
    let ec = ec_4_1();
    let config = test_config(1024 * 1024 * 1024);
    let mut cw = make_writer(chunkdb.clone(), diskio, ec, config);

    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb.clone()),
            ec,
            test_config(1024 * 1024 * 1024),
            crowdb_protocol::chunk_id::CHUNK_TYPE_S3,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();

    // Push partial data (2 blocks = partial strip).
    for i in 0..2u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }

    cw.abort().await.unwrap();

    let st = chunkdb.snapshot();
    assert_eq!(st.delete_calls, 1);
}

#[tokio::test]
async fn chunk_writer_empty_seal() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = Arc::new(LocalFileDiskWriter::new(tmp.path()));
    let ec = ec_4_1();
    let config = test_config(1024 * 1024 * 1024);
    let mut cw = make_writer(chunkdb.clone(), diskio, ec, config);

    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb.clone()),
            ec,
            test_config(1024 * 1024 * 1024),
            crowdb_protocol::chunk_id::CHUNK_TYPE_S3,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();

    // Immediately seal — no data pushed.
    let location = cw.seal().await.unwrap();
    assert_eq!(location.length, 0);

    let st = chunkdb.snapshot();
    assert_eq!(st.seal_calls, 0);
    assert_eq!(st.delete_calls, 1);
}

#[tokio::test]
async fn chunk_writer_bounds_cross_strip_parity_tasks() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = Arc::new(OrderingDiskWriter::default());
    let ec = ec_4_1();
    let mut config = (*test_config(1024 * 1024 * 1024)).clone();
    config.parity_depth = 1;
    let mut cw = ChunkWriter::new(Arc::new(chunkdb.clone()), diskio.clone(), ec, Arc::new(config));
    let pf = crowdb_chunk_client::ChunkPrefetch::new(
        Arc::new(chunkdb),
        ec,
        test_config(1024 * 1024 * 1024),
        crowdb_protocol::chunk_id::CHUNK_TYPE_S3,
    );
    cw.open(pf.on_demand().await.unwrap(), None).unwrap();

    for i in 0..(DATA_NUM * 3) as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }
    cw.seal().await.unwrap();

    assert_eq!(diskio.parity_max(), 1);
}

#[tokio::test]
async fn chunk_writer_submits_independent_data_writes_concurrently() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = Arc::new(ConcurrentDiskWriter::default());
    let ec = ec_4_1();
    let mut cw = ChunkWriter::new(
        Arc::new(chunkdb.clone()),
        diskio.clone(),
        ec,
        test_config(1024 * 1024 * 1024),
    );
    let pf = crowdb_chunk_client::ChunkPrefetch::new(
        Arc::new(chunkdb),
        ec,
        test_config(1024 * 1024 * 1024),
        crowdb_protocol::chunk_id::CHUNK_TYPE_S3,
    );
    cw.open(pf.on_demand().await.unwrap(), None).unwrap();

    for i in 0..DATA_NUM as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }
    cw.seal().await.unwrap();

    assert!(diskio.max_inflight.load(Ordering::Relaxed) > 1);
    assert_eq!(diskio.inflight.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn chunk_writer_abort_drains_submitted_parity_io() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = Arc::new(OrderingDiskWriter::default());
    let ec = ec_4_1();
    let mut cw = ChunkWriter::new(
        Arc::new(chunkdb.clone()),
        diskio.clone(),
        ec,
        test_config(1024 * 1024 * 1024),
    );
    let pf = crowdb_chunk_client::ChunkPrefetch::new(
        Arc::new(chunkdb.clone()),
        ec,
        test_config(1024 * 1024 * 1024),
        crowdb_protocol::chunk_id::CHUNK_TYPE_S3,
    );
    cw.open(pf.on_demand().await.unwrap(), None).unwrap();
    for i in 0..=DATA_NUM as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }

    cw.abort().await.unwrap();

    assert_eq!(diskio.parity_inflight(), 0);
    assert!(diskio.events().iter().any(|event| event == "write:1004"));
    assert_eq!(chunkdb.snapshot().delete_calls, 1);
}
