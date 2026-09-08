// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkAllocator, ChunkIoClient, ChunkIoWriter, DiskWriter, FeedStatus, IoError, Result, SmallWritePolicy,
};
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AdvanceChunkWriteResponse, AllocateChunkRequest, AllocateChunkResponse,
    AppendChunkRequest, AppendChunkResponse, Chunk, ChunkState, ChunkStrip, ChunkType, DeleteChunkRequest,
    DeleteChunkResponse, Location, MirrorStrip, QueryChunkRequest, QueryChunkResponse, SealChunkRequest,
    SealChunkResponse, Strip, StripType, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;

#[derive(Default)]
struct MockState {
    chunks: HashMap<(u64, u64), Chunk>,
    allocations: usize,
    appends: usize,
    advances: usize,
    seals: usize,
    deletes: usize,
}

#[derive(Default)]
struct MockAllocator {
    next_chunk: AtomicU64,
    advance_delay_ms: AtomicU64,
    fail_allocations: AtomicBool,
    fail_on_attempt: AtomicU64,
    state: Mutex<MockState>,
}

impl MockAllocator {
    fn snapshot(&self) -> (usize, usize, usize, usize, usize) {
        let state = self.state.lock().unwrap();
        (
            state.allocations,
            state.appends,
            state.advances,
            state.seals,
            state.deletes,
        )
    }
}

#[async_trait]
impl ChunkAllocator for MockAllocator {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        if self.fail_allocations.load(Ordering::Relaxed) {
            return Err(IoError::AllocationFailed("injected allocation failure".into()));
        }
        let low = self.next_chunk.fetch_add(1, Ordering::Relaxed) + 1;
        if self.fail_on_attempt.load(Ordering::Relaxed) == low {
            return Err(IoError::AllocationFailed("injected allocation failure".into()));
        }
        let chunk_id = req.chunk_id.unwrap_or(ChunkId { high: 7, low });
        let strip = make_strip(chunk_id, 0, req.copy_count.max(1));
        let chunk = Chunk {
            id: Some(chunk_id),
            modify_ts: 1,
            state: ChunkState::Active as i32,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            capacity: strip.capacity,
            sealed_length: 0,
            strips: vec![strip],
            chunk_type: ChunkType::Repo as i32,
            writer_epoch: req.writer_epoch,
            acknowledged_cursor: 0,
            closed_strip_sequence: None,
            writer_lease_deadline_ms: req.writer_lease_ms,
        };
        let mut state = self.state.lock().unwrap();
        state.allocations += 1;
        state.chunks.insert((chunk_id.high, chunk_id.low), chunk.clone());
        Ok(AllocateChunkResponse { chunk: Some(chunk) })
    }

    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let id = req.chunk_id.unwrap();
        let mut state = self.state.lock().unwrap();
        state.appends += 1;
        let chunk = state.chunks.get_mut(&(id.high, id.low)).unwrap();
        if chunk.modify_ts != req.modify_ts {
            return Ok(AppendChunkResponse {
                modify_ts: chunk.modify_ts,
                strips: Vec::new(),
                chunk: Some(chunk.clone()),
            });
        }
        let sequence = u32::try_from(chunk.strips.len()).unwrap();
        let strip = make_strip(id, sequence, req.copy_count.max(1));
        chunk.capacity += strip.capacity;
        chunk.modify_ts += 1;
        chunk.strips.push(strip.clone());
        Ok(AppendChunkResponse {
            modify_ts: chunk.modify_ts,
            strips: vec![strip],
            chunk: None,
        })
    }

    async fn advance_chunk_write(&self, req: AdvanceChunkWriteRequest) -> Result<AdvanceChunkWriteResponse> {
        let delay_ms = self.advance_delay_ms.load(Ordering::Relaxed);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        let id = req.chunk_id.unwrap();
        let mut state = self.state.lock().unwrap();
        state.advances += 1;
        let chunk = state.chunks.get_mut(&(id.high, id.low)).unwrap();
        if chunk.writer_epoch != req.writer_epoch || chunk.modify_ts != req.expected_modify_ts {
            return Err(IoError::AllocationFailed("stale writer".into()));
        }
        chunk.modify_ts += 1;
        chunk.acknowledged_cursor = req.acknowledged_cursor;
        chunk.closed_strip_sequence = req.closed_strip_sequence;
        Ok(AdvanceChunkWriteResponse {
            chunk: Some(chunk.clone()),
        })
    }

    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let id = req.chunk_id.unwrap();
        let mut state = self.state.lock().unwrap();
        state.seals += 1;
        let chunk = state.chunks.get_mut(&(id.high, id.low)).unwrap();
        chunk.state = ChunkState::Sealed as i32;
        chunk.sealed_length = req.seal_length;
        Ok(SealChunkResponse {
            chunk: Some(chunk.clone()),
        })
    }

    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let id = req.chunk_id.unwrap();
        let mut state = self.state.lock().unwrap();
        state.deletes += 1;
        let chunk = state.chunks.get_mut(&(id.high, id.low)).unwrap();
        chunk.state = ChunkState::Deleted as i32;
        Ok(DeleteChunkResponse {
            chunk: Some(chunk.clone()),
        })
    }

    async fn update_chunk_strip(&self, _req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        unreachable!()
    }

    async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let id = req.chunk_id.unwrap();
        let state = self.state.lock().unwrap();
        Ok(QueryChunkResponse {
            chunk: state.chunks.get(&(id.high, id.low)).cloned(),
        })
    }
}

fn make_strip(chunk_id: ChunkId, sequence: u32, copies: u32) -> ChunkStrip {
    let segments = (0..copies)
        .map(|copy| Segment {
            disk_id: Some(DiskId {
                high: u64::from(copy) + 1,
                low: 0,
            }),
            owner_chunk: Some(chunk_id),
            unit_offset: chunk_id.low * 4096 + u64::from(sequence) * 256,
            zone_index: 0,
            unit_count: 256,
            allocation_ts: 0,
        })
        .collect();
    ChunkStrip {
        chunk_offset: sequence * 1024,
        strip_sequence: sequence,
        unit_kb: 4,
        capacity: 1024,
        create_ts_ms: 0,
        sealed_ts_ms: 0,
        sealed_length: 0,
        strip_type: StripType::Mirror as i32,
        strip: Some(Strip::MirrorStrip(MirrorStrip { segments })),
        usage_bitmap: Vec::new(),
    }
}

#[derive(Default)]
struct RecordingDiskWriter {
    writes: Mutex<Vec<(u64, u64, Bytes)>>,
    calls: AtomicUsize,
    fail: AtomicBool,
    delay_ms: AtomicU64,
}

impl RecordingDiskWriter {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl DiskWriter for RecordingDiskWriter {
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let delay_ms = self.delay_ms.load(Ordering::Relaxed);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        if self.fail.load(Ordering::Relaxed) {
            return Err(IoError::WriteFailed("injected failure".into()));
        }
        let disk = seg.disk_id.unwrap_or_default().high;
        self.writes
            .lock()
            .unwrap()
            .push((disk, seg.unit_offset * unit_bytes, data));
        Ok(())
    }
}

fn policy() -> SmallWritePolicy {
    SmallWritePolicy {
        object_limit: 1024 * 1024,
        memory_budget: 4 * 1024 * 1024,
        queue_capacity: 128,
        min_pipelines: 1,
        max_pipelines: 1,
        max_batch_bytes: 1024 * 1024,
        max_batch_objects: 128,
        batch_deadline: Duration::from_millis(20),
        scale_out_queue_bytes: 1024 * 1024,
        scale_out_queue_objects: 128,
        scale_in_delay: Duration::from_secs(1),
        control_interval: Duration::from_millis(10),
        cooldown: Duration::from_millis(10),
        chunk_capacity: 1024 * 1024 * 1024,
        mirror_copies: 3,
        writer_lease: Duration::from_secs(30),
    }
}

#[test]
fn small_object_elasticity_defaults_start_at_one_and_cap_at_thirty_two() {
    let policy = SmallWritePolicy::default();
    assert_eq!(policy.min_pipelines, 1);
    assert_eq!(policy.max_pipelines, 32);
    assert_eq!(policy.scale_out_queue_bytes, 4 * 1024 * 1024);
    assert_eq!(policy.scale_out_queue_objects, 128);
}

fn client(policy: SmallWritePolicy) -> (ChunkIoClient, Arc<MockAllocator>, Arc<RecordingDiskWriter>) {
    let allocator = Arc::new(MockAllocator::default());
    let disk = Arc::new(RecordingDiskWriter::default());
    let client = ChunkIoClient::from_parts_with_small_policy(allocator.clone(), disk.clone(), policy)
        .expect("valid policy");
    (client, allocator, disk)
}

#[tokio::test]
async fn small_object_empty_finishes_without_starting_pool() {
    let (client, allocator, disk) = client(policy());
    let mut writer = client.prepare_small_write(0).await.unwrap();
    assert!(!writer.require_data());
    assert!(writer.on_finish().await.unwrap().is_empty());
    assert!(matches!(writer.on_finish().await, Err(IoError::Finished)));
    assert_eq!(allocator.snapshot().0, 0);
    assert_eq!(disk.calls(), 0);
    let _ = client.shutdown_small_writes().await;
}

#[tokio::test]
async fn small_object_ingress_validates_size_and_releases_reservation() {
    let (client, _, _) = client(policy());
    let mut writer = client.prepare_small_write(64 * 1024).await.unwrap();
    for _ in 0..4 {
        writer.on_data(Bytes::from(vec![7; 16 * 1024])).await.unwrap();
    }
    assert_eq!(client.small_write_metrics().reserved_bytes, 64 * 1024);
    let locations = writer.on_finish().await.unwrap();
    assert_eq!(locations[0].length, 64 * 1024);
    assert_eq!(client.small_write_metrics().reserved_bytes, 0);

    let mut short = client.prepare_small_write(64 * 1024).await.unwrap();
    short.on_data(Bytes::from(vec![1; 32 * 1024])).await.unwrap();
    assert!(matches!(
        short.on_finish().await,
        Err(IoError::ObjectSizeMismatch {
            declared: 65536,
            actual: 32768
        })
    ));
    assert!(matches!(short.on_error().await, Err(IoError::Finished)));

    let mut overflow = client.prepare_small_write(64 * 1024).await.unwrap();
    assert!(matches!(
        overflow.on_data(Bytes::from(vec![1; 65 * 1024])).await,
        Err(IoError::ObjectSizeMismatch { .. })
    ));
    assert_eq!(client.small_write_metrics().reserved_bytes, 0);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_clone_handles_share_one_batch_and_receive_independent_locations() {
    let (client, allocator, disk) = client(policy());
    let sizes = [4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024];
    let mut tasks = Vec::new();
    for (value, size) in sizes.into_iter().enumerate() {
        let cloned = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut writer = cloned.prepare_small_write(size).await.unwrap();
            assert_eq!(
                writer
                    .on_data(Bytes::from(vec![u8::try_from(value).unwrap(); size]))
                    .await
                    .unwrap(),
                FeedStatus::Pause
            );
            writer.on_finish().await.unwrap().remove(0)
        }));
    }
    let mut locations = Vec::<Location>::new();
    for task in tasks {
        locations.push(task.await.unwrap());
    }
    locations.sort_by_key(|location| location.offset);
    assert!(locations
        .iter()
        .all(|location| location.chunk_id == locations[0].chunk_id));
    assert_eq!(
        locations
            .iter()
            .map(|location| location.length)
            .collect::<Vec<_>>(),
        sizes.map(|size| size as u64)
    );
    for pair in locations.windows(2) {
        assert!(pair[0].offset + pair[0].length <= pair[1].offset);
    }
    assert!(locations
        .iter()
        .all(|location| location.logical_offset == 0 && location.logical_length == location.length));
    assert_eq!(allocator.snapshot().0, 1);
    assert_eq!(allocator.snapshot().2, 1);
    assert_eq!(disk.calls(), 3);
    assert_eq!(client.small_write_metrics().batches, 1);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_whole_budget_waits_without_partial_reservation() {
    let mut bounded = policy();
    bounded.object_limit = 64 * 1024;
    bounded.memory_budget = 64 * 1024;
    bounded.scale_out_queue_bytes = 64 * 1024;
    let (client, _, _) = client(bounded);
    let mut first = client.prepare_small_write(64 * 1024).await.unwrap();
    let clone = client.clone();
    let second = tokio::spawn(async move { clone.prepare_small_write(64 * 1024).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!second.is_finished());
    assert_eq!(client.small_write_metrics().reserved_bytes, 64 * 1024);
    first.on_error().await.unwrap();
    let mut second = second.await.unwrap().unwrap();
    assert_eq!(client.small_write_metrics().reserved_bytes, 64 * 1024);
    second.on_error().await.unwrap();
    assert_eq!(client.small_write_metrics().reserved_bytes, 0);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_oversize_is_rejected_before_pool_start() {
    let (client, allocator, _) = client(policy());
    assert!(matches!(
        client.prepare_small_write(2 * 1024 * 1024).await,
        Err(IoError::ObjectTooLarge { .. })
    ));
    assert_eq!(allocator.snapshot().0, 0);
}

#[tokio::test]
async fn small_object_mirror_failure_fails_every_object_without_cursor_commit() {
    let (client, allocator, disk) = client(policy());
    disk.fail.store(true, Ordering::Relaxed);
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let cloned = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut writer = cloned.prepare_small_write(16 * 1024).await.unwrap();
            writer.on_data(Bytes::from(vec![1; 16 * 1024])).await.unwrap();
            writer.on_finish().await
        }));
    }
    for task in tasks {
        assert!(matches!(task.await.unwrap(), Err(IoError::WriteFailed(_))));
    }
    assert_eq!(allocator.snapshot().2, 0);
    assert_eq!(client.small_write_metrics().completed, 0);
    assert_eq!(client.small_write_metrics().failed, 4);
    disk.fail.store(false, Ordering::Relaxed);
    let recovered = tokio::time::timeout(Duration::from_secs(1), async {
        let mut writer = client.prepare_small_write(4096).await.unwrap();
        writer.on_data(Bytes::from(vec![9; 4096])).await.unwrap();
        writer.on_finish().await
    })
    .await
    .expect("manager should replace failed worker")
    .unwrap();
    assert_eq!(recovered[0].length, 4096);
    assert!(allocator.snapshot().0 >= 2);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_segment_relative_write_validates_before_delegating() {
    let disk = RecordingDiskWriter::default();
    let segment = Segment {
        disk_id: Some(DiskId { high: 1, low: 0 }),
        owner_chunk: None,
        unit_offset: 10,
        zone_index: 0,
        unit_count: 4,
        allocation_ts: 0,
    };
    disk.write_at(&segment, 4096, 4096, Bytes::from(vec![1; 4096]))
        .await
        .unwrap();
    assert_eq!(disk.writes.lock().unwrap()[0].1, 11 * 4096);
    assert!(disk
        .write_at(&segment, 4096, 1, Bytes::from(vec![1; 4096]))
        .await
        .is_err());
    assert!(disk
        .write_at(&segment, 4096, 3 * 4096, Bytes::from(vec![1; 2 * 4096]))
        .await
        .is_err());
    assert_eq!(disk.calls(), 1);
}

#[test]
fn small_object_policy_rejects_unreachable_limits() {
    let mut invalid = policy();
    invalid.memory_budget = invalid.object_limit - 1;
    assert!(invalid.validate().is_err());

    let mut invalid = policy();
    invalid.scale_out_queue_bytes = invalid.memory_budget + 1;
    assert!(invalid.validate().is_err());

    let mut invalid = policy();
    invalid.scale_out_queue_objects = invalid.queue_capacity + 1;
    assert!(invalid.validate().is_err());
}

#[tokio::test]
async fn small_object_sixty_four_objects_form_one_mebibyte_batch() {
    let (client, allocator, disk) = client(policy());
    let mut tasks = Vec::new();
    for value in 0..64_u8 {
        let clone = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut writer = clone.prepare_small_write(16 * 1024).await.unwrap();
            writer.on_data(Bytes::from(vec![value; 16 * 1024])).await.unwrap();
            writer.on_finish().await.unwrap()[0].clone()
        }));
    }
    let mut locations = Vec::new();
    for task in tasks {
        locations.push(task.await.unwrap());
    }
    locations.sort_by_key(|location| location.offset);
    assert_eq!(locations.first().unwrap().offset, 0);
    assert_eq!(locations.last().unwrap().offset, 63 * 16 * 1024);
    assert!(locations
        .iter()
        .all(|location| location.chunk_id == locations[0].chunk_id));
    assert_eq!(allocator.snapshot().2, 1);
    assert_eq!(disk.calls(), 3);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_strip_rotation_pads_tail_and_keeps_object_whole() {
    let (client, allocator, _) = client(policy());
    let mut first = client.prepare_small_write(700 * 1024).await.unwrap();
    first.on_data(Bytes::from(vec![1; 700 * 1024])).await.unwrap();
    let first = tokio::spawn(async move { first.on_finish().await.unwrap()[0].clone() });
    tokio::time::sleep(Duration::from_millis(2)).await;
    let mut second = client.prepare_small_write(400 * 1024).await.unwrap();
    second.on_data(Bytes::from(vec![2; 400 * 1024])).await.unwrap();
    let second = tokio::spawn(async move { second.on_finish().await.unwrap()[0].clone() });
    let first = first.await.unwrap();
    let second = second.await.unwrap();
    assert_eq!(first.chunk_id, second.chunk_id);
    assert_eq!(first.offset, 0);
    assert_eq!(second.offset, 1024 * 1024);
    assert_eq!(second.length, 400 * 1024);
    assert_eq!(allocator.snapshot().1, 1);
    assert_eq!(client.small_write_metrics().tail_waste_bytes, 324 * 1024);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_appends_strip_after_exact_physical_boundary() {
    let (client, allocator, _) = client(policy());
    let mut first = client.prepare_small_write(1024 * 1024).await.unwrap();
    first.on_data(Bytes::from(vec![1; 1024 * 1024])).await.unwrap();
    let first = first.on_finish().await.unwrap().remove(0);
    let mut second = client.prepare_small_write(4096).await.unwrap();
    second.on_data(Bytes::from(vec![2; 4096])).await.unwrap();
    let second = second.on_finish().await.unwrap().remove(0);
    assert_eq!(first.chunk_id, second.chunk_id);
    assert_eq!(second.offset, 1024 * 1024);
    assert_eq!(allocator.snapshot().1, 1);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_chunk_rotation_uses_one_prepared_replacement() {
    let mut limited = policy();
    limited.chunk_capacity = 1024 * 1024;
    let (client, allocator, _) = client(limited);
    let mut first = client.prepare_small_write(700 * 1024).await.unwrap();
    first.on_data(Bytes::from(vec![1; 700 * 1024])).await.unwrap();
    let first = tokio::spawn(async move { first.on_finish().await.unwrap()[0].clone() });
    tokio::time::sleep(Duration::from_millis(2)).await;
    let mut second = client.prepare_small_write(400 * 1024).await.unwrap();
    second.on_data(Bytes::from(vec![2; 400 * 1024])).await.unwrap();
    let second = tokio::spawn(async move { second.on_finish().await.unwrap()[0].clone() });
    let first = first.await.unwrap();
    let second = second.await.unwrap();
    assert_ne!(first.chunk_id, second.chunk_id);
    assert_eq!(second.offset, 0);
    let counts = allocator.snapshot();
    assert_eq!(counts.0, 3);
    assert_eq!(counts.3, 1);
    client.shutdown_small_writes().await.unwrap();
    let counts = allocator.snapshot();
    assert_eq!(counts.3, 2);
    assert_eq!(counts.4, 1);
}

#[tokio::test]
async fn small_object_batch_stops_at_configured_chunk_boundary() {
    let mut limited = policy();
    limited.chunk_capacity = 1024 * 1024;
    let (client, _, _) = client(limited);

    let mut prefix = client.prepare_small_write(700 * 1024).await.unwrap();
    prefix.on_data(Bytes::from(vec![1; 700 * 1024])).await.unwrap();
    let prefix = prefix.on_finish().await.unwrap().remove(0);

    let mut tasks = Vec::new();
    for value in [2, 3] {
        let clone = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut writer = clone.prepare_small_write(200 * 1024).await.unwrap();
            writer
                .on_data(Bytes::from(vec![value; 200 * 1024]))
                .await
                .unwrap();
            writer.on_finish().await.unwrap().remove(0)
        }));
    }
    let first = tasks.remove(0).await.unwrap();
    let second = tasks.remove(0).await.unwrap();
    let same_chunk = [&first, &second]
        .iter()
        .filter(|location| location.chunk_id == prefix.chunk_id)
        .count();
    assert_eq!(same_chunk, 1);
    assert!([&first, &second]
        .iter()
        .any(|location| location.chunk_id != prefix.chunk_id && location.offset == 0));
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_completion_waits_for_cursor_commit() {
    let (client, allocator, _) = client(policy());
    allocator.advance_delay_ms.store(50, Ordering::Relaxed);
    let mut writer = client.prepare_small_write(16 * 1024).await.unwrap();
    writer.on_data(Bytes::from(vec![1; 16 * 1024])).await.unwrap();
    let completion = tokio::spawn(async move { writer.on_finish().await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!completion.is_finished());
    let locations = completion.await.unwrap().unwrap();
    assert_eq!(locations[0].length, 16 * 1024);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_manager_scales_out_by_queued_bytes_then_drains_idle_pipeline() {
    let mut elastic = policy();
    elastic.max_pipelines = 2;
    elastic.max_batch_bytes = 16 * 1024;
    elastic.max_batch_objects = 1;
    elastic.batch_deadline = Duration::from_millis(1);
    elastic.scale_out_queue_bytes = 16 * 1024;
    elastic.scale_out_queue_objects = elastic.queue_capacity;
    elastic.scale_in_delay = Duration::from_millis(20);
    elastic.control_interval = Duration::from_millis(2);
    elastic.cooldown = Duration::from_millis(1);
    let (client, allocator, disk) = client(elastic);
    disk.delay_ms.store(30, Ordering::Relaxed);
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let clone = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut writer = clone.prepare_small_write(16 * 1024).await.unwrap();
            writer.on_data(Bytes::from(vec![1; 16 * 1024])).await.unwrap();
            writer.on_finish().await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert!(allocator.snapshot().0 >= 2);
    assert_eq!(client.small_write_metrics().scale_out, 1);
    tokio::time::sleep(Duration::from_millis(80)).await;
    let metrics = client.small_write_metrics();
    assert_eq!(metrics.active_pipelines, 1);
    assert!(metrics.scale_in >= 1);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_manager_scales_out_by_queued_object_count() {
    let mut elastic = policy();
    elastic.max_pipelines = 2;
    elastic.max_batch_bytes = 16 * 1024;
    elastic.max_batch_objects = 1;
    elastic.batch_deadline = Duration::from_millis(1);
    elastic.scale_out_queue_bytes = elastic.memory_budget;
    elastic.scale_out_queue_objects = 1;
    elastic.control_interval = Duration::from_millis(1);
    elastic.cooldown = Duration::from_millis(1);
    let (client, _, disk) = client(elastic);
    disk.delay_ms.store(30, Ordering::Relaxed);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let clone = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut writer = clone.prepare_small_write(4096).await.unwrap();
            writer.on_data(Bytes::from(vec![1; 4096])).await.unwrap();
            writer.on_finish().await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert!(client.small_write_metrics().scale_out >= 1);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_scale_out_failure_keeps_current_pipeline_routable() {
    let mut elastic = policy();
    elastic.max_pipelines = 2;
    elastic.max_batch_bytes = 16 * 1024;
    elastic.max_batch_objects = 1;
    elastic.batch_deadline = Duration::from_millis(1);
    elastic.scale_out_queue_bytes = 16 * 1024;
    elastic.scale_out_queue_objects = 1;
    elastic.control_interval = Duration::from_millis(1);
    elastic.cooldown = Duration::from_millis(1);
    let (client, allocator, disk) = client(elastic);
    let mut warm = client.prepare_small_write(4096).await.unwrap();
    warm.on_error().await.unwrap();
    allocator.fail_allocations.store(true, Ordering::Relaxed);
    disk.delay_ms.store(15, Ordering::Relaxed);
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let clone = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut writer = clone.prepare_small_write(16 * 1024).await.unwrap();
            writer.on_data(Bytes::from(vec![1; 16 * 1024])).await.unwrap();
            writer.on_finish().await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(allocator.snapshot().0, 1);
    assert_eq!(client.small_write_metrics().scale_out, 0);
    assert_eq!(client.small_write_metrics().active_pipelines, 1);
    allocator.fail_allocations.store(false, Ordering::Relaxed);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
async fn small_object_initialization_failure_retires_prepared_pipelines() {
    let mut initial = policy();
    initial.min_pipelines = 2;
    initial.max_pipelines = 2;
    let (client, allocator, _) = client(initial);
    allocator.fail_on_attempt.store(2, Ordering::Relaxed);

    assert!(matches!(
        client.prepare_small_write(4096).await,
        Err(IoError::AllocationFailed(_))
    ));
    assert_eq!(allocator.snapshot().0, 1);
    assert_eq!(allocator.snapshot().4, 1);
}
