// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkAllocator, ChunkIoClient, ChunkReadPolicy, DiskWriter, IoError};
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, CursorAdvance, MirrorStripImage, ProductionStreamChunkStore,
    ProductionStreamRuntime, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig,
    StreamMetadataStore, StreamName, StreamRegistry,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AdvanceChunkWriteResponse, AllocateChunkRequest, AllocateChunkResponse,
    AllocateReplacementSegmentRequest, AllocateReplacementSegmentResponse, AppendChunkRequest,
    AppendChunkResponse, Chunk, ChunkState, ChunkStrip, DeleteChunkRequest, DeleteChunkResponse,
    DiscardReplacementSegmentRequest, DiscardReplacementSegmentResponse, MirrorStrip, QueryChunkRequest,
    QueryChunkResponse, ReplaceChunkStripRangeRequest, ReplaceChunkStripRangeResponse, SealChunkRequest,
    SealChunkResponse, Strip, StripType, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;

struct Allocator {
    chunk: Mutex<Option<Chunk>>,
    fail_advance_after_commit: AtomicBool,
    fail_replacement_response_once: AtomicBool,
    fail_query_once: AtomicBool,
    replacement_calls: AtomicUsize,
    replacements: AtomicUsize,
}

impl Allocator {
    fn new() -> Self {
        Self {
            chunk: Mutex::new(None),
            fail_advance_after_commit: AtomicBool::new(false),
            fail_replacement_response_once: AtomicBool::new(false),
            fail_query_once: AtomicBool::new(false),
            replacement_calls: AtomicUsize::new(0),
            replacements: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl ChunkAllocator for Allocator {
    async fn allocate_chunk(
        &self,
        request: AllocateChunkRequest,
    ) -> crowdb_chunk_client::Result<AllocateChunkResponse> {
        let chunk_id = ChunkId { high: 7, low: 8 };
        let segments = (1..=3)
            .map(|disk| Segment {
                disk_id: Some(DiskId { high: disk, low: 0 }),
                owner_chunk: Some(chunk_id),
                unit_offset: 0,
                zone_index: 0,
                unit_count: request.write_granularity / 4,
                allocation_ts: 1,
            })
            .collect();
        let chunk = Chunk {
            id: Some(chunk_id),
            modify_ts: 1,
            state: ChunkState::Active as i32,
            capacity: request.write_granularity,
            strips: vec![ChunkStrip {
                chunk_offset: 0,
                strip_sequence: 0,
                unit_kb: 4,
                capacity: request.write_granularity,
                strip_type: StripType::Mirror as i32,
                strip: Some(Strip::MirrorStrip(MirrorStrip { segments })),
                ..ChunkStrip::default()
            }],
            chunk_type: request.chunk_type,
            writer_epoch: request.writer_epoch,
            writer_lease_deadline_ms: request.writer_lease_ms,
            next_strip_sequence: 1,
            owner_key: request.owner_key,
            ..Chunk::default()
        };
        *self.chunk.lock().unwrap() = Some(chunk.clone());
        Ok(AllocateChunkResponse { chunk: Some(chunk) })
    }

    async fn append_chunk(
        &self,
        request: AppendChunkRequest,
    ) -> crowdb_chunk_client::Result<AppendChunkResponse> {
        let mut guard = self.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        if chunk.id != request.chunk_id || chunk.modify_ts != request.modify_ts {
            return Ok(AppendChunkResponse {
                modify_ts: chunk.modify_ts,
                strips: Vec::new(),
                chunk: Some(chunk.clone()),
            });
        }
        let first = chunk.strips.first().unwrap();
        let capacity = request.strip_size * first.unit_kb;
        let offset = chunk.capacity;
        let sequence = chunk.next_strip_sequence;
        let chunk_id = chunk.id.unwrap();
        let segments = (1..=request.copy_count)
            .map(|disk| Segment {
                disk_id: Some(DiskId {
                    high: u64::from(sequence) * 10 + u64::from(disk),
                    low: 0,
                }),
                owner_chunk: Some(chunk_id),
                unit_offset: 0,
                zone_index: 0,
                unit_count: request.strip_size,
                allocation_ts: 1,
            })
            .collect();
        let strip = ChunkStrip {
            chunk_offset: offset,
            strip_sequence: sequence,
            unit_kb: first.unit_kb,
            capacity,
            strip_type: StripType::Mirror as i32,
            strip: Some(Strip::MirrorStrip(MirrorStrip { segments })),
            ..ChunkStrip::default()
        };
        chunk.strips.push(strip.clone());
        chunk.capacity += capacity;
        chunk.next_strip_sequence += 1;
        chunk.modify_ts += 1;
        Ok(AppendChunkResponse {
            modify_ts: chunk.modify_ts,
            strips: vec![strip],
            chunk: None,
        })
    }

    async fn advance_chunk_write(
        &self,
        request: AdvanceChunkWriteRequest,
    ) -> crowdb_chunk_client::Result<AdvanceChunkWriteResponse> {
        let mut guard = self.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        if chunk.id != request.chunk_id
            || chunk.writer_epoch != request.writer_epoch
            || chunk.modify_ts != request.expected_modify_ts
        {
            return Err(IoError::MetadataConflict("stale cursor advance".into()));
        }
        chunk.modify_ts += 1;
        chunk.acknowledged_cursor = request.acknowledged_cursor;
        if self.fail_advance_after_commit.swap(false, Ordering::AcqRel) {
            return Err(IoError::AllocationFailed("injected post-commit timeout".into()));
        }
        Ok(AdvanceChunkWriteResponse {
            chunk: Some(chunk.clone()),
        })
    }

    async fn seal_chunk(&self, request: SealChunkRequest) -> crowdb_chunk_client::Result<SealChunkResponse> {
        let mut guard = self.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        assert_eq!(chunk.id, request.chunk_id);
        chunk.state = ChunkState::Sealed as i32;
        chunk.sealed_length = request.seal_length;
        Ok(SealChunkResponse {
            chunk: Some(chunk.clone()),
        })
    }

    async fn delete_chunk(
        &self,
        request: DeleteChunkRequest,
    ) -> crowdb_chunk_client::Result<DeleteChunkResponse> {
        let mut guard = self.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        assert_eq!(chunk.id, request.chunk_id);
        chunk.state = ChunkState::Deleted as i32;
        Ok(DeleteChunkResponse {
            chunk: Some(chunk.clone()),
        })
    }

    async fn update_chunk_strip(
        &self,
        _request: UpdateChunkStripRequest,
    ) -> crowdb_chunk_client::Result<UpdateChunkStripResponse> {
        unreachable!()
    }

    async fn query_chunk(
        &self,
        request: QueryChunkRequest,
    ) -> crowdb_chunk_client::Result<QueryChunkResponse> {
        if self.fail_query_once.swap(false, Ordering::AcqRel) {
            return Err(IoError::AllocationFailed("injected query outage".into()));
        }
        let chunk = self.chunk.lock().unwrap().clone();
        if chunk.as_ref().and_then(|chunk| chunk.id) != request.chunk_id {
            return Err(IoError::ChunkNotFound("missing test chunk".into()));
        }
        Ok(QueryChunkResponse {
            chunk,
            layout_validity_ms: 60_000,
        })
    }

    async fn allocate_replacement_segment(
        &self,
        request: AllocateReplacementSegmentRequest,
    ) -> crowdb_chunk_client::Result<AllocateReplacementSegmentResponse> {
        let old = request.old_segment.unwrap();
        let replacement = self.replacements.fetch_add(1, Ordering::AcqRel) + 1;
        Ok(AllocateReplacementSegmentResponse {
            segment: Some(Segment {
                disk_id: Some(DiskId {
                    high: 100 + replacement as u64,
                    low: 0,
                }),
                allocation_ts: old.allocation_ts + replacement as u64,
                ..old
            }),
        })
    }

    async fn replace_chunk_strip_range(
        &self,
        request: ReplaceChunkStripRangeRequest,
    ) -> crowdb_chunk_client::Result<ReplaceChunkStripRangeResponse> {
        self.replacement_calls.fetch_add(1, Ordering::AcqRel);
        let mut guard = self.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        let index = request.start_index as usize;
        if chunk.modify_ts != request.expected_modify_ts
            || chunk.strips.get(index) != request.old_strips.first()
        {
            return Err(IoError::MetadataConflict("stale replacement".into()));
        }
        chunk.strips[index] = request.replacement_strips[0].clone();
        chunk.modify_ts += 1;
        chunk.last_strip_replacement = request.operation_id;
        if self.fail_replacement_response_once.swap(false, Ordering::AcqRel) {
            return Err(IoError::WriteFailed(
                "replacement response lost after commit".into(),
            ));
        }
        Ok(ReplaceChunkStripRangeResponse {
            chunk: Some(chunk.clone()),
        })
    }

    async fn discard_replacement_segment(
        &self,
        _request: DiscardReplacementSegmentRequest,
    ) -> crowdb_chunk_client::Result<DiscardReplacementSegmentResponse> {
        Ok(DiscardReplacementSegmentResponse {})
    }
}

#[derive(Default)]
struct Disks {
    bytes: Mutex<HashMap<u64, Vec<u8>>>,
    fsyncs: AtomicUsize,
    fail_disk_once: AtomicU64,
    fail_fsync_once: AtomicU64,
}

#[test]
fn production_runtime_shares_connected_chunk_io_parts() {
    let allocator: Arc<dyn ChunkAllocator> = Arc::new(Allocator::new());
    let disks: Arc<dyn DiskWriter> = Arc::new(Disks::default());
    let chunk_io = ChunkIoClient::from_parts(allocator, disks);
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(vec![
        "http://127.0.0.1:1".into()
    ])));

    let runtime = ProductionStreamRuntime::new(
        kv,
        &chunk_io,
        30_000,
        ChunkReadPolicy::default(),
        StreamConfig::default(),
    )
    .unwrap();
    let _registry = runtime.registry();
    let _chunks = runtime.chunks();
}

#[async_trait]
impl DiskWriter for Disks {
    async fn write(
        &self,
        _segment: &Segment,
        _unit_bytes: u64,
        _data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        unreachable!()
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        _unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        let disk = segment.disk_id.unwrap().high;
        if self
            .fail_disk_once
            .compare_exchange(disk, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Err(IoError::WriteFailed("injected mirror write failure".into()));
        }
        let mut disks = self.bytes.lock().unwrap();
        let bytes = disks.entry(disk).or_default();
        let start = usize::try_from(offset).unwrap();
        bytes.resize(bytes.len().max(start + data.len()), 0);
        bytes[start..start + data.len()].copy_from_slice(&data);
        Ok(())
    }

    async fn fsync(&self, segment: &Segment) -> crowdb_chunk_client::Result<()> {
        let disk = segment.disk_id.unwrap().high;
        if self
            .fail_fsync_once
            .compare_exchange(disk, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Err(IoError::WriteFailed("injected mirror fsync failure".into()));
        }
        self.fsyncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn read(
        &self,
        segment: &Segment,
        _unit_bytes: u64,
        offset: u64,
        length: u32,
    ) -> crowdb_chunk_client::Result<Bytes> {
        let disk = segment.disk_id.unwrap().high;
        let disks = self.bytes.lock().unwrap();
        let bytes = disks
            .get(&disk)
            .ok_or_else(|| IoError::ReadFailed("missing disk".into()))?;
        let start = usize::try_from(offset).unwrap();
        let end = start + usize::try_from(length).unwrap();
        Ok(Bytes::copy_from_slice(&bytes[start..end]))
    }
}

#[tokio::test]
async fn production_store_writes_reads_advances_and_releases_one_mirror_chunk() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let allocator_trait: Arc<dyn ChunkAllocator> = allocator;
    let disk_trait: Arc<dyn DiskWriter> = Arc::clone(&disks) as Arc<dyn DiskWriter>;
    let store =
        ProductionStreamChunkStore::new(allocator_trait, disk_trait, 30_000, ChunkReadPolicy::default())
            .unwrap();
    let name = StreamName { high: 1, low: 2 };
    let active = store.allocate_mirrored(name, 9).await.unwrap();
    let data = Bytes::from_static(b"stream");
    store
        .write_mirrors_with_images(
            name,
            9,
            active.chunk_id,
            0,
            data.clone(),
            &[MirrorStripImage {
                block_offset: 0,
                data: data.clone(),
                full_image: data,
            }],
        )
        .await
        .unwrap();
    assert_eq!(disks.fsyncs.load(Ordering::Relaxed), 3);
    assert_eq!(
        store
            .advance_cursor(name, 9, active.chunk_id, 0, 6, 17)
            .await
            .unwrap(),
        CursorAdvance::Committed
    );
    assert_eq!(
        store.read(active.chunk_id, 0, 6).await.unwrap(),
        Bytes::from_static(b"stream")
    );
    store.seal(active.chunk_id, 9, 6).await.unwrap();
    assert_eq!(
        store
            .release_trimmed(active.chunk_id, 6)
            .await
            .unwrap()
            .reclaimed_bytes,
        6
    );
}

#[tokio::test]
async fn production_store_grows_and_writes_across_mirror_strips() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store =
        ProductionStreamChunkStore::new(allocator, disks, 30_000, ChunkReadPolicy::default()).unwrap();
    let name = StreamName { high: 11, low: 12 };
    let active = store.allocate_mirrored(name, 9).await.unwrap();
    assert_eq!(active.capacity, 1024 * 1024);
    let grown = store
        .grow_mirrored(name, 9, active.chunk_id, active.capacity + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(grown.capacity, 2 * 1024 * 1024);
    let offset = active.capacity - 2;
    assert_eq!(
        store
            .advance_cursor(name, 9, active.chunk_id, 0, offset, 0)
            .await
            .unwrap(),
        CursorAdvance::Committed
    );
    let data = Bytes::from_static(b"split");
    let first = data.slice(..2);
    let second = data.slice(2..);
    let mut prefix = vec![0; usize::try_from(offset).unwrap()];
    prefix.extend_from_slice(&first);
    store
        .write_mirrors_with_images(
            name,
            9,
            active.chunk_id,
            offset,
            data,
            &[
                MirrorStripImage {
                    block_offset: offset,
                    data: first,
                    full_image: Bytes::from(prefix),
                },
                MirrorStripImage {
                    block_offset: 0,
                    data: second.clone(),
                    full_image: second,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .advance_cursor(name, 9, active.chunk_id, offset, offset + 5, 0)
            .await
            .unwrap(),
        CursorAdvance::Committed
    );
    assert_eq!(
        store.read(active.chunk_id, offset, 5).await.unwrap(),
        Bytes::from_static(b"split")
    );
}

#[tokio::test]
async fn chunk_stream_runs_end_to_end_over_the_production_chunk_adapter() {
    let allocator: Arc<dyn ChunkAllocator> = Arc::new(Allocator::new());
    let disks: Arc<dyn DiskWriter> = Arc::new(Disks::default());
    let chunks: Arc<dyn StreamChunkStore> = Arc::new(
        ProductionStreamChunkStore::new(allocator, disks, 30_000, ChunkReadPolicy::default()).unwrap(),
    );
    let metadata = Arc::new(MemoryStreamStore::new(1));
    let registry: Arc<dyn StreamRegistry> = metadata.clone();
    let metadata_store: Arc<dyn StreamMetadataStore> = metadata;
    let stream_name = StreamName { high: 40, low: 41 };
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("test".into()),
        },
        9,
        StreamConfig::default(),
        registry,
        metadata_store,
        chunks,
    )
    .await
    .unwrap();
    let range = stream
        .append_chunk_bound(&[Bytes::from_static(b"frame")])
        .await
        .unwrap();
    assert_eq!((range.begin, range.end), (0, 5));
    assert_eq!(stream.read_at(0, 5).await.unwrap(), Bytes::from_static(b"frame"));
    assert_eq!(range.chunk_id.unwrap().low, 8);
}

#[tokio::test]
async fn stream_replaces_failed_mirrors_from_retained_strip_image() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let chunks: Arc<dyn StreamChunkStore> = Arc::new(
        ProductionStreamChunkStore::new(
            Arc::clone(&allocator) as Arc<dyn ChunkAllocator>,
            Arc::clone(&disks) as Arc<dyn DiskWriter>,
            30_000,
            ChunkReadPolicy::default(),
        )
        .unwrap(),
    );
    let metadata = Arc::new(MemoryStreamStore::new(1));
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name: StreamName { high: 50, low: 51 },
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("test".into()),
        },
        9,
        StreamConfig::default(),
        Arc::clone(&metadata) as Arc<dyn StreamRegistry>,
        metadata as Arc<dyn StreamMetadataStore>,
        chunks,
    )
    .await
    .unwrap();
    stream.append(&[Bytes::from_static(b"first")]).await.unwrap();
    allocator
        .fail_replacement_response_once
        .store(true, Ordering::Release);
    allocator.fail_query_once.store(true, Ordering::Release);
    disks.fail_disk_once.store(1, Ordering::Release);
    stream.append(&[Bytes::from_static(b"second")]).await.unwrap();
    assert_eq!(allocator.replacement_calls.load(Ordering::Acquire), 1);
    disks.fail_fsync_once.store(2, Ordering::Release);
    stream.append(&[Bytes::from_static(b"third")]).await.unwrap();

    assert_eq!(
        stream.read_at(0, 16).await.unwrap(),
        Bytes::from_static(b"firstsecondthird")
    );
    assert_eq!(stream.metrics().rollovers, 0);
    let chunk = allocator.chunk.lock().unwrap().clone().unwrap();
    let Some(Strip::MirrorStrip(mirror)) = &chunk.strips[0].strip else {
        panic!("expected mirror strip");
    };
    assert!(mirror
        .segments
        .iter()
        .any(|segment| segment.disk_id.unwrap().high == 101));
    assert!(mirror
        .segments
        .iter()
        .any(|segment| segment.disk_id.unwrap().high == 102));
    let images = disks.bytes.lock().unwrap();
    assert_eq!(images.get(&102), images.get(&101));
}

#[tokio::test]
async fn stream_repair_preserves_a_batch_crossing_mirror_strips() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let chunks: Arc<dyn StreamChunkStore> = Arc::new(
        ProductionStreamChunkStore::new(
            Arc::clone(&allocator) as Arc<dyn ChunkAllocator>,
            Arc::clone(&disks) as Arc<dyn DiskWriter>,
            30_000,
            ChunkReadPolicy::default(),
        )
        .unwrap(),
    );
    let metadata = Arc::new(MemoryStreamStore::new(1));
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name: StreamName { high: 52, low: 53 },
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("test".into()),
        },
        9,
        StreamConfig::default(),
        Arc::clone(&metadata) as Arc<dyn StreamRegistry>,
        metadata as Arc<dyn StreamMetadataStore>,
        chunks,
    )
    .await
    .unwrap();
    let prefix = Bytes::from(vec![b'a'; 1024 * 1024 - 60]);
    stream.append(std::slice::from_ref(&prefix)).await.unwrap();
    disks.fail_disk_once.store(11, Ordering::Release);
    let suffix = Bytes::from_static(b"crossing-the-boundary");
    stream.append(std::slice::from_ref(&suffix)).await.unwrap();

    let read = stream.read_at(0, prefix.len() + suffix.len()).await.unwrap();
    assert_eq!(&read[..prefix.len()], prefix.as_ref());
    assert_eq!(&read[prefix.len()..], suffix.as_ref());
    assert_eq!(stream.metrics().rollovers, 0);
    let chunk = allocator.chunk.lock().unwrap().clone().unwrap();
    assert_eq!(chunk.strips.len(), 2);
    let Some(Strip::MirrorStrip(mirror)) = &chunk.strips[1].strip else {
        panic!("expected mirror strip");
    };
    assert!(mirror
        .segments
        .iter()
        .any(|segment| segment.disk_id.unwrap().high == 101));
    let images = disks.bytes.lock().unwrap();
    assert_eq!(images.get(&101), images.get(&12));
}

#[tokio::test]
async fn production_store_reconciles_a_post_commit_cursor_timeout() {
    let allocator = Arc::new(Allocator::new());
    let disks: Arc<dyn DiskWriter> = Arc::new(Disks::default());
    let allocator_trait: Arc<dyn ChunkAllocator> = allocator.clone();
    let store =
        ProductionStreamChunkStore::new(allocator_trait, disks, 30_000, ChunkReadPolicy::default()).unwrap();
    let name = StreamName { high: 3, low: 4 };
    let active = store.allocate_mirrored(name, 9).await.unwrap();
    let data = Bytes::from_static(b"once");
    store
        .write_mirrors_with_images(
            name,
            9,
            active.chunk_id,
            0,
            data.clone(),
            &[MirrorStripImage {
                block_offset: 0,
                data: data.clone(),
                full_image: data,
            }],
        )
        .await
        .unwrap();
    allocator.fail_advance_after_commit.store(true, Ordering::Release);
    assert_eq!(
        store
            .advance_cursor(name, 9, active.chunk_id, 0, 4, 19)
            .await
            .unwrap(),
        CursorAdvance::Committed
    );
    assert_eq!(store.durable_cursor(active.chunk_id, 9).await.unwrap().offset, 4);
}
