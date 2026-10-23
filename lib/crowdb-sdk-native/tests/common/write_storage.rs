// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crowdb_chunk_client::{ChunkAllocator, ChunkIoClient, DiskWriter, Result};
use crowdb_chunk_kv_client::{ChunkKvClient, ChunkKvRpcTransport, ChunkKvRpcTransportConfig};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::*;
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_rpc_ffi::{ProducerBufferAllocator, RpcServer};
use crowdb_sdk_native::runtime::RuntimeClients;
use crowdb_sdk_native::{ClientRuntime, RuntimeLimits};

type TestDiskAddress = (u64, u64, u64);

pub struct TestWriteStorage {
    chunks: SkipMap<(u64, u64), ArcSwap<Chunk>>,
    pub data: SkipMap<TestDiskAddress, Bytes>,
    pub writes: AtomicUsize,
    pub deletes: AtomicUsize,
    pub fail_write: AtomicBool,
    pub block_write: AtomicBool,
    pub write_started: tokio::sync::Notify,
    pub write_allowed: tokio::sync::Notify,
    producer: ProducerBufferAllocator,
}

impl TestWriteStorage {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            chunks: SkipMap::new(),
            data: SkipMap::new(),
            writes: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            fail_write: AtomicBool::new(false),
            block_write: AtomicBool::new(false),
            write_started: tokio::sync::Notify::new(),
            write_allowed: tokio::sync::Notify::new(),
            producer: ProducerBufferAllocator::new(Arc::new(RpcServer::new(None)), false),
        })
    }

    fn chunk(&self, id: ChunkId) -> Chunk {
        self.chunks
            .get(&(id.high, id.low))
            .unwrap()
            .value()
            .load_full()
            .as_ref()
            .clone()
    }

    fn update(&self, id: ChunkId, mutate: impl Fn(&mut Chunk)) -> Chunk {
        let entry = self.chunks.get(&(id.high, id.low)).unwrap();
        entry.value().rcu(|previous| {
            let mut next = previous.as_ref().clone();
            mutate(&mut next);
            next.modify_ts += 1;
            next
        });
        self.chunk(id)
    }

    pub fn runtime(self: &Arc<Self>, metadata: Arc<ChunkKvClient>) -> ClientRuntime {
        ClientRuntime::from_clients(
            RuntimeClients {
                control: Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new()))),
                metadata,
                metadata_transport: Arc::new(
                    ChunkKvRpcTransport::with_config(8, 1, 1, &ChunkKvRpcTransportConfig::default()).unwrap(),
                ),
                chunks: Arc::new(ChunkIoClient::from_parts(
                    self.clone(),
                    self.clone(),
                    ChunkType::S3,
                )),
            },
            RuntimeLimits::default(),
        )
        .unwrap()
    }
}

fn strip(id: ChunkId, index: u32, size: u32, copies: u32) -> ChunkStrip {
    ChunkStrip {
        unit_kb: size,
        capacity: size,
        chunk_offset: index * size,
        strip_sequence: index,
        strip_type: StripType::Mirror as i32,
        strip: Some(Strip::MirrorStrip(MirrorStrip {
            segments: (0..copies)
                .map(|copy| Segment {
                    disk_id: Some(DiskId {
                        high: id.high ^ u64::from(copy),
                        low: id.low,
                    }),
                    unit_offset: u64::from(index),
                    unit_count: 1,
                    ..Segment::default()
                })
                .collect(),
        })),
        ..ChunkStrip::default()
    }
}

#[async_trait]
impl ChunkAllocator for TestWriteStorage {
    async fn allocate_chunk(&self, request: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        assert_eq!(request.strip_type, StripType::Mirror as i32);
        let id = request.chunk_id.unwrap();
        let chunk = Chunk {
            id: Some(id),
            modify_ts: 1,
            state: ChunkState::Active as i32,
            capacity: request.write_granularity * request.strip_count,
            chunk_type: request.chunk_type,
            strips: (0..request.strip_count)
                .map(|index| strip(id, index, request.write_granularity, request.copy_count))
                .collect(),
            ..Chunk::default()
        };
        self.chunks
            .insert((id.high, id.low), ArcSwap::from_pointee(chunk.clone()));
        Ok(AllocateChunkResponse { chunk: Some(chunk) })
    }
    async fn append_chunk(&self, request: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let id = request.chunk_id.unwrap();
        let previous = self.chunk(id);
        if previous.modify_ts != request.modify_ts {
            return Ok(AppendChunkResponse {
                modify_ts: previous.modify_ts,
                chunk: Some(previous),
                strips: vec![],
            });
        }
        let start = u32::try_from(previous.strips.len()).unwrap();
        let size = previous.strips[0].unit_kb * request.strip_size;
        let strips: Vec<_> = (0..request.strip_count)
            .map(|index| strip(id, start + index, size, request.copy_count))
            .collect();
        let chunk = self.update(id, |chunk| {
            chunk.capacity += size * request.strip_count;
            chunk.strips.extend(strips.clone());
        });
        Ok(AppendChunkResponse {
            modify_ts: chunk.modify_ts,
            chunk: None,
            strips,
        })
    }
    async fn seal_chunk(&self, request: SealChunkRequest) -> Result<SealChunkResponse> {
        let chunk = self.update(request.chunk_id.unwrap(), |chunk| {
            chunk.state = ChunkState::Sealed as i32;
            chunk.sealed_length = request.seal_length;
            chunk.acknowledged_cursor = request.seal_bytes;
        });
        Ok(SealChunkResponse { chunk: Some(chunk) })
    }
    async fn delete_chunk(&self, request: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        self.deletes.fetch_add(1, Ordering::AcqRel);
        let chunk = self.update(request.chunk_id.unwrap(), |chunk| {
            chunk.state = ChunkState::Deleted as i32;
        });
        Ok(DeleteChunkResponse { chunk: Some(chunk) })
    }
    async fn update_chunk_strip(&self, request: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        let chunk = self.update(request.chunk_id.unwrap(), |chunk| {
            chunk.strips[request.strip_index as usize] = request.strip.clone().unwrap();
        });
        Ok(UpdateChunkStripResponse { chunk: Some(chunk) })
    }
    async fn query_chunk(&self, request: QueryChunkRequest) -> Result<QueryChunkResponse> {
        Ok(QueryChunkResponse {
            chunk: Some(self.chunk(request.chunk_id.unwrap())),
            layout_validity_ms: 60_000,
        })
    }
}

#[async_trait]
impl DiskWriter for TestWriteStorage {
    fn producer_allocator(&self) -> Option<ProducerBufferAllocator> {
        Some(self.producer.clone())
    }

    async fn write(&self, segment: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        self.write_views_at_byte_offset(segment, unit_bytes, 0, vec![data])
            .await
    }
    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> Result<()> {
        self.write_views_at_byte_offset(segment, unit_bytes, offset, vec![data])
            .await
    }
    async fn write_views_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Vec<Bytes>,
    ) -> Result<()> {
        self.writes.fetch_add(1, Ordering::AcqRel);
        self.write_started.notify_one();
        if self.block_write.load(Ordering::Acquire) {
            self.write_allowed.notified().await;
        }
        if self.fail_write.load(Ordering::Acquire) {
            return Err(crowdb_chunk_client::IoError::WriteFailed(
                "injected disk failure".into(),
            ));
        }
        let disk = segment.disk_id.unwrap();
        let mut offset = segment.unit_offset * unit_bytes + offset;
        for view in data {
            let count = view.len() as u64;
            self.data.insert((disk.high, disk.low, offset), view);
            offset += count;
        }
        Ok(())
    }
    async fn read(&self, segment: &Segment, unit_bytes: u64, offset: u64, length: u32) -> Result<Bytes> {
        let disk = segment.disk_id.unwrap();
        let start = segment.unit_offset * unit_bytes + offset;
        let end = start + u64::from(length);
        let mut output = vec![0; length as usize];
        for entry in &self.data {
            let (high, low, offset) = *entry.key();
            if (high, low) != (disk.high, disk.low) {
                continue;
            }
            let from = start.max(offset);
            let to = end.min(offset + entry.value().len() as u64);
            if from < to {
                output[usize::try_from(from - start).unwrap()..usize::try_from(to - start).unwrap()]
                    .copy_from_slice(
                        &entry.value()
                            [usize::try_from(from - offset).unwrap()..usize::try_from(to - offset).unwrap()],
                    );
            }
        }
        Ok(Bytes::from(output))
    }
}
