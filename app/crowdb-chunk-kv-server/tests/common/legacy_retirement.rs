// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkAllocator, DiskWriter, IoError, Result};
use crowdb_protocol::{
    chunkdb::rpc::*,
    common::{ChunkId, DiskId},
    diskdb::rpc::Segment,
};
use crowdb_tree_ffi::chunk::legacy::LegacyManifest;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
    time::Instant,
};

pub fn manifest() -> &'static [u8] {
    let fixture = include_bytes!("../../../../lib/crowdb-tree/tests/data/legacy-chunk-v7.bin");
    let length = usize::try_from(u64::from_le_bytes(fixture[8..16].try_into().unwrap())).unwrap();
    &fixture[16..16 + length]
}
pub fn typed_root() -> Vec<u8> {
    let mut bytes = vec![0; 132];
    for (index, value) in [
        (0, 0x3354_4F4F_524B_4843_u64),
        (1, 1),
        (2, 42),
        (3, 2),
        (4, 7),
        (5, 1),
        (6, 1),
        (9, u64::MAX),
    ] {
        bytes[index * 8..index * 8 + 8].copy_from_slice(&value.to_le_bytes());
    }
    let crc = crowdb_tree_ffi::crc32c(&bytes[..128]);
    bytes[128..].copy_from_slice(&crc.to_le_bytes());
    bytes
}
#[derive(Default)]
pub struct TestPhysical {
    chunks: Mutex<HashMap<(u64, u64), Chunk>>,
    pub deletes: AtomicUsize,
    pub drains: AtomicUsize,
    pub lose_reply: AtomicBool,
}
impl TestPhysical {
    pub fn new() -> Self {
        let mut chunks = HashMap::new();
        for pack in LegacyManifest::decode(manifest()).unwrap().packs() {
            let capacity =
                u32::try_from((pack.offset + u64::from(pack.length)).div_ceil(1024) * 1024).unwrap();
            let id = ChunkId {
                high: pack.chunk_high,
                low: pack.chunk_low,
            };
            let chunk = chunks.entry((id.high, id.low)).or_insert_with(|| Chunk {
                id: Some(id),
                create_ts_ms: 10,
                modify_ts: 1,
                chunk_type: i32::try_from(id.high >> 56).unwrap(),
                state: ChunkState::Sealed as i32,
                ..Chunk::default()
            });
            chunk.capacity = chunk.capacity.max(capacity);
        }
        for chunk in chunks.values_mut() {
            chunk.strips.push(ChunkStrip {
                unit_kb: 1,
                capacity: chunk.capacity,
                strip_type: StripType::Mirror as i32,
                strip: Some(Strip::MirrorStrip(MirrorStrip {
                    segments: vec![Segment {
                        disk_id: Some(DiskId { high: 1, low: 2 }),
                        zone_index: 0,
                        unit_offset: 0,
                        unit_count: chunk.capacity / 1024,
                        ..Segment::default()
                    }],
                })),
                ..ChunkStrip::default()
            });
        }
        Self {
            chunks: Mutex::new(chunks),
            ..Self::default()
        }
    }
    pub fn set_physical_layout(&self, unit_kb: u32, unit_count: u32) {
        for chunk in self.chunks.lock().unwrap().values_mut() {
            for strip in &mut chunk.strips {
                strip.unit_kb = unit_kb;
                let Some(Strip::MirrorStrip(mirror)) = &mut strip.strip else {
                    panic!("fixture requires mirror strips");
                };
                for segment in &mut mirror.segments {
                    segment.unit_count = unit_count;
                }
            }
        }
    }
    pub fn count(&self) -> usize {
        self.chunks.lock().unwrap().len()
    }
    pub fn physical_bytes(&self) -> u64 {
        self.chunks
            .lock()
            .unwrap()
            .values()
            .map(|chunk| u64::from(chunk.capacity))
            .sum()
    }
    pub fn all_deleted(&self) -> bool {
        self.chunks.lock().unwrap().values().all(|chunk| {
            chunk.state == ChunkState::Deleted as i32 && chunk.capacity == 0 && chunk.strips.is_empty()
        })
    }
}
#[async_trait]
impl ChunkAllocator for TestPhysical {
    async fn allocate_chunk(&self, _: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        Err(IoError::Unsupported("fixture allocation".into()))
    }
    async fn append_chunk(&self, _: AppendChunkRequest) -> Result<AppendChunkResponse> {
        Err(IoError::Unsupported("fixture append".into()))
    }
    async fn seal_chunk(&self, _: SealChunkRequest) -> Result<SealChunkResponse> {
        Err(IoError::Unsupported("fixture seal".into()))
    }
    async fn query_chunk(&self, request: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let id = request.chunk_id.unwrap();
        let chunk = self
            .chunks
            .lock()
            .unwrap()
            .get(&(id.high, id.low))
            .cloned()
            .ok_or_else(|| IoError::ChunkNotFound("fixture absent".into()))?;
        Ok(QueryChunkResponse {
            chunk: Some(chunk),
            ..QueryChunkResponse::default()
        })
    }
    async fn delete_chunk(&self, request: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let id = request.chunk_id.unwrap();
        let mut chunks = self.chunks.lock().unwrap();
        let chunk = chunks.get_mut(&(id.high, id.low)).unwrap();
        assert!(self.drains.load(Ordering::Relaxed) > self.deletes.load(Ordering::Relaxed));
        self.deletes.fetch_add(1, Ordering::Relaxed);
        chunk.state = ChunkState::Deleted as i32;
        chunk.capacity = 0;
        chunk.strips.clear();
        if self.lose_reply.swap(false, Ordering::Relaxed) {
            return Err(IoError::WriteFailed("delete committed but reply lost".into()));
        }
        Ok(DeleteChunkResponse {
            chunk: Some(chunk.clone()),
        })
    }
}
#[async_trait]
impl DiskWriter for TestPhysical {
    async fn write(&self, _: &Segment, _: u64, _: Bytes) -> Result<()> {
        Err(IoError::Unsupported("fixture write".into()))
    }
    async fn write_at_byte_offset(&self, _: &Segment, _: u64, _: u64, _: Bytes) -> Result<()> {
        Err(IoError::Unsupported("fixture write".into()))
    }
    async fn drain_segment(&self, _: &Segment, _: u64, deadline: Instant) -> Result<()> {
        assert!(deadline > Instant::now());
        self.drains.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

use crowdb_paxos_kv_client::{
    PaxosKvClient, StreamProcessAuthority, StreamProcessLeaseClient, StreamProcessLeaseTiming,
};
use crowdb_protocol::{
    chunk_kv::{
        ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor,
        PartitionArtifact,
    },
    chunk_stream::{StreamName, StreamPurpose},
    key::{StreamProcessLeaseKey, TextKey},
};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
pub fn entry(epoch: u64) -> ChunkKvRangeCatalogEntry {
    ChunkKvRangeCatalogEntry {
        partition_id: Id128 { high: 1, low: 1 },
        range: KeyRange {
            start: vec![],
            end: None,
        },
        owner: OwnerDescriptor {
            instance_id: epoch,
            rpc_endpoint: "127.0.0.1:1".into(),
        },
        owner_epoch: epoch,
        state: ChunkKvRangeCatalogPartitionState::Serving,
        artifact: PartitionArtifact {
            tree_id: 42,
            stream_name: StreamName { high: 1, low: 2 },
            tail_overlay: None,
        },
        transition_id: None,
    }
}
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
pub async fn process(kv: &Arc<PaxosKvClient>, instance: u64) -> Arc<StreamProcessAuthority> {
    let now = now();
    let lease = StreamProcessLeaseClient::new(kv.clone())
        .claim(StreamPurpose::Wal, instance, [1; 16], None, now, 300_000)
        .await
        .unwrap();
    Arc::new(
        StreamProcessAuthority::new(lease, StreamProcessLeaseTiming::default(), now, Instant::now()).unwrap(),
    )
}
pub async fn expire(kv: &Arc<PaxosKvClient>, process: &StreamProcessAuthority) {
    process.fence();
    let mut lease = process.status(Instant::now()).lease;
    lease.expires_at_ms = now() - 60_000;
    let key = StreamProcessLeaseKey {
        purpose: StreamPurpose::Wal,
        instance_id: lease.identity.instance_id,
    }
    .to_path();
    kv.put(0, 0, key.as_bytes(), &serde_json::to_vec(&lease).unwrap(), None)
        .await
        .unwrap();
}

pub fn foreign_manifest(tree: u64) -> Vec<u8> {
    let mut bytes = manifest().to_vec();
    bytes[4..12].copy_from_slice(&tree.to_le_bytes());
    let segments = usize::try_from(u64::from_le_bytes(bytes[72..80].try_into().unwrap())).unwrap();
    let packs = 80 + segments * 33 + 8;
    let crc = crowdb_tree_ffi::crc32c_update(crowdb_tree_ffi::crc32c(&bytes[..68]), &bytes[packs..]);
    let crc = crowdb_tree_ffi::crc32c_update(crc, &bytes[80..packs - 8]);
    bytes[68..72].copy_from_slice(&crc.to_le_bytes());
    LegacyManifest::decode(&bytes).unwrap();
    bytes
}
pub fn foreign_typed_root(tree: u64) -> Vec<u8> {
    let mut bytes = typed_root();
    bytes[16..24].copy_from_slice(&tree.to_le_bytes());
    let crc = crowdb_tree_ffi::crc32c(&bytes[..128]);
    bytes[128..].copy_from_slice(&crc.to_le_bytes());
    bytes
}
