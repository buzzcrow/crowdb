// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Focused strip fallback and recovery tests; full flows live in reader E2E.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkAllocator, ChunkReadPolicy, ChunkReader, DiskWriter, IoError, ReadError, Result, StripReader,
};
use crowdb_common::ec::{encode, EcScheme};
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, Chunk, ChunkState,
    ChunkStrip, DeleteChunkRequest, DeleteChunkResponse, EcState, EcStrip, Location, MirrorStrip,
    QueryChunkRequest, QueryChunkResponse, SealChunkRequest, SealChunkResponse, Strip, StripType,
    UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use tokio::sync::Semaphore;

const KIB: usize = 1024;
const SHARD: usize = 64 * KIB;

struct MemoryDiskIo {
    shards: Vec<(DiskId, Bytes)>,
    failed: Vec<DiskId>,
    reads: Arc<AtomicUsize>,
}

struct SequenceAllocator {
    queries: AtomicUsize,
    first: Chunk,
    current: Chunk,
}

#[async_trait]
impl ChunkAllocator for SequenceAllocator {
    async fn allocate_chunk(&self, _request: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        unreachable!("reader test does not allocate")
    }

    async fn append_chunk(&self, _request: AppendChunkRequest) -> Result<AppendChunkResponse> {
        unreachable!("reader test does not append")
    }

    async fn seal_chunk(&self, _request: SealChunkRequest) -> Result<SealChunkResponse> {
        unreachable!("reader test does not seal")
    }

    async fn delete_chunk(&self, _request: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        unreachable!("reader test does not delete")
    }

    async fn update_chunk_strip(
        &self,
        _request: UpdateChunkStripRequest,
    ) -> Result<UpdateChunkStripResponse> {
        unreachable!("reader test does not update")
    }

    async fn query_chunk(&self, _request: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let query = self.queries.fetch_add(1, Ordering::AcqRel);
        Ok(QueryChunkResponse {
            chunk: Some(if query == 0 {
                self.first.clone()
            } else {
                self.current.clone()
            }),
            layout_validity_ms: if query == 0 { 1 } else { 1_000 },
        })
    }
}

#[async_trait]
impl DiskWriter for MemoryDiskIo {
    async fn write(&self, _segment: &Segment, _unit_bytes: u64, _data: Bytes) -> Result<()> {
        unreachable!("reader test does not write")
    }

    async fn read(
        &self,
        segment: &Segment,
        _unit_bytes: u64,
        segment_offset: u64,
        length: u32,
    ) -> Result<Bytes> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let disk_id = segment.disk_id.unwrap();
        if self.failed.contains(&disk_id) {
            return Err(IoError::ReadFailed("injected read failure".into()));
        }
        let data = self
            .shards
            .iter()
            .find_map(|(id, data)| (*id == disk_id).then_some(data))
            .ok_or_else(|| IoError::ReadFailed("missing test shard".into()))?;
        tokio::time::sleep(Duration::from_millis(3)).await;
        let start = usize::try_from(segment_offset).unwrap();
        Ok(data.slice(start..start + length as usize))
    }
}

fn segment(index: u64) -> Segment {
    Segment {
        disk_id: Some(DiskId { high: 0, low: index }),
        unit_count: 1,
        ..Segment::default()
    }
}

fn reader(shards: Vec<(DiskId, Bytes)>, failed: Vec<DiskId>) -> StripReader {
    reader_with_count(shards, failed).0
}

fn reader_with_count(shards: Vec<(DiskId, Bytes)>, failed: Vec<DiskId>) -> (StripReader, Arc<AtomicUsize>) {
    let reads = Arc::new(AtomicUsize::new(0));
    let reader = StripReader::new(
        Arc::new(MemoryDiskIo {
            shards,
            failed,
            reads: Arc::clone(&reads),
        }),
        Arc::new(Semaphore::new(8 * 1024 * 1024)),
        8 * 1024 * 1024,
    );
    (reader, reads)
}

#[tokio::test]
async fn mirror_reader_falls_back_in_metadata_order() {
    let first = segment(1);
    let second = segment(2);
    let data = Bytes::from(vec![0x5a; SHARD]);
    let strip = ChunkStrip {
        unit_kb: 64,
        capacity: 64,
        sealed_length: 64,
        sealed_ts_ms: 1,
        strip_type: StripType::Mirror as i32,
        strip: Some(Strip::MirrorStrip(MirrorStrip {
            segments: vec![first, second],
        })),
        ..ChunkStrip::default()
    };
    let disk_io = reader(
        vec![
            (first.disk_id.unwrap(), data.clone()),
            (second.disk_id.unwrap(), data.clone()),
        ],
        vec![first.disk_id.unwrap()],
    );
    assert_eq!(
        disk_io.read(&strip, SHARD as u64, 73, 4096).await.unwrap(),
        data.slice(73..4169)
    );
}

#[tokio::test]
async fn ec_reader_recovers_partial_slice_and_rejects_excess_loss() {
    let scheme = EcScheme::new(4, 1);
    let data: Vec<u8> = (0..4 * SHARD)
        .map(|index| u8::try_from((index * 13 + 7) % 251).unwrap())
        .collect();
    let encoded = encode(scheme, &data).unwrap();
    let segments: Vec<_> = (1..=5).map(segment).collect();
    let shards = segments
        .iter()
        .zip(encoded)
        .map(|(segment, bytes)| (segment.disk_id.unwrap(), Bytes::from(bytes)))
        .collect::<Vec<_>>();
    let strip = ChunkStrip {
        unit_kb: 64,
        capacity: 256,
        sealed_length: 256,
        sealed_ts_ms: 1,
        strip_type: StripType::Ec as i32,
        strip: Some(Strip::EcStrip(EcStrip {
            data_num: 4,
            code_num: 1,
            ec_state: EcState::Parity as i32,
            segments: segments.clone(),
        })),
        ..ChunkStrip::default()
    };
    let offset = SHARD as u64 + 113;
    let offset_index = usize::try_from(offset).unwrap();
    let recovered = reader(shards.clone(), vec![segments[1].disk_id.unwrap()]);
    assert_eq!(
        recovered
            .read(&strip, 4 * SHARD as u64, offset, 8192)
            .await
            .unwrap(),
        data[offset_index..offset_index + 8192]
    );
    let lost = reader(
        shards.clone(),
        vec![segments[0].disk_id.unwrap(), segments[1].disk_id.unwrap()],
    );
    assert!(matches!(
        lost.read(&strip, 4 * SHARD as u64, 0, 8192).await,
        Err(ReadError::DataLoss(_))
    ));

    let mut degraded = strip.clone();
    degraded.unavailable_segments.push(segments[4]);
    let direct = reader(shards.clone(), Vec::new());
    assert_eq!(
        direct
            .read(&degraded, 4 * SHARD as u64, 2 * SHARD as u64 + 17, 4096)
            .await
            .unwrap(),
        data[2 * SHARD + 17..2 * SHARD + 17 + 4096]
    );
    let mut no_parity = strip.clone();
    let Some(Strip::EcStrip(ec)) = no_parity.strip.as_mut() else {
        unreachable!();
    };
    ec.ec_state = EcState::NoParity as i32;
    assert_eq!(
        reader(shards.clone(), Vec::new())
            .read(&no_parity, 4 * SHARD as u64, SHARD as u64 + 31, 4096)
            .await
            .unwrap(),
        data[SHARD + 31..SHARD + 31 + 4096]
    );
    let no_parity_failure = reader(shards.clone(), vec![segments[2].disk_id.unwrap()]);
    assert!(matches!(
        no_parity_failure
            .read(&no_parity, 4 * SHARD as u64, 2 * SHARD as u64, 4096)
            .await,
        Err(ReadError::DataLoss(_))
    ));

    let degraded_loss = reader(shards, vec![segments[2].disk_id.unwrap()]);
    assert!(matches!(
        degraded_loss
            .read(&degraded, 4 * SHARD as u64, 2 * SHARD as u64, 4096)
            .await,
        Err(ReadError::DataLoss(_))
    ));
}

#[tokio::test]
async fn ec_recovery_reads_only_the_minimum_surviving_shards() {
    let scheme = EcScheme::new(4, 2);
    let data: Vec<u8> = (0..4 * SHARD)
        .map(|index| u8::try_from((index * 17 + 11) % 251).unwrap())
        .collect();
    let encoded = encode(scheme, &data).unwrap();
    let segments: Vec<_> = (1..=6).map(segment).collect();
    let shards = segments
        .iter()
        .zip(encoded)
        .map(|(segment, bytes)| (segment.disk_id.unwrap(), Bytes::from(bytes)))
        .collect();
    let strip = ChunkStrip {
        unit_kb: 64,
        capacity: 256,
        sealed_length: 256,
        sealed_ts_ms: 1,
        strip_type: StripType::Ec as i32,
        strip: Some(Strip::EcStrip(EcStrip {
            data_num: 4,
            code_num: 2,
            ec_state: EcState::Parity as i32,
            segments: segments.clone(),
        })),
        ..ChunkStrip::default()
    };
    let (reader, reads) = reader_with_count(shards, vec![segments[0].disk_id.unwrap()]);
    assert_eq!(
        reader
            .read(&strip, data.len() as u64, 97, 16 * KIB as u64)
            .await
            .unwrap(),
        data[97..97 + 16 * KIB]
    );
    // One failed direct read plus exactly data_num recovery reads. The second
    // parity shard is not touched.
    assert_eq!(reads.load(Ordering::Relaxed), 5);
}

#[tokio::test]
async fn object_reader_discards_bytes_from_an_expired_layout() {
    let id = ChunkId { high: 7, low: 11 };
    let old_segment = segment(1);
    let new_segment = segment(2);
    let make_chunk = |segment| Chunk {
        id: Some(id),
        state: ChunkState::Sealed as i32,
        capacity: 1,
        sealed_length: 1,
        strips: vec![ChunkStrip {
            unit_kb: 1,
            capacity: 1,
            sealed_length: 1,
            sealed_ts_ms: 1,
            strip_type: StripType::Mirror as i32,
            strip: Some(Strip::MirrorStrip(MirrorStrip {
                segments: vec![segment],
            })),
            ..ChunkStrip::default()
        }],
        ..Chunk::default()
    };
    let allocator = Arc::new(SequenceAllocator {
        queries: AtomicUsize::new(0),
        first: make_chunk(old_segment),
        current: make_chunk(new_segment),
    });
    let disk_io = Arc::new(MemoryDiskIo {
        shards: vec![
            (old_segment.disk_id.unwrap(), Bytes::from_static(b"old!")),
            (new_segment.disk_id.unwrap(), Bytes::from_static(b"new!")),
        ],
        failed: Vec::new(),
        reads: Arc::new(AtomicUsize::new(0)),
    });
    let reader = ChunkReader::new(
        allocator.clone(),
        disk_io,
        ChunkReadPolicy {
            layout_safety_margin: Duration::ZERO,
            ..ChunkReadPolicy::default()
        },
    )
    .unwrap();
    let location = Location {
        chunk_id: Some(id),
        length: 4,
        logical_length: 4,
        ..Location::default()
    };
    assert_eq!(reader.read_object(&[location]).await.unwrap(), b"new!".as_slice());
    assert_eq!(allocator.queries.load(Ordering::Acquire), 2);
}
