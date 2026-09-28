// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Focused strip fallback and recovery tests; full flows live in reader E2E.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkAllocator, ChunkReadPolicy, ChunkReader, DiskWriter, IoError, ReadError, Result, StripReader,
};
use crowdb_common::ec::{encode, EcScheme};
use crowdb_protocol::chunkdb::rpc::{
    AdHocEcRecoveryDisposition, AdHocEcRecoveryRequest, AdHocEcRecoveryResponse, AllocateChunkRequest,
    AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, Chunk, ChunkState, ChunkStrip,
    DeleteChunkRequest, DeleteChunkResponse, EcState, EcStrip, Location, MirrorStrip, QueryChunkRequest,
    QueryChunkResponse, ReplaceChunkStripRangeRequest, ReplaceChunkStripRangeResponse, SealChunkRequest,
    SealChunkResponse, Strip, StripType, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::{encode_frame, FrameMagic, MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES};
use tokio::sync::{Notify, Semaphore};

const KIB: usize = 1024;
const SHARD: usize = 64 * KIB;

struct MemoryDiskIo {
    shards: Vec<(DiskId, Bytes)>,
    failed: Vec<DiskId>,
    reads: Arc<AtomicUsize>,
}

struct BlockingDiskIo {
    inner: MemoryDiskIo,
    release_first: Arc<Notify>,
    started: Arc<AtomicUsize>,
}

#[async_trait]
impl DiskWriter for BlockingDiskIo {
    async fn write(&self, _seg: &Segment, _unit_bytes: u64, _data: Bytes) -> Result<()> {
        unreachable!("reader test does not write")
    }

    async fn write_at_byte_offset(
        &self,
        _seg: &Segment,
        _unit_bytes: u64,
        _byte_offset: u64,
        _data: Bytes,
    ) -> Result<()> {
        unreachable!("reader test does not write")
    }

    async fn read(&self, segment: &Segment, unit_bytes: u64, offset: u64, length: u32) -> Result<Bytes> {
        self.started.fetch_add(1, Ordering::AcqRel);
        if offset == 0 {
            self.release_first.notified().await;
        }
        self.inner.read(segment, unit_bytes, offset, length).await
    }
}

struct SequenceAllocator {
    queries: AtomicUsize,
    first: Chunk,
    current: Mutex<Chunk>,
    first_layout_validity_ms: u64,
    full_reply: Option<Bytes>,
    full_calls: AtomicUsize,
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
                self.current.lock().unwrap().clone()
            }),
            layout_validity_ms: if query == 0 {
                self.first_layout_validity_ms
            } else {
                1_000
            },
        })
    }

    async fn ad_hoc_ec_recovery(&self, request: AdHocEcRecoveryRequest) -> Result<AdHocEcRecoveryResponse> {
        if request.request_full_block {
            self.full_calls.fetch_add(1, Ordering::AcqRel);
            tokio::time::sleep(Duration::from_millis(20)).await;
            return Ok(AdHocEcRecoveryResponse {
                disposition: AdHocEcRecoveryDisposition::Started,
                data: self.full_reply.as_ref().expect("full reply configured").to_vec(),
            });
        }
        let mut current = self.current.lock().unwrap();
        let strip = current
            .strips
            .iter_mut()
            .find(|strip| strip.strip_sequence == request.strip_sequence)
            .unwrap();
        let segment = request.failed_segment.unwrap();
        if !strip.unavailable_segments.contains(&segment) {
            strip.unavailable_segments.push(segment);
        }
        current.modify_ts = current.modify_ts.saturating_add(1);
        Ok(AdHocEcRecoveryResponse {
            disposition: AdHocEcRecoveryDisposition::Marked,
            data: Vec::new(),
        })
    }

    async fn replace_chunk_strip_range(
        &self,
        request: ReplaceChunkStripRangeRequest,
    ) -> Result<ReplaceChunkStripRangeResponse> {
        let mut current = self.current.lock().unwrap();
        let index = usize::try_from(request.start_index).unwrap();
        current.strips[index] = request.replacement_strips[0].clone();
        current.modify_ts = current.modify_ts.saturating_add(1);
        Ok(ReplaceChunkStripRangeResponse {
            chunk: Some(current.clone()),
        })
    }
}

#[async_trait]
impl DiskWriter for MemoryDiskIo {
    async fn write(&self, _segment: &Segment, _unit_bytes: u64, _data: Bytes) -> Result<()> {
        unreachable!("reader test does not write")
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
    // A connected failed target is tried at most three times, followed by
    // exactly data_num recovery reads. The second parity shard is not touched.
    assert_eq!(reads.load(Ordering::Relaxed), 7);
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
        current: Mutex::new(make_chunk(new_segment)),
        first_layout_validity_ms: 1,
        full_reply: None,
        full_calls: AtomicUsize::new(0),
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
    assert_eq!(
        reader.read_object(&[location]).await.unwrap().concat(),
        b"new!".as_slice()
    );
    assert_eq!(allocator.queries.load(Ordering::Acquire), 2);
    assert_eq!(reader.flow_metrics_snapshot().location_normalizations, 1);
}

#[tokio::test]
async fn framed_read_stream_keeps_whole_frame_under_small_window_setting() {
    let id = ChunkId { high: 13, low: 37 };
    let segment = segment(1);
    let payload = b"abcdefghij";
    let frame = Bytes::from(encode_frame(FrameMagic::RepoLargeV1, id, payload, 1).unwrap());
    let chunk = Chunk {
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
        first: chunk.clone(),
        current: Mutex::new(chunk),
        first_layout_validity_ms: 1_000,
        full_reply: None,
        full_calls: AtomicUsize::new(0),
    });
    let disk_io = Arc::new(MemoryDiskIo {
        shards: vec![(segment.disk_id.unwrap(), frame.clone())],
        failed: Vec::new(),
        reads: Arc::new(AtomicUsize::new(0)),
    });
    let reader = ChunkReader::new(
        allocator.clone(),
        disk_io,
        ChunkReadPolicy {
            stream_window_bytes: 4,
            ..ChunkReadPolicy::default()
        },
    )
    .unwrap();
    let location = Location {
        chunk_id: Some(id),
        length: frame.len() as u64,
        logical_length: payload.len() as u64,
        ..Location::default()
    };
    let mut stream = reader.read_stream(&[location]).unwrap();
    assert_eq!(stream.next_chunk().await.unwrap().unwrap(), payload.as_slice());
    assert!(stream.next_chunk().await.is_none());
    assert_eq!(allocator.queries.load(Ordering::Relaxed), 1);
    let metrics = reader.flow_metrics_snapshot();
    assert_eq!(metrics.location_normalizations, 1);
    assert_eq!(metrics.locations_examined, 1);
    assert_eq!(metrics.range_locations_examined, 1);
    assert_eq!(metrics.stream_windows, 1);
    assert_eq!(metrics.layout_queries, 1);
}

#[tokio::test]
async fn read_stream_selects_only_locations_overlapping_each_window() {
    let id = ChunkId { high: 19, low: 41 };
    let segment = segment(1);
    let mut physical = Vec::new();
    let mut locations = Vec::new();
    for (index, payload) in [b"abcd", b"efgh", b"ijkl"].into_iter().enumerate() {
        let frame = encode_frame(FrameMagic::RepoLargeV1, id, payload, 1).unwrap();
        locations.push(Location {
            chunk_id: Some(id),
            offset: physical.len() as u64,
            length: frame.len() as u64,
            logical_offset: (index * 4) as u64,
            logical_length: 4,
        });
        physical.extend_from_slice(&frame);
    }
    let chunk = Chunk {
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
        first: chunk.clone(),
        current: Mutex::new(chunk),
        first_layout_validity_ms: 1_000,
        full_reply: None,
        full_calls: AtomicUsize::new(0),
    });
    let disk_io = Arc::new(MemoryDiskIo {
        shards: vec![(segment.disk_id.unwrap(), Bytes::from(physical))],
        failed: Vec::new(),
        reads: Arc::new(AtomicUsize::new(0)),
    });
    let reader = ChunkReader::new(
        allocator.clone(),
        disk_io,
        ChunkReadPolicy {
            stream_window_bytes: 4,
            ..ChunkReadPolicy::default()
        },
    )
    .unwrap();
    let mut stream = reader.read_stream(&locations).unwrap();
    for expected in [b"abcd".as_slice(), b"efgh", b"ijkl"] {
        assert_eq!(stream.next_chunk().await.unwrap().unwrap(), expected);
    }
    assert!(stream.next_chunk().await.is_none());
    let metrics = reader.flow_metrics_snapshot();
    assert_eq!(metrics.location_normalizations, 1);
    assert_eq!(metrics.locations_examined, 3);
    assert_eq!(metrics.range_locations_examined, 3);
    assert_eq!(metrics.stream_windows, 3);
    assert_eq!(metrics.layout_queries, 1);
}

#[tokio::test]
async fn stream_waits_for_first_result_and_retains_three_slots_until_consumer_releases_buffers() {
    let id = ChunkId { high: 23, low: 51 };
    let segment = segment(1);
    let mut physical = Vec::new();
    let mut locations = Vec::new();
    for (index, payload) in [b"aaaa", b"bbbb", b"cccc", b"dddd"].into_iter().enumerate() {
        let frame = encode_frame(FrameMagic::RepoLargeV1, id, payload, 1).unwrap();
        locations.push(Location {
            chunk_id: Some(id),
            offset: physical.len() as u64,
            length: frame.len() as u64,
            logical_offset: (index * 4) as u64,
            logical_length: 4,
        });
        physical.extend_from_slice(&frame);
    }
    let chunk = Chunk {
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
        first: chunk.clone(),
        current: Mutex::new(chunk),
        first_layout_validity_ms: 1_000,
        full_reply: None,
        full_calls: AtomicUsize::new(0),
    });
    let started = Arc::new(AtomicUsize::new(0));
    let release_first = Arc::new(Notify::new());
    let disk_io = Arc::new(BlockingDiskIo {
        inner: MemoryDiskIo {
            shards: vec![(segment.disk_id.unwrap(), Bytes::from(physical))],
            failed: Vec::new(),
            reads: Arc::new(AtomicUsize::new(0)),
        },
        release_first: Arc::clone(&release_first),
        started: Arc::clone(&started),
    });
    let reader = ChunkReader::new(
        allocator,
        disk_io,
        ChunkReadPolicy {
            stream_slots: 3,
            stream_window_bytes: 4,
            layout_safety_margin: Duration::ZERO,
            ..ChunkReadPolicy::default()
        },
    )
    .unwrap();
    let mut stream = reader.read_stream(&locations).unwrap();
    let waiting = tokio::spawn(async move {
        let first = stream.next_chunk().await.unwrap().unwrap();
        (stream, first)
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while started.load(Ordering::Acquire) < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(started.load(Ordering::Acquire), 3);
    release_first.notify_one();
    let (mut stream, first) = waiting.await.unwrap();
    assert_eq!(&first[..], b"aaaa");
    let second = stream.next_chunk().await.unwrap().unwrap();
    let third = stream.next_chunk().await.unwrap().unwrap();
    assert_eq!(&second[..], b"bbbb");
    assert_eq!(&third[..], b"cccc");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.next_chunk())
            .await
            .is_err()
    );
    assert_eq!(started.load(Ordering::Acquire), 3);
    drop(first);
    assert_eq!(stream.next_chunk().await.unwrap().unwrap(), b"dddd".as_slice());
    assert!(stream.next_chunk().await.is_none());
}

#[tokio::test]
async fn global_read_budget_remains_charged_while_http_keeps_frame_views() {
    let id = ChunkId { high: 29, low: 57 };
    let mut segment = segment(1);
    segment.unit_count = 2;
    let payload = vec![0x5a; MAX_FRAME_PAYLOAD_BYTES];
    let frame = encode_frame(FrameMagic::RepoLargeV1, id, &payload, 1).unwrap();
    assert_eq!(frame.len(), MAX_FRAME_BYTES);
    let physical = Bytes::from(frame.repeat(32));
    let chunk = Chunk {
        id: Some(id),
        state: ChunkState::Sealed as i32,
        capacity: 2048,
        sealed_length: 2048,
        strips: vec![ChunkStrip {
            unit_kb: 1024,
            capacity: 2048,
            sealed_length: 2048,
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
        first: chunk.clone(),
        current: Mutex::new(chunk),
        first_layout_validity_ms: 1_000,
        full_reply: None,
        full_calls: AtomicUsize::new(0),
    });
    let reads = Arc::new(AtomicUsize::new(0));
    let disk_io = Arc::new(MemoryDiskIo {
        shards: vec![(segment.disk_id.unwrap(), physical)],
        failed: Vec::new(),
        reads: Arc::clone(&reads),
    });
    let reader = ChunkReader::new(
        allocator,
        disk_io,
        ChunkReadPolicy {
            stream_slots: 3,
            global_stream_bytes: 1024 * 1024,
            layout_safety_margin: Duration::ZERO,
            ..ChunkReadPolicy::default()
        },
    )
    .unwrap();
    let location = Location {
        chunk_id: Some(id),
        length: 32 * MAX_FRAME_BYTES as u64,
        logical_length: 32 * MAX_FRAME_PAYLOAD_BYTES as u64,
        ..Location::default()
    };
    let mut stream = reader.read_stream(&[location]).unwrap();
    let mut retained = Vec::new();
    for _ in 0..16 {
        retained.push(stream.next_chunk().await.unwrap().unwrap());
    }
    assert_eq!(reads.load(Ordering::Acquire), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.next_chunk())
            .await
            .is_err()
    );
    drop(retained);
    assert_eq!(stream.next_chunk().await.unwrap().unwrap(), payload.as_slice());
    assert_eq!(reads.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn frame_crossing_physical_parts_returns_two_original_payload_views() {
    let id = ChunkId { high: 31, low: 59 };
    let first_segment = segment(1);
    let second_segment = segment(2);
    let payload = b"abcdefghijklmnopqrst";
    let frame = encode_frame(FrameMagic::RepoSmallV1, id, payload, 1).unwrap();
    let frame_offset = 1000_usize;
    let first_length = 1024 - frame_offset;
    let mut first_data = vec![0; 1024];
    first_data[frame_offset..].copy_from_slice(&frame[..first_length]);
    let first_data = Bytes::from(first_data);
    let second_data = Bytes::from(frame[first_length..].to_vec());
    let strips = [first_segment, second_segment]
        .into_iter()
        .enumerate()
        .map(|(index, segment)| ChunkStrip {
            strip_sequence: u32::try_from(index).unwrap(),
            chunk_offset: u32::try_from(index).unwrap(),
            unit_kb: 1,
            capacity: 1,
            sealed_length: 1,
            sealed_ts_ms: 1,
            strip_type: StripType::Mirror as i32,
            strip: Some(Strip::MirrorStrip(MirrorStrip {
                segments: vec![segment],
            })),
            ..ChunkStrip::default()
        })
        .collect();
    let chunk = Chunk {
        id: Some(id),
        state: ChunkState::Sealed as i32,
        capacity: 2,
        sealed_length: 2,
        strips,
        ..Chunk::default()
    };
    let allocator = Arc::new(SequenceAllocator {
        queries: AtomicUsize::new(0),
        first: chunk.clone(),
        current: Mutex::new(chunk),
        first_layout_validity_ms: 1_000,
        full_reply: None,
        full_calls: AtomicUsize::new(0),
    });
    let disk_io = Arc::new(MemoryDiskIo {
        shards: vec![
            (first_segment.disk_id.unwrap(), first_data.clone()),
            (second_segment.disk_id.unwrap(), second_data.clone()),
        ],
        failed: Vec::new(),
        reads: Arc::new(AtomicUsize::new(0)),
    });
    let reader = ChunkReader::new(
        allocator,
        disk_io,
        ChunkReadPolicy {
            layout_safety_margin: Duration::ZERO,
            ..ChunkReadPolicy::default()
        },
    )
    .unwrap();
    let location = Location {
        chunk_id: Some(id),
        offset: frame_offset as u64,
        length: frame.len() as u64,
        logical_length: payload.len() as u64,
        ..Location::default()
    };
    let views = reader
        .read_range(&[location], 0, payload.len() as u64)
        .await
        .unwrap();
    assert_eq!(views.concat(), payload);
    assert_eq!(views.len(), 2);
    assert_eq!(views[0].as_ptr(), first_data.slice(frame_offset + 14..).as_ptr());
    assert_eq!(views[1].as_ptr(), second_data.as_ptr());
}

#[tokio::test]
async fn framed_mirror_crc_failure_uses_a_verified_fallback() {
    let id = ChunkId { high: 17, low: 23 };
    let first = segment(1);
    let second = segment(2);
    let payload = b"verified mirror payload";
    let valid = Bytes::from(encode_frame(FrameMagic::RepoSmallV1, id, payload, 1).unwrap());
    let mut corrupt = valid.to_vec();
    corrupt[14] ^= 0x80;
    let chunk = Chunk {
        id: Some(id),
        state: ChunkState::Sealed as i32,
        capacity: 64,
        sealed_length: 64,
        strips: vec![ChunkStrip {
            unit_kb: 1,
            capacity: 64,
            sealed_length: 64,
            sealed_ts_ms: 1,
            strip_type: StripType::Mirror as i32,
            strip: Some(Strip::MirrorStrip(MirrorStrip {
                segments: vec![first, second],
            })),
            ..ChunkStrip::default()
        }],
        ..Chunk::default()
    };
    let allocator = Arc::new(SequenceAllocator {
        queries: AtomicUsize::new(0),
        first: chunk.clone(),
        current: Mutex::new(chunk),
        first_layout_validity_ms: 1_000,
        full_reply: None,
        full_calls: AtomicUsize::new(0),
    });
    let disk_io = Arc::new(MemoryDiskIo {
        shards: vec![
            (first.disk_id.unwrap(), Bytes::from(corrupt)),
            (second.disk_id.unwrap(), valid.clone()),
        ],
        failed: Vec::new(),
        reads: Arc::new(AtomicUsize::new(0)),
    });
    let reader = ChunkReader::new(allocator.clone(), disk_io, ChunkReadPolicy::default()).unwrap();
    let location = Location {
        chunk_id: Some(id),
        length: valid.len() as u64,
        logical_length: payload.len() as u64,
        ..Location::default()
    };
    assert_eq!(
        reader.read_object(&[location]).await.unwrap().concat(),
        payload.as_slice()
    );
    assert!(allocator.queries.load(Ordering::Acquire) >= 2);
    assert_eq!(
        allocator.current.lock().unwrap().strips[0].unavailable_segments,
        vec![first]
    );
}

#[tokio::test]
async fn marked_ec_fragment_shares_one_full_recovery_future() {
    let id = ChunkId { high: 41, low: 43 };
    let scheme = EcScheme::new(4, 1);
    let data: Vec<u8> = (0..4 * SHARD)
        .map(|index| u8::try_from((index * 7 + 3) % 251).unwrap())
        .collect();
    let encoded = encode(scheme, &data).unwrap();
    let segments: Vec<_> = (1..=5)
        .map(|index| Segment {
            owner_chunk: Some(id),
            ..segment(index)
        })
        .collect();
    let shards = segments
        .iter()
        .zip(&encoded)
        .map(|(segment, bytes)| (segment.disk_id.unwrap(), Bytes::from(bytes.clone())))
        .collect();
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
        unavailable_segments: vec![segments[0]],
        ..ChunkStrip::default()
    };
    let chunk = Chunk {
        id: Some(id),
        state: ChunkState::Sealed as i32,
        capacity: 256,
        sealed_length: 256,
        modify_ts: 3,
        strips: vec![strip],
        ..Chunk::default()
    };
    let allocator = Arc::new(SequenceAllocator {
        queries: AtomicUsize::new(0),
        first: chunk.clone(),
        current: Mutex::new(chunk),
        first_layout_validity_ms: 1_000,
        full_reply: Some(Bytes::from(encoded[0].clone())),
        full_calls: AtomicUsize::new(0),
    });
    let disk_io = Arc::new(MemoryDiskIo {
        shards,
        failed: vec![segments[0].disk_id.unwrap()],
        reads: Arc::new(AtomicUsize::new(0)),
    });
    let reader = ChunkReader::new(allocator.clone(), disk_io.clone(), ChunkReadPolicy::default()).unwrap();
    let location = Location {
        chunk_id: Some(id),
        length: SHARD as u64,
        logical_length: SHARD as u64,
        ..Location::default()
    };
    let (first, second) = tokio::join!(
        reader.read_range(std::slice::from_ref(&location), 0, SHARD as u64),
        reader.read_range(std::slice::from_ref(&location), 0, SHARD as u64)
    );
    assert_eq!(first.unwrap().concat(), data[..SHARD]);
    assert_eq!(second.unwrap().concat(), data[..SHARD]);
    assert_eq!(allocator.full_calls.load(Ordering::Acquire), 1);

    let bounded = ChunkReader::new(
        allocator.clone(),
        disk_io,
        ChunkReadPolicy {
            recovery_memory_bytes: 32 * KIB,
            ..ChunkReadPolicy::default()
        },
    )
    .unwrap();
    assert_eq!(
        bounded
            .read_range(&[location], 0, SHARD as u64)
            .await
            .unwrap()
            .concat(),
        data[..SHARD]
    );
    assert_eq!(allocator.full_calls.load(Ordering::Acquire), 1);
}
