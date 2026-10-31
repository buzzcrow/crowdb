// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_diskio_client::{DiskTopologyView, TestDiskRoute};

pub(super) struct TestLocalityDiskIo {
    bytes: Bytes,
    topology: Option<DiskTopologyView>,
    pub(super) served: Mutex<Vec<u64>>,
    corrupt: Option<u64>,
}

#[async_trait]
impl DiskWriter for TestLocalityDiskIo {
    fn disk_topology(&self) -> Option<DiskTopologyView> {
        self.topology.clone()
    }

    async fn write(&self, _: &Segment, _: u64, _: Bytes) -> Result<()> {
        unreachable!("reader test does not write")
    }

    async fn write_at_byte_offset(&self, _: &Segment, _: u64, _: u64, _: Bytes) -> Result<()> {
        unreachable!("reader test does not write")
    }

    async fn read(&self, segment: &Segment, _: u64, offset: u64, length: u32) -> Result<Bytes> {
        let id = segment.disk_id.unwrap().low;
        self.served.lock().unwrap().push(id);
        let offset = usize::try_from(offset).unwrap();
        let length = usize::try_from(length).unwrap();
        let bytes = self.bytes.slice(offset..offset + length);
        if self.corrupt == Some(id) {
            let mut bytes = bytes.to_vec();
            bytes[crowdb_protocol::frame::FRAME_V2_HEADER_BYTES] ^= 1;
            Ok(Bytes::from(bytes))
        } else {
            Ok(bytes)
        }
    }
}

pub(super) fn fixture(
    preferred: Option<u64>,
    missing: Option<u64>,
    corrupt: Option<u64>,
) -> (
    ChunkReader,
    Arc<TestLocalityDiskIo>,
    Arc<SequenceAllocator>,
    Bytes,
) {
    let id = ChunkId { high: 83, low: 89 };
    let frame = Bytes::from(encode_frame(FrameMagic::RepoSmallV2, id, b"exact payload", 1).unwrap());
    // Metadata deliberately starts in another rack, followed by a sibling
    // node in the preferred rack and finally the preferred node itself.
    let replicas = vec![segment(1), segment(2), segment(3)];
    let chunk = Chunk {
        id: Some(id),
        state: ChunkState::Active as i32,
        modify_ts: 1,
        capacity: 1,
        acknowledged_cursor: frame.len() as u64,
        strips: vec![ChunkStrip {
            capacity: 1,
            unit_kb: 1,
            strip: Some(Strip::MirrorStrip(MirrorStrip { segments: replicas })),
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
    let disks = Arc::new(TestLocalityDiskIo {
        bytes: frame.clone(),
        topology: Some(DiskTopologyView::from_routes_for_tests(
            (1..=3)
                .filter(|id| Some(*id) != missing)
                .map(|id| TestDiskRoute {
                    disk_id: crowdb_diskio_client::DiskId::new(0, id),
                    rack_id: if id == 1 { 2 } else { 1 },
                    node_id: id,
                    disk_group_id: id,
                    instance_id: id,
                    endpoint: format!("127.0.0.1:{}", 12000 + id),
                })
                .collect(),
        )),
        served: Mutex::new(Vec::new()),
        corrupt,
    });
    let reader = ChunkReader::new(
        allocator.clone(),
        disks.clone(),
        ChunkReadPolicy {
            preferred_node_id: preferred,
            ..ChunkReadPolicy::default()
        },
    )
    .unwrap();
    (reader, disks, allocator, frame)
}

pub(super) async fn read(reader: &ChunkReader, frame: &Bytes) {
    assert_eq!(
        reader
            .read_verified_frame(
                ChunkId { high: 83, low: 89 },
                0,
                frame.len() as u64,
                FrameMagic::RepoSmallV2,
            )
            .await
            .unwrap(),
        *frame
    );
}
