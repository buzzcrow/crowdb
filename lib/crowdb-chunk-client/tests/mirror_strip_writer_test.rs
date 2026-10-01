// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{DiskWriter, IoError, MirrorStripWriter, Result};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, MirrorStrip, Strip};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;

#[derive(Default)]
struct TestDiskWriter {
    data: Mutex<HashMap<u64, Vec<u8>>>,
    fail_disk: Option<u64>,
}

#[async_trait]
impl DiskWriter for TestDiskWriter {
    async fn write(&self, segment: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        self.write_at_byte_offset(segment, unit_bytes, 0, data).await
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        _unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let disk = segment.disk_id.unwrap().high;
        if self.fail_disk == Some(disk) {
            return Err(IoError::WriteFailed("injected mirror failure".into()));
        }
        let mut all = self.data.lock().unwrap();
        let target = all.entry(disk).or_default();
        let start = usize::try_from(offset).unwrap();
        target.resize(target.len().max(start + data.len()), 0);
        target[start..start + data.len()].copy_from_slice(&data);
        Ok(())
    }
}

fn chunk() -> Arc<Chunk> {
    Arc::new(Chunk {
        id: Some(ChunkId { high: 1, low: 2 }),
        strips: vec![ChunkStrip {
            unit_kb: 4,
            capacity: 8,
            strip: Some(Strip::MirrorStrip(MirrorStrip {
                segments: [11, 12]
                    .map(|high| Segment {
                        disk_id: Some(DiskId { high, low: 0 }),
                        unit_count: 2,
                        ..Segment::default()
                    })
                    .to_vec(),
            })),
            ..ChunkStrip::default()
        }],
        ..Chunk::default()
    })
}

#[tokio::test]
async fn mirror_strip_writes_unaligned_inputs_to_every_copy() {
    let disk = Arc::new(TestDiskWriter::default());
    let mut writer = MirrorStripWriter::new(chunk(), 0, disk.clone());
    writer.push(Bytes::from(vec![3; 3 * 1024])).await.unwrap();
    writer.push(Bytes::from(vec![7; 5 * 1024])).await.unwrap();
    assert!(!writer.ready());
    assert_eq!(writer.finish().await.unwrap().bytes_written, 8 * 1024);
    let copies = disk.data.lock().unwrap();
    for id in [11, 12] {
        assert_eq!(&copies[&id][..3 * 1024], vec![3; 3 * 1024]);
        assert_eq!(&copies[&id][3 * 1024..], vec![7; 5 * 1024]);
    }
}

#[tokio::test]
async fn mirror_strip_keeps_writing_surviving_copies_after_a_failure() {
    let disk = Arc::new(TestDiskWriter {
        fail_disk: Some(12),
        ..TestDiskWriter::default()
    });
    let mut writer = MirrorStripWriter::new(chunk(), 0, disk.clone());
    writer.push(Bytes::from_static(b"data")).await.unwrap();
    writer.push(Bytes::from_static(b"more")).await.unwrap();
    assert_eq!(writer.finish().await.unwrap().bytes_written, 8);
    let copies = disk.data.lock().unwrap();
    assert_eq!(&copies[&11], b"datamore");
    assert!(!copies.contains_key(&12));
}
