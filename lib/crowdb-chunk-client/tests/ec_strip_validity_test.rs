// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! EC geometry, sealed prefixes and recovery with nonzero unused storage.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{DiskWriter, EcStripWriter, IoError, ReadError, Result, StripReader};
use crowdb_common::ec::{decode, encode_parity_from_shards, EcScheme};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, EcState, EcStrip, Strip};
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::Segment;
use tokio::sync::Semaphore;

const KIB: usize = 1024;
const SHARD: usize = 1024 * KIB;
const DATA: usize = 8;
const CODE: usize = 4;

struct TestDisk {
    blocks: Mutex<Vec<Vec<u8>>>,
    reads: Mutex<Vec<(usize, usize, usize)>>,
}

impl TestDisk {
    fn index(segment: &Segment) -> usize {
        usize::try_from(segment.disk_id.unwrap().low).unwrap()
    }
}

#[async_trait]
impl DiskWriter for TestDisk {
    async fn write(&self, segment: &Segment, unit: u64, data: Bytes) -> Result<()> {
        self.write_at_byte_offset(segment, unit, 0, data).await
    }

    async fn write_views(&self, segment: &Segment, unit: u64, views: Vec<Bytes>) -> Result<()> {
        let mut offset = 0;
        for view in views {
            let length = view.len() as u64;
            self.write_at_byte_offset(segment, unit, offset, view).await?;
            offset += length;
        }
        Ok(())
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        _unit: u64,
        offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let offset = usize::try_from(offset).unwrap();
        self.blocks.lock().unwrap()[Self::index(segment)][offset..offset + data.len()].copy_from_slice(&data);
        Ok(())
    }

    async fn read(&self, segment: &Segment, _unit: u64, offset: u64, length: u32) -> Result<Bytes> {
        let offset = usize::try_from(offset).unwrap();
        let length = usize::try_from(length).unwrap();
        let index = Self::index(segment);
        self.reads.lock().unwrap().push((index, offset, length));
        Ok(Bytes::copy_from_slice(
            &self.blocks.lock().unwrap()[index][offset..offset + length],
        ))
    }
}

fn test_chunk(unit_kb: u32) -> Arc<Chunk> {
    Arc::new(Chunk {
        strips: vec![ChunkStrip {
            unit_kb,
            capacity: u32::try_from(DATA * SHARD / KIB).unwrap(),
            strip: Some(Strip::EcStrip(EcStrip {
                data_num: u32::try_from(DATA).unwrap(),
                code_num: u32::try_from(CODE).unwrap(),
                ec_state: EcState::Parity as i32,
                segments: (0..DATA + CODE)
                    .map(|index| Segment {
                        disk_id: Some(DiskId {
                            high: 0,
                            low: index as u64,
                        }),
                        unit_count: u32::try_from(SHARD / (unit_kb as usize * KIB)).unwrap(),
                        ..Segment::default()
                    })
                    .collect(),
            })),
            ..ChunkStrip::default()
        }],
        ..Chunk::default()
    })
}

async fn write_strip(size: usize, unit_kb: u32) -> (ChunkStrip, Arc<TestDisk>, Vec<u8>) {
    let disk = Arc::new(TestDisk {
        blocks: Mutex::new(vec![vec![0xa5; SHARD]; DATA + CODE]),
        reads: Mutex::new(Vec::new()),
    });
    let chunk = test_chunk(unit_kb);
    let mut writer = EcStripWriter::new(chunk.clone(), 0, disk.clone(), EcScheme::new(DATA, CODE));
    let data: Vec<u8> = (0..size)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    for part in data.chunks(65_503) {
        writer.push(Bytes::copy_from_slice(part)).unwrap();
    }
    assert_eq!(writer.remaining_capacity(), (DATA * SHARD - size) as u64);
    let mut completion = writer.finish().await.unwrap();
    assert_eq!(completion.bytes_written, size as u64);
    assert_eq!(completion.data_blocks_written as usize, size.div_ceil(SHARD));
    completion.wait_for_completion_for_tests().await.unwrap();
    let mut strip = chunk.strips[0].clone();
    strip.sealed_length = u32::try_from(size.div_ceil(KIB)).unwrap();
    (strip, disk, data)
}

fn check_storage(disk: &TestDisk, data: &[u8]) {
    let sealed = data.len();
    let shards: Vec<Vec<u8>> = (0..DATA)
        .map(|index| {
            let start = (index * SHARD).min(sealed);
            let end = ((index + 1) * SHARD).min(sealed);
            let mut shard = data[start..end].to_vec();
            shard.resize(sealed.min(SHARD), 0);
            shard
        })
        .collect();
    let refs: Vec<&[u8]> = shards.iter().map(Vec::as_slice).collect();
    let parity = encode_parity_from_shards(EcScheme::new(DATA, CODE), &refs).unwrap();
    let blocks = disk.blocks.lock().unwrap();
    for index in 0..DATA {
        let start = (index * SHARD).min(sealed);
        let end = ((index + 1) * SHARD).min(sealed);
        assert_eq!(&blocks[index][..end - start], &data[start..end]);
        assert!(blocks[index][end - start..].iter().all(|byte| *byte == 0xa5));
    }
    for (block, expected) in blocks[DATA..].iter().zip(&parity) {
        assert_eq!(&block[..expected.len()], expected);
        assert!(block[expected.len()..].iter().all(|byte| *byte == 0xa5));
    }
    for failed in [
        vec![0],
        vec![0, 1],
        vec![0, 1, 2],
        vec![0, 1, 2, 3],
        vec![0, 8, 9, 10],
    ] {
        let mut available: Vec<_> = shards.iter().chain(&parity).cloned().map(Some).collect();
        for index in failed {
            available[index] = None;
        }
        let recovered = decode(EcScheme::new(DATA, CODE), available).unwrap();
        assert_eq!(&recovered[..DATA], shards);
    }
}

#[tokio::test]
async fn sealed_sizes_and_recovery_ignore_nonzero_unused_storage() {
    for unit_kb in [128, 1024] {
        for size in [
            1,
            KIB - 1,
            KIB,
            KIB + 1,
            256 * KIB,
            SHARD - 1,
            SHARD,
            SHARD + KIB,
            SHARD + KIB + 17,
            7 * SHARD + 123,
            DATA * SHARD - 1,
            DATA * SHARD,
        ] {
            let (strip, disk, data) = write_strip(size, unit_kb).await;
            check_storage(&disk, &data);
            let reader = StripReader::new(disk.clone(), Arc::new(Semaphore::new(32 * SHARD)), 32 * SHARD);
            assert_eq!(
                reader.read(&strip, size as u64, 0, size as u64).await.unwrap(),
                data
            );
            let Some(Strip::EcStrip(ec)) = &strip.strip else {
                unreachable!()
            };
            for failed in [
                vec![0],
                vec![1],
                vec![7],
                vec![0, 1],
                vec![0, 1, 2],
                vec![0, 1, 2, 3],
                vec![0, 8, 9, 10],
            ] {
                let mut unavailable = strip.clone();
                unavailable.unavailable_segments = failed.iter().map(|index| ec.segments[*index]).collect();
                disk.reads.lock().unwrap().clear();
                let actual = reader
                    .read(&unavailable, size as u64, 0, size as u64)
                    .await
                    .unwrap();
                assert_eq!(actual, data);
                for &(index, offset, length) in disk.reads.lock().unwrap().iter() {
                    let sealed = size;
                    let valid = if index < DATA {
                        sealed.saturating_sub(index * SHARD).min(SHARD)
                    } else {
                        sealed.min(SHARD)
                    };
                    assert!(offset + length <= valid, "read stale tail at shard {index}");
                }
            }
            let mut lost = strip.clone();
            lost.unavailable_segments = [0, 8, 9, 10, 11].map(|index| ec.segments[index]).to_vec();
            assert!(matches!(
                reader.read(&lost, size as u64, 0, size.min(SHARD) as u64).await,
                Err(ReadError::DataLoss(_))
            ));
            assert!(matches!(
                reader.read(&strip, size as u64, size as u64, 1).await,
                Err(ReadError::NotYetAvailable(_))
            ));
        }
    }
}

#[tokio::test]
async fn empty_strip_and_overflow_preserve_writer_state() {
    let disk = Arc::new(TestDisk {
        blocks: Mutex::new(vec![vec![0xa5; SHARD]; DATA + CODE]),
        reads: Mutex::new(Vec::new()),
    });
    let mut empty = EcStripWriter::new(test_chunk(128), 0, disk.clone(), EcScheme::new(DATA, CODE));
    assert!(matches!(empty.finish().await, Err(IoError::EcEncodeFailed(_))));
    assert!(disk
        .blocks
        .lock()
        .unwrap()
        .iter()
        .flatten()
        .all(|byte| *byte == 0xa5));
    let mut writer = EcStripWriter::new(test_chunk(128), 0, disk, EcScheme::new(DATA, CODE));
    assert!(matches!(
        writer.push(Bytes::from(vec![0; DATA * SHARD + 1])),
        Err(IoError::WriteFailed(_))
    ));
    assert_eq!(writer.remaining_capacity(), (DATA * SHARD) as u64);
    assert_eq!(writer.data_blocks_written(), 0);
}
