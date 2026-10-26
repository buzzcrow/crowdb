// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable frame inspection and controlled storage backpressure.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkClientConfig, DiskWriter, LargeAsyncObjectWriter, ProtoLocation, Result};
use crowdb_common::ec::EcScheme;
use crowdb_diskio_client::DiskId;
use crowdb_protocol::chunkdb::rpc::Strip;
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::{encode_frame, parse_frame, FrameMagic};
use crowdb_test_harness::test_dirs::{self, TestDir};
use tokio::sync::{Notify, Semaphore};

use super::allocator::TestChunkAllocator;
use super::common::LocalFileDiskWriter;

pub(crate) struct TestFrameBatch {
    pub(crate) allocator: TestChunkAllocator,
    pub(crate) disk: Arc<TestPausedDisk>,
    _directory: TestDir,
}

impl TestFrameBatch {
    pub(crate) fn new() -> Self {
        let directory = test_dirs::tempdir_in_test_data("chunk-frame-batch");
        Self {
            allocator: TestChunkAllocator::new(),
            disk: Arc::new(TestPausedDisk {
                inner: LocalFileDiskWriter::new(directory.path()),
                pause: AtomicBool::new(false),
                entered: Notify::new(),
                release: Semaphore::new(0),
                held: Mutex::new(None),
            }),
            _directory: directory,
        }
    }

    pub(crate) fn writer(&self, chunk_bytes: u64) -> LargeAsyncObjectWriter {
        let mut config = ChunkClientConfig::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3);
        config.max_chunk_size = chunk_bytes;
        config.large_mirror_copies = None;
        config.read_buffer_size = 1 << 20;
        config.large_held_buffers = 1;
        config.large_parallel_strip_writes = 1;
        config.chunk_preparation_depth = 1;
        LargeAsyncObjectWriter::new(
            Arc::new(self.allocator.clone()),
            self.disk.clone(),
            EcScheme::new(4, 1),
            Arc::new(config),
        )
    }

    pub(crate) fn verify(&self, locations: &[ProtoLocation], expected: &[u8]) -> Vec<usize> {
        let state = self.allocator.snapshot();
        let mut logical_offset = 0;
        let mut lengths = Vec::new();
        for location in locations {
            assert_eq!(location.logical_offset, logical_offset as u64);
            let chunk = location.chunk_id.unwrap();
            let (strips, sealed, deleted) = &state.chunks[&(chunk.high, chunk.low)];
            assert!(!deleted);
            assert!(*sealed > 0);
            let mut physical = Vec::new();
            for strip in strips {
                let remaining = usize::try_from(location.length).unwrap() - physical.len();
                if remaining == 0 {
                    break;
                }
                let Some(Strip::EcStrip(ec)) = &strip.strip else {
                    panic!("EC test layout");
                };
                for segment in ec.segments.iter().take(ec.data_num as usize) {
                    let remaining = usize::try_from(location.length).unwrap() - physical.len();
                    if remaining == 0 {
                        break;
                    }
                    let disk = segment.disk_id.unwrap();
                    physical.extend(
                        self.disk
                            .inner
                            .read_block(
                                DiskId::new(disk.high, disk.low),
                                segment.unit_offset * u64::from(strip.unit_kb) * 1024,
                                remaining.min(strip.unit_kb as usize * 1024),
                            )
                            .unwrap(),
                    );
                }
            }
            assert_eq!(physical.len() as u64, location.length);
            let mut cursor = 0;
            let logical_start = logical_offset;
            while cursor < physical.len() {
                let frame = parse_frame(&physical[cursor..], chunk).unwrap();
                assert_eq!(frame.header.magic, FrameMagic::RepoLargeV2);
                let canonical = encode_frame(
                    frame.header.magic,
                    chunk,
                    frame.payload,
                    frame.header.write_time_ms,
                )
                .unwrap();
                assert_eq!(&physical[cursor..cursor + frame.physical_length], canonical);
                assert_eq!(
                    frame.payload,
                    &expected[logical_offset..logical_offset + frame.payload.len()]
                );
                lengths.push(frame.payload.len());
                logical_offset += frame.payload.len();
                cursor += frame.physical_length;
            }
            assert_eq!((logical_offset - logical_start) as u64, location.logical_length);
        }
        assert_eq!(logical_offset, expected.len());
        lengths
    }
}

pub(crate) struct TestPausedDisk {
    inner: LocalFileDiskWriter,
    pause: AtomicBool,
    entered: Notify,
    release: Semaphore,
    held: Mutex<Option<Bytes>>,
}

impl TestPausedDisk {
    pub(crate) fn pause_next(&self) {
        self.pause.store(true, Ordering::Release);
    }

    pub(crate) async fn wait_until_paused(&self) -> Bytes {
        self.entered.notified().await;
        self.held.lock().unwrap().as_ref().unwrap().clone()
    }

    pub(crate) fn resume(&self) {
        self.release.add_permits(1);
    }

    async fn pause_write(&self, data: &Bytes) {
        if self.pause.swap(false, Ordering::AcqRel) {
            *self.held.lock().unwrap() = Some(data.clone());
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            self.held.lock().unwrap().take();
        }
    }
}

#[async_trait]
impl DiskWriter for TestPausedDisk {
    async fn write(&self, segment: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        self.pause_write(&data).await;
        self.inner.write(segment, unit_bytes, data).await
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> Result<()> {
        self.pause_write(&data).await;
        self.inner
            .write_at_byte_offset(segment, unit_bytes, offset, data)
            .await
    }
}
