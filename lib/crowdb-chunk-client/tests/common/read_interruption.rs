// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{DiskWriter, IoError, Result};
use crowdb_protocol::diskdb::rpc::Segment;

pub struct TestInterruptedDisk {
    pub calls: AtomicUsize,
    /// A normal disk failure triggers EC recovery before another read interrupts.
    pub first_disk_unavailable: bool,
}

#[async_trait]
impl DiskWriter for TestInterruptedDisk {
    async fn write(&self, _: &Segment, _: u64, _: Bytes) -> Result<()> {
        panic!("read wrote storage")
    }

    async fn write_at_byte_offset(&self, _: &Segment, _: u64, _: u64, _: Bytes) -> Result<()> {
        panic!("read wrote storage")
    }

    async fn read(&self, segment: &Segment, _: u64, _: u64, _: u32) -> Result<Bytes> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.first_disk_unavailable && segment.disk_id.unwrap().low == 1 {
            Err(IoError::ReadFailed("ordinary disk failure".into()))
        } else {
            Err(IoError::ReadInterrupted("accepted RDMA connection failed".into()))
        }
    }
}
