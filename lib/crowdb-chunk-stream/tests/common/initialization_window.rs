// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::Disks;
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{DiskWriter, IoError};
use crowdb_protocol::diskdb::rpc::Segment;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub(crate) struct TestInitializationWindow {
    pub(crate) inner: Arc<Disks>,
    limit: usize,
    active: AtomicUsize,
    rejected: AtomicUsize,
}

impl TestInitializationWindow {
    pub(crate) fn new(inner: Arc<Disks>, limit: usize) -> Self {
        Self {
            inner,
            limit,
            active: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
        }
    }

    pub(crate) fn rejected(&self) -> usize {
        self.rejected.load(Ordering::Acquire)
    }
}

#[async_trait]
impl DiskWriter for TestInitializationWindow {
    async fn write(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        self.inner.write(segment, unit_bytes, data).await
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        self.inner
            .write_at_byte_offset(segment, unit_bytes, offset, data)
            .await
    }

    async fn initialize_synchronized_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        let result = if active > self.limit {
            self.rejected.fetch_add(1, Ordering::AcqRel);
            Err(IoError::WriteFailed(
                "bounded initialization transport is full".into(),
            ))
        } else {
            tokio::task::yield_now().await;
            self.inner
                .initialize_synchronized_at_byte_offset(segment, unit_bytes, offset, data)
                .await
        };
        self.active.fetch_sub(1, Ordering::AcqRel);
        result
    }
}
