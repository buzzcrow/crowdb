// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{FrameExtentSink, FrameWindowReader, IoError};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{encode_append_group_views, FrameScanConfig, FrameScanMode, ScannedFrameExtent};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

pub struct TestObservation;

impl TestObservation {
    pub const CHUNK: ChunkId = ChunkId {
        high: 4 << 56,
        low: 23,
    };
    pub fn group(offset: usize, payload: &[u8]) -> Vec<u8> {
        encode_append_group_views(Self::CHUNK, offset as u64, &Bytes::copy_from_slice(payload), 1)
            .unwrap()
            .concat()
    }
    pub fn config(capacity: usize) -> FrameScanConfig {
        FrameScanConfig {
            chunk_id: Self::CHUNK,
            mode: FrameScanMode::Grouped,
            physical_start: 0,
            physical_limit: capacity as u64,
            required_end: 0,
            logical_start: 0,
            max_append_bytes: 64 * 1024 * 1024,
        }
    }
    pub fn backing(prefix: &[u8], capacity: usize) -> Bytes {
        let mut bytes = prefix.to_vec();
        bytes.resize(capacity, 0);
        Bytes::from(bytes)
    }
}

pub struct TestWindowReader {
    data: Bytes,
    pub delay: Duration,
    pub fail_at: Option<u64>,
    pub short_at: Option<u64>,
    pub fail_on_call: Option<usize>,
    pub stall_on_call: Option<usize>,
    calls: AtomicUsize,
    largest: AtomicU32,
    last_end: AtomicU64,
}

impl TestWindowReader {
    pub fn new(data: Bytes) -> Self {
        Self {
            data,
            delay: Duration::ZERO,
            fail_at: None,
            short_at: None,
            fail_on_call: None,
            stall_on_call: None,
            calls: AtomicUsize::new(0),
            largest: AtomicU32::new(0),
            last_end: AtomicU64::new(0),
        }
    }
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
    pub fn largest(&self) -> u32 {
        self.largest.load(Ordering::Relaxed)
    }
    pub fn last_end(&self) -> u64 {
        self.last_end.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl FrameWindowReader for TestWindowReader {
    async fn read_window(&self, offset: u64, length: u32) -> crowdb_chunk_client::Result<Bytes> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        self.largest.fetch_max(length, Ordering::Relaxed);
        self.last_end.store(offset + u64::from(length), Ordering::Relaxed);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        if self.stall_on_call == Some(call) {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        if self.fail_on_call == Some(call) || self.fail_at.is_some_and(|at| offset >= at) {
            return Err(IoError::ReadFailed("injected replica read failure".into()));
        }
        let start =
            usize::try_from(offset).map_err(|_| IoError::ReadFailed("read offset overflows".into()))?;
        let end = start + length as usize;
        if end > self.data.len() {
            return Err(IoError::ReadFailed("read exceeds allocated data".into()));
        }
        let end = if self.short_at.is_some_and(|at| offset >= at) {
            end - 1
        } else {
            end
        };
        Ok(self.data.slice(start..end))
    }
}

#[derive(Default)]
pub struct TestExtentSink {
    pub records: Vec<ScannedFrameExtent>,
    pub largest_batch: usize,
    pub delay: Duration,
    pub fail: bool,
}

#[async_trait]
impl FrameExtentSink for TestExtentSink {
    async fn append_extents(&mut self, extents: &[ScannedFrameExtent]) -> crowdb_chunk_client::Result<()> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        if self.fail {
            return Err(IoError::Internal("injected mapping failure".into()));
        }
        self.largest_batch = self.largest_batch.max(extents.len());
        self.records.extend_from_slice(extents);
        Ok(())
    }
}
