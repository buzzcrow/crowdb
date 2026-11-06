// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{DiskWriter, IoError, Result};
use crowdb_paxos_kv_client::DiskRecord;
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    diskdb::rpc::{DiskValue, Segment},
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

pub enum TestHeaderReply {
    Ready,
    Rejected,
    Short,
    Held,
}

pub struct TestReadinessWriter {
    pub delay: Duration,
    pub reply: TestHeaderReply,
    pub started: AtomicUsize,
    pub active: Arc<AtomicUsize>,
    pub maximum: AtomicUsize,
    pub read_started: tokio::sync::Notify,
}

impl TestReadinessWriter {
    pub fn new(delay: Duration, reply: TestHeaderReply) -> Arc<Self> {
        Arc::new(Self {
            delay,
            reply,
            started: AtomicUsize::new(0),
            active: Arc::default(),
            maximum: AtomicUsize::new(0),
            read_started: tokio::sync::Notify::new(),
        })
    }
}

struct TestActiveRead(Arc<AtomicUsize>);
impl Drop for TestActiveRead {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl DiskWriter for TestReadinessWriter {
    async fn write(&self, _: &Segment, _: u64, _: Bytes) -> Result<()> {
        panic!("readiness must not write")
    }

    async fn write_at_byte_offset(&self, _: &Segment, _: u64, _: u64, _: Bytes) -> Result<()> {
        panic!("readiness must not write")
    }

    async fn read_priority(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        length: u32,
    ) -> Result<Bytes> {
        assert!(segment.disk_id.is_some());
        assert_eq!((unit_bytes, offset, length), (4096, 0, 4096));
        self.started.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        let _active = TestActiveRead(self.active.clone());
        self.read_started.notify_one();
        tokio::time::sleep(self.delay).await;
        match self.reply {
            TestHeaderReply::Ready => Ok(Bytes::from(vec![7; 4096])),
            TestHeaderReply::Rejected => Err(IoError::ReadFailed("injected physical rejection".into())),
            TestHeaderReply::Short => Ok(Bytes::from_static(b"short")),
            TestHeaderReply::Held => std::future::pending().await,
        }
    }
}

pub fn test_disks(count: usize) -> Vec<DiskRecord> {
    (0..count)
        .map(|n| DiskRecord {
            rack_id: 1,
            node_id: 1,
            disk_group_id: n as u64 + 1,
            disk_id: DiskId {
                high: 1,
                low: n as u64 + 1,
            },
            value: DiskValue {
                status: HwStatus::Up as i32,
                ..Default::default()
            },
        })
        .collect()
}
