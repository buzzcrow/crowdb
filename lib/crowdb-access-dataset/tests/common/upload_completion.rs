// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "upload.rs"]
mod upload;
pub use upload::TestStore;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_dataset::{PayloadDurability, PayloadStorage, PayloadStream, UploadError};
use crowdb_chunk_client::SmallWriteIntent;
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_rpc_ffi::OwnedPayload;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

pub const TRUNCATED: usize = 1;
pub const CORRUPT: usize = 2;
pub const OVERRUN: usize = 3;
pub const EMPTY_FRAME: usize = 4;
pub const LATE_ERROR: usize = 5;
pub const PARTIAL_ERROR: usize = 6;
pub const PAUSE_PARTIAL: usize = 7;
pub const PAUSE_EOF: usize = 8;

#[derive(Default)]
pub struct TestCompletionState {
    pub active: AtomicUsize,
    pub streams: AtomicUsize,
    pub delivered: AtomicUsize,
    pub eof: AtomicUsize,
    pub paused: AtomicBool,
}

pub struct TestCompletionStorage {
    inner: upload::TestPayload,
    fragment: usize,
    pub fault: AtomicUsize,
    pub fault_stream: AtomicUsize,
    pub buffered_reads: AtomicUsize,
    pub stream_reads: AtomicUsize,
    pub state: Arc<TestCompletionState>,
}

impl TestCompletionStorage {
    pub fn new(fragment: usize) -> Self {
        assert!(fragment > 0);
        Self {
            inner: upload::TestPayload::default(),
            fragment,
            fault: AtomicUsize::new(0),
            fault_stream: AtomicUsize::new(0),
            buffered_reads: AtomicUsize::new(0),
            stream_reads: AtomicUsize::new(0),
            state: Arc::new(TestCompletionState::default()),
        }
    }
}

#[async_trait]
impl PayloadStorage for TestCompletionStorage {
    async fn write(
        &self,
        key: &[u8],
        data: Bytes,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError> {
        self.inner.write(key, data, intent).await
    }

    async fn read(&self, _locations: &[Location]) -> Result<Bytes, UploadError> {
        self.buffered_reads.fetch_add(1, Ordering::Relaxed);
        Err(UploadError::Storage(
            "completion must not collect payloads".into(),
        ))
    }

    async fn read_stream(&self, locations: &[Location]) -> Result<Box<dyn PayloadStream>, UploadError> {
        let index = self.stream_reads.fetch_add(1, Ordering::Relaxed);
        let fault = if index == self.fault_stream.load(Ordering::Acquire) {
            self.fault.load(Ordering::Acquire)
        } else {
            0
        };
        let mut bytes = self.inner.read(locations).await?;
        match fault {
            TRUNCATED => bytes.truncate(bytes.len() - 1),
            CORRUPT | OVERRUN => {
                let mut data = bytes.to_vec();
                if fault == CORRUPT {
                    data[0] ^= 1;
                } else {
                    data.push(0);
                }
                bytes = Bytes::from(data);
            }
            _ => {}
        }
        self.state.streams.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(TestCompletionStream {
            bytes,
            offset: 0,
            fragment: self.fragment,
            fault,
            state: Arc::clone(&self.state),
        }))
    }

    async fn durability(&self, locations: &[Location]) -> Result<PayloadDurability, UploadError> {
        self.inner.durability(locations).await
    }

    async fn discard(&self, locations: &[Location]) -> Result<bool, UploadError> {
        self.inner.discard(locations).await
    }
}

struct TestCompletionStream {
    bytes: Bytes,
    offset: usize,
    fragment: usize,
    fault: usize,
    state: Arc<TestCompletionState>,
}

impl Drop for TestCompletionStream {
    fn drop(&mut self) {
        self.state.streams.fetch_sub(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl PayloadStream for TestCompletionStream {
    async fn next(&mut self) -> Result<Option<OwnedPayload>, UploadError> {
        assert_eq!(
            self.state.active.load(Ordering::Relaxed),
            0,
            "previous owner must release its read credit before the next pull"
        );
        if self.offset > 0 {
            match self.fault {
                EMPTY_FRAME => return Ok(Some(OwnedPayload::new())),
                PARTIAL_ERROR => return Err(UploadError::Storage("partial read failed".into())),
                PAUSE_PARTIAL => {
                    self.state.paused.store(true, Ordering::Release);
                    futures::future::pending::<()>().await;
                }
                _ => {}
            }
        }
        if self.offset == self.bytes.len() {
            if self.fault == LATE_ERROR {
                return Err(UploadError::Storage("final read failed".into()));
            }
            if self.fault == PAUSE_EOF {
                self.state.paused.store(true, Ordering::Release);
                futures::future::pending::<()>().await;
            }
            self.state.eof.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let end = (self.offset + self.fragment).min(self.bytes.len());
        let bytes = self.bytes.slice(self.offset..end);
        let pointer = bytes.as_ptr();
        self.offset = end;
        self.state.delivered.fetch_add(bytes.len(), Ordering::Relaxed);
        self.state.active.fetch_add(1, Ordering::Relaxed);
        let payload = OwnedPayload::from(Bytes::from_owner(TestCompletionOwner {
            bytes,
            state: Arc::clone(&self.state),
        }));
        assert_eq!(payload.as_ptr(), pointer, "the storage owner must not be copied");
        Ok(Some(payload))
    }
}

struct TestCompletionOwner {
    bytes: Bytes,
    state: Arc<TestCompletionState>,
}

impl AsRef<[u8]> for TestCompletionOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for TestCompletionOwner {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::Relaxed);
    }
}
