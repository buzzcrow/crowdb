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
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Default)]
pub struct TestPayloadStreamStorage {
    inner: upload::TestPayload,
    pub mode: AtomicUsize,
    pub pulls: Arc<AtomicUsize>,
}
#[async_trait]
impl PayloadStorage for TestPayloadStreamStorage {
    async fn write(
        &self,
        key: &[u8],
        data: Bytes,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError> {
        self.inner.write(key, data, intent).await
    }
    async fn read(&self, locations: &[Location]) -> Result<Bytes, UploadError> {
        self.inner.read(locations).await
    }
    async fn durability(&self, locations: &[Location]) -> Result<PayloadDurability, UploadError> {
        self.inner.durability(locations).await
    }
    async fn discard(&self, locations: &[Location]) -> Result<bool, UploadError> {
        self.inner.discard(locations).await
    }
    async fn read_stream(&self, locations: &[Location]) -> Result<Box<dyn PayloadStream>, UploadError> {
        let mut bytes = self.inner.read(locations).await?.to_vec();
        let mode = self.mode.load(Ordering::Acquire);
        if mode == 1 {
            bytes.pop();
        }
        if mode == 2 {
            bytes[0] ^= 1;
        }
        if mode == 4 {
            bytes.push(0);
        }
        bytes.reserve(4096);
        Ok(Box::new(TestPayloadStream {
            payload: OwnedPayload::from(bytes),
            offset: 0,
            mode,
            pulls: Arc::clone(&self.pulls),
        }))
    }
}

struct TestPayloadStream {
    payload: OwnedPayload,
    offset: usize,
    mode: usize,
    pulls: Arc<AtomicUsize>,
}
#[async_trait]
impl PayloadStream for TestPayloadStream {
    async fn next(&mut self) -> Result<Option<OwnedPayload>, UploadError> {
        self.pulls.fetch_add(1, Ordering::Relaxed);
        if self.mode == 3 && self.offset > 0 {
            return Err(UploadError::Storage("accepted partial read failed".into()));
        }
        if self.offset == self.payload.len() {
            return Ok(None);
        }
        let end = (self.offset + 2).min(self.payload.len());
        let payload = self.payload.slice(self.offset..end);
        self.offset = end;
        Ok(Some(payload))
    }
}
