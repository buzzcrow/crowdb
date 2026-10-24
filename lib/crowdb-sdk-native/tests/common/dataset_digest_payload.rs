// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_dataset::{PayloadDurability, PayloadStorage, UploadError};
use crowdb_chunk_client::SmallWriteIntent;
use crowdb_protocol::{chunkdb::rpc::Location, common::ChunkId};
use std::sync::Arc;

#[derive(Default)]
pub struct TestDigestPayload {
    pub bytes: ArcSwap<Vec<u8>>,
}

#[async_trait]
impl PayloadStorage for TestDigestPayload {
    async fn write(
        &self,
        _: &[u8],
        data: Bytes,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError> {
        let location = Location {
            chunk_id: Some(ChunkId { high: 1, low: 1 }),
            offset: 0,
            length: data.len() as u64,
            logical_offset: 0,
            logical_length: data.len() as u64,
        };
        intent
            .before_write(&location)
            .await
            .map_err(|error| UploadError::Storage(error.to_string()))?;
        self.bytes.store(Arc::new(data.to_vec()));
        Ok(vec![location])
    }

    async fn read(&self, _: &[Location]) -> Result<Bytes, UploadError> {
        Ok(Bytes::copy_from_slice(self.bytes.load().as_slice()))
    }

    async fn durability(&self, _: &[Location]) -> Result<PayloadDurability, UploadError> {
        Ok(PayloadDurability::Durable)
    }

    async fn discard(&self, _: &[Location]) -> Result<bool, UploadError> {
        Ok(true)
    }
}
