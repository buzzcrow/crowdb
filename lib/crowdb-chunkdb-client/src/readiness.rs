// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only point observations whose work does not grow with catalog size.

use crowdb_protocol::{
    chunk_id::{CHUNK_TYPE_S3, CHUNK_TYPE_WAL},
    chunkdb::rpc::QueryChunkRequest,
    common::ChunkId,
};

use crate::{ChunkdbClient, ChunkdbClientError, Result};

impl ChunkdbClient {
    /// Read one key through each maintenance domain's current owner and storage route.
    /// An authoritative not-found response is valid evidence for an empty catalog.
    /// This neither creates probe objects nor enumerates unrelated chunks.
    ///
    /// # Errors
    /// Returns routing, ownership, storage, transport and malformed response errors.
    pub async fn observe_readiness(&self) -> Result<()> {
        tokio::try_join!(
            self.observe_domain(CHUNK_TYPE_WAL),
            self.observe_domain(CHUNK_TYPE_S3),
        )?;
        Ok(())
    }

    async fn observe_domain(&self, purpose: u8) -> Result<()> {
        let id = ChunkId {
            high: u64::from(purpose) << 56,
            low: 0,
        };
        match self.query_chunk(QueryChunkRequest { chunk_id: Some(id) }).await {
            Ok(reply) if reply.chunk.as_ref().is_some_and(|chunk| chunk.id == Some(id)) => Ok(()),
            Err(ChunkdbClientError::NotFound(_)) => Ok(()),
            Ok(_) => Err(ChunkdbClientError::Rpc(
                "readiness query returned a missing or different chunk identity".into(),
            )),
            Err(error) => Err(error),
        }
    }
}
