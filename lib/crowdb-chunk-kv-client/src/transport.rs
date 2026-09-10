// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_protocol::chunk_kv::{
    BatchMutationRequest, BatchMutationResponse, ChunkKvResponse, MultiGetRequest, MultiGetResponse,
    PointRequest, ScanRequest, SeekRequest,
};

use crate::{ClientError, Result};

#[async_trait]
pub trait ChunkKvTransport: Send + Sync {
    /// Sends directly to one catalog owner endpoint.
    ///
    /// # Errors
    ///
    /// Returns only connection/transport failures; typed server outcomes remain
    /// inside `ChunkKvResponse`.
    async fn point(&self, endpoint: &str, request: &PointRequest) -> Result<ChunkKvResponse>;

    /// Sends one range-validated multi-get group directly to its owner.
    ///
    /// # Errors
    ///
    /// Returns only transport failures; group and item outcomes stay typed.
    async fn multi_get(&self, _endpoint: &str, _request: &MultiGetRequest) -> Result<MultiGetResponse> {
        Err(ClientError::Transport(
            "multi-get transport is not implemented".into(),
        ))
    }

    /// Sends one ordered partition-local mutation group directly to its owner.
    ///
    /// # Errors
    ///
    /// Returns only transport failures; group and item outcomes stay typed.
    async fn batch_mutate(
        &self,
        _endpoint: &str,
        _request: &BatchMutationRequest,
    ) -> Result<BatchMutationResponse> {
        Err(ClientError::Transport(
            "batch mutation transport is not implemented".into(),
        ))
    }

    /// Sends one ordered seek directly to a partition owner.
    ///
    /// # Errors
    ///
    /// Returns only transport failures; typed server outcomes remain encoded.
    async fn seek(&self, _endpoint: &str, _request: &SeekRequest) -> Result<ChunkKvResponse> {
        Err(ClientError::Transport("seek transport is not implemented".into()))
    }

    /// Sends one bounded directional partition scan directly to its owner.
    ///
    /// # Errors
    ///
    /// Returns only transport failures; typed server outcomes remain encoded.
    async fn scan(&self, _endpoint: &str, _request: &ScanRequest) -> Result<ChunkKvResponse> {
        Err(ClientError::Transport("scan transport is not implemented".into()))
    }
}
