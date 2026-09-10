// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_protocol::chunk_kv::{ChunkKvResponse, PointRequest};

use crate::Result;

#[async_trait]
pub trait ChunkKvTransport: Send + Sync {
    /// Sends directly to one catalog owner endpoint.
    ///
    /// # Errors
    ///
    /// Returns only connection/transport failures; typed server outcomes remain
    /// inside `ChunkKvResponse`.
    async fn point(&self, endpoint: &str, request: &PointRequest) -> Result<ChunkKvResponse>;
}
