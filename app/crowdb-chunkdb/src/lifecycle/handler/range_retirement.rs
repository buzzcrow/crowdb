// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{LifecycleError, LifecycleHandler};
use crowdb_protocol::chunkdb::rpc::{DeleteChunkRangeRequest, DeleteChunkRangeResponse};

impl LifecycleHandler {
    pub async fn delete_chunk_range(
        &self,
        request: &DeleteChunkRangeRequest,
    ) -> Result<DeleteChunkRangeResponse, LifecycleError> {
        let id = request
            .chunk_id
            .ok_or_else(|| LifecycleError::InvalidRequest("missing chunk identity".into()))?;
        self.check_range(&id)?;
        self.store
            .retire_chunk_range(request)
            .await
            .map_err(|error| match error {
                crate::storage::StoreError::ChunkNotFound => LifecycleError::ChunkNotFound,
                other => other.into(),
            })
    }
}
