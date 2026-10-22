// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{StreamDirectoryPageId, StreamName};
use crowdb_protocol::key::{BinaryKey, StreamDirectoryPageKey};

use super::{kv_error, GetOutcome, KvStreamMetadataStore, ReadMode};
use crate::{Result, StreamDirectoryStore};

#[async_trait]
impl StreamDirectoryStore for KvStreamMetadataStore {
    async fn load_directory_record(
        &self,
        stream_name: StreamName,
        page_id: StreamDirectoryPageId,
    ) -> Result<Option<Bytes>> {
        let key = StreamDirectoryPageKey { stream_name, page_id }.to_bytes();
        match self
            .kv
            .get(self.store_id, self.group_id, &key, ReadMode::Linearizable, None)
            .await
            .map_err(kv_error)?
        {
            GetOutcome::Found { value, .. } => Ok(Some(value)),
            GetOutcome::NotFound => Ok(None),
        }
    }
}
