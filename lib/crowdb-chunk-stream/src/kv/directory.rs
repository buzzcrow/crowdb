// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{
    decode_stream_head_record, StreamDirectoryHead, StreamDirectoryPageId, StreamHeadRecord, StreamName,
    StreamRootIdentity,
};
use crowdb_protocol::key::{
    BinaryKey, StreamDirectoryPageKey, StreamDirectoryRootKey, StreamManifestHeadKey,
};

use super::{kv_error, GetOutcome, KvStreamMetadataStore, ReadMode};
use crate::{Result, StreamDirectoryStore, StreamError};

mod control;
mod publication;

/// An immutable indexed head and the KV revision that fenced its observation.
#[derive(Clone, Debug)]
pub struct DirectoryHeadSnapshot {
    pub(super) head: StreamDirectoryHead,
    pub(super) revision: u64,
    pub(super) store_id: u64,
}

impl DirectoryHeadSnapshot {
    #[must_use]
    pub const fn head(&self) -> &StreamDirectoryHead {
        &self.head
    }
}

impl KvStreamMetadataStore {
    /// # Errors
    /// Rejects legacy heads until migration, unknown versions and wrong metadata scope.
    pub async fn load_directory_head(&self, name: StreamName) -> Result<Option<DirectoryHeadSnapshot>> {
        let key = StreamManifestHeadKey { stream_name: name }.to_bytes();
        match self
            .kv
            .get(self.store_id, self.group_id, &key, ReadMode::Linearizable, None)
            .await
            .map_err(kv_error)?
        {
            GetOutcome::Found { value, revision } => Ok(Some(DirectoryHeadSnapshot {
                head: self.decode_indexed(&value, name)?,
                revision,
                store_id: self.store_id,
            })),
            GetOutcome::NotFound => Ok(None),
        }
    }

    /// # Errors
    /// Returns unavailable/malformed root failures and never substitutes a newer root.
    pub async fn load_directory_root(
        &self,
        name: StreamName,
        identity: StreamRootIdentity,
    ) -> Result<Option<StreamDirectoryHead>> {
        let key = StreamDirectoryRootKey {
            stream_name: name,
            identity,
        }
        .to_bytes();
        match self
            .kv
            .get(self.store_id, self.group_id, &key, ReadMode::Linearizable, None)
            .await
            .map_err(kv_error)?
        {
            GetOutcome::Found { value, .. } => {
                let head = self.decode_indexed(&value, name)?;
                if head.identity() != identity {
                    return Err(StreamError::Corruption(
                        "directory root identity differs from its key".into(),
                    ));
                }
                Ok(Some(head))
            }
            GetOutcome::NotFound => Ok(None),
        }
    }

    fn decode_indexed(&self, bytes: &[u8], name: StreamName) -> Result<StreamDirectoryHead> {
        let record = decode_stream_head_record(bytes)
            .map_err(|error| StreamError::Corruption(format!("stream head decode failed: {error}")))?;
        let StreamHeadRecord::Indexed(head) = record else {
            return Err(StreamError::InvalidRequest(
                "legacy stream requires indexed migration".into(),
            ));
        };
        if head.stream_name != name || head.metadata_group_id != self.group_id {
            return Err(StreamError::Corruption(
                "indexed head identity or metadata group is invalid".into(),
            ));
        }
        Ok(head)
    }

    async fn put_directory_immutable(&self, key: &[u8], bytes: &[u8]) -> Result<()> {
        match self.kv.put_cas(self.store_id, self.group_id, key, bytes, 0).await {
            Ok(_) => Ok(()),
            Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown) => {
                match self
                    .kv
                    .get(self.store_id, self.group_id, key, ReadMode::Linearizable, None)
                    .await
                    .map_err(kv_error)?
                {
                    GetOutcome::Found { value, .. } if value.as_ref() == bytes => Ok(()),
                    GetOutcome::Found { .. } => Err(StreamError::Corruption(
                        "immutable directory key contains different bytes".into(),
                    )),
                    GetOutcome::NotFound => Err(StreamError::WriteStalled),
                }
            }
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(error) => Err(kv_error(error)),
        }
    }
}

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
