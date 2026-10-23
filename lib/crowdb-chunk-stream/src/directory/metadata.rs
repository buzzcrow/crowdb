// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{
    decode_stream_head_record, StreamDirectoryHead, StreamDirectoryPageId, StreamName, StreamRootIdentity,
};
use crowdb_protocol::key::{
    BinaryKey, StreamDirectoryPageKey, StreamDirectoryRootKey, StreamManifestHeadKey,
};
use futures::{stream, StreamExt, TryStreamExt};

use crate::{DirectoryMutation, Result, StreamDirectoryStore, StreamError};
use crowdb_kv_client::GetOutcome;
use std::sync::Arc;

mod cache;
mod collection;
mod control;
mod pin;
mod publication;
mod publication_retirement;
pub use publication_retirement::DirectoryPublicationRetirement;
mod records;
mod retirement;
mod span;
pub use collection::{DirectoryChunkRelease, DirectoryCollection, DirectoryCollectionStep};
pub use pin::DirectoryRootPinSnapshot;
mod scan;
use cache::DirectoryRecordCache;
pub use cache::{DirectoryCacheSnapshot, DIRECTORY_PAGE_CACHE_SLOTS};
pub(crate) use records::DirectoryRecords;
pub use scan::{DirectoryKeyScan, MAX_DIRECTORY_SCAN_KEYS};

/// Shared indexed metadata state machine over conditional immutable records.
#[derive(Clone)]
pub struct DirectoryMetadataStore {
    records: Arc<dyn DirectoryRecords>,
    store_id: u64,
    group_id: u64,
    directory_pages: Arc<DirectoryRecordCache>,
}

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

impl DirectoryMetadataStore {
    pub(crate) fn new(records: Arc<dyn DirectoryRecords>, store_id: u64, group_id: u64) -> Result<Self> {
        if group_id == 0 {
            return Err(StreamError::InvalidRequest(
                "directory metadata requires a nonzero group".into(),
            ));
        }
        Ok(Self {
            records,
            store_id,
            group_id,
            directory_pages: Arc::new(DirectoryRecordCache::default()),
        })
    }

    async fn store_directory_pages(&self, mutation: &DirectoryMutation) -> Result<()> {
        stream::iter(mutation.pages())
            .map(|page| async move {
                let key = StreamDirectoryPageKey {
                    stream_name: mutation.index().stream_name(),
                    page_id: page.reference().id,
                }
                .to_bytes();
                self.put_directory_immutable(&key, page.bytes()).await
            })
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }
    #[must_use]
    pub fn directory_cache_snapshot(&self) -> DirectoryCacheSnapshot {
        self.directory_pages.snapshot()
    }
    /// # Errors
    /// Rejects unknown versions and wrong metadata scope.
    pub async fn load_directory_head(&self, name: StreamName) -> Result<Option<DirectoryHeadSnapshot>> {
        let key = StreamManifestHeadKey { stream_name: name }.to_bytes();
        match self.records.get(&key).await.map_err(kv_error)? {
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
        match self.records.get(&key).await.map_err(kv_error)? {
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
        let head = decode_stream_head_record(bytes)
            .map_err(|error| StreamError::Corruption(format!("stream head decode failed: {error}")))?;
        if head.stream_name != name || head.metadata_group_id != self.group_id {
            return Err(StreamError::Corruption(
                "indexed head identity or metadata group is invalid".into(),
            ));
        }
        Ok(head)
    }

    async fn put_directory_immutable(&self, key: &[u8], bytes: &[u8]) -> Result<()> {
        match self.records.put_cas(key, bytes, 0).await {
            Ok(_) => Ok(()),
            Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown) => {
                match self.records.get(key).await.map_err(kv_error)? {
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
impl StreamDirectoryStore for DirectoryMetadataStore {
    async fn load_directory_record(
        &self,
        stream_name: StreamName,
        page_id: StreamDirectoryPageId,
    ) -> Result<Option<Bytes>> {
        if let Some(bytes) = self.directory_pages.get(stream_name, page_id) {
            return Ok(Some(bytes));
        }
        let key = StreamDirectoryPageKey { stream_name, page_id }.to_bytes();
        match self.records.get(&key).await.map_err(kv_error)? {
            GetOutcome::Found { value, .. } => {
                self.directory_pages.insert(stream_name, page_id, &value)?;
                Ok(Some(value))
            }
            GetOutcome::NotFound => Ok(None),
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn kv_error(error: crowdb_kv_client::Error) -> StreamError {
    StreamError::Internal(format!("directory storage operation failed: {error}"))
}
