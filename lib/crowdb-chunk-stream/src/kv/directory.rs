// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV metadata facade for the shared indexed-directory state machine.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{
    StreamDirectoryHead, StreamDirectoryPageId, StreamName, StreamRootIdentity,
};

use super::KvStreamMetadataStore;
use crate::directory::DirectoryMetadataStore;
pub use crate::directory::{DirectoryCacheSnapshot, DirectoryHeadSnapshot, DIRECTORY_PAGE_CACHE_SLOTS};
use crate::{DirectoryMutation, Result, StreamDirectoryStore};

impl KvStreamMetadataStore {
    #[must_use]
    pub const fn directory_metadata(&self) -> &DirectoryMetadataStore {
        &self.directory
    }

    #[must_use]
    pub fn directory_cache_snapshot(&self) -> DirectoryCacheSnapshot {
        self.directory.directory_cache_snapshot()
    }

    /// # Errors
    /// Returns indexed head IO/version/scope errors.
    pub async fn load_directory_head(&self, name: StreamName) -> Result<Option<DirectoryHeadSnapshot>> {
        self.directory.load_directory_head(name).await
    }

    /// # Errors
    /// Returns retained root IO/version/scope errors.
    pub async fn load_directory_root(
        &self,
        name: StreamName,
        identity: StreamRootIdentity,
    ) -> Result<Option<StreamDirectoryHead>> {
        self.directory.load_directory_root(name, identity).await
    }

    /// # Errors
    /// Returns protected publication validation/CAS errors.
    pub async fn publish_directory(
        &self,
        expected: Option<&DirectoryHeadSnapshot>,
        head: StreamDirectoryHead,
        mutation: &DirectoryMutation,
    ) -> Result<DirectoryHeadSnapshot> {
        self.directory.publish_directory(expected, head, mutation).await
    }
}

#[async_trait]
impl StreamDirectoryStore for KvStreamMetadataStore {
    async fn load_directory_record(
        &self,
        name: StreamName,
        id: StreamDirectoryPageId,
    ) -> Result<Option<Bytes>> {
        self.directory.load_directory_record(name, id).await
    }
}
