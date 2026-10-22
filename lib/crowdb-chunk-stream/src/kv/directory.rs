// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV metadata facade for the shared indexed-directory state machine.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{
    StreamDirectoryHead, StreamDirectoryPageId, StreamMigrationProgress, StreamName, StreamRootIdentity,
};

use super::KvStreamMetadataStore;
use crate::directory::DirectoryMetadataStore;
pub use crate::directory::{
    DirectoryCacheSnapshot, DirectoryHeadSnapshot, StreamMigrationSnapshot, StreamMigrationStep,
    DIRECTORY_PAGE_CACHE_SLOTS,
};
use crate::{DirectoryMutation, Result, StreamChunkStore, StreamDirectoryStore};

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

    /// # Errors
    /// Returns migration marker/source IO/version/scope errors.
    pub async fn load_directory_migration(
        &self,
        name: StreamName,
    ) -> Result<Option<StreamMigrationSnapshot>> {
        self.directory.load_directory_migration(name).await
    }

    /// # Errors
    /// Returns legacy source preservation or migration head CAS errors.
    pub async fn begin_directory_migration(
        &self,
        name: StreamName,
        epoch: u64,
        session: [u8; 16],
    ) -> Result<StreamMigrationSnapshot> {
        self.directory
            .begin_directory_migration(name, epoch, session)
            .await
    }

    /// # Errors
    /// Returns conversion progress IO/version/scope errors.
    pub async fn load_directory_migration_progress(
        &self,
        snapshot: &StreamMigrationSnapshot,
    ) -> Result<Option<StreamMigrationProgress>> {
        self.directory.load_directory_migration_progress(snapshot).await
    }

    /// # Errors
    /// Returns bounded conversion IO/validation/CAS errors.
    pub async fn advance_directory_migration(
        &self,
        snapshot: &StreamMigrationSnapshot,
        chunks: &dyn StreamChunkStore,
        limit: usize,
    ) -> Result<StreamMigrationStep> {
        self.directory
            .advance_directory_migration(snapshot, chunks, limit)
            .await
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
