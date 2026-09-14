// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production assembly for KV metadata and mirrored chunk storage.

use std::sync::Arc;

use crowdb_chunk_client::{ChunkIoClient, ChunkReadPolicy};
use crowdb_kv_client::CrowdbKvClient;
use crowdb_protocol::chunk_stream::{StreamBinding, StreamName};

use crate::{
    ChunkStream, KvStreamMetadataStore, KvStreamRegistry, ProductionStreamChunkStore, Result,
    StreamChunkStore, StreamConfig, StreamError, StreamMetadataStore, StreamRegistry,
};

/// Shared production dependencies used to create and reopen stream handles.
pub struct ProductionStreamRuntime {
    kv: Arc<CrowdbKvClient>,
    registry: Arc<KvStreamRegistry>,
    chunks: Arc<ProductionStreamChunkStore>,
    config: StreamConfig,
}

impl ProductionStreamRuntime {
    /// Builds the stream runtime over one discovered chunk IO client.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid writer lease, read policy, or stream
    /// configuration.
    pub fn new(
        kv: Arc<CrowdbKvClient>,
        chunk_io: &ChunkIoClient,
        writer_lease_ms: u64,
        read_policy: ChunkReadPolicy,
        config: StreamConfig,
    ) -> Result<Self> {
        config.validate()?;
        let (allocator, disk_writer) = chunk_io.storage_parts();
        let chunks = Arc::new(ProductionStreamChunkStore::new(
            allocator,
            disk_writer,
            writer_lease_ms,
            read_policy,
        )?);
        Ok(Self {
            registry: Arc::new(KvStreamRegistry::new(Arc::clone(&kv))),
            kv,
            chunks,
            config,
        })
    }

    /// Returns the group-0 registry adapter used by the control plane.
    #[must_use]
    pub fn registry(&self) -> Arc<KvStreamRegistry> {
        Arc::clone(&self.registry)
    }

    /// Initializes metadata for a binding already activated by the control
    /// plane.
    ///
    /// # Errors
    ///
    /// Returns a registry, metadata, configuration, or fencing error.
    pub async fn create_registered(
        &self,
        stream_name: StreamName,
        metadata_store_id: u64,
        writer_epoch: u64,
    ) -> Result<ChunkStream> {
        let binding = self.active_binding(stream_name).await?;
        let metadata = self.metadata(metadata_store_id, binding.metadata_group_id)?;
        ChunkStream::create_registered(
            stream_name,
            writer_epoch,
            self.config.clone(),
            self.registry.clone(),
            metadata,
            self.chunks.clone(),
        )
        .await
    }

    /// Reopens the authoritative stream binding under a current writer epoch.
    ///
    /// # Errors
    ///
    /// Returns a registry, metadata, chunk IO, or fencing error.
    pub async fn open(
        &self,
        stream_name: StreamName,
        metadata_store_id: u64,
        writer_epoch: u64,
    ) -> Result<ChunkStream> {
        let binding = self.active_binding(stream_name).await?;
        let metadata = self.metadata(metadata_store_id, binding.metadata_group_id)?;
        ChunkStream::open(
            stream_name,
            writer_epoch,
            self.config.clone(),
            self.registry.clone(),
            metadata,
            self.chunks.clone(),
        )
        .await
    }

    async fn active_binding(&self, stream_name: StreamName) -> Result<StreamBinding> {
        let registry: &dyn StreamRegistry = self.registry.as_ref();
        let binding = registry
            .load(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream binding does not exist".into()))?;
        if binding.state != crowdb_protocol::chunk_stream::StreamBindingState::Active {
            return Err(StreamError::InvalidRequest("stream binding is not active".into()));
        }
        Ok(binding)
    }

    fn metadata(
        &self,
        metadata_store_id: u64,
        metadata_group_id: u64,
    ) -> Result<Arc<dyn StreamMetadataStore>> {
        Ok(Arc::new(KvStreamMetadataStore::new(
            Arc::clone(&self.kv),
            metadata_store_id,
            metadata_group_id,
        )?))
    }

    /// Returns the shared production chunk adapter for diagnostics and
    /// lifecycle integration.
    #[must_use]
    pub fn chunks(&self) -> Arc<dyn StreamChunkStore> {
        self.chunks.clone()
    }
}
