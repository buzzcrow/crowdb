// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production assembly for KV metadata and mirrored chunk storage.

use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use crowdb_chunk_client::{ChunkIoClient, ChunkReadPolicy};
use crowdb_kv_client::CrowdbKvClient;
use crowdb_protocol::chunk_stream::{StreamName, StreamRecord};

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
    owners: SkipMap<(u64, StreamName), ChunkStream>,
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
        Self::new_with_mirror_copies(kv, chunk_io, writer_lease_ms, read_policy, config, 2)
    }

    /// Builds the stream runtime with an explicit stream mirror count.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid writer lease, mirror count, read policy, or stream configuration.
    pub fn new_with_mirror_copies(
        kv: Arc<CrowdbKvClient>,
        chunk_io: &ChunkIoClient,
        writer_lease_ms: u64,
        read_policy: ChunkReadPolicy,
        mut config: StreamConfig,
        mirror_copies: u32,
    ) -> Result<Self> {
        config.validate()?;
        config.liveness_interval = config
            .liveness_interval
            .min(std::time::Duration::from_millis((writer_lease_ms / 3).max(1)));
        let (allocator, disk_writer) = chunk_io.storage_parts();
        let chunks = Arc::new(ProductionStreamChunkStore::new_with_mirror_copies_and_capacity(
            allocator,
            disk_writer,
            writer_lease_ms,
            read_policy,
            mirror_copies,
            config.chunk_capacity_bytes,
        )?);
        Ok(Self {
            registry: Arc::new(KvStreamRegistry::new(Arc::clone(&kv))),
            kv,
            chunks,
            config,
            owners: SkipMap::new(),
        })
    }

    /// Returns the group-0 registry adapter used by the control plane.
    #[must_use]
    pub fn registry(&self) -> Arc<KvStreamRegistry> {
        Arc::clone(&self.registry)
    }

    /// Initializes metadata for a record already activated by the control
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
        let record = self.active_record(stream_name).await?;
        let metadata = self.metadata(metadata_store_id, record.metadata_group_id)?;
        let stream = ChunkStream::create_registered(
            stream_name,
            writer_epoch,
            self.config.clone(),
            self.registry.clone(),
            metadata,
            self.chunks.clone(),
        )
        .await?;
        self.owners
            .insert((metadata_store_id, stream_name), stream.clone());
        Ok(stream)
    }

    /// Reopens the authoritative stream record under a current writer epoch.
    /// A higher epoch drains and releases this runtime's cached predecessor
    /// before claiming the successor. The caller supplies assignment authority.
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
        if let Some(owner) = self.owners.get(&(metadata_store_id, stream_name)) {
            if owner.value().writer_epoch() == writer_epoch && owner.value().writer_context().is_ok() {
                return Ok(owner.value().clone());
            }
            if owner.value().writer_epoch() > writer_epoch {
                return Err(StreamError::StaleWriter);
            }
            if owner.value().writer_epoch() < writer_epoch {
                let previous = owner.value().clone();
                drop(owner);
                previous.release_writer().await?;
            }
        }
        let record = self.active_record(stream_name).await?;
        let metadata = self.metadata(metadata_store_id, record.metadata_group_id)?;
        let stream = ChunkStream::open(
            stream_name,
            writer_epoch,
            self.config.clone(),
            self.registry.clone(),
            metadata,
            self.chunks.clone(),
        )
        .await?;
        self.owners
            .insert((metadata_store_id, stream_name), stream.clone());
        Ok(stream)
    }

    /// Opens an exact-epoch read-only stream view without changing writer
    /// authority or rotating the active chunk.
    ///
    /// # Errors
    ///
    /// Returns a registry, metadata, configuration, corruption, or epoch error.
    pub async fn open_read_only(
        &self,
        stream_name: StreamName,
        metadata_store_id: u64,
        writer_epoch: u64,
    ) -> Result<ChunkStream> {
        let record = self.active_record(stream_name).await?;
        if let Some(owner) = self.owners.get(&(metadata_store_id, stream_name)) {
            if owner.value().writer_epoch() == writer_epoch && owner.value().writer_context().is_ok() {
                return owner.value().capture_read_only().await;
            }
        }
        let metadata = self.metadata(metadata_store_id, record.metadata_group_id)?;
        ChunkStream::open_read_only(
            stream_name,
            writer_epoch,
            self.config.clone(),
            self.registry.clone(),
            metadata,
            self.chunks.clone(),
        )
        .await
    }

    /// Opens the current physical WAL generation without claiming writer authority.
    /// Historical record authority remains the replay caller's responsibility.
    ///
    /// # Errors
    /// Returns a record, state, durable cursor or concurrent epoch error.
    pub async fn open_read_only_current(
        &self,
        stream_name: StreamName,
        metadata_store_id: u64,
    ) -> Result<ChunkStream> {
        let record = self.active_record(stream_name).await?;
        let metadata = self.metadata(metadata_store_id, record.metadata_group_id)?;
        let state = metadata
            .record_store(record.metadata_group_id)?
            .load_tail(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("read-only stream state does not exist".into()))?;
        self.open_read_only(stream_name, metadata_store_id, state.record().writer_epoch)
            .await
    }

    async fn active_record(&self, stream_name: StreamName) -> Result<StreamRecord> {
        let registry: &dyn StreamRegistry = self.registry.as_ref();
        let record = registry
            .load(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream record does not exist".into()))?;
        if record.state != crowdb_protocol::chunk_stream::StreamRecordState::Active {
            return Err(StreamError::InvalidRequest("stream record is not active".into()));
        }
        Ok(record)
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
