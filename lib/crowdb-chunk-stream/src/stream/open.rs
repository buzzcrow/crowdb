// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Creation and ownership-fenced recovery of writer handles.

use super::{
    build_extent_pages, collect_extents, fences_for, load_extent_pages, recovery, validate_manifest, Arc,
    ChunkStream, Ordering, Result, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig,
    StreamError, StreamManifest, StreamMetadataStore, StreamName, StreamRegistry,
};

impl ChunkStream {
    /// Creates and publishes a new registered stream.
    ///
    /// # Errors
    ///
    /// Returns a typed storage, validation, fencing, or duplicate-name error.
    pub async fn create(
        binding: StreamBinding,
        writer_epoch: u64,
        config: StreamConfig,
        registry: Arc<dyn StreamRegistry>,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        config.validate()?;
        if binding.state != StreamBindingState::Active
            || binding.metadata_group_id == 0
            || binding.binding_generation == 0
            || writer_epoch == 0
        {
            return Err(StreamError::InvalidRequest(
                "stream binding or writer epoch is invalid".into(),
            ));
        }
        if registry.load(binding.stream_name).await?.is_some() {
            return Err(StreamError::InvalidRequest("stream already exists".into()));
        }
        registry.create(binding.clone()).await?;
        Self::initialize(binding, writer_epoch, config, registry, metadata, chunks).await
    }

    /// Initializes stream metadata for an active binding already published by
    /// the control plane.
    ///
    /// # Errors
    ///
    /// Returns a typed error if the binding is absent or inactive, metadata
    /// already exists, or initial metadata publication fails.
    pub async fn create_registered(
        stream_name: StreamName,
        writer_epoch: u64,
        config: StreamConfig,
        registry: Arc<dyn StreamRegistry>,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        config.validate()?;
        let binding = registry
            .load(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream binding does not exist".into()))?;
        if binding.state != StreamBindingState::Active
            || binding.metadata_group_id == 0
            || binding.binding_generation == 0
            || writer_epoch == 0
        {
            return Err(StreamError::InvalidRequest(
                "stream binding or writer epoch is invalid".into(),
            ));
        }
        if metadata.load_current(stream_name).await?.is_some() {
            return Err(StreamError::InvalidRequest(
                "stream metadata already exists".into(),
            ));
        }
        Self::initialize(binding, writer_epoch, config, registry, metadata, chunks).await
    }

    async fn initialize(
        binding: StreamBinding,
        writer_epoch: u64,
        config: StreamConfig,
        registry: Arc<dyn StreamRegistry>,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        let authority = registry.claim_writer(binding.stream_name, writer_epoch).await?;
        chunks.bind_writer(binding.stream_name, writer_epoch, authority.clone());
        let manifest = StreamManifest {
            purpose: binding.purpose,
            stream_name: binding.stream_name,
            metadata_group_id: binding.metadata_group_id,
            writer_epoch,
            generation: 1,
            trim_offset: 0,
            sealed_tail: 0,
            active: None,
            extent_pages: Vec::new(),
            previous_generation: None,
            closed: false,
        };
        metadata.publish(None, manifest.clone(), Vec::new()).await?;
        Self::start(
            config,
            metadata,
            chunks,
            manifest,
            Vec::new(),
            &[],
            Some(authority),
        )
    }

    /// Opens an existing stream and recovers its durable active cursor.
    ///
    /// # Errors
    ///
    /// Returns a typed storage, corruption, or stale-writer error.
    pub async fn open(
        stream_name: StreamName,
        writer_epoch: u64,
        config: StreamConfig,
        registry: Arc<dyn StreamRegistry>,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        config.validate()?;
        let binding = registry
            .load(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream binding does not exist".into()))?;
        if binding.state != StreamBindingState::Active {
            return Err(StreamError::InvalidRequest("stream binding is not active".into()));
        }
        let mut manifest = metadata
            .load_current(stream_name)
            .await?
            .ok_or_else(|| StreamError::Corruption("stream manifest does not exist".into()))?;
        if manifest.purpose != binding.purpose {
            return Err(StreamError::Corruption(
                "stream binding purpose differs from manifest".into(),
            ));
        }
        if manifest.metadata_group_id != binding.metadata_group_id || manifest.writer_epoch > writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        let pages = load_extent_pages(metadata.as_ref(), &manifest).await?;
        validate_manifest(&manifest, &pages)?;
        let authority = registry.claim_writer(stream_name, writer_epoch).await?;
        chunks.bind_writer(stream_name, writer_epoch, authority.clone());
        let expected = (manifest.writer_epoch, manifest.generation);
        let mut needs_publish = manifest.writer_epoch < writer_epoch;
        let mut rotate_active = false;
        let mut extents = collect_extents(&pages);
        let mut tail = manifest.sealed_tail;
        if let Some(active) = &mut manifest.active {
            let recovered = chunks.recover_tail(stream_name, writer_epoch, active).await?;
            tail = recovered.logical_end;
            recovery::append_recovered(active.chunk_id, recovered.extents, &mut extents)?;
            chunks
                .seal(active.chunk_id, writer_epoch, recovered.physical_end)
                .await?;
            active.acknowledged_cursor = recovered.physical_end;
            rotate_active = true;
        }
        manifest.writer_epoch = writer_epoch;
        if rotate_active {
            manifest.active.take();
            manifest.sealed_tail = tail;
            let mut successor = chunks
                .allocate_mirrored(stream_name, writer_epoch, manifest.purpose)
                .await?;
            successor.logical_start = tail;
            manifest.active = Some(successor);
            needs_publish = true;
        }
        if needs_publish {
            manifest.previous_generation = Some(manifest.generation);
            manifest.generation = manifest
                .generation
                .checked_add(1)
                .ok_or_else(|| StreamError::Corruption("manifest generation overflows".into()))?;
            let adopted_pages = build_extent_pages(
                &extents,
                manifest.stream_name,
                writer_epoch,
                manifest.generation,
                config.extent_page_entries,
            );
            manifest.extent_pages = fences_for(&adopted_pages);
            validate_manifest(&manifest, &adopted_pages)?;
            metadata
                .publish(Some(expected), manifest.clone(), adopted_pages)
                .await?;
        }
        let stream = Self::start(
            config,
            metadata,
            chunks,
            manifest,
            extents,
            &pages,
            Some(authority),
        )?;
        stream.tail.store(tail, Ordering::Release);
        Ok(stream)
    }
}
