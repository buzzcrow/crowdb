// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Creation and ownership-fenced recovery of writer handles.

use super::{
    Arc, ChunkStream, Result, StreamChunkStore, StreamConfig, StreamError, StreamMetadataStore, StreamName,
    StreamRecord, StreamRecordState, StreamRegistry,
};
use crate::perf::{perf, PhaseTimer};

impl ChunkStream {
    /// Creates and publishes a new registered stream.
    ///
    /// # Errors
    ///
    /// Returns a typed storage, validation, fencing, or duplicate-name error.
    pub async fn create(
        record: StreamRecord,
        writer_epoch: u64,
        config: StreamConfig,
        registry: Arc<dyn StreamRegistry>,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        config.validate()?;
        if record.state != StreamRecordState::Active
            || record.metadata_group_id == 0
            || record.record_version != 1
            || record.created_at_ms == 0
            || writer_epoch == 0
        {
            return Err(StreamError::InvalidRequest(
                "stream record or writer epoch is invalid".into(),
            ));
        }
        if registry.load(record.stream_name).await?.is_some() {
            return Err(StreamError::InvalidRequest("stream already exists".into()));
        }
        registry.create(record.clone()).await?;
        Self::initialize(record, writer_epoch, config, registry, metadata, chunks).await
    }

    /// Initializes stream metadata for an active record already published by
    /// the control plane.
    ///
    /// # Errors
    ///
    /// Returns a typed error if the record is absent or inactive, metadata
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
        let record = registry
            .load(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream record does not exist".into()))?;
        if record.state != StreamRecordState::Active
            || record.metadata_group_id == 0
            || record.record_version != 1
            || record.created_at_ms == 0
            || writer_epoch == 0
        {
            return Err(StreamError::InvalidRequest(
                "stream record or writer epoch is invalid".into(),
            ));
        }
        if metadata
            .record_store(record.metadata_group_id)?
            .load_tail(stream_name)
            .await?
            .is_some()
        {
            return Err(StreamError::InvalidRequest(
                "stream metadata already exists".into(),
            ));
        }
        Self::initialize(record, writer_epoch, config, registry, metadata, chunks).await
    }

    async fn initialize(
        record: StreamRecord,
        writer_epoch: u64,
        config: StreamConfig,
        registry: Arc<dyn StreamRegistry>,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        let metadata = metadata.record_store(record.metadata_group_id)?;
        let previous = metadata
            .candidates(record.stream_name)
            .await?
            .into_iter()
            .find(|candidate| candidate.record().sequence == 0);
        let authority = registry.claim_writer(record.stream_name, writer_epoch).await?;
        chunks.bind_writer(record.stream_name, writer_epoch, authority.clone());
        let initialized = async {
            let active = chunks
                .allocate_mirrored(record.stream_name, writer_epoch, record.purpose)
                .await?;
            authority.check()?;
            let record = crowdb_protocol::chunk_stream::StreamChunkRecord {
                stream_id: record.stream_name,
                purpose: record.purpose,
                chunk_id: active.chunk_id,
                state: crowdb_protocol::chunk_stream::StreamChunkState::Active,
                logical_start: Some(0),
                logical_end: Some(0),
                sequence: 0,
                writer_epoch,
                generation: 1,
                owner_session: owner_session()?,
                trim_offset: 0,
                closed: false,
                mappings: crowdb_protocol::chunk_stream::StreamMappings::default(),
            };
            let _publication = PhaseTimer::start(&perf().publish);
            let mut candidate = record.clone();
            candidate.state = crowdb_protocol::chunk_stream::StreamChunkState::Candidate;
            candidate.logical_start = None;
            candidate.logical_end = None;
            candidate.generation = 0;
            let candidate = metadata.reserve_initial(&candidate, previous.as_ref()).await?;
            authority.check()?;
            let snapshot = metadata.publish(None, record, None, Some(&candidate)).await?;
            authority.check()?;
            Self::start(
                config,
                metadata,
                chunks,
                snapshot,
                Some(active),
                Some(authority.clone()),
            )
            .await
        }
        .await;
        if initialized.is_err() {
            // No data has been admitted; retain chunk expiry fences for abandoned allocations.
            if let Err(error) = authority.release().await {
                tracing::warn!(%error, "failed initialization owner release is unconfirmed");
            }
        }
        initialized
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
        let record = registry
            .load(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream record does not exist".into()))?;
        if record.state != StreamRecordState::Active {
            return Err(StreamError::InvalidRequest("stream record is not active".into()));
        }
        let metadata = metadata.record_store(record.metadata_group_id)?;
        let snapshot = metadata
            .load_tail(stream_name)
            .await?
            .ok_or_else(|| StreamError::Corruption("Stream tail record does not exist".into()))?;
        let base = snapshot.record();
        if base.purpose != record.purpose {
            return Err(StreamError::Corruption(
                "Stream purpose differs from identity record".into(),
            ));
        }
        if base.writer_epoch > writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        let authority = registry.claim_writer(stream_name, writer_epoch).await?;
        chunks.bind_writer(stream_name, writer_epoch, authority.clone());
        let recovered = async {
            // Claiming may wait for the old writer to drain and publish its
            // final checkpoint. Recover from that completed publication.
            let snapshot = metadata
                .load_tail(stream_name)
                .await?
                .ok_or_else(|| StreamError::Corruption("Stream tail record does not exist".into()))?;
            if snapshot.record().purpose != record.purpose {
                return Err(StreamError::Corruption(
                    "Stream purpose differs from identity record".into(),
                ));
            }
            if snapshot.record().writer_epoch > writer_epoch {
                return Err(StreamError::StaleWriter);
            }
            Self::recover_owned(
                writer_epoch,
                config,
                metadata,
                chunks,
                snapshot,
                authority.clone(),
            )
            .await
        }
        .await;
        if recovered.is_err() {
            if let Err(error) = authority.release().await {
                tracing::warn!(%error, "failed recovery owner release is unconfirmed");
            }
        }
        recovered
    }

    async fn recover_owned(
        writer_epoch: u64,
        config: StreamConfig,
        metadata: crate::StreamChunkRecordStore,
        chunks: Arc<dyn StreamChunkStore>,
        snapshot: crate::StreamChunkRecordSnapshot,
        authority: Arc<crate::StreamWriterSession>,
    ) -> Result<Self> {
        let base = snapshot.record();
        let stream_name = base.stream_id;
        let session = owner_session()?;
        let mut completed = base.clone();
        let mut active = super::state::load_active(&snapshot, chunks.as_ref()).await?;
        let mut previous = None;
        if let Some(descriptor) = active.take() {
            let recovered = chunks
                .recover_tail(stream_name, writer_epoch, &descriptor)
                .await?;
            let mut lengths = base
                .mappings
                .lengths()
                .map_err(|error| StreamError::Corruption(error.to_string()))?;
            for extent in recovered.extents {
                lengths.push(
                    u32::try_from(extent.logical_length)
                        .map_err(|_| StreamError::Corruption("recovered append length".into()))?,
                );
            }
            chunks
                .seal(descriptor.chunk_id, writer_epoch, recovered.physical_end)
                .await?;
            completed.logical_end = Some(recovered.logical_end);
            completed.mappings = crowdb_protocol::chunk_stream::StreamMappings::new(&lengths)
                .map_err(|error| StreamError::Corruption(error.to_string()))?;
            completed.state = crowdb_protocol::chunk_stream::StreamChunkState::Sealed;
            completed.writer_epoch = writer_epoch;
            completed.owner_session = session;
            let mut successor = chunks
                .allocate_mirrored(stream_name, writer_epoch, base.purpose)
                .await?;
            successor.logical_start = recovered.logical_end;
            let mut next = completed.clone();
            next.chunk_id = successor.chunk_id;
            next.state = crowdb_protocol::chunk_stream::StreamChunkState::Active;
            next.logical_start = Some(recovered.logical_end);
            next.sequence = base
                .sequence
                .checked_add(1)
                .ok_or_else(|| StreamError::Corruption("chunk sequence overflow".into()))?;
            next.mappings = crowdb_protocol::chunk_stream::StreamMappings::default();
            previous = Some(completed);
            completed = next;
            active = Some(successor);
        }
        completed.writer_epoch = writer_epoch;
        completed.owner_session = session;
        completed.generation = base
            .generation
            .checked_add(1)
            .ok_or_else(|| StreamError::Corruption("Stream generation overflow".into()))?;
        if let Some(previous) = &mut previous {
            previous.generation = completed.generation;
        }
        authority.check()?;
        let _publication = PhaseTimer::start(&perf().publish);
        let snapshot = metadata
            .publish(Some(&snapshot), completed, previous, None)
            .await?;
        super::successor::reclaim_excluded(&metadata, chunks.as_ref(), stream_name, writer_epoch).await?;
        Self::start(
            config,
            metadata,
            chunks,
            snapshot,
            active,
            Some(authority.clone()),
        )
        .await
    }
}

fn owner_session() -> Result<[u8; 16]> {
    crowdb_protocol::chunk_stream::StreamProcessLease::generate_boot_nonce()
        .map_err(|error| StreamError::Internal(error.to_string()))
}
