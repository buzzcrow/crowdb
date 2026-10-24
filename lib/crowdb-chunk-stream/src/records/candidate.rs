// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{corrupt, publication::put, storage_error, StreamChunkRecordSnapshot, StreamChunkRecordStore};
use crate::{ChunkId, Result, StreamError};
use bytes::Bytes;
use crowdb_kv_client::{BatchOp, Error};
use crowdb_protocol::chunk_stream::{StreamChunkRecord, StreamChunkState, StreamName};
use crowdb_protocol::key::{BinaryKey, StreamChunkRecordKey, StreamChunkRecordOrder};

impl StreamChunkRecordStore {
    /// Reserves the first chunk under a concrete revision before attachment.
    /// # Errors
    /// Rejects an initialized stream, superseding owner or conflicting reservation.
    pub async fn reserve_initial(&self, candidate: &StreamChunkRecord) -> Result<StreamChunkRecordSnapshot> {
        candidate.validate().map_err(super::codec_error)?;
        if candidate.state != StreamChunkState::Candidate || candidate.sequence != 0 {
            return Err(corrupt("initial reservation state or sequence"));
        }
        if self.load_tail(candidate.stream_id).await?.is_some() {
            return Err(StreamError::StaleWriter);
        }
        let key = super::record_key(candidate)?;
        let current = self.load_key(&key).await?;
        if let Some(current) = &current {
            if current.record.as_ref() == candidate {
                return Ok(current.clone());
            }
            if current.record.writer_epoch >= candidate.writer_epoch {
                return Err(StreamError::StaleWriter);
            }
        }
        let revision = current.as_ref().map_or(0, |current| current.revision);
        match self
            .backend
            .batch(&[put(candidate)?], &key.to_bytes(), revision)
            .await
        {
            Ok(_) | Err(Error::CasFailed { .. } | Error::OutcomeUnknown) => {
                let registered = self.load_key(&key).await?.ok_or(StreamError::StaleWriter)?;
                if registered.record.as_ref() == candidate {
                    Ok(registered)
                } else {
                    Err(StreamError::StaleWriter)
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    /// # Errors
    /// Rejects changed ownership, conflicting reservation identity and unavailable KV.
    pub async fn register_candidate(
        &self,
        candidate: &StreamChunkRecord,
    ) -> Result<StreamChunkRecordSnapshot> {
        if candidate.state != StreamChunkState::Candidate {
            return Err(corrupt("reservation state"));
        }
        candidate.validate().map_err(super::codec_error)?;
        let tail = self
            .load_tail(candidate.stream_id)
            .await?
            .ok_or(StreamError::WriteStalled)?;
        if tail.record.writer_epoch != candidate.writer_epoch
            || tail.record.owner_session != candidate.owner_session
            || tail.record.closed
            || tail.record.sequence.checked_add(1) != Some(candidate.sequence)
        {
            return Err(StreamError::StaleWriter);
        }
        let key = StreamChunkRecordKey {
            stream_id: candidate.stream_id,
            order: StreamChunkRecordOrder::Candidate(candidate.sequence),
        };
        if let Some(current) = self.load_key(&key).await? {
            return if current.record.as_ref() == candidate {
                Ok(current)
            } else {
                Err(StreamError::Backpressure)
            };
        }
        self.fenced_batch(&tail, vec![put(candidate)?]).await?;
        self.load_key(&key).await?.ok_or(StreamError::WriteStalled)
    }

    /// # Errors
    /// Rejects unavailable/corrupt record scans.
    pub async fn candidates(&self, id: StreamName) -> Result<Vec<StreamChunkRecordSnapshot>> {
        let records = self
            .backend
            .scan(&StreamChunkRecordKey::candidate_prefix(id), &[], 8, false)
            .await
            .map_err(storage_error)?;
        if records.truncated || records.items.len() != records.commit_slots.len() {
            return Err(corrupt("candidate count exceeds bound"));
        }
        records
            .items
            .into_iter()
            .zip(records.commit_slots)
            .map(|((key, value), revision)| Self::decode(&key, &value, revision, id))
            .collect()
    }

    /// # Errors
    /// Rejects stale owner scope or an unresolved conditional delete.
    pub async fn remove_candidate(&self, id: StreamName, sequence: u64, chunk: ChunkId) -> Result<()> {
        let key = StreamChunkRecordKey {
            stream_id: id,
            order: StreamChunkRecordOrder::Candidate(sequence),
        };
        for _ in 0..16 {
            let tail = self.load_tail(id).await?.ok_or(StreamError::WriteStalled)?;
            let Some(current) = self.load_key(&key).await? else {
                return Ok(());
            };
            if current.record.chunk_id != chunk {
                return Err(StreamError::StaleWriter);
            }
            match self
                .fenced_batch(
                    &tail,
                    vec![BatchOp::Delete {
                        key: Bytes::from(key.to_bytes()),
                    }],
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(StreamError::Backpressure | StreamError::WriteStalled) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        Err(StreamError::Backpressure)
    }
}
