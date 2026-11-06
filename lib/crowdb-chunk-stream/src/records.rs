// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Ordered Stream chunk records over conditional KV operations.

use crate::{Result, StreamError};
use crowdb_paxos_kv_client::GetOutcome;
use crowdb_protocol::chunk_stream::{
    decode_stream_chunk_record, StreamChunkRecord, StreamChunkState, StreamMappingIndex, StreamName,
};
use crowdb_protocol::key::{BinaryKey, StreamChunkRecordKey, StreamChunkRecordOrder};
use std::sync::Arc;

mod backend;
pub(crate) use backend::RecordBackend;
mod cache;
mod candidate;
mod collection;
mod pin;
mod publication;
pub use collection::StreamCollectionResult;
pub use pin::StreamReadPin;

#[derive(Clone)]
pub struct StreamChunkRecordStore {
    backend: Arc<dyn RecordBackend>,
    group_id: u64,
    cache: Arc<cache::RecordCache>,
}

#[derive(Clone, Debug)]
pub struct StreamChunkRecordSnapshot {
    pub(crate) record: Arc<StreamChunkRecord>,
    pub(crate) index: Arc<StreamMappingIndex>,
    key: Vec<u8>,
    revision: u64,
}

impl StreamChunkRecordSnapshot {
    #[must_use]
    pub fn record(&self) -> &StreamChunkRecord {
        &self.record
    }
    #[must_use]
    pub const fn index(&self) -> &Arc<StreamMappingIndex> {
        &self.index
    }
}

impl StreamChunkRecordStore {
    pub(crate) fn new(backend: Arc<dyn RecordBackend>, group_id: u64) -> Result<Self> {
        if group_id == 0 {
            return Err(StreamError::InvalidRequest(
                "Stream chunk records require a nonzero metadata group".into(),
            ));
        }
        Ok(Self {
            backend,
            group_id,
            cache: Arc::new(cache::RecordCache::default()),
        })
    }

    #[must_use]
    pub const fn group_id(&self) -> u64 {
        self.group_id
    }

    /// # Errors
    /// Rejects IO, malformed records and incorrect key/value identities.
    pub async fn load_tail(&self, id: StreamName) -> Result<Option<StreamChunkRecordSnapshot>> {
        let prefix = StreamChunkRecordKey::attached_prefix(id);
        let records = self
            .backend
            .scan(&prefix, &[], 1, true)
            .await
            .map_err(storage_error)?;
        if records.items.len() > 1 || records.items.len() != records.commit_slots.len() {
            return Err(corrupt("tail scan shape"));
        }
        match records.items.into_iter().zip(records.commit_slots).next() {
            Some(((key, value), revision)) => Self::decode(&key, &value, revision, id).map(Some),
            None => Ok(None),
        }
    }

    /// # Errors
    /// Rejects missing/corrupt retained coverage and unavailable KV queries.
    pub async fn locate(&self, id: StreamName, offset: u64) -> Result<StreamChunkRecordSnapshot> {
        self.locate_for_read(id, offset).await.map(|(record, _)| record)
    }

    pub(crate) async fn locate_for_read(
        &self,
        id: StreamName,
        offset: u64,
    ) -> Result<(StreamChunkRecordSnapshot, bool)> {
        if let Some(record) = self.cache.find(id, offset) {
            return Ok((record, true));
        }
        let prefix = StreamChunkRecordKey::attached_prefix(id);
        let mut before = StreamChunkRecordKey {
            stream_id: id,
            order: StreamChunkRecordOrder::Attached(offset),
        }
        .to_bytes();
        // Reverse scan has an exclusive upper bound. No valid fixed-width key
        // extends this key, so appending zero includes an exact-start match.
        before.push(0);
        let records = self
            .backend
            .scan(&prefix, &before, 1, true)
            .await
            .map_err(storage_error)?;
        let Some(((key, value), revision)) = records.items.into_iter().zip(records.commit_slots).next()
        else {
            return Err(StreamError::ReadUnavailable(
                "Stream chunk record is absent".into(),
            ));
        };
        let record = Self::decode(&key, &value, revision, id)?;
        if record.record.logical_end.map_or(true, |end| offset >= end)
            || record.record.state == StreamChunkState::Deleting
        {
            return Err(corrupt("logical offset is not covered by the chunk record"));
        }
        self.cache.insert(record.clone());
        Ok((record, false))
    }

    fn decode(key: &[u8], bytes: &[u8], revision: u64, id: StreamName) -> Result<StreamChunkRecordSnapshot> {
        let key_value = StreamChunkRecordKey::from_bytes(key).map_err(|_| corrupt("record key"))?;
        let record = decode_stream_chunk_record(bytes).map_err(codec_error)?;
        if key_value.stream_id != id || record.stream_id != id || revision == 0 {
            return Err(corrupt("record identity"));
        }
        match key_value.order {
            StreamChunkRecordOrder::Attached(start)
                if record.state != StreamChunkState::Candidate && record.logical_start == Some(start) => {}
            StreamChunkRecordOrder::Candidate(sequence)
                if record.state == StreamChunkState::Candidate && record.sequence == sequence => {}
            _ => return Err(corrupt("record key/order differs from value")),
        }
        let index = Arc::new(record.validate().map_err(codec_error)?);
        Ok(StreamChunkRecordSnapshot {
            record: Arc::new(record),
            index,
            key: key.to_vec(),
            revision,
        })
    }

    async fn load_key(&self, key: &StreamChunkRecordKey) -> Result<Option<StreamChunkRecordSnapshot>> {
        let bytes = key.to_bytes();
        match self.backend.get(&bytes).await.map_err(storage_error)? {
            GetOutcome::NotFound => Ok(None),
            GetOutcome::Found { value, revision } => {
                Self::decode(&bytes, &value, revision, key.stream_id).map(Some)
            }
        }
    }
}

fn record_key(record: &StreamChunkRecord) -> Result<StreamChunkRecordKey> {
    Ok(StreamChunkRecordKey {
        stream_id: record.stream_id,
        order: if record.state == StreamChunkState::Candidate {
            StreamChunkRecordOrder::Candidate(record.sequence)
        } else {
            StreamChunkRecordOrder::Attached(
                record
                    .logical_start
                    .ok_or_else(|| corrupt("attached start is absent"))?,
            )
        },
    })
}

fn codec_error(error: impl std::fmt::Display) -> StreamError {
    StreamError::Corruption(error.to_string())
}
fn corrupt(reason: &str) -> StreamError {
    StreamError::Corruption(format!("invalid Stream chunk record: {reason}"))
}
fn storage_error(error: crowdb_paxos_kv_client::Error) -> StreamError {
    match error {
        crowdb_paxos_kv_client::Error::CasBusy | crowdb_paxos_kv_client::Error::CasFailed { .. } => {
            StreamError::Backpressure
        }
        crowdb_paxos_kv_client::Error::OutcomeUnknown => StreamError::WriteStalled,
        other => StreamError::ReadUnavailable(format!("Stream chunk record KV operation failed: {other}")),
    }
}
