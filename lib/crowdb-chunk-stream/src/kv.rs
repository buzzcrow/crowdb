// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production registry and metadata adapters backed by CROWDB KV.

use std::sync::Arc;

use async_trait::async_trait;
use crowdb_paxos_kv_client::{GetOutcome, PaxosKvClient, ReadMode};
use crowdb_protocol::chunk_stream::{StreamName, StreamRecord};
use crowdb_protocol::key::{BinaryKey, StreamRecordKey};

mod owner;
mod record_backend;

use crate::{Result, StreamError, StreamMetadataStore, StreamRegistry};

/// Group-0 readable stream record registry.
pub struct KvStreamRegistry {
    kv: Arc<PaxosKvClient>,
    processes: owner::OwnerProcesses,
}

impl KvStreamRegistry {
    #[must_use]
    pub fn new(kv: Arc<PaxosKvClient>) -> Self {
        Self {
            kv,
            processes: owner::OwnerProcesses::default(),
        }
    }
}

#[async_trait]
impl StreamRegistry for KvStreamRegistry {
    async fn claim_writer(&self, name: StreamName, epoch: u64) -> Result<Arc<crate::StreamWriterSession>> {
        let record = self
            .load(name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream record is absent".into()))?;
        self.acquire_owner(name, epoch, record.purpose).await
    }
    async fn load(&self, stream_name: StreamName) -> Result<Option<StreamRecord>> {
        let key = StreamRecordKey { stream_name }.to_bytes();
        match self
            .kv
            .get(0, 0, &key, ReadMode::Linearizable, None)
            .await
            .map_err(|error| kv_error(&error))?
        {
            GetOutcome::Found { value, .. } => {
                let record: StreamRecord = serde_json::from_slice(&value).map_err(|error| {
                    StreamError::Corruption(format!("stream record decode failed: {error}"))
                })?;
                if record.stream_name != stream_name
                    || record.metadata_group_id == 0
                    || record.record_version != 1
                    || record.created_at_ms == 0
                {
                    return Err(StreamError::Corruption(
                        "stream record identity or metadata group is invalid".into(),
                    ));
                }
                Ok(Some(record))
            }
            GetOutcome::NotFound => Ok(None),
        }
    }

    async fn create(&self, record: StreamRecord) -> Result<()> {
        if record.metadata_group_id == 0 || record.record_version != 1 || record.created_at_ms == 0 {
            return Err(StreamError::InvalidRequest(
                "stream record requires a valid current version, creation time and metadata group".into(),
            ));
        }
        let key = StreamRecordKey {
            stream_name: record.stream_name,
        }
        .to_bytes();
        let value = serde_json::to_vec(&record)
            .map_err(|error| StreamError::Internal(format!("stream record encode failed: {error}")))?;
        match self.kv.put_cas(0, 0, &key, &value, 0).await {
            Ok(_) => Ok(()),
            Err(crowdb_paxos_kv_client::Error::CasFailed { .. }) => {
                Err(StreamError::InvalidRequest("stream record already exists".into()))
            }
            Err(crowdb_paxos_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(crowdb_paxos_kv_client::Error::OutcomeUnknown) => {
                match self.load(record.stream_name).await? {
                    Some(observed) if observed == record => Ok(()),
                    _ => Err(StreamError::WriteStalled),
                }
            }
            Err(error) => Err(kv_error(&error)),
        }
    }
}

/// KV adapter for one nonzero Stream metadata group.
pub struct KvStreamMetadataStore {
    kv: Arc<PaxosKvClient>,
    store_id: u64,
    group_id: u64,
}
impl KvStreamMetadataStore {
    /// # Errors
    /// Rejects group zero, reserved for Stream identity and ownership records.
    pub fn new(kv: Arc<PaxosKvClient>, store_id: u64, group_id: u64) -> Result<Self> {
        if group_id == 0 {
            return Err(StreamError::InvalidRequest(
                "Stream chunk records require a nonzero KV group".into(),
            ));
        }
        Ok(Self {
            kv,
            store_id,
            group_id,
        })
    }
}
impl StreamMetadataStore for KvStreamMetadataStore {
    fn record_store(&self, group: u64) -> Result<crate::StreamChunkRecordStore> {
        if group != self.group_id {
            return Err(StreamError::InvalidRequest(
                "Stream record group differs from store".into(),
            ));
        }
        crate::StreamChunkRecordStore::new(
            Arc::new(record_backend::KvRecordBackend {
                kv: self.kv.clone(),
                store: self.store_id,
                group,
            }),
            group,
        )
    }
}
fn kv_error(error: &crowdb_paxos_kv_client::Error) -> StreamError {
    StreamError::ReadUnavailable(format!("Stream KV operation failed: {error}"))
}
