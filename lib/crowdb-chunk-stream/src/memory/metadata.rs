// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{record_backend::MemoryRecordBackend, MemoryStreamStore};
use crate::{Result, StreamChunkRecordSnapshot, StreamChunkRecordStore, StreamMetadataStore, StreamName};
use std::sync::Arc;
impl StreamMetadataStore for MemoryStreamStore {
    fn record_store(&self, group: u64) -> Result<StreamChunkRecordStore> {
        StreamChunkRecordStore::new(
            Arc::new(MemoryRecordBackend::new(
                self.state.clone(),
                group,
                self.record_faults.clone(),
            )),
            group,
        )
    }
}
impl MemoryStreamStore {
    /// # Errors
    /// Rejects corrupt stored record bytes.
    pub async fn chunk_records_for_tests(
        &self,
        id: StreamName,
    ) -> Result<Vec<crowdb_protocol::chunk_stream::StreamChunkRecord>> {
        use crowdb_protocol::key::{BinaryKey, StreamChunkRecordKey, StreamChunkRecordOrder};
        let state = self.state.lock().await;
        state
            .metadata_records
            .iter()
            .filter(|((_, key), _)| {
                StreamChunkRecordKey::from_bytes(key).is_ok_and(|key| {
                    key.stream_id == id && matches!(key.order, StreamChunkRecordOrder::Attached(_))
                })
            })
            .map(|(_, record)| {
                crowdb_protocol::chunk_stream::decode_stream_chunk_record(&record.value)
                    .map_err(|error| crate::StreamError::Corruption(error.to_string()))
            })
            .collect()
    }

    /// # Errors
    /// Rejects missing/corrupt attached records and unavailable chunk capacity.
    pub async fn active_checkpoint_for_tests(&self, id: StreamName) -> Result<crate::ActiveChunkDescriptor> {
        let snapshot = self
            .load_tail_for_tests(id)
            .await?
            .ok_or(crate::StreamError::WriteStalled)?;
        let record = snapshot.record();
        let capacity = crate::StreamChunkStore::current_capacity(self, record.chunk_id).await?;
        let physical = snapshot.index().physical_end();
        Ok(crate::ActiveChunkDescriptor {
            chunk_id: record.chunk_id,
            physical_start: physical,
            logical_start: record.logical_end.ok_or(crate::StreamError::WriteStalled)?,
            acknowledged_cursor: physical,
            capacity,
        })
    }
    /// # Errors
    /// Returns the same typed errors as the production record state machine.
    pub async fn load_tail_for_tests(&self, id: StreamName) -> Result<Option<StreamChunkRecordSnapshot>> {
        let group = self.state.lock().await.metadata_groups.get(&id).copied();
        match group {
            Some(group) => self.record_store(group)?.load_tail(id).await,
            None => Ok(None),
        }
    }
}
