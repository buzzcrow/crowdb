// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Logical-interval retention and retryable whole-chunk reclamation.

use super::{
    corrupt,
    pin::{now_ms, PIN_GRACE_MS},
    publication::put,
    storage_error, StreamChunkRecordStore,
};
use crate::{Result, StreamChunkStore, StreamError};
use crowdb_kv_client::{BatchOp, GetOutcome};
use crowdb_protocol::chunk_stream::{StreamChunkState, StreamName};
use crowdb_protocol::key::{BinaryKey, StreamChunkRecordKey, StreamChunkRecordOrder};

#[derive(Default)]
pub struct StreamCollectionResult {
    pub records: u64,
    pub physical_bytes: u64,
    pub next: Option<u64>,
}

impl StreamChunkRecordStore {
    /// # Errors
    /// Rejects stale retention proofs, malformed spans and failed physical release.
    /// A deleting record remains retryable until the chunk layer confirms release.
    pub async fn collect(
        &self,
        stream: StreamName,
        retained_generation: u64,
        after: Option<u64>,
        max_records: usize,
        byte_budget: u64,
        chunks: &dyn StreamChunkStore,
    ) -> Result<StreamCollectionResult> {
        if max_records == 0 || max_records > 128 || byte_budget == 0 {
            return Err(StreamError::InvalidRequest("Stream collection budget".into()));
        }
        let tail = self.load_tail(stream).await?.ok_or(StreamError::WriteStalled)?;
        let pins = self.read_pins(stream).await?;
        if self.prune_pins(&tail, &pins).await? {
            return Ok(StreamCollectionResult {
                next: after,
                ..StreamCollectionResult::default()
            });
        }
        let after = after
            .map(|start| {
                StreamChunkRecordKey {
                    stream_id: stream,
                    order: StreamChunkRecordOrder::Attached(start),
                }
                .to_bytes()
            })
            .unwrap_or_default();
        let records = self
            .backend
            .scan_keys(
                &StreamChunkRecordKey::attached_prefix(stream),
                &after,
                u32::try_from(max_records)
                    .map_err(|_| StreamError::InvalidRequest("record budget overflow".into()))?,
            )
            .await
            .map_err(storage_error)?;
        let more = records.truncated;
        let mut result = StreamCollectionResult::default();
        let mut decoded_bytes = 0_u64;
        for (key, _) in records.items {
            if decoded_bytes >= byte_budget {
                return Ok(result);
            }
            let GetOutcome::Found { value, revision } =
                self.backend.get(&key).await.map_err(storage_error)?
            else {
                return Err(StreamError::Backpressure);
            };
            decoded_bytes = decoded_bytes.saturating_add(value.len() as u64);
            let snapshot = Self::decode(&key, &value, revision, stream)?;
            let record = &snapshot.record;
            let start = record
                .logical_start
                .ok_or_else(|| corrupt("collection start missing"))?;
            let end = record
                .logical_end
                .ok_or_else(|| corrupt("collection end missing"))?;
            result.next = Some(start);
            if record.chunk_id == tail.record.chunk_id
                || end > tail.record.trim_offset
                || record.generation >= retained_generation
                || !matches!(
                    record.state,
                    StreamChunkState::Sealed | StreamChunkState::Deleting
                )
            {
                continue;
            }
            if pins.iter().any(|pin| {
                pin.expires.saturating_add(PIN_GRACE_MS) > now_ms()
                    && (pin.active == Some(record.chunk_id) || (pin.begin < end && pin.end > start))
            }) {
                continue;
            }
            if record.state != StreamChunkState::Deleting {
                let mut deleting = record.as_ref().clone();
                deleting.state = StreamChunkState::Deleting;
                // A changed tail revision invalidates the entire pin observation.
                self.fenced_batch(&tail, vec![put(&deleting)?]).await?;
            }
            let reclaimed = chunks
                .release_trimmed(record.chunk_id, end, snapshot.index.physical_end())
                .await?;
            let current = self.load_tail(stream).await?.ok_or(StreamError::WriteStalled)?;
            self.fenced_batch(&current, vec![BatchOp::Delete { key }]).await?;
            result.records += 1;
            result.physical_bytes = result.physical_bytes.saturating_add(reclaimed.reclaimed_bytes);
            // Re-capture retention before another target because deletion changed its revision.
            return Ok(result);
        }
        if !more {
            result.next = None;
        }
        Ok(result)
    }
}
