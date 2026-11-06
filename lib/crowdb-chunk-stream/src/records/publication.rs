// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    codec_error, corrupt, record_key, storage_error, StreamChunkRecordSnapshot, StreamChunkRecordStore,
};
use crate::{Result, StreamError};
use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, Error};
use crowdb_protocol::chunk_stream::{encode_stream_chunk_record, StreamChunkRecord, StreamChunkState};
use crowdb_protocol::key::BinaryKey;

impl StreamChunkRecordStore {
    /// Atomically finalize an old tail and attach a prepared successor, or
    /// checkpoint/control-update the current tail. No Stream identity is changed.
    /// # Errors
    /// Rejects wrong scope/progress, stale writers, conflicting revisions and unavailable KV.
    pub async fn publish(
        &self,
        expected: Option<&StreamChunkRecordSnapshot>,
        tail: StreamChunkRecord,
        previous: Option<StreamChunkRecord>,
        candidate: Option<&StreamChunkRecordSnapshot>,
    ) -> Result<StreamChunkRecordSnapshot> {
        validate_transition(expected, &tail, previous.as_ref())?;
        let guarded = expected
            .or(candidate)
            .ok_or_else(|| corrupt("missing attachment guard"))?;
        if expected.is_none() {
            if let Some(current) = self.load_tail(tail.stream_id).await? {
                return if current.record.as_ref() == &tail {
                    Ok(current)
                } else {
                    Err(StreamError::StaleWriter)
                };
            }
        }
        let key = record_key(&tail)?.to_bytes();
        let value = encode_stream_chunk_record(&tail).map_err(codec_error)?;
        let mut ops = Vec::with_capacity(3);
        if let Some(previous) = &previous {
            let old_key = record_key(previous)?.to_bytes();
            if old_key != key {
                ops.push(put(previous)?);
            } else if previous.logical_start != previous.logical_end {
                return Err(corrupt("nonempty predecessor key reused"));
            }
        }
        if let Some(candidate) = candidate {
            if candidate.record.state != StreamChunkState::Candidate
                || candidate.record.chunk_id != tail.chunk_id
                || candidate.record.sequence != tail.sequence
                || candidate.record.stream_id != tail.stream_id
                || candidate.record.writer_epoch != tail.writer_epoch
                || candidate.record.owner_session != tail.owner_session
            {
                return Err(corrupt("candidate identity differs from attachment"));
            }
            ops.push(BatchOp::Delete {
                key: Bytes::copy_from_slice(&candidate.key),
            });
        }
        ops.push(BatchOp::Put {
            key: Bytes::copy_from_slice(&key),
            value: Bytes::from(value.clone()),
        });
        let guard = guarded.key.as_slice();
        let mut revision = guarded.revision;
        for attempt in 0_u32..8 {
            match self.backend.batch(&ops, guard, revision).await {
                Ok(outcome) => return Self::decode(&key, &value, outcome.revision, tail.stream_id),
                Err(Error::CasBusy) => {
                    tokio::time::sleep(std::time::Duration::from_millis(1 << attempt.min(4))).await;
                }
                Err(Error::CasFailed { .. } | Error::OutcomeUnknown) => {
                    let current = self.load_tail(tail.stream_id).await?;
                    if let Some(current) = &current {
                        if current.record.as_ref() == &tail {
                            return Ok(current.clone());
                        }
                    }
                    match (expected, current) {
                        (Some(expected), Some(current)) if current.record == expected.record => {
                            revision = current.revision;
                            if let Some(candidate) = candidate {
                                let registered = self.load_key(&record_key(&candidate.record)?).await?;
                                if registered.map_or(true, |registered| registered.record != candidate.record)
                                {
                                    return Err(StreamError::StaleWriter);
                                }
                            }
                        }
                        (_, Some(current)) if current.record.writer_epoch >= tail.writer_epoch => {
                            return Err(StreamError::StaleWriter)
                        }
                        _ => return Err(StreamError::WriteStalled),
                    }
                }
                Err(error) => return Err(storage_error(error)),
            }
        }
        Err(StreamError::Backpressure)
    }

    pub(super) async fn fenced_batch(
        &self,
        expected: &StreamChunkRecordSnapshot,
        mut ops: Vec<BatchOp>,
    ) -> Result<()> {
        ops.push(put(&expected.record)?);
        for attempt in 0_u32..8 {
            match self.backend.batch(&ops, &expected.key, expected.revision).await {
                Ok(_) => return Ok(()),
                Err(Error::CasBusy) => {
                    tokio::time::sleep(std::time::Duration::from_millis(1 << attempt.min(4))).await;
                }
                Err(error) => return Err(storage_error(error)),
            }
        }
        Err(StreamError::Backpressure)
    }
}

pub(super) fn put(record: &StreamChunkRecord) -> Result<BatchOp> {
    Ok(BatchOp::Put {
        key: Bytes::from(record_key(record)?.to_bytes()),
        value: Bytes::from(encode_stream_chunk_record(record).map_err(codec_error)?),
    })
}

fn validate_transition(
    expected: Option<&StreamChunkRecordSnapshot>,
    tail: &StreamChunkRecord,
    previous: Option<&StreamChunkRecord>,
) -> Result<()> {
    tail.validate().map_err(codec_error)?;
    if tail.state == StreamChunkState::Candidate || tail.state == StreamChunkState::Deleting {
        return Err(corrupt("invalid tail state"));
    }
    let Some(expected) = expected else {
        return if tail.generation == 1
            && tail.sequence == 0
            && tail.logical_start == Some(0)
            && tail.logical_end == Some(0)
        {
            Ok(())
        } else {
            Err(corrupt("initial tail bounds"))
        };
    };
    let base = &expected.record;
    if base.stream_id != tail.stream_id
        || base.purpose != tail.purpose
        || base.writer_epoch > tail.writer_epoch
        || base.generation.checked_add(1) != Some(tail.generation)
        || tail.trim_offset < base.trim_offset
        || tail.logical_end < base.logical_end
        || (base.closed && !tail.closed)
    {
        return Err(corrupt("tail transition regresses or changes scope"));
    }
    if base.chunk_id == tail.chunk_id {
        if previous.is_some() || tail.sequence != base.sequence || tail.logical_start != base.logical_start {
            return Err(corrupt("same-chunk transition changes attachment"));
        }
        validate_prefix(base, tail)?;
    } else {
        let previous = previous.ok_or_else(|| corrupt("missing finalized predecessor"))?;
        previous.validate().map_err(codec_error)?;
        if previous.chunk_id != base.chunk_id
            || previous.logical_start != base.logical_start
            || previous.stream_id != base.stream_id
            || previous.purpose != base.purpose
            || previous.sequence != base.sequence
            || previous.writer_epoch != tail.writer_epoch
            || previous.owner_session != tail.owner_session
            || previous.generation != tail.generation
            || previous.state != StreamChunkState::Sealed
            || previous.logical_end != tail.logical_start
            || previous.logical_end < base.logical_end
            || base.sequence.checked_add(1) != Some(tail.sequence)
            || base.closed
            || tail.state != StreamChunkState::Active
        {
            return Err(corrupt("predecessor/successor continuity"));
        }
        validate_prefix(base, previous)?;
    }
    Ok(())
}

fn validate_prefix(base: &StreamChunkRecord, next: &StreamChunkRecord) -> Result<()> {
    let base_lengths = base.mappings.lengths().map_err(codec_error)?;
    let next_lengths = next.mappings.lengths().map_err(codec_error)?;
    if !next_lengths.starts_with(&base_lengths) {
        return Err(corrupt("checkpoint changes acknowledged append boundaries"));
    }
    Ok(())
}
