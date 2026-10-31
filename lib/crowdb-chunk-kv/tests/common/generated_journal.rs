// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_kv::{
    canonical_operation_digest, encode_frame, Checkpoint, ChunkKvError, JournalPosition, MutationOperation,
    MutationResult, PartitionId, PartitionJournal, RequestId, Result, WalRecord,
};
use crowdb_chunk_stream::StreamName;

pub struct TestGeneratedJournal {
    count: u64,
    frame_bytes: usize,
    duplicate: Option<bool>,
    request_conflict: bool,
}

impl TestGeneratedJournal {
    pub fn new(count: u64, duplicate: Option<bool>, request_conflict: bool) -> Self {
        Self {
            count,
            frame_bytes: encode_frame(&Self::record(1, false)).unwrap().len(),
            duplicate,
            request_conflict,
        }
    }

    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            tree_id: 1,
            tree_manifest: 0,
            root_manifest_generation: 0,
            applied_seq: 0,
            stream_name: self.stream_name(),
            stream_generation: 1,
            replay_offset: 0,
        }
    }

    fn record(sequence: u64, changed: bool) -> WalRecord {
        let operation = MutationOperation::Put {
            key: b"overwritten".to_vec(),
            value: vec![if changed { 255 } else { (sequence % 251) as u8 }; 65536],
        };
        WalRecord {
            partition_id: PartitionId { high: 1, low: 1 },
            ownership_epoch: 1,
            mutation_seq: sequence,
            request_id: RequestId {
                client_high: 1,
                client_low: 1,
                client_sequence: sequence,
            },
            operation_digest: canonical_operation_digest(&operation),
            result: MutationResult::Applied { revision: sequence },
            operation,
        }
    }
}

#[async_trait]
impl PartitionJournal for TestGeneratedJournal {
    async fn append_frames(&self, _: &[Bytes]) -> Result<Vec<JournalPosition>> {
        Err(ChunkKvError::Internal("generated journal is read-only".into()))
    }

    async fn read_window(&self, offset: u64, max_bytes: usize) -> Result<Bytes> {
        let length = usize::try_from(self.tail().saturating_sub(offset))
            .unwrap()
            .min(max_bytes);
        let mut out = Vec::with_capacity(length);
        while out.len() < length {
            let position = usize::try_from(offset).unwrap() + out.len();
            let ordinal = position / self.frame_bytes;
            let within = position % self.frame_bytes;
            let (sequence, changed) = if ordinal as u64 == self.count {
                (1, self.duplicate.unwrap())
            } else {
                (ordinal as u64 + 1, false)
            };
            let mut record = Self::record(sequence, changed);
            if ordinal as u64 == self.count && self.request_conflict {
                record.request_id.client_low = 2;
            }
            let frame = encode_frame(&record)?;
            assert_eq!(frame.len(), self.frame_bytes);
            let count = (frame.len() - within).min(length - out.len());
            out.extend_from_slice(&frame[within..within + count]);
        }
        Ok(Bytes::from(out))
    }

    async fn trim_prefix(&self, _: u64) -> Result<u64> {
        Ok(0)
    }

    async fn reclaim_metadata_before(&self, _: u64, _: usize) -> Result<u64> {
        Ok(0)
    }

    async fn checkpoint(&self) -> Result<u64> {
        Ok(1)
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn stream_name(&self) -> StreamName {
        StreamName { high: 1, low: 1 }
    }

    fn record_generation(&self) -> u64 {
        1
    }

    fn tail(&self) -> u64 {
        (self.count + u64::from(self.duplicate.is_some())) * self.frame_bytes as u64
    }
}
