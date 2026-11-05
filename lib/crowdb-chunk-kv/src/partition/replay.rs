// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Streaming WAL replay with compact, complete-record duplicate identities.

use super::{
    canonical_operation_digest, decode_frame, retain_recovered, Buf, BytesMut, Checkpoint, ChunkKvError,
    FrameDecode, HashMap, JournalPosition, MutationResponse, MutationResult, PartitionId, PartitionJournal,
    PartitionRange, PartitionTree, RecoverySeed, Result, StreamName, WalRecord,
};
use sha2::{Digest, Sha256};
use std::io::Write;

pub(super) struct ReplayState {
    pub(super) seed: RecoverySeed,
    pub(super) checkpoint_applied_seq: u64,
    pub(super) stream_name: StreamName,
    pub(super) retained_results: usize,
    pub(super) replayed: HashMap<u64, [u8; 32]>,
    pub(super) last_new_sequence: Option<u64>,
    pub(super) applied_bytes_since_drain: usize,
}

pub(super) async fn replay_suffix(
    partition_id: PartitionId,
    ownership_epoch: u64,
    range: &PartitionRange,
    checkpoint: &Checkpoint,
    retained_results: usize,
    tree: &dyn PartitionTree,
    journal: &dyn PartitionJournal,
) -> Result<RecoverySeed> {
    const WINDOW_BYTES: usize = 1024 * 1024;
    let tail = journal.tail();
    if checkpoint.replay_offset > tail {
        return Err(ChunkKvError::JournalCorruption(
            "checkpoint replay offset exceeds journal tail".into(),
        ));
    }
    let mut replay = ReplayState::new(checkpoint, retained_results);
    let mut read_offset = checkpoint.replay_offset;
    let mut frame_offset = read_offset;
    let mut buffered = BytesMut::new();
    while read_offset < tail {
        let bytes = journal.read_window(read_offset, WINDOW_BYTES).await?;
        if bytes.is_empty() {
            return Err(ChunkKvError::JournalCorruption(
                "journal returned no bytes before durable tail".into(),
            ));
        }
        read_offset = read_offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| ChunkKvError::JournalCorruption("journal replay offset overflows".into()))?;
        buffered.extend_from_slice(&bytes);
        loop {
            let decoded = decode_frame(&buffered)?;
            let FrameDecode::Complete(decoded) = decoded else {
                break;
            };
            validate_replay_record(partition_id, ownership_epoch, &decoded.record)?;
            let belongs = range.contains(decoded.record.operation.key());
            replay
                .process(tree, frame_offset, decoded.record, belongs)
                .await?;
            let consumed = decoded.bytes_consumed;
            buffered.advance(consumed);
            frame_offset = frame_offset
                .checked_add(consumed as u64)
                .ok_or_else(|| ChunkKvError::JournalCorruption("journal frame offset overflows".into()))?;
            replay.seed.applied_end = frame_offset;
        }
        if buffered.len() > crate::MAX_FRAME_BYTES {
            return Err(ChunkKvError::JournalCorruption(
                "incomplete frame exceeds maximum size".into(),
            ));
        }
    }
    if !buffered.is_empty() {
        return Err(ChunkKvError::IncompleteFrame);
    }
    Ok(replay.seed)
}

impl ReplayState {
    pub(super) fn new(checkpoint: &Checkpoint, retained_results: usize) -> Self {
        Self {
            seed: RecoverySeed {
                applied_seq: checkpoint.applied_seq,
                applied_position: checkpoint.replay_offset,
                applied_end: checkpoint.replay_offset,
                retry_replay_offset: checkpoint.replay_offset,
                results: HashMap::new(),
                result_order: std::collections::VecDeque::new(),
                expired_floor: HashMap::new(),
                recovered: true,
            },
            checkpoint_applied_seq: checkpoint.applied_seq,
            stream_name: checkpoint.stream_name,
            retained_results,
            replayed: HashMap::new(),
            last_new_sequence: None,
            applied_bytes_since_drain: 0,
        }
    }

    pub(super) async fn process(
        &mut self,
        tree: &dyn PartitionTree,
        frame_offset: u64,
        record: WalRecord,
        belongs_to_partition: bool,
    ) -> Result<()> {
        let fingerprint = record_fingerprint(&record)?;
        if let Some(previous) = self.replayed.get(&record.mutation_seq) {
            if previous != &fingerprint {
                return Err(ChunkKvError::JournalCorruption(
                    "conflicting duplicate mutation sequence".into(),
                ));
            }
            return Ok(());
        }
        if let Some(last) = self.last_new_sequence {
            let expected = last.checked_add(1).ok_or_else(|| {
                ChunkKvError::JournalCorruption("journal mutation sequence overflows".into())
            })?;
            if expected != record.mutation_seq {
                return Err(ChunkKvError::JournalCorruption(format!(
                    "journal mutation sequence has a gap: expected {expected}, got {} at offset {frame_offset}",
                    record.mutation_seq
                )));
            }
        }
        if record.mutation_seq > self.checkpoint_applied_seq {
            let expected = self
                .seed
                .applied_seq
                .checked_add(1)
                .ok_or_else(|| ChunkKvError::JournalCorruption("mutation sequence overflows".into()))?;
            if record.mutation_seq != expected {
                return Err(ChunkKvError::JournalCorruption(format!(
                    "journal mutation sequence has a gap after checkpoint {}: expected {expected}, got {} at offset {frame_offset}",
                    self.checkpoint_applied_seq, record.mutation_seq
                )));
            }
            if belongs_to_partition && record.result.applied() {
                tree.apply(record.mutation_seq, &record.operation)
                    .await
                    .map_err(|error| {
                        ChunkKvError::Internal(format!(
                            "replay tree apply failed at mutation sequence {}: {error}",
                            record.mutation_seq
                        ))
                    })?;
                self.applied_bytes_since_drain = self
                    .applied_bytes_since_drain
                    // Include arena/index overhead for small values and deletes.
                    .saturating_add(256)
                    .saturating_add(record.operation.key().len())
                    .saturating_add(record.operation.successful_value().map_or(0, <[u8]>::len));
                if self.applied_bytes_since_drain >= 16 * 1024 * 1024 {
                    tree.drain_replay().await?;
                    self.applied_bytes_since_drain = 0;
                }
            } else {
                tree.advance_noop(record.mutation_seq).await.map_err(|error| {
                    ChunkKvError::Internal(format!(
                        "replay tree no-op failed at mutation sequence {}: {error}",
                        record.mutation_seq
                    ))
                })?;
            }
            self.seed.applied_seq = record.mutation_seq;
        }
        self.seed.applied_position = frame_offset;
        if belongs_to_partition {
            let response = MutationResponse {
                mutation_seq: record.mutation_seq,
                result: record.result.clone(),
                journal_position: JournalPosition {
                    stream_name: self.stream_name,
                    offset: frame_offset,
                },
            };
            retain_recovered(
                &mut self.seed,
                self.retained_results,
                record.request_id,
                record.operation_digest,
                response,
            )?;
        }
        self.last_new_sequence = Some(record.mutation_seq);
        self.replayed.insert(record.mutation_seq, fingerprint);
        Ok(())
    }
}

pub(super) fn validate_replay_record(
    partition_id: PartitionId,
    ownership_epoch: u64,
    record: &WalRecord,
) -> Result<()> {
    if record.partition_id != partition_id || record.ownership_epoch > ownership_epoch {
        return Err(ChunkKvError::JournalCorruption(
            "WAL record partition or epoch is invalid".into(),
        ));
    }
    if record.operation_digest != canonical_operation_digest(&record.operation) {
        return Err(ChunkKvError::JournalCorruption(
            "WAL operation digest mismatch".into(),
        ));
    }
    match record.result {
        MutationResult::Applied { revision } if revision != record.mutation_seq => Err(
            ChunkKvError::JournalCorruption("applied revision differs from mutation sequence".into()),
        ),
        _ => Ok(()),
    }
}

fn record_fingerprint(record: &WalRecord) -> Result<[u8; 32]> {
    struct HashWriter(Sha256);
    impl Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = HashWriter(Sha256::new());
    writer.0.update(b"crowdb-chunk-kv-replay-record-v1");
    bincode::serialize_into(&mut writer, record).map_err(|error| {
        ChunkKvError::JournalCorruption(format!("WAL record fingerprint failed: {error}"))
    })?;
    Ok(writer.0.finalize().into())
}
