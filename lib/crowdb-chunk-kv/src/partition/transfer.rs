// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Incremental live-target replay for one ownership handoff.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::sync::oneshot;

use super::{PartitionJournal, PreparedSplitWriterArtifact};
use crate::{ChunkKvError, JournalPosition, PartitionLifecycle, Result};

pub(super) struct TransferCatchUpRequest {
    artifact: PreparedSplitWriterArtifact,
    parent_journal: Arc<dyn PartitionJournal>,
    completion: oneshot::Sender<Result<()>>,
}

pub(super) async fn catch_up(state: &mut super::WorkerState, request: TransferCatchUpRequest) {
    let result = catch_up_inner(state, &request).await;
    if result.is_err() {
        state.lifecycle.store(
            super::lifecycle_code(PartitionLifecycle::Recovering),
            Ordering::Release,
        );
    }
    let _ = request.completion.send(result);
}

async fn catch_up_inner(state: &mut super::WorkerState, request: &TransferCatchUpRequest) -> Result<()> {
    let current = state
        .prepared_artifact
        .load_full()
        .ok_or_else(|| ChunkKvError::InvalidRequest("transfer target has no prepared artifact".into()))?;
    if *current == request.artifact {
        return Ok(());
    }
    validate_extension(&current, &request.artifact, request.parent_journal.as_ref())?;
    let seed = super::RecoverySeed {
        applied_seq: state.applied_seq.load(Ordering::Acquire),
        applied_position: 0,
        retry_replay_offset: state.retry_replay_offset.load(Ordering::Acquire),
        results: state.results.clone(),
        result_order: state.result_order.clone(),
        expired_floor: state.expired_floor.clone(),
        recovered: true,
    };
    let mut replay = super::ReplayState {
        seed,
        checkpoint_applied_seq: current.applied_seq,
        stream_name: request.parent_journal.stream_name(),
        retained_results: state.config.retained_results,
        replayed: HashMap::new(),
        last_new_sequence: None,
    };
    replay_extension(
        &mut replay,
        state.tree.as_ref(),
        request.parent_journal.as_ref(),
        &request.artifact,
        current.parent_cutover_offset,
    )
    .await?;
    state.next_seq = request.artifact.child_stream_start_seq;
    state.results = replay.seed.results;
    state.result_order = replay.seed.result_order;
    state.expired_floor = replay.seed.expired_floor;
    state
        .journal_durable_seq
        .store(request.artifact.applied_seq, Ordering::Release);
    state
        .applied_seq
        .store(request.artifact.applied_seq, Ordering::Release);
    state.inherited_position.store(Some(Arc::new(JournalPosition {
        stream_name: request.artifact.parent_stream_name,
        offset: request.artifact.parent_cutover_offset,
    })));
    state
        .prepared_artifact
        .store(Some(Arc::new(request.artifact.clone())));
    state.applied_notify.notify_waiters();
    Ok(())
}

async fn replay_extension(
    replay: &mut super::ReplayState,
    tree: &dyn super::PartitionTree,
    parent_journal: &dyn PartitionJournal,
    artifact: &PreparedSplitWriterArtifact,
    mut read_offset: u64,
) -> Result<()> {
    let mut frame_offset = read_offset;
    let mut buffered = BytesMut::new();
    while read_offset < artifact.parent_cutover_offset {
        let remaining = artifact.parent_cutover_offset - read_offset;
        let max_bytes = usize::try_from(remaining.min(1024 * 1024)).unwrap_or(1024 * 1024);
        let bytes = parent_journal.read_window(read_offset, max_bytes).await?;
        if bytes.is_empty() || bytes.len() as u64 > remaining {
            return Err(ChunkKvError::JournalCorruption(
                "transfer source replay did not preserve its cutover".into(),
            ));
        }
        read_offset += bytes.len() as u64;
        buffered.extend_from_slice(&bytes);
        while let super::FrameDecode::Complete(decoded) = super::decode_frame(&buffered)? {
            super::validate_replay_record(artifact.parent_id, artifact.parent_epoch, &decoded.record)?;
            let belongs = artifact.range.contains(decoded.record.operation.key());
            replay.process(tree, frame_offset, decoded.record, belongs).await?;
            buffered.advance(decoded.bytes_consumed);
            frame_offset += decoded.bytes_consumed as u64;
        }
        if buffered.len() > crate::MAX_FRAME_BYTES {
            return Err(ChunkKvError::JournalCorruption(
                "transfer source frame exceeds maximum size".into(),
            ));
        }
    }
    if !buffered.is_empty()
        || frame_offset != artifact.parent_cutover_offset
        || replay.seed.applied_seq != artifact.applied_seq
    {
        return Err(ChunkKvError::JournalCorruption(
            "transfer source replay did not reach the exact handoff cursor".into(),
        ));
    }
    Ok(())
}

fn validate_extension(
    current: &PreparedSplitWriterArtifact,
    final_artifact: &PreparedSplitWriterArtifact,
    parent_journal: &dyn PartitionJournal,
) -> Result<()> {
    let same_base = current.partition_id == final_artifact.partition_id
        && current.range == final_artifact.range
        && current.ownership_epoch == final_artifact.ownership_epoch
        && current.tree_id == final_artifact.tree_id
        && current.tree_manifest == final_artifact.tree_manifest
        && current.root_manifest_generation == final_artifact.root_manifest_generation
        && current.stream_name == final_artifact.stream_name
        && current.base_applied_seq == final_artifact.base_applied_seq
        && current.parent_id == final_artifact.parent_id
        && current.parent_epoch == final_artifact.parent_epoch
        && current.parent_stream_name == final_artifact.parent_stream_name
        && current.parent_stream_manifest_generation == final_artifact.parent_stream_manifest_generation
        && current.parent_replay_offset == final_artifact.parent_replay_offset;
    let valid_frontier = current.applied_seq <= final_artifact.applied_seq
        && current.parent_cutover_offset <= final_artifact.parent_cutover_offset
        && final_artifact.child_stream_start_seq == final_artifact.applied_seq.checked_add(1).unwrap_or(0);
    let valid_journal = parent_journal.stream_name() == final_artifact.parent_stream_name
        && parent_journal.manifest_generation() >= final_artifact.parent_stream_manifest_generation
        && parent_journal.tail() >= final_artifact.parent_cutover_offset;
    if !same_base || !valid_frontier || !valid_journal {
        return Err(ChunkKvError::InvalidRequest(
            "transfer catch-up does not extend the prepared target exactly".into(),
        ));
    }
    Ok(())
}

impl super::Partition {
    /// Incrementally advances one live prepared transfer target to its final
    /// source release cursor without reopening the pinned base.
    ///
    /// # Errors
    ///
    /// Returns an identity, lifecycle, source journal, or replay error.
    pub async fn catch_up_prepared_transfer(
        &self,
        artifact: PreparedSplitWriterArtifact,
        parent_journal: Arc<dyn PartitionJournal>,
    ) -> Result<()> {
        if self.lifecycle() != PartitionLifecycle::Prepared {
            return Err(ChunkKvError::InvalidRequest(
                "transfer catch-up requires a prepared target".into(),
            ));
        }
        let (completion, response) = oneshot::channel();
        self.sender
            .send(super::WorkerRequest::TransferCatchUp(Box::new(
                TransferCatchUpRequest {
                    artifact,
                    parent_journal,
                    completion,
                },
            )))
            .await
            .map_err(|_| ChunkKvError::WriteStalled)?;
        response.await.map_err(|_| ChunkKvError::WriteStalled)?
    }
}
