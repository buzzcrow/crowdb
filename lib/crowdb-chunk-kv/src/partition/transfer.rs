// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Incremental live-target replay for one ownership handoff.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::sync::oneshot;

use super::{MutationRequest, PartitionJournal, PreparedSplitWriterArtifact};
use crate::{
    canonical_operation_digest, ChunkKvError, JournalPosition, MutationOperation, MutationResponse,
    MutationResult, PartitionLifecycle, RequestId, Result, WalRecord,
};

pub(super) struct TransferCatchUpRequest {
    artifact: PreparedSplitWriterArtifact,
    parent_journal: Arc<dyn PartitionJournal>,
    completion: oneshot::Sender<Result<()>>,
}

pub(super) struct TransferAppendRequest {
    artifact: PreparedSplitWriterArtifact,
    mutation: MutationRequest,
}

pub(super) async fn append_mutation(state: &mut super::WorkerState, request: TransferAppendRequest) {
    if super::lifecycle_from_code(state.lifecycle.load(Ordering::Acquire)) == PartitionLifecycle::Serving {
        super::process_batch(state, vec![request.mutation]).await;
        return;
    }
    let result = append_mutation_inner(state, &request).await;
    super::finish_request(state, request.mutation, result);
}

async fn append_mutation_inner(
    state: &mut super::WorkerState,
    request: &TransferAppendRequest,
) -> Result<MutationResponse> {
    if super::lifecycle_from_code(state.lifecycle.load(Ordering::Acquire)) != PartitionLifecycle::Prepared {
        return Err(super::write_state_error(super::lifecycle_from_code(
            state.lifecycle.load(Ordering::Acquire),
        )));
    }
    let current = state
        .prepared_artifact
        .load_full()
        .ok_or_else(|| ChunkKvError::InvalidRequest("transfer target has no prepared artifact".into()))?;
    validate_artifact_extension(&current, &request.artifact)?;
    if super::operation_needs_current(&request.mutation.operation) {
        return Err(ChunkKvError::InvalidRequest(
            "conditional transfer mutation requires initialized state".into(),
        ));
    }
    if let Some(retained) = state.results.get(&request.mutation.request_id) {
        return if retained.digest == request.mutation.digest {
            Ok(retained.response.clone())
        } else {
            Err(ChunkKvError::RequestConflict)
        };
    }
    if let Some((digest, response)) = find_appended_mutation(
        state.partition_id,
        state.ownership_epoch.load(Ordering::Acquire),
        state.journal.as_ref(),
        request.mutation.request_id,
    )
    .await?
    {
        return if digest == request.mutation.digest {
            Ok(response)
        } else {
            Err(ChunkKvError::RequestConflict)
        };
    }
    let client = (
        request.mutation.request_id.client_high,
        request.mutation.request_id.client_low,
    );
    if state
        .expired_floor
        .get(&client)
        .is_some_and(|floor| request.mutation.request_id.client_sequence <= *floor)
    {
        return Err(ChunkKvError::RequestExpired);
    }
    state.next_seq = state.next_seq.max(request.artifact.child_stream_start_seq);
    let mutation_seq = state.next_seq;
    state.next_seq = mutation_seq
        .checked_add(1)
        .ok_or_else(|| ChunkKvError::Faulted("mutation sequence exhausted".into()))?;
    let result = MutationResult::Applied {
        revision: mutation_seq,
    };
    let record = WalRecord {
        partition_id: state.partition_id,
        ownership_epoch: state.ownership_epoch.load(Ordering::Acquire),
        mutation_seq,
        request_id: request.mutation.request_id,
        operation_digest: request.mutation.digest,
        result: result.clone(),
        operation: request.mutation.operation.clone(),
    };
    let frame = bytes::Bytes::from(super::encode_frame(&record)?);
    let positions = state.journal.append_frames(&[frame]).await.map_err(|error| {
        let stream_name = state.journal.stream_name();
        tracing::warn!(
            partition_id_high = state.partition_id.high,
            partition_id_low = state.partition_id.low,
            ownership_epoch = state.ownership_epoch.load(Ordering::Acquire),
            stream_high = stream_name.high,
            stream_low = stream_name.low,
            %error,
            "chunk KV transfer journal append failed"
        );
        super::journal_append_failed(state, &error);
        error
    })?;
    if positions.len() != 1 {
        let error =
            ChunkKvError::Internal("journal returned wrong position count for transfer mutation".into());
        super::journal_append_failed(state, &error);
        return Err(error);
    }
    let position = positions[0];
    let response = MutationResponse {
        mutation_seq,
        result,
        journal_position: position,
    };
    state.journal_durable_seq.store(mutation_seq, Ordering::Release);
    state.metrics.mutation_result(true);
    Ok(response)
}

async fn find_appended_mutation(
    partition_id: crate::PartitionId,
    ownership_epoch: u64,
    journal: &dyn PartitionJournal,
    request_id: RequestId,
) -> Result<Option<([u8; 32], MutationResponse)>> {
    let tail = journal.tail();
    let mut read_offset = 0;
    let mut frame_offset = 0;
    let mut buffered = BytesMut::new();
    let mut found = None;
    while read_offset < tail {
        let bytes = journal.read_window(read_offset, 1024 * 1024).await?;
        if bytes.is_empty() {
            return Err(ChunkKvError::JournalCorruption(
                "target WAL returned no bytes before its tail".into(),
            ));
        }
        read_offset += bytes.len() as u64;
        buffered.extend_from_slice(&bytes);
        while let super::FrameDecode::Complete(decoded) = super::decode_frame(&buffered)? {
            super::validate_replay_record(partition_id, ownership_epoch, &decoded.record)?;
            if decoded.record.request_id == request_id {
                let response = MutationResponse {
                    mutation_seq: decoded.record.mutation_seq,
                    result: decoded.record.result.clone(),
                    journal_position: JournalPosition {
                        stream_name: journal.stream_name(),
                        offset: frame_offset,
                    },
                };
                match &found {
                    Some((digest, previous))
                        if *digest != decoded.record.operation_digest || *previous != response =>
                    {
                        return Err(ChunkKvError::JournalCorruption(
                            "target WAL reuses one request identity".into(),
                        ));
                    }
                    None => found = Some((decoded.record.operation_digest, response)),
                    Some(_) => {}
                }
            }
            buffered.advance(decoded.bytes_consumed);
            frame_offset += decoded.bytes_consumed as u64;
        }
    }
    if !buffered.is_empty() {
        return Err(ChunkKvError::IncompleteFrame);
    }
    Ok(found)
}

pub(super) async fn catch_up(state: &mut super::WorkerState, request: TransferCatchUpRequest) {
    let result = catch_up_inner(state, &request).await;
    if result.is_err() {
        state.lifecycle.store(
            super::lifecycle_code(PartitionLifecycle::Recovering),
            Ordering::Release,
        );
    } else {
        state.lifecycle.store(
            super::lifecycle_code(PartitionLifecycle::Serving),
            Ordering::Release,
        );
    }
    if let Some(completion) = state.initialization_completion.take() {
        let _ = completion.send(result.clone());
    }
    let _ = request.completion.send(result);
}

async fn catch_up_inner(state: &mut super::WorkerState, request: &TransferCatchUpRequest) -> Result<()> {
    let current = state
        .prepared_artifact
        .load_full()
        .ok_or_else(|| ChunkKvError::InvalidRequest("transfer target has no prepared artifact".into()))?;
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
    let seed = super::replay_child_overlay(
        state.partition_id,
        state.ownership_epoch.load(Ordering::Acquire),
        request.artifact.applied_seq,
        state.config.retained_results,
        state.tree.as_ref(),
        state.journal.as_ref(),
        replay.seed,
    )
    .await?;
    state.next_seq = seed
        .applied_seq
        .checked_add(1)
        .ok_or_else(|| ChunkKvError::Faulted("mutation sequence exhausted after transfer catch-up".into()))?;
    state.results = seed.results;
    state.result_order = seed.result_order;
    state.expired_floor = seed.expired_floor;
    state
        .journal_durable_seq
        .store(seed.applied_seq, Ordering::Release);
    state.applied_seq.store(seed.applied_seq, Ordering::Release);
    state
        .applied_position
        .store(seed.applied_position, Ordering::Release);
    state
        .retry_replay_offset
        .store(seed.retry_replay_offset, Ordering::Release);
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
            replay
                .process(tree, frame_offset, decoded.record, belongs)
                .await?;
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
    let same_base = validate_artifact_extension(current, final_artifact).is_ok();
    let valid_journal = parent_journal.stream_name() == final_artifact.parent_stream_name
        && parent_journal.manifest_generation() >= final_artifact.parent_stream_manifest_generation
        && parent_journal.tail() >= final_artifact.parent_cutover_offset;
    if !same_base || !valid_journal {
        return Err(ChunkKvError::InvalidRequest(
            "transfer catch-up does not extend the prepared target exactly".into(),
        ));
    }
    Ok(())
}

fn validate_artifact_extension(
    current: &PreparedSplitWriterArtifact,
    final_artifact: &PreparedSplitWriterArtifact,
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
    if !same_base || !valid_frontier {
        return Err(ChunkKvError::InvalidRequest(
            "transfer catch-up does not extend the prepared target exactly".into(),
        ));
    }
    Ok(())
}

impl super::Partition {
    /// Durably appends an unconditional mutation to a prepared target's own
    /// WAL without waiting for source catch-up or tree application.
    ///
    /// # Errors
    ///
    /// Returns a validation, admission, journal, or lifecycle error.
    pub async fn append_prepared_transfer_mutation(
        &self,
        artifact: PreparedSplitWriterArtifact,
        ownership_epoch: u64,
        request_id: RequestId,
        operation: MutationOperation,
    ) -> Result<MutationResponse> {
        self.metrics.mutation_request();
        self.validate_epoch(ownership_epoch)?;
        self.validate_operation(&operation)?;
        if super::operation_needs_current(&operation) {
            return Err(ChunkKvError::InvalidRequest(
                "conditional transfer mutation requires initialized state".into(),
            ));
        }
        let reserved_bytes = super::estimated_request_bytes(&operation)?;
        super::reserve_requests(&self.queued_requests, self.config.queue_requests)?;
        if let Err(error) = super::reserve_bytes(&self.queued_bytes, self.config.queue_bytes, reserved_bytes)
        {
            super::release_admission(
                &self.queued_requests,
                &self.queued_bytes,
                &self.admission_notify,
                reserved_bytes,
            );
            return Err(error);
        }
        let (completion, response) = oneshot::channel();
        let mutation = MutationRequest {
            request_id,
            digest: canonical_operation_digest(&operation),
            operation,
            reserved_bytes,
            completion,
        };
        if self
            .sender
            .try_send(super::WorkerRequest::TransferAppend(Box::new(
                TransferAppendRequest { artifact, mutation },
            )))
            .is_err()
        {
            super::release_admission(
                &self.queued_requests,
                &self.queued_bytes,
                &self.admission_notify,
                reserved_bytes,
            );
            return Err(ChunkKvError::Overloaded);
        }
        response.await.map_err(|_| ChunkKvError::WriteStalled)?
    }

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
