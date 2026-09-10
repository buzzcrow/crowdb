// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_stream::StreamName;
use tokio::sync::{mpsc, oneshot, Notify};

use crate::{
    canonical_operation_digest, encode_frame, ChunkKvError, CompareCondition, JournalPosition,
    MutationOperation, MutationResult, PartitionId, PartitionJournal, PartitionLifecycle, PartitionRange,
    PartitionTree, RequestId, Result, ValueRevision, WalRecord,
};

#[derive(Clone, Debug)]
pub struct PartitionConfig {
    pub queue_requests: usize,
    pub queue_bytes: u64,
    pub batch_requests: usize,
    pub batch_bytes: u64,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub retained_results: usize,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            queue_requests: 1_024,
            queue_bytes: 64 * 1024 * 1024,
            batch_requests: 64,
            batch_bytes: 4 * 1024 * 1024,
            max_key_bytes: 64 * 1024,
            max_value_bytes: 16 * 1024 * 1024,
            retained_results: 65_536,
        }
    }
}

impl PartitionConfig {
    fn validate(&self) -> Result<()> {
        if self.queue_requests == 0
            || self.queue_bytes == 0
            || self.batch_requests == 0
            || self.batch_bytes == 0
            || self.max_key_bytes == 0
            || self.max_value_bytes == 0
            || self.retained_results == 0
        {
            return Err(ChunkKvError::InvalidRequest(
                "partition bounds must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationResponse {
    pub mutation_seq: u64,
    pub result: MutationResult,
    pub journal_position: JournalPosition,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionSnapshot {
    pub partition_id: PartitionId,
    pub range: PartitionRange,
    pub ownership_epoch: u64,
    pub lifecycle: PartitionLifecycle,
    pub stream_name: StreamName,
    pub journal_durable_seq: u64,
    pub applied_seq: u64,
}

struct MutationRequest {
    request_id: RequestId,
    operation: MutationOperation,
    digest: [u8; 32],
    reserved_bytes: u64,
    completion: oneshot::Sender<Result<MutationResponse>>,
}

#[derive(Clone)]
pub struct Partition {
    id: PartitionId,
    range: PartitionRange,
    ownership_epoch: Arc<AtomicU64>,
    lifecycle: Arc<AtomicU8>,
    journal: Arc<dyn PartitionJournal>,
    tree: Arc<dyn PartitionTree>,
    sender: mpsc::Sender<MutationRequest>,
    queued_requests: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicU64>,
    journal_durable_seq: Arc<AtomicU64>,
    applied_seq: Arc<AtomicU64>,
    applied_position: Arc<AtomicU64>,
    applied_notify: Arc<Notify>,
    config: Arc<PartitionConfig>,
}

struct WorkerState {
    partition_id: PartitionId,
    ownership_epoch: u64,
    lifecycle: Arc<AtomicU8>,
    journal: Arc<dyn PartitionJournal>,
    tree: Arc<dyn PartitionTree>,
    queued_requests: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicU64>,
    journal_durable_seq: Arc<AtomicU64>,
    applied_seq: Arc<AtomicU64>,
    applied_position: Arc<AtomicU64>,
    applied_notify: Arc<Notify>,
    config: Arc<PartitionConfig>,
    next_seq: u64,
    results: HashMap<RequestId, RetainedResult>,
    result_order: std::collections::VecDeque<RequestId>,
    expired_floor: HashMap<(u64, u64), u64>,
}

#[derive(Clone)]
struct RetainedResult {
    digest: [u8; 32],
    response: MutationResponse,
}

struct PreparedMutation {
    request: MutationRequest,
    response: MutationResponse,
    record: WalRecord,
    frame: Bytes,
    followers: Vec<MutationRequest>,
}

impl Partition {
    /// Opens a serving partition over injected tree and journal owners.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid range/configuration, zero epoch, or
    /// inconsistent recovered frontiers.
    pub fn open(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        config: PartitionConfig,
        tree: Arc<dyn PartitionTree>,
        journal: Arc<dyn PartitionJournal>,
    ) -> Result<Self> {
        range.validate()?;
        config.validate()?;
        if ownership_epoch == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "ownership epoch must be nonzero".into(),
            ));
        }
        let applied = tree.last_applied_seq();
        let (sender, receiver) = mpsc::channel(config.queue_requests);
        let lifecycle = Arc::new(AtomicU8::new(lifecycle_code(PartitionLifecycle::Serving)));
        let queued_requests = Arc::new(AtomicUsize::new(0));
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let journal_durable_seq = Arc::new(AtomicU64::new(applied));
        let applied_seq = Arc::new(AtomicU64::new(applied));
        let applied_position = Arc::new(AtomicU64::new(0));
        let applied_notify = Arc::new(Notify::new());
        let config = Arc::new(config);
        let state = WorkerState {
            partition_id,
            ownership_epoch,
            lifecycle: Arc::clone(&lifecycle),
            journal: Arc::clone(&journal),
            tree: Arc::clone(&tree),
            queued_requests: Arc::clone(&queued_requests),
            queued_bytes: Arc::clone(&queued_bytes),
            journal_durable_seq: Arc::clone(&journal_durable_seq),
            applied_seq: Arc::clone(&applied_seq),
            applied_position: Arc::clone(&applied_position),
            applied_notify: Arc::clone(&applied_notify),
            config: Arc::clone(&config),
            next_seq: applied.saturating_add(1),
            results: HashMap::new(),
            result_order: std::collections::VecDeque::new(),
            expired_floor: HashMap::new(),
        };
        tokio::spawn(run_worker(state, receiver));
        Ok(Self {
            id: partition_id,
            range,
            ownership_epoch: Arc::new(AtomicU64::new(ownership_epoch)),
            lifecycle,
            journal,
            tree,
            sender,
            queued_requests,
            queued_bytes,
            journal_durable_seq,
            applied_seq,
            applied_position,
            applied_notify,
            config,
        })
    }

    /// Journals and applies one mutation under the supplied ownership epoch.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, lifecycle, admission, journal, or tree error.
    pub async fn mutate(
        &self,
        ownership_epoch: u64,
        request_id: RequestId,
        operation: MutationOperation,
    ) -> Result<MutationResponse> {
        self.validate_epoch(ownership_epoch)?;
        if self.lifecycle() != PartitionLifecycle::Serving {
            return Err(write_state_error(self.lifecycle()));
        }
        self.validate_operation(&operation)?;
        let reserved_bytes = estimated_request_bytes(&operation)?;
        reserve_requests(&self.queued_requests, self.config.queue_requests)?;
        if let Err(error) = reserve_bytes(&self.queued_bytes, self.config.queue_bytes, reserved_bytes) {
            self.queued_requests.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        let (completion, response) = oneshot::channel();
        let request = MutationRequest {
            request_id,
            digest: canonical_operation_digest(&operation),
            operation,
            reserved_bytes,
            completion,
        };
        if self.sender.try_send(request).is_err() {
            release_admission(&self.queued_requests, &self.queued_bytes, reserved_bytes);
            return Err(ChunkKvError::Overloaded);
        }
        response.await.map_err(|_| ChunkKvError::WriteStalled)?
    }

    /// Reads one key from the applied prefix, optionally waiting for a journal position.
    ///
    /// # Errors
    ///
    /// Returns a typed range, epoch, lifecycle, or tree-read error.
    pub async fn get(
        &self,
        ownership_epoch: u64,
        key: &[u8],
        min_journal_position: Option<JournalPosition>,
    ) -> Result<Option<ValueRevision>> {
        self.validate_epoch(ownership_epoch)?;
        if !self.range.contains(key) {
            return Err(ChunkKvError::OutOfRange);
        }
        match self.lifecycle() {
            PartitionLifecycle::Serving
            | PartitionLifecycle::WriteStalled
            | PartitionLifecycle::SplitPreparing
            | PartitionLifecycle::SplitFenced => {}
            state => return Err(read_state_error(state)),
        }
        if let Some(position) = min_journal_position {
            self.wait_applied(position).await?;
        }
        self.tree.get(key).await
    }

    #[must_use]
    pub fn snapshot(&self) -> PartitionSnapshot {
        PartitionSnapshot {
            partition_id: self.id,
            range: self.range.clone(),
            ownership_epoch: self.ownership_epoch.load(Ordering::Acquire),
            lifecycle: self.lifecycle(),
            stream_name: self.journal.stream_name(),
            journal_durable_seq: self.journal_durable_seq.load(Ordering::Acquire),
            applied_seq: self.applied_seq.load(Ordering::Acquire),
        }
    }

    #[must_use]
    pub fn lifecycle(&self) -> PartitionLifecycle {
        lifecycle_from_code(self.lifecycle.load(Ordering::Acquire))
    }

    fn validate_epoch(&self, ownership_epoch: u64) -> Result<()> {
        if ownership_epoch != self.ownership_epoch.load(Ordering::Acquire) {
            return Err(ChunkKvError::StaleEpoch);
        }
        Ok(())
    }

    fn validate_operation(&self, operation: &MutationOperation) -> Result<()> {
        if !self.range.contains(operation.key()) {
            return Err(ChunkKvError::OutOfRange);
        }
        if operation.key().len() > self.config.max_key_bytes {
            return Err(ChunkKvError::InvalidRequest(
                "key exceeds configured limit".into(),
            ));
        }
        if operation
            .successful_value()
            .is_some_and(|value| value.len() > self.config.max_value_bytes)
        {
            return Err(ChunkKvError::InvalidRequest(
                "value exceeds configured limit".into(),
            ));
        }
        Ok(())
    }

    async fn wait_applied(&self, position: JournalPosition) -> Result<()> {
        if position.stream_name != self.journal.stream_name() {
            return Err(ChunkKvError::InvalidRequest(
                "journal position belongs to another stream".into(),
            ));
        }
        loop {
            let notified = self.applied_notify.notified();
            if self.applied_position.load(Ordering::Acquire) >= position.offset {
                return Ok(());
            }
            match self.lifecycle() {
                PartitionLifecycle::Recovering => return Err(ChunkKvError::Recovering),
                PartitionLifecycle::Faulted => return Err(ChunkKvError::Faulted("partition faulted".into())),
                _ => notified.await,
            }
        }
    }
}

async fn run_worker(mut state: WorkerState, mut receiver: mpsc::Receiver<MutationRequest>) {
    let mut pending = None;
    loop {
        let first = match pending.take() {
            Some(request) => request,
            None => match receiver.recv().await {
                Some(request) => request,
                None => break,
            },
        };
        if lifecycle_from_code(state.lifecycle.load(Ordering::Acquire)) != PartitionLifecycle::Serving {
            finish_request(
                &state,
                first,
                Err(write_state_error(lifecycle_from_code(
                    state.lifecycle.load(Ordering::Acquire),
                ))),
            );
            continue;
        }
        let mut requests = vec![first];
        let mut bytes = requests[0].reserved_bytes;
        while requests.len() < state.config.batch_requests && bytes < state.config.batch_bytes {
            let Ok(request) = receiver.try_recv() else {
                break;
            };
            if bytes
                .checked_add(request.reserved_bytes)
                .is_some_and(|total| total <= state.config.batch_bytes)
            {
                bytes += request.reserved_bytes;
                requests.push(request);
            } else {
                pending = Some(request);
                break;
            }
        }
        process_batch(&mut state, requests).await;
    }
}

async fn process_batch(state: &mut WorkerState, requests: Vec<MutationRequest>) {
    let prepared = prepare_batch(state, requests).await;
    append_and_apply(state, prepared).await;
}

async fn prepare_batch(state: &mut WorkerState, requests: Vec<MutationRequest>) -> Vec<PreparedMutation> {
    let mut prepared: Vec<PreparedMutation> = Vec::new();
    let mut local_ids: HashMap<RequestId, ([u8; 32], usize)> = HashMap::new();
    let mut overlay: HashMap<Vec<u8>, Option<ValueRevision>> = HashMap::new();

    for request in requests {
        if let Some(retained) = state.results.get(&request.request_id) {
            let result = if retained.digest == request.digest {
                Ok(retained.response.clone())
            } else {
                Err(ChunkKvError::RequestConflict)
            };
            finish_request(state, request, result);
            continue;
        }
        let client = (request.request_id.client_high, request.request_id.client_low);
        if state
            .expired_floor
            .get(&client)
            .is_some_and(|floor| request.request_id.client_sequence <= *floor)
        {
            finish_request(state, request, Err(ChunkKvError::RequestExpired));
            continue;
        }
        if let Some((digest, index)) = local_ids.get(&request.request_id).copied() {
            if digest == request.digest {
                prepared[index].followers.push(request);
            } else {
                finish_request(state, request, Err(ChunkKvError::RequestConflict));
            }
            continue;
        }
        let mutation_seq = state.next_seq;
        let Some(next_seq) = mutation_seq.checked_add(1) else {
            state
                .lifecycle
                .store(lifecycle_code(PartitionLifecycle::Faulted), Ordering::Release);
            finish_request(
                state,
                request,
                Err(ChunkKvError::Faulted("mutation sequence exhausted".into())),
            );
            continue;
        };
        let current = if let Some(value) = overlay.get(request.operation.key()) {
            value.clone()
        } else if operation_needs_current(&request.operation) {
            match state.tree.get(request.operation.key()).await {
                Ok(value) => value,
                Err(error) => {
                    finish_request(state, request, Err(error));
                    continue;
                }
            }
        } else {
            None
        };
        let result = resolve_condition(mutation_seq, &request.operation, current.as_ref());
        let record = WalRecord {
            partition_id: state.partition_id,
            ownership_epoch: state.ownership_epoch,
            mutation_seq,
            request_id: request.request_id,
            operation_digest: request.digest,
            result: result.clone(),
            operation: request.operation.clone(),
        };
        let frame = match encode_frame(&record) {
            Ok(frame) => Bytes::from(frame),
            Err(error) => {
                finish_request(state, request, Err(error));
                continue;
            }
        };
        if result.applied() {
            overlay.insert(
                request.operation.key().to_vec(),
                request.operation.successful_value().map(|value| ValueRevision {
                    revision: mutation_seq,
                    value: value.to_vec(),
                }),
            );
        }
        state.next_seq = next_seq;
        let index = prepared.len();
        local_ids.insert(request.request_id, (request.digest, index));
        prepared.push(PreparedMutation {
            request,
            response: MutationResponse {
                mutation_seq,
                result,
                journal_position: JournalPosition::default(),
            },
            record,
            frame,
            followers: Vec::new(),
        });
    }
    prepared
}

async fn append_and_apply(state: &mut WorkerState, prepared: Vec<PreparedMutation>) {
    if prepared.is_empty() {
        return;
    }
    let frames: Vec<Bytes> = prepared.iter().map(|entry| entry.frame.clone()).collect();
    let positions = match state.journal.append_frames(&frames).await {
        Ok(positions) if positions.len() == prepared.len() => positions,
        Ok(_) => {
            fail_prepared(
                state,
                prepared,
                &ChunkKvError::Internal("journal returned wrong position count".into()),
            );
            return;
        }
        Err(error) => {
            state.lifecycle.store(
                lifecycle_code(PartitionLifecycle::WriteStalled),
                Ordering::Release,
            );
            fail_prepared(state, prepared, &error);
            return;
        }
    };
    if let Some(last) = prepared.last() {
        state
            .journal_durable_seq
            .store(last.record.mutation_seq, Ordering::Release);
    }
    let mut entries = prepared.into_iter().zip(positions);
    while let Some((mut entry, position)) = entries.next() {
        entry.response.journal_position = position;
        let apply = if entry.response.result.applied() {
            state
                .tree
                .apply(entry.record.mutation_seq, &entry.record.operation)
                .await
        } else {
            state.tree.advance_noop(entry.record.mutation_seq).await
        };
        if apply.is_err() {
            state
                .lifecycle
                .store(lifecycle_code(PartitionLifecycle::Recovering), Ordering::Release);
            finish_request(state, entry.request, Err(ChunkKvError::ApplyStateUnknown));
            for follower in entry.followers {
                finish_request(state, follower, Err(ChunkKvError::ApplyStateUnknown));
            }
            for (remaining, _) in entries {
                finish_request(state, remaining.request, Err(ChunkKvError::ApplyStateUnknown));
                for follower in remaining.followers {
                    finish_request(state, follower, Err(ChunkKvError::ApplyStateUnknown));
                }
            }
            return;
        }
        state
            .applied_seq
            .store(entry.record.mutation_seq, Ordering::Release);
        state.applied_position.store(position.offset, Ordering::Release);
        state.applied_notify.notify_waiters();
        retain_result(
            state,
            entry.record.request_id,
            entry.record.operation_digest,
            entry.response.clone(),
        );
        finish_request(state, entry.request, Ok(entry.response.clone()));
        for follower in entry.followers {
            finish_request(state, follower, Ok(entry.response.clone()));
        }
    }
}

fn operation_needs_current(operation: &MutationOperation) -> bool {
    matches!(
        operation,
        MutationOperation::PutIfAbsent { .. }
            | MutationOperation::CompareExchange { .. }
            | MutationOperation::ConditionalDelete { .. }
    )
}

fn resolve_condition(
    mutation_seq: u64,
    operation: &MutationOperation,
    current: Option<&ValueRevision>,
) -> MutationResult {
    let matches = match operation {
        MutationOperation::Put { .. } | MutationOperation::Delete { .. } => true,
        MutationOperation::PutIfAbsent { .. } => current.is_none(),
        MutationOperation::CompareExchange { condition, .. }
        | MutationOperation::ConditionalDelete { condition, .. } => condition_matches(condition, current),
    };
    if matches {
        MutationResult::Applied {
            revision: mutation_seq,
        }
    } else {
        MutationResult::ConditionFailed {
            observed: current.cloned(),
        }
    }
}

fn condition_matches(condition: &CompareCondition, current: Option<&ValueRevision>) -> bool {
    match condition {
        CompareCondition::Revision(revision) => current.is_some_and(|value| value.revision == *revision),
        CompareCondition::Value(expected) => current.is_some_and(|value| value.value == *expected),
    }
}

fn retain_result(
    state: &mut WorkerState,
    request_id: RequestId,
    digest: [u8; 32],
    response: MutationResponse,
) {
    state
        .results
        .insert(request_id, RetainedResult { digest, response });
    state.result_order.push_back(request_id);
    while state.result_order.len() > state.config.retained_results {
        let Some(expired) = state.result_order.pop_front() else {
            break;
        };
        state.results.remove(&expired);
        state
            .expired_floor
            .entry((expired.client_high, expired.client_low))
            .and_modify(|floor| *floor = (*floor).max(expired.client_sequence))
            .or_insert(expired.client_sequence);
    }
}

fn fail_prepared(state: &WorkerState, prepared: Vec<PreparedMutation>, error: &ChunkKvError) {
    for entry in prepared {
        finish_request(state, entry.request, Err(error.clone()));
        for follower in entry.followers {
            finish_request(state, follower, Err(error.clone()));
        }
    }
}

fn finish_request(state: &WorkerState, request: MutationRequest, result: Result<MutationResponse>) {
    release_admission(
        &state.queued_requests,
        &state.queued_bytes,
        request.reserved_bytes,
    );
    let _ = request.completion.send(result);
}

fn estimated_request_bytes(operation: &MutationOperation) -> Result<u64> {
    let value = operation.successful_value().map_or(0, <[u8]>::len);
    let condition = match operation {
        MutationOperation::CompareExchange {
            condition: CompareCondition::Value(value),
            ..
        }
        | MutationOperation::ConditionalDelete {
            condition: CompareCondition::Value(value),
            ..
        } => value.len(),
        _ => 0,
    };
    operation
        .key()
        .len()
        .checked_add(value)
        .and_then(|bytes| bytes.checked_add(condition))
        .and_then(|bytes| bytes.checked_add(512))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| ChunkKvError::InvalidRequest("request memory estimate overflows".into()))
}

fn reserve_requests(counter: &AtomicUsize, limit: usize) -> Result<()> {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        if current >= limit {
            return Err(ChunkKvError::Overloaded);
        }
        match counter.compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

fn reserve_bytes(counter: &AtomicU64, limit: u64, bytes: u64) -> Result<()> {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let Some(next) = current.checked_add(bytes) else {
            return Err(ChunkKvError::Overloaded);
        };
        if next > limit {
            return Err(ChunkKvError::Overloaded);
        }
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

fn release_admission(requests: &AtomicUsize, bytes: &AtomicU64, reserved_bytes: u64) {
    requests.fetch_sub(1, Ordering::AcqRel);
    bytes.fetch_sub(reserved_bytes, Ordering::AcqRel);
}

fn lifecycle_code(state: PartitionLifecycle) -> u8 {
    match state {
        PartitionLifecycle::Closed => 0,
        PartitionLifecycle::Recovering => 1,
        PartitionLifecycle::WriteStalled => 2,
        PartitionLifecycle::Prepared => 3,
        PartitionLifecycle::Serving => 4,
        PartitionLifecycle::SplitPreparing => 5,
        PartitionLifecycle::SplitFenced => 6,
        PartitionLifecycle::Retired => 7,
        PartitionLifecycle::Faulted => 8,
    }
}

fn lifecycle_from_code(code: u8) -> PartitionLifecycle {
    match code {
        0 => PartitionLifecycle::Closed,
        1 => PartitionLifecycle::Recovering,
        2 => PartitionLifecycle::WriteStalled,
        3 => PartitionLifecycle::Prepared,
        4 => PartitionLifecycle::Serving,
        5 => PartitionLifecycle::SplitPreparing,
        6 => PartitionLifecycle::SplitFenced,
        7 => PartitionLifecycle::Retired,
        _ => PartitionLifecycle::Faulted,
    }
}

fn write_state_error(state: PartitionLifecycle) -> ChunkKvError {
    match state {
        PartitionLifecycle::Recovering => ChunkKvError::Recovering,
        PartitionLifecycle::WriteStalled => ChunkKvError::WriteStalled,
        PartitionLifecycle::Faulted => ChunkKvError::Faulted("partition faulted".into()),
        _ => ChunkKvError::NotServing(format!("{state:?}")),
    }
}

fn read_state_error(state: PartitionLifecycle) -> ChunkKvError {
    match state {
        PartitionLifecycle::Recovering => ChunkKvError::Recovering,
        PartitionLifecycle::Faulted => ChunkKvError::Faulted("partition faulted".into()),
        _ => ChunkKvError::NotServing(format!("{state:?}")),
    }
}
