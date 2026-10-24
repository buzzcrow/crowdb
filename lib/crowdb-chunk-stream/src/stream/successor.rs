// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One unwritten successor overlaps appends to the current attached chunk.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::WorkerState;
use crate::perf::{perf, PhaseTimer};
use crate::{ActiveChunkDescriptor, Result, StreamChunkStore, StreamError, StreamWriterSession};

pub(super) struct SuccessorPreparation {
    handoff: oneshot::Sender<bool>,
    task: JoinHandle<Result<ActiveChunkDescriptor>>,
}

pub(super) fn start(state: &mut WorkerState) {
    let Some(authority) = state.authority.as_ref() else {
        return;
    };
    if state.successor.is_some() || state.progress.active.is_none() || authority.check().is_err() {
        return;
    }
    let request = SuccessorRequest {
        chunks: Arc::clone(&state.chunks),
        metadata: state.metadata.clone(),
        sequence: state.snapshot.record().sequence + 1,
        owner_session: state.owner_session,
        authority: Arc::clone(authority),
        name: state.stream_name,
        epoch: state.writer_epoch,
        purpose: state.progress.purpose,
        interval: state.config.liveness_interval,
        required: state
            .capacity_plan
            .target(
                0,
                0,
                state.config.chunk_capacity_bytes,
                state.config.batch_bytes as u64,
            )
            .expect("nonzero chunk capacity"),
    };
    let (handoff, receive) = oneshot::channel();
    state.successor = Some(SuccessorPreparation {
        handoff,
        task: tokio::spawn(request.run(receive)),
    });
}

struct SuccessorRequest {
    chunks: Arc<dyn StreamChunkStore>,
    metadata: crate::StreamChunkRecordStore,
    sequence: u64,
    owner_session: [u8; 16],
    authority: Arc<StreamWriterSession>,
    name: crate::StreamName,
    epoch: u64,
    purpose: crate::StreamPurpose,
    interval: Duration,
    required: u64,
}

impl SuccessorRequest {
    async fn register(&self, active: &ActiveChunkDescriptor) -> Result<()> {
        use crowdb_protocol::chunk_stream::{StreamChunkRecord, StreamChunkState, StreamMappings};
        let record = StreamChunkRecord {
            stream_id: self.name,
            purpose: self.purpose,
            chunk_id: active.chunk_id,
            state: StreamChunkState::Candidate,
            logical_start: None,
            logical_end: None,
            sequence: self.sequence,
            writer_epoch: self.epoch,
            generation: 0,
            owner_session: self.owner_session,
            trim_offset: 0,
            closed: false,
            mappings: StreamMappings::default(),
        };
        loop {
            self.authority.check()?;
            match self.metadata.register_candidate(&record).await {
                Ok(_) => return Ok(()),
                Err(
                    error @ (StreamError::StaleWriter
                    | StreamError::Corruption(_)
                    | StreamError::InvalidRequest(_)),
                ) => return Err(error),
                Err(error) => {
                    tracing::warn!(%error,"candidate registration unavailable; retrying");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn run(self, mut receive: oneshot::Receiver<bool>) -> Result<ActiveChunkDescriptor> {
        let mut use_requested = false;
        loop {
            self.authority.check()?;
            let result = {
                let _timer = PhaseTimer::start(&perf().successor_prepare);
                allocate(
                    self.chunks.as_ref(),
                    self.name,
                    self.epoch,
                    self.purpose,
                    self.required,
                )
                .await
            };
            let active = match result {
                Ok(active) => active,
                Err(
                    error @ (StreamError::ReadUnavailable(_)
                    | StreamError::Internal(_)
                    | StreamError::Backpressure),
                ) => {
                    tracing::warn!(%error, "successor allocation unavailable; retrying in background");
                    if !use_requested {
                        match receive.try_recv() {
                            Ok(true) => use_requested = true,
                            Ok(false) | Err(oneshot::error::TryRecvError::Closed) => return Err(error),
                            Err(oneshot::error::TryRecvError::Empty) => {}
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if active.physical_start != 0 || active.acknowledged_cursor != 0 || active.capacity == 0 {
                return Err(StreamError::Corruption(
                    "prepared successor is not an initialized empty chunk".into(),
                ));
            }
            self.register(&active).await?;
            if use_requested
                || await_handoff(
                    self.chunks.as_ref(),
                    &self.authority,
                    self.epoch,
                    &active,
                    self.interval,
                    &mut receive,
                )
                .await?
            {
                self.authority.check()?;
                return Ok(active);
            }
            self.metadata
                .remove_candidate(self.name, self.sequence, active.chunk_id)
                .await?;
            if let Err(error) = reclaim(self.chunks.as_ref(), self.epoch, &active).await {
                tracing::warn!(?active.chunk_id, %error, "failed candidate cleanup deferred to chunk expiry");
            }
        }
    }
}

async fn allocate(
    chunks: &dyn StreamChunkStore,
    name: crate::StreamName,
    epoch: u64,
    purpose: crate::StreamPurpose,
    required: u64,
) -> Result<ActiveChunkDescriptor> {
    let mut active = chunks.allocate_mirrored(name, epoch, purpose).await?;
    if active.capacity < required {
        match chunks.grow_mirrored(name, epoch, active.chunk_id, required).await {
            Ok(Some(grown)) if grown.chunk_id == active.chunk_id && grown.capacity >= required => {
                active = grown;
            }
            Ok(None) => {}
            result => {
                let error = result.err().unwrap_or_else(|| {
                    StreamError::Corruption("successor growth changed its identity".into())
                });
                // No user data was admitted. Initialization has drained, so the
                // failed candidate can be sealed empty and reclaimed safely.
                reclaim(chunks, epoch, &active).await?;
                return Err(match error {
                    StreamError::StaleWriter | StreamError::Corruption(_) => error,
                    other => StreamError::ReadUnavailable(format!(
                        "unused successor initialization failed: {other}"
                    )),
                });
            }
        }
    }
    Ok(active)
}

pub(super) async fn take(state: &mut WorkerState) -> Result<ActiveChunkDescriptor> {
    let Some(preparation) = state.successor.take() else {
        start(state);
        let preparation = state.successor.take().ok_or(StreamError::WriteStalled)?;
        return join(preparation, true).await;
    };
    let _timer = PhaseTimer::start(&perf().successor_wait);
    join(preparation, true).await
}

async fn await_handoff(
    chunks: &dyn StreamChunkStore,
    authority: &StreamWriterSession,
    epoch: u64,
    active: &ActiveChunkDescriptor,
    interval: Duration,
    receive: &mut oneshot::Receiver<bool>,
) -> Result<bool> {
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticks.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = &mut *receive => {
                authority.check()?;
                return Ok(true);
            }
            _ = ticks.tick() => {
                authority.check()?;
                if let Err(error) = chunks.renew_liveness(active.chunk_id, epoch).await {
                    tracing::warn!(?active.chunk_id, %error, "successor liveness renewal failed; replacing candidate");
                    return Ok(false);
                }
            }
        }
    }
}

pub(super) async fn discard(state: &mut WorkerState) -> Result<()> {
    let Some(preparation) = state.successor.take() else {
        return Ok(());
    };
    // Drain initialization before sealing or releasing owner authority. Never
    // abort submitted IO: a late initializer must not overwrite a later writer.
    let active = match join(preparation, false).await {
        Ok(active) => active,
        Err(error) => {
            // A failed unused allocation cannot invalidate the attached chunk.
            // No descriptor was returned; chunk-layer expiry owns its cleanup.
            tracing::warn!(%error, "unused successor preparation failed");
            return Ok(());
        }
    };
    state
        .metadata
        .remove_candidate(
            state.stream_name,
            state.snapshot.record().sequence + 1,
            active.chunk_id,
        )
        .await?;
    reclaim(state.chunks.as_ref(), state.writer_epoch, &active).await
}

async fn reclaim(chunks: &dyn StreamChunkStore, epoch: u64, active: &ActiveChunkDescriptor) -> Result<()> {
    chunks.seal(active.chunk_id, epoch, 0).await?;
    chunks.release_trimmed(active.chunk_id, 0, 0).await?;
    Ok(())
}

pub(super) async fn reclaim_excluded(
    metadata: &crate::StreamChunkRecordStore,
    chunks: &dyn StreamChunkStore,
    name: crate::StreamName,
    epoch: u64,
) -> Result<()> {
    // Reservations from the excluded owner contain no acknowledged data.
    for candidate in metadata.candidates(name).await? {
        let record = candidate.record();
        if record.writer_epoch <= epoch {
            let cursor = chunks.durable_cursor(record.chunk_id, epoch).await?;
            if cursor.offset != 0 {
                return Err(StreamError::Corruption(
                    "unattached candidate contains committed data".into(),
                ));
            }
            chunks.seal(record.chunk_id, epoch, 0).await?;
            chunks.release_trimmed(record.chunk_id, 0, 0).await?;
            metadata
                .remove_candidate(name, record.sequence, record.chunk_id)
                .await?;
        }
    }
    Ok(())
}

async fn join(preparation: SuccessorPreparation, use_candidate: bool) -> Result<ActiveChunkDescriptor> {
    let _ = preparation.handoff.send(use_candidate);
    preparation
        .task
        .await
        .map_err(|error| StreamError::Internal(format!("successor preparation task failed: {error}")))?
}
